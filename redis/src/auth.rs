use crate::types::RedisResult;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Represents authentication credentials for Redis connection
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    /// The authentication token (password for Redis AUTH command)
    pub token: String,
    /// Optional expiration time for the token
    pub expires_at: Option<SystemTime>,
}

impl AuthCredentials {
    /// Create new credentials with a token
    pub fn new(token: String) -> Self {
        Self {
            token,
            expires_at: None,
        }
    }

    /// Create new credentials with a token and expiration time
    pub fn with_expiration(token: String, expires_at: SystemTime) -> Self {
        Self {
            token,
            expires_at: Some(expires_at),
        }
    }

    /// Check if the credentials are expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            SystemTime::now() >= expires_at
        } else {
            false
        }
    }

    /// Check if the credentials will expire within the given duration
    pub fn expires_within(&self, duration: std::time::Duration) -> bool {
        if let Some(expires_at) = self.expires_at {
            if let Ok(time_until_expiry) = expires_at.duration_since(SystemTime::now()) {
                time_until_expiry <= duration
            } else {
                true // Already expired
            }
        } else {
            false // No expiration
        }
    }
}

/// Trait for providing authentication credentials
///
/// This trait allows different authentication mechanisms to be plugged into
/// the Redis client, such as static passwords, token-based authentication,
/// or dynamic credential providers like Azure Entra ID.
pub trait CredentialsProvider: Send + Sync + std::fmt::Debug {
    /// Get the current authentication credentials
    ///
    /// This method should return valid credentials that can be used for
    /// Redis authentication. If the credentials are expired or invalid,
    /// the implementation should refresh them before returning.
    fn get_credentials(&self) -> RedisResult<AuthCredentials>;

    /// Clone the credentials provider
    fn clone_box(&self) -> Box<dyn CredentialsProvider>;
}

impl Clone for Box<dyn CredentialsProvider> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Async version of the credentials provider trait
#[cfg(feature = "aio")]
pub trait AsyncCredentialsProvider: Send + Sync {
    /// Get the current authentication credentials asynchronously
    fn get_credentials(
        &self,
    ) -> impl std::future::Future<Output = RedisResult<AuthCredentials>> + Send;
}

/// A simple credentials provider that always returns the same static credentials
#[derive(Debug, Clone)]
pub struct StaticCredentialsProvider {
    credentials: AuthCredentials,
}

impl StaticCredentialsProvider {
    /// Create a new static credentials provider
    pub fn new(token: String) -> Self {
        Self {
            credentials: AuthCredentials::new(token),
        }
    }

    /// Create a new static credentials provider with expiration
    pub fn with_expiration(token: String, expires_at: SystemTime) -> Self {
        Self {
            credentials: AuthCredentials::with_expiration(token, expires_at),
        }
    }
}

impl CredentialsProvider for StaticCredentialsProvider {
    fn get_credentials(&self) -> RedisResult<AuthCredentials> {
        Ok(self.credentials.clone())
    }

    fn clone_box(&self) -> Box<dyn CredentialsProvider> {
        Box::new(self.clone())
    }
}

#[cfg(feature = "aio")]
impl AsyncCredentialsProvider for StaticCredentialsProvider {
    async fn get_credentials(&self) -> RedisResult<AuthCredentials> {
        Ok(self.credentials.clone())
    }
}

/// Configuration for token refresh behavior
#[derive(Debug, Clone)]
pub struct TokenRefreshConfig {
    /// Fraction of token lifetime after which refresh should be triggered (0.0 to 1.0)
    /// For example, 0.8 means refresh when 80% of the token's lifetime has elapsed
    pub expiration_refresh_ratio: f64,
    /// Retry configuration for failed refresh attempts
    pub retry_config: RetryConfig,
}

impl Default for TokenRefreshConfig {
    fn default() -> Self {
        Self {
            expiration_refresh_ratio: 0.8,
            retry_config: RetryConfig::default(),
        }
    }
}

/// Configuration for retry behavior when token refresh fails
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Maximum random jitter as a percentage of the delay (0.0 to 1.0)
    pub jitter_percentage: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_percentage: 0.1,
        }
    }
}

/// Common logic shared between sync and async token managers
mod token_manager_common {
    use super::*;

    /// Check if credentials should be refreshed based on expiration ratio
    pub fn should_refresh(creds: &AuthCredentials, config: &TokenRefreshConfig) -> bool {
        if creds.is_expired() {
            return true;
        }

        if let Some(expires_at) = creds.expires_at {
            if let Ok(total_lifetime) = expires_at.duration_since(SystemTime::now()) {
                let refresh_threshold = Duration::from_secs_f64(
                    total_lifetime.as_secs_f64() * config.expiration_refresh_ratio,
                );
                return creds.expires_within(refresh_threshold);
            }
        }

        false
    }

    /// Apply jitter to a delay duration
    pub fn apply_jitter(delay: Duration, jitter_percentage: f64) -> Duration {
        if jitter_percentage <= 0.0 {
            return delay;
        }

        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        let random_factor = (hasher.finish() % 1000) as f64 / 1000.0; // 0.0 to 1.0

        let jitter_range = delay.as_secs_f64() * jitter_percentage;
        let jitter = jitter_range * (random_factor - 0.5) * 2.0; // -jitter_range to +jitter_range

        let jittered_duration = delay.as_secs_f64() + jitter;
        Duration::from_secs_f64(jittered_duration.max(0.0)) // Ensure non-negative delay
    }

    /// Calculate next delay with exponential backoff
    pub fn calculate_next_delay(
        current_delay: Duration,
        backoff_multiplier: f64,
        max_delay: Duration,
    ) -> Duration {
        Duration::from_millis((current_delay.as_millis() as f64 * backoff_multiplier) as u64)
            .min(max_delay)
    }
}

/// Token manager that handles automatic token refresh and caching
pub struct TokenManager<P> {
    provider: P,
    config: TokenRefreshConfig,
    cached_credentials: Arc<Mutex<Option<AuthCredentials>>>,
}

impl<P> TokenManager<P>
where
    P: CredentialsProvider,
{
    /// Create a new token manager with the given provider and default configuration
    pub fn new(provider: P) -> Self {
        Self::with_config(provider, TokenRefreshConfig::default())
    }

    /// Create a new token manager with the given provider and configuration
    pub fn with_config(provider: P, config: TokenRefreshConfig) -> Self {
        Self {
            provider,
            config,
            cached_credentials: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns authentication credentials, refreshing them if they have expired.
    ///
    /// If cached credentials are still valid, they are returned.
    /// Otherwise, new credentials are fetched and cached before returning.
    pub fn get_credentials(&self) -> RedisResult<AuthCredentials> {
        if let Ok(cached) = self.cached_credentials.lock() {
            if let Some(ref creds) = *cached {
                if !token_manager_common::should_refresh(creds, &self.config) {
                    return Ok(creds.clone());
                }
            }
        }

        self.refresh_credentials()
    }

    /// Force refresh of credentials
    pub fn refresh_credentials(&self) -> RedisResult<AuthCredentials> {
        let mut attempt = 0;
        let mut delay = self.config.retry_config.initial_delay;

        loop {
            match self.provider.get_credentials() {
                Ok(creds) => {
                    if let Ok(mut cached) = self.cached_credentials.lock() {
                        *cached = Some(creds.clone());
                    }
                    return Ok(creds);
                }
                Err(err) => {
                    attempt += 1;
                    if attempt >= self.config.retry_config.max_attempts {
                        return Err(err);
                    }

                    let jittered_delay = token_manager_common::apply_jitter(
                        delay,
                        self.config.retry_config.jitter_percentage,
                    );
                    std::thread::sleep(jittered_delay);

                    // Calculate next delay with exponential backoff
                    delay = token_manager_common::calculate_next_delay(
                        delay,
                        self.config.retry_config.backoff_multiplier,
                        self.config.retry_config.max_delay,
                    );
                }
            }
        }
    }
}

/// Async token manager for use with async credentials providers
#[cfg(feature = "aio")]
pub struct AsyncTokenManager<P> {
    provider: P,
    config: TokenRefreshConfig,
    cached_credentials: Arc<tokio::sync::Mutex<Option<AuthCredentials>>>,
}

#[cfg(feature = "aio")]
impl<P> AsyncTokenManager<P>
where
    P: AsyncCredentialsProvider,
{
    /// Create a new async token manager with the given provider and default configuration
    pub fn new(provider: P) -> Self {
        Self::with_config(provider, TokenRefreshConfig::default())
    }

    /// Create a new async token manager with the given provider and configuration
    pub fn with_config(provider: P, config: TokenRefreshConfig) -> Self {
        Self {
            provider,
            config,
            cached_credentials: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Returns authentication credentials, refreshing them if they have expired.
    ///
    /// If cached credentials are still valid, they are returned.
    /// Otherwise, new credentials are fetched and cached before returning.
    pub async fn get_credentials(&self) -> RedisResult<AuthCredentials> {
        {
            let cached = self.cached_credentials.lock().await;
            if let Some(ref creds) = *cached {
                if !token_manager_common::should_refresh(creds, &self.config) {
                    return Ok(creds.clone());
                }
            }
        }

        self.refresh_credentials().await
    }

    /// Force refresh of credentials
    pub async fn refresh_credentials(&self) -> RedisResult<AuthCredentials> {
        let mut attempt = 0;
        let mut delay = self.config.retry_config.initial_delay;

        loop {
            match self.provider.get_credentials().await {
                Ok(creds) => {
                    {
                        let mut cached = self.cached_credentials.lock().await;
                        *cached = Some(creds.clone());
                    }
                    return Ok(creds);
                }
                Err(err) => {
                    attempt += 1;
                    if attempt >= self.config.retry_config.max_attempts {
                        return Err(err);
                    }

                    let jittered_delay = token_manager_common::apply_jitter(
                        delay,
                        self.config.retry_config.jitter_percentage,
                    );
                    tokio::time::sleep(jittered_delay).await;

                    // Calculate next delay with exponential backoff
                    delay = token_manager_common::calculate_next_delay(
                        delay,
                        self.config.retry_config.backoff_multiplier,
                        self.config.retry_config.max_delay,
                    );
                }
            }
        }
    }
}

/// Background token refresh service for async connections
#[cfg(feature = "aio")]
pub struct AsyncTokenRefreshService<P> {
    token_manager: Arc<AsyncTokenManager<P>>,
    refresh_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(feature = "aio")]
impl<P> AsyncTokenRefreshService<P>
where
    P: AsyncCredentialsProvider + 'static,
{
    /// Create a new background token refresh service
    pub fn new(provider: P, config: TokenRefreshConfig) -> Self {
        let token_manager = Arc::new(AsyncTokenManager::with_config(provider, config));
        Self {
            token_manager,
            refresh_handle: None,
            shutdown_sender: None,
        }
    }

    /// Start the background refresh service
    pub async fn start(&mut self) -> RedisResult<()> {
        if self.refresh_handle.is_some() {
            return Ok(());
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let token_manager = self.token_manager.clone();

        let handle = tokio::spawn(async move {
            Self::refresh_loop(token_manager, shutdown_rx).await;
        });

        self.refresh_handle = Some(handle);
        self.shutdown_sender = Some(shutdown_tx);
        Ok(())
    }

    /// Stop the background refresh service
    pub async fn stop(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }

        if let Some(handle) = self.refresh_handle.take() {
            let _ = handle.await;
        }
    }

    /// Get the token manager for manual credential access
    pub fn token_manager(&self) -> Arc<AsyncTokenManager<P>> {
        self.token_manager.clone()
    }

    /// Background refresh loop
    async fn refresh_loop(
        token_manager: Arc<AsyncTokenManager<P>>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut refresh_interval = tokio::time::interval(Duration::from_secs(60));
        refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = refresh_interval.tick() => {
                    // Try to refresh credentials if needed
                    if let Err(err) = token_manager.get_credentials().await {
                        eprintln!("Token refresh failed: {err}");
                    }
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    }
}

#[cfg(feature = "aio")]
impl<P> Drop for AsyncTokenRefreshService<P> {
    fn drop(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_auth_credentials_creation() {
        let creds = AuthCredentials::new("test_token".to_string());
        assert_eq!(creds.token, "test_token");
        assert!(creds.expires_at.is_none());
        assert!(!creds.is_expired());
    }

    #[test]
    fn test_auth_credentials_with_expiration() {
        let future_time = SystemTime::now() + Duration::from_secs(3600);
        let creds = AuthCredentials::with_expiration("test_token".to_string(), future_time);
        assert_eq!(creds.token, "test_token");
        assert!(creds.expires_at.is_some());
        assert!(!creds.is_expired());
        assert!(!creds.expires_within(Duration::from_secs(1800))); // 30 minutes
        assert!(creds.expires_within(Duration::from_secs(7200))); // 2 hours
    }

    #[test]
    fn test_static_credentials_provider() {
        let provider = StaticCredentialsProvider::new("static_token".to_string());
        let creds = CredentialsProvider::get_credentials(&provider).unwrap();
        assert_eq!(creds.token, "static_token");
    }

    #[cfg(feature = "aio")]
    #[tokio::test]
    async fn test_async_static_credentials_provider() {
        let provider = StaticCredentialsProvider::new("async_token".to_string());
        let creds = AsyncCredentialsProvider::get_credentials(&provider)
            .await
            .unwrap();
        assert_eq!(creds.token, "async_token");
    }
}
