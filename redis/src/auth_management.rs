use crate::auth::{AuthCredentials, BasicAuth, CredentialsProvider};
use crate::types::RedisResult;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Configuration for token refresh behavior
#[derive(Debug, Clone)]
pub struct TokenRefreshConfig {
    /// Fraction of token lifetime after which refresh should be triggered (0.0 to 1.0)
    /// For example, 0.8 means refresh when 80% of the token's lifetime has elapsed
    pub expiration_refresh_ratio: f64,
    /// Retry configuration for failed refresh attempts
    pub retry_config: RetryConfig,
}

impl TokenRefreshConfig {
    /// Set the expiration refresh ratio
    pub fn set_expiration_refresh_ratio(mut self, ratio: f64) -> Self {
        self.expiration_refresh_ratio = ratio;
        self
    }

    /// Set the retry configuration
    pub fn set_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }
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

/// Configuration for the token refresh services
#[derive(Debug, Clone)]
pub struct TokenRefreshServiceConfig {
    /// Refresh interval
    pub refresh_interval: Duration,
    /// Token refresh config
    pub token_refresh_config: TokenRefreshConfig,
}

impl TokenRefreshServiceConfig {
    /// Create a new token refresh service config
    pub fn new(refresh_interval: Duration, token_refresh_config: TokenRefreshConfig) -> Self {
        Self {
            refresh_interval,
            token_refresh_config,
        }
    }

    /// Set the refresh interval
    pub fn set_refresh_interval(mut self, refresh_interval: Duration) -> Self {
        self.refresh_interval = refresh_interval;
        self
    }

    /// Set the token refresh config
    pub fn set_token_refresh_config(mut self, token_refresh_config: TokenRefreshConfig) -> Self {
        self.token_refresh_config = token_refresh_config;
        self
    }
}

impl Default for TokenRefreshServiceConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(60),
            token_refresh_config: TokenRefreshConfig::default(),
        }
    }
}

/// Common logic shared between sync and async token managers
mod token_manager_common {
    use super::*;

    /// Check if the provided credentials should be refreshed based on the expiration ratio in the provided config
    pub fn should_refresh_credentials_based_on_config<T>(
        credentials: &AuthCredentials<T>,
        config: &TokenRefreshConfig,
    ) -> bool {
        if credentials.is_expired() {
            return true;
        }

        if let Some(expires_at) = credentials.expires_at {
            if let Ok(total_lifetime) = expires_at.duration_since(credentials.received_at) {
                let refresh_threshold = Duration::from_secs_f64(
                    total_lifetime.as_secs_f64() * config.expiration_refresh_ratio,
                );
                return credentials.eligible_for_refresh(refresh_threshold);
            } else {
                // If the duration is somehow negative, consider the credentials as expired and force refresh
                return true;
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
    cached_credentials: Arc<Mutex<Option<BasicAuth>>>,
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
    pub fn get_credentials(&self) -> RedisResult<BasicAuth> {
        if let Ok(cached) = self.cached_credentials.lock() {
            if let Some(ref creds) = *cached {
                // For BasicAuth, we don't have expiration logic, so just return cached
                return Ok(creds.clone());
            }
        }

        self.refresh_credentials()
    }

    /// Force refresh of credentials
    pub fn refresh_credentials(&self) -> RedisResult<BasicAuth> {
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

// /// Async token manager for use with async credentials providers
// #[cfg(feature = "aio")]
// pub struct AsyncTokenManager<P, T> {
//     provider: P,
//     config: TokenRefreshConfig,
//     cached_credentials: Arc<tokio::sync::Mutex<Option<AuthCredentials<T>>>>,
// }

// #[cfg(feature = "aio")]
// impl<P, T> AsyncTokenManager<P, T>
// where
//     P: AsyncCredentialsProvider<T>,
//     T: Clone,
// {
//     /// Create a new async token manager with the given provider and default configuration
//     pub fn new(provider: P) -> Self {
//         Self::with_config(provider, TokenRefreshConfig::default())
//     }

//     /// Create a new async token manager with the given provider and configuration
//     pub fn with_config(provider: P, config: TokenRefreshConfig) -> Self {
//         Self {
//             provider,
//             config,
//             cached_credentials: Arc::new(tokio::sync::Mutex::new(None)),
//         }
//     }

//     /// Returns authentication credentials, refreshing them if they have expired.
//     ///
//     /// If cached credentials are still valid, they are returned.
//     /// Otherwise, new credentials are fetched and cached before returning.
//     pub async fn get_credentials(&self) -> RedisResult<AuthCredentials<T>> {
//         {
//             let cached = self.cached_credentials.lock().await;
//             if let Some(ref creds) = *cached {
//                 if !token_manager_common::should_refresh_credentials_based_on_config(
//                     creds,
//                     &self.config,
//                 ) {
//                     return Ok(creds.clone());
//                 }
//             }
//         }

//         self.refresh_credentials().await
//     }

//     /// Force refresh of credentials
//     pub async fn refresh_credentials(&self) -> RedisResult<AuthCredentials<T>> {
//         let mut attempt = 0;
//         let mut delay = self.config.retry_config.initial_delay;

//         loop {
//             match self.provider.get_credentials().await {
//                 Ok(creds) => {
//                     {
//                         let mut cached = self.cached_credentials.lock().await;
//                         *cached = Some(creds.clone());
//                     }
//                     return Ok(creds);
//                 }
//                 Err(err) => {
//                     attempt += 1;
//                     if attempt >= self.config.retry_config.max_attempts {
//                         return Err(err);
//                     }

//                     let jittered_delay = token_manager_common::apply_jitter(
//                         delay,
//                         self.config.retry_config.jitter_percentage,
//                     );
//                     tokio::time::sleep(jittered_delay).await;

//                     // Calculate next delay with exponential backoff
//                     delay = token_manager_common::calculate_next_delay(
//                         delay,
//                         self.config.retry_config.backoff_multiplier,
//                         self.config.retry_config.max_delay,
//                     );
//                 }
//             }
//         }
//     }
// }

// /// Background token refresh service for async connections
// #[cfg(feature = "aio")]
// pub struct AsyncTokenRefreshService<P, T> {
//     token_manager: Arc<AsyncTokenManager<P, T>>,
//     refresh_interval: Duration,
//     refresh_handle: Option<tokio::task::JoinHandle<()>>,
//     shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
// }

// #[cfg(feature = "aio")]
// impl<P, T> AsyncTokenRefreshService<P, T>
// where
//     P: AsyncCredentialsProvider<T> + 'static,
//     T: Clone + Send + Sync + 'static,
// {
//     /// Create a new background token refresh service
//     pub fn new(provider: P, config: TokenRefreshServiceConfig) -> Self {
//         let token_manager = Arc::new(AsyncTokenManager::with_config(provider, config.token_refresh_config));
//         Self {
//             token_manager,
//             refresh_interval: config.refresh_interval,
//             refresh_handle: None,
//             shutdown_sender: None,
//         }
//     }

//     /// Start the background refresh service
//     pub async fn start(&mut self) -> RedisResult<()> {
//         if self.refresh_handle.is_some() {
//             return Ok(());
//         }

//         let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
//         let token_manager = self.token_manager();
//         let refresh_interval = self.refresh_interval;

//         let handle = tokio::spawn(async move {
//             Self::refresh_loop(token_manager, refresh_interval, shutdown_rx).await;
//         });

//         self.refresh_handle = Some(handle);
//         self.shutdown_sender = Some(shutdown_tx);
//         Ok(())
//     }

//     /// Stop the background refresh service
//     pub async fn stop(&mut self) {
//         if let Some(sender) = self.shutdown_sender.take() {
//             let _ = sender.send(());
//         }

//         if let Some(handle) = self.refresh_handle.take() {
//             let _ = handle.await;
//         }
//     }

//     /// Get the token manager for manual credential access
//     pub fn token_manager(&self) -> Arc<AsyncTokenManager<P, T>> {
//         self.token_manager.clone()
//     }

//     /// Background refresh loop
//     async fn refresh_loop(
//         token_manager: Arc<AsyncTokenManager<P, T>>,
//         refresh_interval: Duration,
//         mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
//     ) {
//         let mut refresh_interval = tokio::time::interval(refresh_interval);
//         refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

//         loop {
//             tokio::select! {
//                 _ = refresh_interval.tick() => {
//                     // Try to refresh credentials if needed
//                     if let Err(err) = token_manager.get_credentials().await {
//                         eprintln!("Token refresh failed: {err}");
//                     }
//                 }
//                 _ = &mut shutdown_rx => {
//                     break;
//                 }
//             }
//         }
//     }
// }

// #[cfg(feature = "aio")]
// impl<P, T> Drop for AsyncTokenRefreshService<P, T> {
//     fn drop(&mut self) {
//         if let Some(sender) = self.shutdown_sender.take() {
//             let _ = sender.send(());
//         }
//     }
// }

// /// Subscription handle for streaming credentials
// #[cfg(feature = "aio")]
// pub struct CredentialsSubscription<T> {
//     id: usize,
//     sender: tokio::sync::mpsc::UnboundedSender<SubscriptionCommand<T>>,
// }

// #[cfg(feature = "aio")]
// impl<T> Disposable for CredentialsSubscription<T> where T: Send + Sync {
//     fn dispose(&self) {
//         let _ = self.sender.send(SubscriptionCommand::Unsubscribe(self.id));
//     }
// }

// /// Commands for managing subscriptions
// #[cfg(feature = "aio")]
// enum SubscriptionCommand {
//     Subscribe(usize, Arc<dyn StreamingCredentialsListener>),
//     Unsubscribe(usize),
//     NotifyAll(AuthCredentials),
//     NotifyError(RedisError),
// }

// /// Streaming credentials provider that integrates with AsyncTokenRefreshService
// #[cfg(feature = "aio")]
// pub struct StreamingTokenManager<P, T> {
//     token_manager: Arc<AsyncTokenManager<P, T>>,
//     subscription_sender: tokio::sync::mpsc::UnboundedSender<SubscriptionCommand<T>>,
//     subscription_handle: Option<tokio::task::JoinHandle<()>>,
//     next_subscription_id: std::sync::atomic::AtomicUsize,
// }

// #[cfg(feature = "aio")]
// impl<P, T> StreamingTokenManager<P, T>
// where
//     P: AsyncCredentialsProvider<T> + 'static,
//     T: Clone,
// {
//     /// Create a new streaming token manager
//     pub fn new(provider: P, config: TokenRefreshServiceConfig) -> Self {
//         let token_manager = Arc::new(AsyncTokenManager::with_config(provider, config.token_refresh_config));
//         let (subscription_sender, subscription_receiver) = tokio::sync::mpsc::unbounded_channel();

//         let mut manager = Self {
//             token_manager,
//             subscription_sender,
//             subscription_handle: None,
//             next_subscription_id: std::sync::atomic::AtomicUsize::new(0),
//         };

//         // Start the subscription management task
//         manager.start_subscription_manager(subscription_receiver);

//         manager
//     }

//     /// Start the subscription management task
//     fn start_subscription_manager(&mut self, mut receiver: tokio::sync::mpsc::UnboundedReceiver<SubscriptionCommand<T>>) {
//         let handle = tokio::spawn(async move {
//             let mut subscribers: std::collections::HashMap<usize, Arc<dyn StreamingCredentialsListener<T>>> = std::collections::HashMap::new();

//             while let Some(command) = receiver.recv().await {
//                 match command {
//                     SubscriptionCommand::Subscribe(id, listener) => {
//                         subscribers.insert(id, listener);
//                     }
//                     SubscriptionCommand::Unsubscribe(id) => {
//                         subscribers.remove(&id);
//                     }
//                     SubscriptionCommand::NotifyAll(credentials) => {
//                         for listener in subscribers.values() {
//                             listener.on_credentials_update(credentials.clone());
//                         }
//                     }
//                     SubscriptionCommand::NotifyError(error) => {
//                         // Create a new error for each listener since RedisError doesn't implement Clone
//                         for listener in subscribers.values() {
//                             let new_error = RedisError::from((
//                                 ErrorKind::AuthenticationFailed,
//                                 "Credential stream error",
//                                 format!("{error}"),
//                             ));
//                             listener.on_error(new_error);
//                         }
//                     }
//                 }
//             }
//         });

//         self.subscription_handle = Some(handle);
//     }

//     /// Notify all subscribers when there are new credentials
//     pub fn notify_credentials_update(&self, credentials: AuthCredentials<T>) {
//         let _ = self.subscription_sender.send(SubscriptionCommand::NotifyAll(credentials));
//     }

//     /// Notify all subscribers when there is an error
//     pub fn notify_error(&self, error: RedisError) {
//         let _ = self.subscription_sender.send(SubscriptionCommand::NotifyError(error));
//     }
// }

// #[cfg(feature = "aio")]
// impl StreamingCredentialsProvider for StreamingTokenManager<P, T>
// where
//     P: AsyncCredentialsProvider + 'static,
//     T: Clone,
// {
//     async fn subscribe(&self, listener: Arc<dyn StreamingCredentialsListener<T>>)
//         -> RedisResult<(AuthCredentials<T>, Box<dyn Disposable>)> {

//         // Get initial credentials
//         let initial_credentials = self.token_manager.get_credentials().await?;

//         // Generate subscription ID
//         let subscription_id = self.next_subscription_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

//         // Subscribe the listener
//         let _ = self.subscription_sender.send(SubscriptionCommand::Subscribe(subscription_id, listener));

//         // Create disposable subscription handle
//         let subscription = CredentialsSubscription {
//             id: subscription_id,
//             sender: self.subscription_sender.clone(),
//         };

//         Ok((initial_credentials, Box::new(subscription)))
//     }
// }

// /// Streaming background token refresh service that notifies subscribers
// #[cfg(feature = "aio")]
// pub struct StreamingTokenRefreshService<P, T> {
//     streaming_token_manager: Arc<StreamingTokenManager<P, T>>,
//     refresh_interval: Duration,
//     refresh_handle: Option<tokio::task::JoinHandle<()>>,
//     shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
// }

// #[cfg(feature = "aio")]
// impl<P, T> StreamingTokenRefreshService<P, T>
// where
//     P: AsyncCredentialsProvider<T> + 'static,
//     T: Clone + Send + Sync + 'static,
// {
//     /// Create a new streaming background token refresh service
//     pub fn new(provider: P, config: TokenRefreshServiceConfig) -> Self {
//         let refresh_interval = config.refresh_interval;

//         let streaming_token_manager = Arc::new(StreamingTokenManager::new(provider, config));
//         Self {
//             streaming_token_manager,
//             refresh_interval: refresh_interval,
//             refresh_handle: None,
//             shutdown_sender: None,
//         }
//     }

//     /// Get the streaming token manager
//     pub fn get_streaming_token_manager(&self) -> Arc<StreamingTokenManager<P, T>> {
//         self.streaming_token_manager.clone()
//     }

//     /// Start the background refresh service with subscriber notifications
//     pub async fn start(&mut self) -> RedisResult<()> {
//         if self.refresh_handle.is_some() {
//             return Err(RedisError::from((
//                 ErrorKind::InvalidClientConfig,
//                 "Streaming refresh service is already running",
//             )));
//         }

//         let (shutdown_sender, mut shutdown_receiver) = tokio::sync::oneshot::channel();
//         self.shutdown_sender = Some(shutdown_sender);

//         let streaming_token_manager = self.get_streaming_token_manager();
//         let refresh_interval = self.refresh_interval;

//         let handle = tokio::spawn(async move {
//             let mut refresh_interval = tokio::time::interval(refresh_interval);
//             refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

//             loop {
//                 tokio::select! {
//                     _ = &mut shutdown_receiver => {
//                         break;
//                     }
//                     _ = refresh_interval.tick() => {
//                         match streaming_token_manager.token_manager.get_credentials().await {
//                             Ok(new_credentials) => {
//                                 // Notify all subscribers of the new credentials
//                                 streaming_token_manager.notify_credentials_update(new_credentials);
//                             }
//                             Err(err) => {
//                                 eprintln!("Token refresh failed: {err}");
//                                 // Notify subscribers of the error
//                                 streaming_token_manager.notify_error(err);
//                             }
//                         }
//                     }
//                 }
//             }
//         });

//         self.refresh_handle = Some(handle);
//         Ok(())
//     }

//     /// Stop the background refresh service
//     pub async fn stop(&mut self) -> RedisResult<()> {
//         if let Some(sender) = self.shutdown_sender.take() {
//             let _ = sender.send(());
//         }

//         if let Some(handle) = self.refresh_handle.take() {
//             handle.await.map_err(|e| {
//                 RedisError::from((
//                     ErrorKind::IoError,
//                     "Failed to stop streaming refresh service",
//                     e.to_string(),
//                 ))
//             })?;
//         }

//         Ok(())
//     }
// }

// #[cfg(feature = "aio")]
// impl<P> Drop for StreamingTokenRefreshService<P> {
//     fn drop(&mut self) {
//         if let Some(sender) = self.shutdown_sender.take() {
//             let _ = sender.send(());
//         }
//     }
// }
