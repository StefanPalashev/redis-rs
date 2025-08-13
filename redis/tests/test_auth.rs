use redis::{
    AuthCredentials, CredentialsProvider, RedisResult, StaticCredentialsProvider, TokenManager,
    TokenRefreshConfig,
};
use std::time::{Duration, SystemTime};

#[test]
fn test_auth_credentials_creation() {
    let before = SystemTime::now();
    let creds = AuthCredentials::new("test_token".to_string());
    let after = SystemTime::now();

    assert_eq!(creds.token, "test_token");
    assert!(creds.expires_at.is_none());
    assert!(!creds.is_expired());

    // Verify received_at is set to current time
    assert!(creds.received_at >= before);
    assert!(creds.received_at <= after);

    // Test credentials refresh eligibility
    // Credentials with no expiration are never eligible for refresh regardless of the refresh threshold
    assert!(!creds.eligible_for_refresh(Duration::ZERO));
    assert!(!creds.eligible_for_refresh(Duration::from_secs(1800)));
}

#[test]
fn test_auth_credentials_with_expiration() {
    let before = SystemTime::now();
    let future_time = SystemTime::now() + Duration::from_secs(3600);
    let creds = AuthCredentials::with_expiration("test_token".to_string(), future_time);
    let after = SystemTime::now();

    assert_eq!(creds.token, "test_token");
    assert!(creds.expires_at.is_some());
    assert!(!creds.is_expired());

    // Verify received_at is set to current time
    assert!(creds.received_at >= before);
    assert!(creds.received_at <= after);

    // Test credentials refresh eligibility
    // Credentials with no refresh threshold are always eligible for refresh
    assert!(creds.eligible_for_refresh(Duration::ZERO));

    // Fresh credentials with 1 hour expiry should not be eligible for refresh within the first 30 minutes
    assert!(!creds.eligible_for_refresh(Duration::from_secs(1800)));
}

#[test]
fn test_auth_credentials_expired() {
    let past_time = SystemTime::now() - Duration::from_secs(3600);
    let creds = AuthCredentials::with_expiration("test_token".to_string(), past_time);
    assert!(creds.is_expired());
}

#[test]
fn test_static_credentials_provider() {
    let provider = StaticCredentialsProvider::new("static_token".to_string());
    let creds = provider.get_credentials().unwrap();
    assert_eq!(creds.token, "static_token");
}

#[test]
fn test_static_credentials_provider_clone() {
    let provider = StaticCredentialsProvider::new("static_token".to_string());
    let cloned_provider = provider.clone();
    let creds = cloned_provider.get_credentials().unwrap();
    assert_eq!(creds.token, "static_token");
}

#[test]
fn test_credentials_provider_trait_object() {
    let provider: Box<dyn CredentialsProvider> =
        Box::new(StaticCredentialsProvider::new("boxed_token".to_string()));
    let creds = provider.get_credentials().unwrap();
    assert_eq!(creds.token, "boxed_token");
}

#[test]
fn test_credentials_provider_trait_object_clone() {
    let provider: Box<dyn CredentialsProvider> =
        Box::new(StaticCredentialsProvider::new("boxed_token".to_string()));
    let cloned_provider = provider.clone();
    let creds = cloned_provider.get_credentials().unwrap();
    assert_eq!(creds.token, "boxed_token");
}

#[test]
fn test_token_manager_basic() {
    let provider = StaticCredentialsProvider::new("managed_token".to_string());
    let token_manager = TokenManager::new(provider);
    let creds = token_manager.get_credentials().unwrap();
    assert_eq!(creds.token, "managed_token");
}

#[test]
fn test_token_manager_with_config() {
    let provider = StaticCredentialsProvider::new("configured_token".to_string());
    let config = TokenRefreshConfig {
        expiration_refresh_ratio: 0.5,
        retry_config: redis::RetryConfig {
            max_attempts: 2,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 1.5,
            jitter_percentage: 0.05,
        },
    };
    let token_manager = TokenManager::with_config(provider, config);
    let creds = token_manager.get_credentials().unwrap();
    assert_eq!(creds.token, "configured_token");
}

#[test]
fn test_token_refresh_config_default() {
    let config = TokenRefreshConfig::default();
    assert_eq!(config.expiration_refresh_ratio, 0.8);
    assert_eq!(config.retry_config.max_attempts, 3);
    assert_eq!(
        config.retry_config.initial_delay,
        Duration::from_millis(100)
    );
    assert_eq!(config.retry_config.max_delay, Duration::from_secs(30));
    assert_eq!(config.retry_config.backoff_multiplier, 2.0);
    assert_eq!(config.retry_config.jitter_percentage, 0.1);
}

#[cfg(feature = "aio")]
mod async_tests {
    use super::*;
    use redis::{AsyncCredentialsProvider, AsyncTokenManager, AsyncTokenRefreshService};

    #[tokio::test]
    async fn test_async_static_credentials_provider() {
        let provider = StaticCredentialsProvider::new("async_token".to_string());
        let creds = AsyncCredentialsProvider::get_credentials(&provider)
            .await
            .unwrap();
        assert_eq!(creds.token, "async_token");
    }

    #[tokio::test]
    async fn test_async_token_manager() {
        let provider = StaticCredentialsProvider::new("async_managed_token".to_string());
        let token_manager = AsyncTokenManager::new(provider);
        let creds = token_manager.get_credentials().await.unwrap();
        assert_eq!(creds.token, "async_managed_token");
    }

    #[tokio::test]
    async fn test_async_token_refresh_service() {
        let provider = StaticCredentialsProvider::new("refresh_service_token".to_string());
        let mut refresh_service =
            AsyncTokenRefreshService::new(provider, TokenRefreshConfig::default());

        // Start the service
        refresh_service.start().await.unwrap();

        // Get credentials through the token manager
        let token_manager = refresh_service.token_manager();
        let creds = token_manager.get_credentials().await.unwrap();
        assert_eq!(creds.token, "refresh_service_token");

        // Stop the service
        refresh_service.stop().await;
    }

    #[tokio::test]
    async fn test_async_token_refresh_service_multiple_start_stop() {
        let provider = StaticCredentialsProvider::new("multi_start_token".to_string());
        let mut refresh_service =
            AsyncTokenRefreshService::new(provider, TokenRefreshConfig::default());

        // Start multiple times should be safe
        refresh_service.start().await.unwrap();
        refresh_service.start().await.unwrap();

        // Stop multiple times should be safe
        refresh_service.stop().await;
        refresh_service.stop().await;
    }
}

// Mock provider for testing error scenarios
#[derive(Debug, Clone)]
struct FailingCredentialsProvider {
    should_fail: bool,
}

impl FailingCredentialsProvider {
    fn new(should_fail: bool) -> Self {
        Self { should_fail }
    }
}

impl CredentialsProvider for FailingCredentialsProvider {
    fn get_credentials(&self) -> RedisResult<AuthCredentials> {
        if self.should_fail {
            Err(redis::RedisError::from((
                redis::ErrorKind::AuthenticationFailed,
                "Mock authentication failure",
            )))
        } else {
            Ok(AuthCredentials::new("success_token".to_string()))
        }
    }

    fn clone_box(&self) -> Box<dyn CredentialsProvider> {
        Box::new(self.clone())
    }
}

#[test]
fn test_token_manager_retry_on_failure() {
    let provider = FailingCredentialsProvider::new(true);
    let config = TokenRefreshConfig {
        expiration_refresh_ratio: 0.8,
        retry_config: redis::RetryConfig {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1), // Very short for testing
            max_delay: Duration::from_millis(10),
            backoff_multiplier: 2.0,
            jitter_percentage: 0.0, // No jitter for predictable testing
        },
    };
    let token_manager = TokenManager::with_config(provider, config);

    // Should fail after retries
    let result = token_manager.get_credentials();
    assert!(result.is_err());
}

#[test]
fn test_token_manager_success_after_retry() {
    let provider = FailingCredentialsProvider::new(false);
    let token_manager = TokenManager::new(provider);

    // Should succeed
    let result = token_manager.get_credentials();
    assert!(result.is_ok());
    assert_eq!(result.unwrap().token, "success_token");
}
