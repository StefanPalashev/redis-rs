use crate::types::{RedisError, RedisResult};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use std::future::Future;
use futures_util::Stream;

/// Basic authentication credentials for Redis connection
#[cfg(feature = "token-based-authentication")]
#[derive(Debug, Clone)]
pub struct BasicAuth {
    /// The username for authentication
    pub username: String,
    /// The password for authentication
    pub password: String,
}

/// Represents authentication credentials for Redis connection
#[cfg(feature = "token-based-authentication")]
#[derive(Debug, Clone)]
pub struct AuthCredentials<T> {
    /// The authentication token (password for Redis AUTH command)
    pub token: T,
    /// Optional expiration time for the token
    pub expires_at: Option<SystemTime>,
    /// The time when the credentials were received/created
    pub received_at: SystemTime,
}

#[cfg(feature = "token-based-authentication")]
impl<T> AuthCredentials<T> {
    /// Create new credentials with a token
    pub fn new(token: T) -> Self {
        Self {
            token,
            expires_at: None,
            received_at: SystemTime::now(),
        }
    }

    /// Create new credentials with a token and expiration time
    pub fn with_expiration(token: T, expires_at: SystemTime) -> Self {
        Self {
            token,
            expires_at: Some(expires_at),
            received_at: SystemTime::now(),
        }
    }

    /// Check if the credentials have expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            SystemTime::now() >= expires_at
        } else {
            false
        }
    }

    /// Check if the credentials are eligible for refresh.
    /// Note that only credentials with an expiration time are considered for refresh.
    ///
    /// If the time elapsed since the credentials were received is greater than the refresh threshold, the credentials are considered eligible for refresh.
    pub fn eligible_for_refresh(&self, refresh_threshold: std::time::Duration) -> bool {
        if self.expires_at.is_some() {
            match SystemTime::now().duration_since(self.received_at) {
                Ok(elapsed_time) => elapsed_time >= refresh_threshold,
                Err(_) => true, // Should be unreachable. Force refresh just in case.
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
#[cfg(feature = "token-based-authentication")]
pub trait CredentialsProvider: Send + Sync {
    /// Get the current authentication credentials
    ///
    /// This method should return valid credentials that can be used for
    /// Redis authentication. If the credentials are expired or invalid,
    /// the implementation should refresh them before returning.
    fn get_credentials(&self) -> RedisResult<BasicAuth>;
}

/// Async version of the credentials provider trait
#[cfg(feature = "token-based-authentication")]
pub trait AsyncCredentialsProvider: Send + Sync {
    /// Get the current authentication credentials asynchronously
    fn get_credentials(&self) -> Box<dyn Future<Output = RedisResult<BasicAuth>> + Send>;
}

/// Trait for listening to credential updates in a streaming fashion
#[cfg(feature = "token-based-authentication")]
pub trait StreamingCredentialsListener: Send + Sync + 'static {
    /// Called when new credentials are available
    fn on_credentials_update(&mut self, credentials: BasicAuth) -> Box<dyn Future<Output = ()> + Send>;
    /// Called when an error occurs in the credential stream
    fn on_error(&self, error: RedisError);
}

/// Trait for providing credentials in a streaming fashion
///
/// This allows connections to subscribe to credential updates and automatically
/// re-authenticate when tokens are refreshed, preventing connection failures
/// due to token expiration.
#[cfg(feature = "token-based-authentication")]
pub trait StreamingCredentialsProvider: Send + Sync {
    /// Get the current authentication credentials
    fn get_credentials(&self) -> Pin<Box<dyn Future<Output = RedisResult<BasicAuth>> + Send>>;
    
    /// Subscribe to credential updates
    ///
    /// Returns the initial credentials and a disposable subscription handle
    fn subscribe(&self, listener: Arc<dyn StreamingCredentialsListener>)
        //-> impl std::future::Future<Output = RedisResult<(BasicAuth, Box<dyn Disposable>)>> + Send;
        -> Box<dyn Disposable>;
    
    // Clone the credentials provider - we might need this
    // clone was implemented now for the connection object along with display
    // there's a dyn box crate, which implements something like the clone box under the hood
    // Trait objects (dyn Trait) are not Clone by default
    /*
        let cloned = original.clone();
        Will fail unless the trait StreamingCredentialsProvider itself is declared as Clone-compatible in a trait-object-friendly way. 
        Which by default it isn't.

        This doesn't work:

        pub trait StreamingCredentialsProvider: Clone + Send + Sync
        That’s not object-safe, because Clone has a method that returns Self:

        fn clone(&self) -> Self;
        Which makes the whole trait not usable as a dyn Trait (since Self isn’t known).
     */

    // fn clone_box(&self) -> Box<dyn CredentialsProvider>;
}

#[cfg(feature = "token-based-authentication")]
pub trait SStreamingCredentialsProvider: Send + Sync {

  /// Get a fresh, independent stream of credentials.
  fn subscribe(&self) -> impl Stream<Item = Arc<BasicAuth>> + Unpin + Send + 'static;

  /// Stop background work; subscribers will end as updates cease.
  fn stop(&self);
}

/// Handle for disposing of subscriptions
#[cfg(feature = "token-based-authentication")]
pub trait Disposable: Send + Sync + 'static {
    /// Dispose of the subscription, stopping further credential updates
    fn dispose(&self);
}


/// Async connection listener that re-authenticates async connections when credentials are updated
#[cfg(feature = "token-based-authentication")]
pub struct AsyncConnectionReAuthenticator {
    connection: crate::aio::MultiplexedConnection,
}

#[cfg(feature = "token-based-authentication")]
impl AsyncConnectionReAuthenticator {
    /// Create a new async connection re-authenticator
    pub fn new(connection: crate::aio::MultiplexedConnection) -> Self {
        Self { connection }
    }
}

#[cfg(feature = "token-based-authentication")]
impl StreamingCredentialsListener for AsyncConnectionReAuthenticator {
    fn on_credentials_update(&mut self, credentials: BasicAuth) -> Box<dyn Future<Output = ()> + Send> {
        let mut connection = self.connection.clone();
        Box::new(Box::pin(async move {
            if let Err(err) = connection.re_authenticate_with_credentials(&credentials).await {
                eprintln!("Failed to re-authenticate async connection: {}", err);
            }
        }))
    }

    fn on_error(&self, error: RedisError) {
        eprintln!("Credential stream error for async connection: {}", error);
    }
}

/// Static credentials provider that always returns the same credentials
#[cfg(feature = "token-based-authentication")]
#[derive(Debug, Clone)]
pub struct StaticCredentialsProvider {
    credentials: BasicAuth,
}

#[cfg(feature = "token-based-authentication")]
impl StaticCredentialsProvider {
    /// Create a new static credentials provider with a password
    pub fn new(username: String, password: String) -> Self {
        Self {
            credentials: BasicAuth {
                username,
                password
            },
        }
    }
}

#[cfg(feature = "token-based-authentication")]
impl CredentialsProvider for StaticCredentialsProvider {
    fn get_credentials(&self) -> RedisResult<BasicAuth> {
        Ok(self.credentials.clone())
    }
}
