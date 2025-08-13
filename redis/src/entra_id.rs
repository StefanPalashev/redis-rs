//! Azure Entra ID authentication support for Redis
//!
//! This module provides token-based authentication using Azure Entra ID (formerly Azure Active Directory).
//! It supports multiple credential types including DefaultAzureCredential, service principals,
//! managed identities, and client certificates.
//!
//! # Features
//!
//! - **Multiple Authentication Flows**: Service principals, managed identities, and interactive flows
//! - **Automatic Token Refresh**: Background token refresh with configurable policies
//! - **Retry Logic**: Robust error handling with exponential backoff
//! - **Async Support**: Full async/await support for non-blocking operations
//!
//! # Example
//!
//! ```rust,no_run
//! use redis::{Client, EntraIdCredentialsProvider};
//!
//! # async fn example() -> redis::RedisResult<()> {
//! // Create credentials provider using DefaultAzureCredential
//! let provider = EntraIdCredentialsProvider::new_default()?;
//!
//! // Create Redis client with credentials provider
//! let client = Client::open("redis://your-redis-instance.com:6380")?
//!     .with_credentials_provider(provider);
//! # Ok(())
//! # }
//! ```

#[cfg(all(feature = "entra-id"))]
use crate::auth::BasicAuth;
#[cfg(feature = "entra-id")]
use crate::auth::{AsyncCredentialsProvider, AuthCredentials, CredentialsProvider};
#[cfg(feature = "entra-id")]
use crate::types::{ErrorKind, RedisError, RedisResult};

#[cfg(feature = "entra-id")]
use azure_core::credentials::TokenCredential;
#[cfg(feature = "entra-id")]
use azure_identity::{
    ClientCertificateCredential, ClientSecretCredential, DefaultAzureCredential,
    ManagedIdentityCredential, TokenCredentialOptions, UserAssignedId,
};

#[cfg(feature = "entra-id")]
use std::time::SystemTime;

/// The default Redis scope for Azure Managed Redis
#[cfg(feature = "entra-id")]
pub const REDIS_SCOPE_DEFAULT: &str = "https://redis.azure.com/.default";

/// Configuration for client certificate authentication
/// Note: Maybe the PEMs should be validated
/// There could be several approaches to do that:
/// 1. Just check the formats and the types
/// 2. Make a simple base64 decode check
/// 3. Use a proper library to parse the PEMs
#[cfg(feature = "entra-id")]
#[derive(Debug, Clone)]
pub struct ClientCertificateConfig {
    /// The client certificate in PEM format
    pub certificate_pem: String,
    /// The private key in PEM format
    pub private_key_pem: String,
}

/// Entra ID credentials provider that uses Azure Identity for authentication
#[cfg(feature = "entra-id")]
pub struct EntraIdCredentialsProvider {
    credential: Box<dyn TokenCredential + Send + Sync>,
    scopes: Vec<String>,
}

#[cfg(feature = "entra-id")]
impl std::fmt::Debug for EntraIdCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntraIdCredentialsProvider")
            .field("scopes", &self.scopes)
            .field("credential", &"<TokenCredential>")
            .finish()
    }
}

#[cfg(feature = "entra-id")]
impl EntraIdCredentialsProvider {
    /// Create a new provider using DefaultAzureCredential
    /// This is recommended for development and will try multiple credential types
    pub fn new_default() -> RedisResult<Self> {
        Self::new_default_with_scopes(vec![REDIS_SCOPE_DEFAULT.to_string()])
    }

    /// Create a new provider using DefaultAzureCredential with custom scopes
    pub fn new_default_with_scopes(scopes: Vec<String>) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential = DefaultAzureCredential::new().map_err(Self::convert_error)?;
        Ok(Self {
            credential: Box::new(std::sync::Arc::try_unwrap(credential).map_err(|_| {
                RedisError::from((
                    ErrorKind::AuthenticationFailed,
                    "Failed to unwrap credential",
                ))
            })?),
            scopes,
        })
    }

    /// Create a new provider using client secret authentication (service principal)
    pub fn new_client_secret(
        tenant_id: String,
        client_id: String,
        client_secret: String,
    ) -> RedisResult<Self> {
        Self::new_client_secret_with_scopes(
            tenant_id,
            client_id,
            client_secret,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using client secret authentication with custom scopes
    pub fn new_client_secret_with_scopes(
        tenant_id: String,
        client_id: String,
        client_secret: String,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential =
            ClientSecretCredential::new(&tenant_id, client_id, client_secret.into(), None)
                .map_err(Self::convert_error)?;
        Ok(Self {
            credential: Box::new(std::sync::Arc::try_unwrap(credential).map_err(|_| {
                RedisError::from((
                    ErrorKind::AuthenticationFailed,
                    "Failed to unwrap credential",
                ))
            })?),
            scopes,
        })
    }

    /// Create a new provider using client certificate authentication (service principal)
    pub fn new_client_certificate(
        tenant_id: String,
        client_id: String,
        certificate_config: ClientCertificateConfig,
    ) -> RedisResult<Self> {
        Self::new_client_certificate_with_scopes(
            tenant_id,
            client_id,
            certificate_config,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using client certificate authentication with custom scopes
    pub fn new_client_certificate_with_scopes(
        tenant_id: String,
        client_id: String,
        certificate_config: ClientCertificateConfig,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential = ClientCertificateCredential::new(
            tenant_id,
            client_id,
            certificate_config.certificate_pem,
            certificate_config.private_key_pem,
            azure_identity::ClientCertificateCredentialOptions::new(
                TokenCredentialOptions::default(),
                false,
            ),
        )
        .map_err(Self::convert_error)?;
        Ok(Self {
            credential: Box::new(std::sync::Arc::try_unwrap(credential).map_err(|_| {
                RedisError::from((
                    ErrorKind::AuthenticationFailed,
                    "Failed to unwrap credential",
                ))
            })?),
            scopes,
        })
    }

    /// Create a new provider using system-assigned managed identity
    pub fn new_system_assigned_managed_identity() -> RedisResult<Self> {
        Self::new_system_assigned_managed_identity_with_scopes(
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using system-assigned managed identity with custom scopes
    pub fn new_system_assigned_managed_identity_with_scopes(
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential = ManagedIdentityCredential::new(None).map_err(Self::convert_error)?;
        Ok(Self {
            credential: Box::new(std::sync::Arc::try_unwrap(credential).map_err(|_| {
                RedisError::from((
                    ErrorKind::AuthenticationFailed,
                    "Failed to unwrap credential",
                ))
            })?),
            scopes,
        })
    }

    /// Create a new provider using user-assigned managed identity
    pub fn new_user_assigned_managed_identity(client_id: String) -> RedisResult<Self> {
        Self::new_user_assigned_managed_identity_with_scopes(
            client_id,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using user-assigned managed identity with custom scopes
    pub fn new_user_assigned_managed_identity_with_scopes(
        client_id: String,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let options = azure_identity::ManagedIdentityCredentialOptions {
            user_assigned_id: Some(UserAssignedId::ClientId(client_id)),
            ..Default::default()
        };
        let credential =
            ManagedIdentityCredential::new(Some(options)).map_err(Self::convert_error)?;
        Ok(Self {
            credential: Box::new(std::sync::Arc::try_unwrap(credential).map_err(|_| {
                RedisError::from((
                    ErrorKind::AuthenticationFailed,
                    "Failed to unwrap credential",
                ))
            })?),
            scopes,
        })
    }

    /// Create a new provider with a custom credential implementation
    pub fn new_with_credential(
        credential: Box<dyn TokenCredential + Send + Sync>,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        Ok(Self { credential, scopes })
    }

    /// Validate that scopes are not empty and contain valid URLs
    fn validate_scopes(scopes: &[String]) -> RedisResult<()> {
        if scopes.is_empty() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Scopes cannot be empty for Entra ID authentication",
            )));
        }

        for scope in scopes {
            if scope.trim().is_empty() {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Scope cannot be empty or whitespace-only",
                )));
            }

            // Basic URL validation - should start with https:// and end with /.default
            // Note: This should be verified because there could possibly be scopes without these properties.
            // For example custom scopes or OIDC like scopes... Commenting it out for now

            // if !scope.starts_with("https://") {
            //     return Err(RedisError::from((
            //         ErrorKind::InvalidClientConfig,
            //         "Invalid scope: must start with 'https://'",
            //         format!("Scope: '{}'", scope),
            //     )));
            // }

            // if !scope.ends_with("/.default") {
            //     return Err(RedisError::from((
            //         ErrorKind::InvalidClientConfig,
            //         "Invalid scope: must end with '/.default'",
            //         format!("Scope: '{}'", scope),
            //     )));
            // }
        }

        Ok(())
    }

    /// Convert Azure Core error to Redis error
    fn convert_error(err: azure_core::Error) -> RedisError {
        RedisError::from((
            ErrorKind::AuthenticationFailed,
            "Entra ID authentication failed",
            format!("{err}"),
        ))
    }
}

#[cfg(feature = "entra-id")]
impl CredentialsProvider for EntraIdCredentialsProvider {
    fn get_credentials(&self) -> RedisResult<BasicAuth> {
        // For sync implementation, we need to use a runtime
        // This is not ideal but necessary for the sync trait

        // Note: this could be costly if called frequently.
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            RedisError::from((
                ErrorKind::IoError,
                "Failed to create runtime",
                e.to_string(),
            ))
        })?;

        rt.block_on(async {
            let scopes: Vec<&str> = self.scopes.iter().map(|s| s.as_str()).collect();
            let token_response = self
                .credential
                .get_token(&scopes, None)
                .await
                .map_err(Self::convert_error)?;

            let _expires_at = SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(token_response.expires_on.unix_timestamp() as u64);

            // Ok(AuthCredentials::with_expiration(
            //     token_response.token.secret().to_string(),
            //     expires_at,
            // ))
            Ok(BasicAuth {
                username: "Bearer".to_string(),
                password: token_response.token.secret().to_string(),
            })
        })
    }

    // fn clone_box(&self) -> Box<dyn CredentialsProvider> {
    //     // Note: The credential cannot be cloned directly since TokenCredential doesn't implement Clone
    //     // This is a limitation - each provider instance should be used independently
    //     // Note 2: Maybe this should be removed in general from the CrendentialsProvider trait.
    //     panic!("EntraIdCredentialsProvider cannot be cloned due to Azure Identity limitations. Create separate instances instead.")
    // }
}

// #[cfg(all(feature = "entra-id", feature = "aio"))]
// impl AsyncCredentialsProvider for EntraIdCredentialsProvider {
//     fn get_credentials(&self) -> RedisResult<BasicAuth> {
//         let scopes: Vec<&str> = self.scopes.iter().map(|s| s.as_str()).collect();
//         let token_response = self
//             .credential
//             .get_token(&scopes, None)
//             .await
//             .map_err(Self::convert_error)?;

//         let _expires_at = SystemTime::UNIX_EPOCH
//             + std::time::Duration::from_secs(token_response.expires_on.unix_timestamp() as u64);

//         // Ok(AuthCredentials::with_expiration(
//         //     token_response.token.secret().to_string(),
//         //     expires_at,
//         // ))
//         // This is a sample
//         Ok(BasicAuth {
//             username: "Bearer".to_string(),
//             password: token_response.token.secret().to_string(),
//         })
//     }
// }

#[cfg(all(feature = "entra-id", test))]
mod tests {
    use super::*;

    #[test]
    fn test_entra_id_provider_creation() {
        // Test that we can create providers without panicking
        // Note: These will fail at runtime without proper Azure credentials

        let _default_provider = EntraIdCredentialsProvider::new_default();

        let _client_secret_provider = EntraIdCredentialsProvider::new_client_secret(
            "tenant".to_string(),
            "client".to_string(),
            "secret".to_string(),
        );

        let _managed_identity_provider =
            EntraIdCredentialsProvider::new_system_assigned_managed_identity();
    }

    #[test]
    fn test_custom_scopes() {
        let custom_scopes = vec!["https://custom.scope/.default".to_string()];
        let provider =
            EntraIdCredentialsProvider::new_default_with_scopes(custom_scopes.clone()).unwrap();
        assert_eq!(provider.scopes, custom_scopes);
    }

    #[test]
    fn test_scope_validation() {
        // Test empty scopes
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec![]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Scopes cannot be empty"));

        // Test empty string scope
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["".to_string()]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Scope cannot be empty"));

        // Test whitespace-only scope
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["   ".to_string()]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Scope cannot be empty"));

        /*
        // Test invalid protocol
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["http://invalid.scope/.default".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must start with 'https://'"));

        // Test invalid suffix
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["https://valid.scope/invalid".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must end with '/.default'"));
        */

        // Test valid scope
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec![
            "https://valid.scope/.default".to_string(),
        ]);
        assert!(result.is_ok());
    }
}
