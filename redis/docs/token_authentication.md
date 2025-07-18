# Token-Based Authentication with Azure Entra ID

This document describes how to use token-based authentication with Azure Entra ID in redis-rs, providing secure, dynamic authentication for Redis connections.

## Overview

Token-based authentication allows you to authenticate to Redis using Azure Entra ID tokens instead of static passwords. This provides several benefits:

- **Enhanced Security**: Tokens have limited lifetimes and can be automatically refreshed
- **Centralized Identity Management**: Leverage Azure Entra ID for user and service authentication
- **Audit and Compliance**: Better tracking and auditing of authentication events
- **Zero-Trust Architecture**: Support for modern security models

## Features

- **Multiple Authentication Flows**: Support for service principals, managed identities, and interactive flows
- **Automatic Token Refresh**: Background token refresh with configurable policies
- **Retry Logic**: Robust error handling with exponential backoff and jitter
- **Async Support**: Full async/await support for non-blocking operations
- **Connection Pooling**: Compatible with existing connection pooling mechanisms

## Quick Start

### 1. Enable the Feature

Add the `entra-id` feature to your `Cargo.toml`:

```toml
[dependencies]
redis = { version = "0.32.4", features = ["entra-id", "tokio-comp"] }
```

### 2. Basic Usage with DefaultAzureCredential

```rust
use redis::{Client, Commands, EntraIdCredentialsProvider};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    // Create credentials provider using DefaultAzureCredential
    let provider = EntraIdCredentialsProvider::new_default()?;
    
    // Create Redis client with credentials provider
    let client = Client::open("redis://your-redis-instance.com:6380")?
        .with_credentials_provider(provider);
    
    // Use the client normally
    let mut con = client.get_connection()?;
    con.set("key", "value")?;
    let result: String = con.get("key")?;
    println!("Value: {}", result);
    
    Ok(())
}
```

## Authentication Methods

### DefaultAzureCredential (Recommended for Development)

The `DefaultAzureCredential` tries multiple credential sources in order:

```rust
let provider = EntraIdCredentialsProvider::new_default()?;
```

### Service Principal with Client Secret

For production applications:

```rust
let provider = EntraIdCredentialsProvider::new_client_secret(
    "your-tenant-id".to_string(),
    "your-client-id".to_string(),
    "your-client-secret".to_string(),
)?;
```

### Service Principal with Certificate

For enhanced security:

```rust
use redis::ClientCertificateConfig;
use std::fs;

// Load certificate and private key from files
let certificate_pem = fs::read_to_string("path/to/certificate.pem")?;
let private_key_pem = fs::read_to_string("path/to/private_key.pem")?;

let cert_config = ClientCertificateConfig {
    certificate_pem,
    private_key_pem,
};

let provider = EntraIdCredentialsProvider::new_client_certificate(
    "your-tenant-id".to_string(),
    "your-client-id".to_string(),
    cert_config,
)?;
```

### Managed Identity

For Azure-hosted applications:

```rust
// System-assigned managed identity
let provider = EntraIdCredentialsProvider::new_system_assigned_managed_identity()?;

// User-assigned managed identity
let provider = EntraIdCredentialsProvider::new_user_assigned_managed_identity(
    "your-user-assigned-client-id".to_string()
)?;
```

## Advanced Configuration

### Token Manager with Custom Configuration

```rust
use redis::{TokenManager, TokenRefreshConfig, RetryConfig};
use std::time::Duration;

let provider = EntraIdCredentialsProvider::new_default()?;

let config = TokenRefreshConfig {
    expiration_refresh_ratio: 0.75, // Refresh at 75% of token lifetime
    retry_config: RetryConfig {
        max_attempts: 5,
        initial_delay: Duration::from_millis(200),
        max_delay: Duration::from_secs(60),
        backoff_multiplier: 2.0,
        jitter_percentage: 0.1,
    },
};

let token_manager = TokenManager::with_config(provider, config);
```

### Background Token Refresh Service

For long-running applications:

```rust
use redis::AsyncTokenRefreshService;

let provider = EntraIdCredentialsProvider::new_default()?;
let mut refresh_service = AsyncTokenRefreshService::new(
    provider,
    TokenRefreshConfig::default(),
);

// Start background refresh
refresh_service.start().await?;

// Get token manager for accessing credentials
let token_manager = refresh_service.token_manager();

// Your application logic here...

// Stop the service when done
refresh_service.stop().await;
```

## Configuration Options

### TokenRefreshConfig

- `expiration_refresh_ratio`: Fraction of token lifetime before refresh (0.0-1.0)
- `retry_config`: Configuration for retry behavior on failures

### RetryConfig

- `max_attempts`: Maximum number of retry attempts
- `initial_delay`: Initial delay before first retry
- `max_delay`: Maximum delay between retries
- `backoff_multiplier`: Exponential backoff multiplier
- `jitter_percentage`: Random jitter to prevent thundering herd

## Error Handling

The library provides comprehensive error handling:

```rust
match client.get_connection() {
    Ok(connection) => {
        // Use connection
    }
    Err(redis::RedisError { kind: redis::ErrorKind::AuthenticationFailed, .. }) => {
        // Handle authentication failure
        eprintln!("Authentication failed - check your credentials");
    }
    Err(e) => {
        // Handle other errors
        eprintln!("Connection failed: {}", e);
    }
}
```

## Best Practices

### 1. Use Appropriate Credential Types

- **Development**: `DefaultAzureCredential`
- **Production Services**: Service Principal with certificate
- **Azure-hosted Apps**: Managed Identity

### 2. Configure Appropriate Refresh Ratios

- **High-frequency apps**: 0.5-0.7 (refresh early)
- **Low-frequency apps**: 0.8-0.9 (refresh later)

### 3. Handle Token Expiration

- Use background refresh services for long-running applications
- Implement proper error handling for authentication failures
- Consider connection pooling for better resource management

### 4. Security Considerations

- Store client secrets securely (Azure Key Vault, environment variables)
- Use certificates instead of secrets when possible
- Regularly rotate credentials
- Monitor authentication logs

## Troubleshooting

### Common Issues

1. **"Authentication failed"**: Check your credentials and permissions
2. **"Token expired"**: Ensure automatic refresh is configured
3. **"Connection timeout"**: Check network connectivity and Redis endpoint

### Debug Logging

Enable debug logging to troubleshoot authentication issues:

```rust
env_logger::init();
```

### Testing Authentication

Use the provided example to test your configuration:

```bash
cargo run --example entra_id_auth --features entra-id
```

## Compatibility

- **Redis Versions**: Compatible with Redis 6.0+ (ACL support required)
- **Azure Redis**: Fully compatible with Azure Cache for Redis
- **Protocol Support**: Works with both RESP2 and RESP3
- **Connection Types**: TCP, TLS, Unix sockets (where supported)

## Migration from Password Authentication

To migrate from password-based authentication:

1. Set up Azure Entra ID authentication in your Redis instance
2. Create appropriate service principals or managed identities
3. Replace password-based connection strings with credential providers
4. Test thoroughly in a staging environment
5. Deploy with monitoring and rollback plans

## Examples

See the `examples/entra_id_auth.rs` file for comprehensive examples of all authentication methods and configurations.
