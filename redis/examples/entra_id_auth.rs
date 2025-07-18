//! Example demonstrating Entra ID authentication with Redis
//!
//! This example shows how to use Azure Entra ID for Redis authentication
//! using various credential types like DefaultAzureCredential, service principals,
//! and managed identities.

#[cfg(feature = "entra-id")]
use redis::{
    AsyncTokenRefreshService, Client, EntraIdCredentialsProvider, RedisResult, TokenManager,
    TokenRefreshConfig,
};

#[cfg(feature = "entra-id")]
#[tokio::main]
async fn main() -> RedisResult<()> {
    println!("Redis Entra ID Authentication Example");

    // Example 1: Using DefaultAzureCredential (recommended for development)
    example_default_credential().await?;

    // Example 2: Using Service Principal with client secret
    example_service_principal().await?;

    // Example 3: Using Managed Identity
    example_managed_identity().await?;

    // Example 4: Using Token Manager with automatic refresh
    example_token_manager().await?;

    // Example 5: Using background token refresh service
    example_background_refresh().await?;

    Ok(())
}

#[cfg(feature = "entra-id")]
async fn example_default_credential() -> RedisResult<()> {
    println!("\n=== Example 1: DefaultAzureCredential ===");

    // Create credentials provider using DefaultAzureCredential
    // This will try multiple credential sources in order:
    // 1. Environment variables
    // 2. Managed Identity
    // 3. Azure CLI
    // 4. Visual Studio Code
    // 5. Azure PowerShell
    let provider = EntraIdCredentialsProvider::new_default()?;

    // Create Redis client with credentials provider
    let _client =
        Client::open("redis://your-redis-instance.com:6380")?.with_credentials_provider(provider);

    // Note: This would fail without proper Azure credentials configured
    // let mut con = client.get_connection()?;
    // let _: () = con.set("key", "value")?;
    // let result: String = con.get("key")?;
    // println!("Retrieved value: {}", result);

    println!("DefaultAzureCredential provider created successfully");
    Ok(())
}

#[cfg(feature = "entra-id")]
async fn example_service_principal() -> RedisResult<()> {
    println!("\n=== Example 2: Service Principal ===");

    // Create credentials provider using service principal
    let provider = EntraIdCredentialsProvider::new_client_secret(
        "your-tenant-id".to_string(),
        "your-client-id".to_string(),
        "your-client-secret".to_string(),
    )?;

    let _client =
        Client::open("redis://your-redis-instance.com:6380")?.with_credentials_provider(provider);

    println!("Service Principal provider created successfully");
    Ok(())
}

#[cfg(feature = "entra-id")]
async fn example_managed_identity() -> RedisResult<()> {
    println!("\n=== Example 3: Managed Identity ===");

    // System-assigned managed identity
    let provider = EntraIdCredentialsProvider::new_system_assigned_managed_identity()?;

    let _client =
        Client::open("redis://your-redis-instance.com:6380")?.with_credentials_provider(provider);

    // For user-assigned managed identity:
    // let provider = EntraIdCredentialsProvider::new_user_assigned_managed_identity(
    //     "your-user-assigned-client-id".to_string()
    // )?;

    println!("Managed Identity provider created successfully");
    Ok(())
}

#[cfg(feature = "entra-id")]
async fn example_token_manager() -> RedisResult<()> {
    println!("\n=== Example 4: Token Manager ===");

    // Create provider
    let provider = EntraIdCredentialsProvider::new_default()?;

    // Create token manager with custom configuration
    let config = TokenRefreshConfig {
        expiration_refresh_ratio: 0.75, // Refresh when 75% of token lifetime has elapsed
        retry_config: redis::RetryConfig {
            max_attempts: 5,
            initial_delay: std::time::Duration::from_millis(200),
            max_delay: std::time::Duration::from_secs(60),
            backoff_multiplier: 2.0,
            jitter_percentage: 0.1,
        },
    };

    let _token_manager = TokenManager::with_config(provider, config);

    // Get credentials (will be cached and refreshed automatically)
    // let credentials = token_manager.get_credentials()?;
    // println!("Token expires at: {:?}", credentials.expires_at);

    println!("Token Manager created successfully");
    Ok(())
}

#[cfg(feature = "entra-id")]
async fn example_background_refresh() -> RedisResult<()> {
    println!("\n=== Example 5: Background Token Refresh ===");

    // Create provider
    let provider = EntraIdCredentialsProvider::new_default()?;

    // Create background refresh service
    let mut refresh_service =
        AsyncTokenRefreshService::new(provider, TokenRefreshConfig::default());

    // Start background refresh
    refresh_service.start().await?;

    // Get token manager for accessing credentials
    let _token_manager = refresh_service.token_manager();

    // Simulate some work
    println!("Background refresh service started");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Stop the service
    refresh_service.stop().await;
    println!("Background refresh service stopped");

    Ok(())
}

#[cfg(not(feature = "entra-id"))]
fn main() {
    println!("This example requires the 'entra-id' feature to be enabled.");
    println!("Run with: cargo run --example entra_id_auth --features entra-id");
}
