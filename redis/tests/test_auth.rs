#[cfg(feature = "entra-id")]
mod entra_id_tests {
    use redis::{Client, ClientCertificate, EntraIdCredentialsProvider, RetryConfig};

    const REDIS_URL: &str = "REDIS_URL";

    const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
    const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
    const AZURE_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
    const AZURE_CLIENT_CERTIFICATE_PATH: &str = "AZURE_CLIENT_CERTIFICATE_PATH";
    const AZURE_USER_ASSIGNED_CLIENT_ID: &str = "AZURE_USER_ASSIGNED_CLIENT_ID";

    fn get_redis_url() -> String {
        std::env::var(REDIS_URL)
            .unwrap_or_else(|_| panic!("The `REDIS_URL` environment variable is not set."))
    }

    fn get_env_var(var_name: &str) -> String {
        std::env::var(var_name)
            .unwrap_or_else(|_| panic!("The `{var_name}` environment variable is not set."))
    }

    async fn test_redis_connection(mut provider: EntraIdCredentialsProvider, test_key: &str) {
        provider.start(RetryConfig::default());

        let client = Client::open(get_redis_url())
            .unwrap()
            .with_credentials_provider(provider);

        let mut con = client.get_multiplexed_async_connection().await.unwrap();

        redis::cmd("SET")
            .arg(test_key)
            .arg(42i32)
            .exec_async(&mut con)
            .await
            .unwrap();

        let result: Option<String> = redis::cmd("GET")
            .arg(test_key)
            .query_async(&mut con)
            .await
            .unwrap();

        assert_eq!(result, Some("42".to_string()));
    }

    #[tokio::test]
    #[ignore]
    async fn test_default_azure_credential() {
        let provider = EntraIdCredentialsProvider::new_default().unwrap();
        test_redis_connection(provider, "default_azure_credential").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_service_principal_client_secret() {
        let provider = EntraIdCredentialsProvider::new_client_secret(
            get_env_var(AZURE_TENANT_ID),
            get_env_var(AZURE_CLIENT_ID),
            get_env_var(AZURE_CLIENT_SECRET),
        )
        .unwrap();
        test_redis_connection(provider, "service_principal_client_secret").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_service_principal_client_certificate() {
        use base64::Engine;
        use std::fs;

        let certificate_path = get_env_var(AZURE_CLIENT_CERTIFICATE_PATH);
        let certificate_data =
            fs::read(&certificate_path).expect("Failed to read client certificate");

        // Convert the certificate data to base64
        let certificate_base64 =
            base64::engine::general_purpose::STANDARD.encode(&certificate_data);

        let provider = EntraIdCredentialsProvider::new_client_certificate(
            get_env_var(AZURE_TENANT_ID),
            get_env_var(AZURE_CLIENT_ID),
            ClientCertificate {
                base64_pkcs12: certificate_base64,
                password: None,
            },
        )
        .unwrap();
        test_redis_connection(provider, "service_principal_client_certificate").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_system_assigned_managed_identity() {
        let provider = EntraIdCredentialsProvider::new_system_assigned_managed_identity().unwrap();
        test_redis_connection(provider, "system_assigned_managed_identity").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_user_assigned_managed_identity() {
        let provider = EntraIdCredentialsProvider::new_user_assigned_managed_identity(get_env_var(
            AZURE_USER_ASSIGNED_CLIENT_ID,
        ))
        .unwrap();
        test_redis_connection(provider, "user_assigned_managed_identity").await;
    }
}
