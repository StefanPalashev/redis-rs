use redis::AuthCredentials;
use std::time::{Duration, SystemTime};

const TOKEN: &str = "test_token";

#[test]
fn test_auth_credentials_creation_without_expiration() {
    let timestamp_before_creation = SystemTime::now();
    let credentials = AuthCredentials::new(TOKEN.to_string());
    let timestamp_after_creation = SystemTime::now();

    assert_eq!(credentials.token, TOKEN);
    assert!(credentials.expires_at.is_none());
    assert!(!credentials.is_expired());

    assert!(credentials.received_at >= timestamp_before_creation);
    assert!(credentials.received_at <= timestamp_after_creation);

    // Test credentials refresh eligibility
    // Credentials with no expiration are never eligible for refresh regardless of the refresh threshold
    assert!(!credentials.eligible_for_refresh(Duration::ZERO));
    assert!(!credentials.eligible_for_refresh(Duration::from_secs(1800)));
}

#[test]
fn test_auth_credentials_creation_with_expiration() {
    let timestamp_before_creation = SystemTime::now();
    let future_time = SystemTime::now() + Duration::from_secs(3600);
    let credentials = AuthCredentials::with_expiration(TOKEN.to_string(), future_time);
    let timestamp_after_creation = SystemTime::now();

    assert_eq!(credentials.token, TOKEN);
    assert!(credentials.expires_at.is_some());
    assert!(!credentials.is_expired());

    assert!(credentials.received_at >= timestamp_before_creation);
    assert!(credentials.received_at <= timestamp_after_creation);

    // Test credentials refresh eligibility
    // Credentials with no refresh threshold are always eligible for refresh
    assert!(credentials.eligible_for_refresh(Duration::ZERO));

    // Fresh credentials with 1 hour expiry should not be eligible for refresh within the first 30 minutes
    assert!(!credentials.eligible_for_refresh(Duration::from_secs(1800)));
}

#[test]
fn test_auth_expired_credentials() {
    let past_time = SystemTime::now() - Duration::from_secs(3600);
    let credentials = AuthCredentials::with_expiration(TOKEN.to_string(), past_time);
    assert!(credentials.is_expired());
}
