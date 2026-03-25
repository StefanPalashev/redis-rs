#![cfg(feature = "search")]

#[path = "../support/mod.rs"]
mod support;
use crate::support::*;
use redis::{Commands, RedisResult, schema, search::*};
use redis::{ProtocolVersion, RedisConnectionInfo};
use rstest::rstest;
use serde_json::json;
use serial_test::serial;

#[rstest]
#[case(ProtocolVersion::RESP2)]
#[case(ProtocolVersion::RESP3)]
#[serial]
fn test_ft_dropindex_non_existent_index(#[case] protocol: ProtocolVersion) {
    println!(
        "Starting test_ft_dropindex_non_existent_index - Protocol: {:?}",
        protocol
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = create_connection_with_protocol(&ctx, protocol);
    let _: () = con.flushdb().unwrap();

    const NON_EXISTENT_INDEX: &str = "non_existent_index";

    // Try to drop a non-existent index should result in an error
    let result: RedisResult<String> = con.ft_dropindex(NON_EXISTENT_INDEX);
    assert!(result.is_err());
}

const INDEX_NAME: &str = "idx:products";

fn create_test_index(con: &mut redis::Connection, data_type: IndexDataType) {
    let schema = match data_type {
        IndexDataType::Hash => schema! {
            "title" => SchemaTextField::new(),
            "price" => SchemaNumericField::new(),
        },
        IndexDataType::Json => schema! {
            "$.title" => SchemaTextField::new().alias("title"),
            "$.price" => SchemaNumericField::new().alias("price"),
        },
        _ => panic!("Unsupported data type: {:?}", data_type),
    };

    let options = CreateOptions::new().on(data_type).prefix("product:");

    let result: RedisResult<String> = con.ft_create(INDEX_NAME, &options, &schema);
    assert_eq!(result, Ok("OK".to_string()));
}

fn add_test_documents(con: &mut redis::Connection, data_type: IndexDataType) {
    match data_type {
        IndexDataType::Hash => {
            let _: () = con
                .hset_multiple("product:1", &[("title", "Laptop"), ("price", "1000")])
                .unwrap();
            let _: () = con
                .hset_multiple("product:2", &[("title", "Mouse"), ("price", "25")])
                .unwrap();
        }
        IndexDataType::Json => {
            use redis::JsonCommands;
            let product1 = json!({
                "title": "Laptop",
                "price": 1000
            });
            let product2 = json!({
                "title": "Mouse",
                "price": 25
            });
            let _: bool = con.json_set("product:1", "$", &product1).unwrap();
            let _: bool = con.json_set("product:2", "$", &product2).unwrap();
        }
        _ => panic!("Unsupported data type: {:?}", data_type),
    }
}

/// Helper function to create a connection with the specified protocol
fn create_connection_with_protocol(
    ctx: &TestContext,
    protocol: ProtocolVersion,
) -> redis::Connection {
    let redis = RedisConnectionInfo::default().set_protocol(protocol);
    let connection_info = ctx.server.connection_info().set_redis_settings(redis);
    let client = redis::Client::open(connection_info).unwrap();
    client.get_connection().unwrap()
}

/// Helper function to set up test environment
fn setup_test_env(
    ctx: &TestContext,
    protocol: ProtocolVersion,
    data_type: IndexDataType,
) -> redis::Connection {
    let mut con = create_connection_with_protocol(ctx, protocol);
    let _: () = con.flushdb().unwrap();
    create_test_index(&mut con, data_type);
    add_test_documents(&mut con, data_type);
    // Wait for the data to be indexed
    std::thread::sleep(std::time::Duration::from_millis(500));
    con
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_dropindex_without_dd_keeps_documents(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_dropindex_without_dd_keeps_documents - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // Verify documents exist
    for i in 1..=2 {
        let exists: bool = con.exists(format!("product:{}", i)).unwrap();
        assert!(exists);
    }

    // Drop index without deleting documents
    let result: RedisResult<String> = con.ft_dropindex(INDEX_NAME);
    assert_eq!(result, Ok("OK".to_string()));

    // Verify documents still exist
    for i in 1..=2 {
        let exists: bool = con.exists(format!("product:{}", i)).unwrap();
        assert!(exists);
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_dropindex_with_dd_deletes_documents(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_dropindex_with_dd_deletes_documents - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // Verify documents exist
    for i in 1..=2 {
        let exists: bool = con.exists(format!("product:{}", i)).unwrap();
        assert!(exists);
    }

    // Drop index and delete documents
    let result: RedisResult<String> = con.ft_dropindex_dd(INDEX_NAME);
    assert_eq!(result, Ok("OK".to_string()));

    // Verify documents are deleted
    for i in 1..=2 {
        let exists: bool = con.exists(format!("product:{}", i)).unwrap();
        assert!(!exists);
    }
}
