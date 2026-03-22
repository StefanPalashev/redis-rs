#![cfg(feature = "search")]

#[path = "../support/mod.rs"]
mod support;
use crate::support::*;
use redis::Commands;
use redis::Value;
use redis::geo::Unit;
use redis::schema;
use redis::search::*;
use redis::{ProtocolVersion, RedisConnectionInfo, RedisResult};
use rstest::rstest;
use serde_json::json;
use serial_test::serial;
use std::ops::Bound;

/// Extension trait for RESP3 map lookups
trait MapExtension {
    fn find(&self, key: &str) -> &Value;
    fn find_opt(&self, key: &str) -> Option<&Value>;
}

impl MapExtension for [(Value, Value)] {
    fn find(&self, key: &str) -> &Value {
        self.iter()
            .find(|(k, _)| match k {
                Value::SimpleString(s) => s == key,
                Value::BulkString(s) => s == key.as_bytes(),
                _ => false,
            })
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("Could not find '{}' in map", key))
    }

    fn find_opt(&self, key: &str) -> Option<&Value> {
        self.iter()
            .find(|(k, _)| match k {
                Value::SimpleString(s) => s == key,
                Value::BulkString(s) => s == key.as_bytes(),
                _ => false,
            })
            .map(|(_, v)| v)
    }
}

const INDEX_NAME: &str = "idx:products";
const SMART: &str = "smart";

fn create_products_index(con: &mut redis::Connection, data_type: IndexDataType) {
    let schema = match data_type {
        IndexDataType::Hash => schema! {
            "title" => SchemaTextField::new().weight(5.0).sortable(Sortable::Yes),
            "description" => SchemaTextField::new(),
            "category" => SchemaTagField::new(),
            "brand" => SchemaTagField::new(),
            "price" => SchemaNumericField::new().sortable(Sortable::Yes),
            "rating" => SchemaNumericField::new().sortable(Sortable::Yes),
            "number_of_comments" => SchemaNumericField::new(),
            "stock" => SchemaNumericField::new(),
            "location" => SchemaGeoField::new(),
            "created_at" => SchemaNumericField::new().sortable(Sortable::Yes),
        },
        IndexDataType::Json => schema! {
            "$.title" => SchemaTextField::new().alias("title").weight(5.0).sortable(Sortable::Yes),
            "$.description" => SchemaTextField::new().alias("description"),
            "$.category" => SchemaTagField::new().alias("category"),
            "$.brand" => SchemaTagField::new().alias("brand"),
            "$.price" => SchemaNumericField::new().alias("price").sortable(Sortable::Yes),
            "$.rating" => SchemaNumericField::new().alias("rating").sortable(Sortable::Yes),
            "$.number_of_comments" => SchemaNumericField::new().alias("number_of_comments"),
            "$.stock" => SchemaNumericField::new().alias("stock"),
            "$.location" => SchemaGeoField::new().alias("location"),
            "$.created_at" => SchemaNumericField::new().alias("created_at").sortable(Sortable::Yes),
        },
        _ => panic!("Unsupported data type: {:?}", data_type),
    };
    assert_eq!(
        con.ft_create(
            INDEX_NAME,
            &CreateOptions::new().on(data_type).prefix("product:"),
            &schema
        ),
        Ok("OK".to_string())
    );
}

#[derive(Debug)]
struct Product {
    id: u16,
    title: &'static str,
    description: &'static str,
    category: &'static str,
    brand: &'static str,
    price: f64,
    rating: f32,
    number_of_comments: u16,
    stock: u16,
    location: &'static str,
    created_at: i64,
}

// Extract the products as a static array
const PRODUCTS: [Product; 10] = [
    Product {
        id: 1,
        title: "Wireless Noise Cancelling Headphones",
        description: "Premium over-ear Bluetooth headphones with active noise cancellation",
        category: "electronics",
        brand: "Sony",
        price: 299.0,
        rating: 4.7,
        number_of_comments: 160,
        stock: 25,
        location: "-73.935242,40.730610", // New York City (Brooklyn)
        created_at: 1700000000,
    },
    Product {
        id: 2,
        title: "Gaming Mechanical Keyboard",
        description: "RGB backlit mechanical keyboard",
        category: "electronics",
        brand: "Razer",
        price: 149.0,
        rating: 4.5,
        number_of_comments: 70,
        stock: 40,
        location: "-118.243683,34.052235", // Los Angeles
        created_at: 1700500000,
    },
    Product {
        id: 3,
        title: "4K Ultra HD OLED Smart TV 77 inch",
        description: "77 inch TV with HDR and Dolby Vision",
        category: "electronics",
        brand: "Sony",
        price: 4299.99,
        rating: 4.9,
        number_of_comments: 590,
        stock: 15,
        location: "-73.7949, 40.7282", // New York City (Queens)
        created_at: 1699000000,
    },
    Product {
        id: 4,
        title: "Robot Vacuum Cleaner with LiDAR Navigation",
        description: "Smart robotic vacuum cleaner with LiDAR mapping, app control, automatic charging and mopping function",
        category: "home_appliance",
        brand: "Roborock",
        price: 699.0,
        rating: 4.7,
        number_of_comments: 32,
        stock: 22,
        location: "2.352222,48.856613", // Paris
        created_at: 1698500000,
    },
    Product {
        id: 5,
        title: "Smart Air Purifier Pro with HEPA Filter",
        description: "WiFi-enabled smart air purifier with True HEPA H13 filter, real-time air quality monitoring and auto mode",
        category: "home_appliance",
        brand: "Dyson",
        price: 549.0,
        rating: 4.6,
        number_of_comments: 12,
        stock: 18,
        location: "-0.127758,51.507351", // London
        created_at: 1698000000,
    },
    Product {
        id: 6,
        title: "Running Shoes Pro",
        description: "Lightweight running shoes with breathable mesh",
        category: "sports",
        brand: "Nike",
        price: 129.0,
        rating: 4.8,
        number_of_comments: 170,
        stock: 60,
        location: "139.691711,35.689487", // Tokyo
        created_at: 1701000000,
    },
    Product {
        id: 7,
        title: "Yoga Mat Eco Friendly",
        description: "Non-slip yoga mat made from recycled materials",
        category: "sports",
        brand: "Adidas",
        price: 49.0,
        rating: 4.2,
        number_of_comments: 3,
        stock: 100,
        location: "151.209900,-33.865143", // Sydney
        created_at: 1701200000,
    },
    Product {
        id: 8,
        title: "Smartphone 128GB 5G",
        description: "Latest generation smartphone with 5G connectivity",
        category: "electronics",
        brand: "Apple",
        price: 999.0,
        rating: 4.9,
        number_of_comments: 212,
        stock: 20,
        location: "-122.419418,37.774929", // San Francisco
        created_at: 1702000000,
    },
    Product {
        id: 9,
        title: "Bluetooth Portable Speaker",
        description: "Waterproof portable speaker with deep bass",
        category: "electronics",
        brand: "JBL",
        price: 89.0,
        rating: 4.4,
        number_of_comments: 15,
        stock: 70,
        location: "13.405000,52.520008", // Berlin
        created_at: 1701500000,
    },
    Product {
        id: 10,
        title: "Mountain Bike 21 Speed",
        description: "Durable mountain bike with aluminum frame",
        category: "sports",
        brand: "Trek",
        price: 599.0,
        rating: 4.6,
        number_of_comments: 23,
        stock: 8,
        location: "144.963058,-37.813629", // Melbourne
        created_at: 1697000000,
    },
];

const PRODUCTS_COUNT: usize = PRODUCTS.len();

fn setup_products(con: &mut redis::Connection, data_type: IndexDataType) {
    for product in &PRODUCTS {
        match data_type {
            IndexDataType::Hash => {
                let _: () = con
                    .hset_multiple(
                        format!("product:{}", product.id),
                        &[
                            ("title", product.title),
                            ("description", product.description),
                            ("category", product.category),
                            ("brand", product.brand),
                            ("price", &product.price.to_string()),
                            ("rating", &product.rating.to_string()),
                            (
                                "number_of_comments",
                                &product.number_of_comments.to_string(),
                            ),
                            ("stock", &product.stock.to_string()),
                            ("location", product.location),
                            ("created_at", &product.created_at.to_string()),
                        ],
                    )
                    .unwrap();
            }
            IndexDataType::Json => {
                use redis::JsonCommands;
                let json_data = json!({
                    "title": product.title,
                    "description": product.description,
                    "category": product.category,
                    "brand": product.brand,
                    "price": product.price,
                    "rating": product.rating,
                    "number_of_comments": product.number_of_comments,
                    "stock": product.stock,
                    "location": product.location,
                    "created_at": product.created_at,
                });
                let _: bool = con
                    .json_set(format!("product:{}", product.id), "$", &json_data)
                    .unwrap();
            }
            _ => panic!("Unsupported data type: {:?}", data_type),
        }
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
    // It is important to create the index first and then insert the data.
    create_products_index(&mut con, data_type);
    setup_products(&mut con, data_type);
    // Wait for the data to be indexed.
    // TODO: This can be changed to use FT.INFO to check the index status.
    std::thread::sleep(std::time::Duration::from_millis(500));
    con
}

/// Extract the total number of results from FT.SEARCH result
fn extract_search_count(result: &Value) -> usize {
    match result {
        // RESP2 format: Array with the first element being the total number of results, followed by document IDs and their field-value pairs as arrays.
        Value::Array(arr) if !arr.is_empty() => {
            if let Value::Int(count) = arr[0] {
                count as usize
            } else {
                panic!(
                    "Expected first element to be the total number of results, got {:?}",
                    arr[0]
                );
            }
        }
        // RESP3 format: Map with "total_results" key
        Value::Map(map) => {
            let count_value = map.find("total_results");
            if let Value::Int(count) = count_value {
                *count as usize
            } else {
                panic!(
                    "extract_search_count: Expected 'total_results' to be Int, got {:?}",
                    count_value
                );
            }
        }
        _ => panic!("Unexpected result format: {:?}", result),
    }
}

/// Extract the actual number of results returned in the FT.SEARCH result
/// This is different from extract_search_count which returns the total number of matching documents
///
/// # Returns
/// The number of results actually returned for RESP3, or None for RESP2
fn extract_returned_results_count(result: &Value) -> Option<usize> {
    match result {
        // RESP2 format: Return early without validation
        Value::Array(_) => None,

        // RESP3 format: Map with "results" array
        Value::Map(map) => {
            let results_value = map.find("results");
            if let Value::Array(results) = results_value {
                Some(results.len())
            } else {
                panic!(
                    "extract_returned_results_count: Expected 'results' to be Array, got {:?}",
                    results_value
                );
            }
        }
        _ => panic!("Unexpected result format: {:?}", result),
    }
}

/// Extract the document ID from a specific document in FT.SEARCH results
///
/// # Arguments
/// * `result` - The FT.SEARCH result Value
/// * `doc_index` - The index of the document (0-based)
///
/// # Returns
/// The document ID as a String for RESP3, or None for RESP2
///
/// # Panics
/// Panics if:
/// - The document index is out of bounds
/// - The RESP3 result format is unexpected
/// - The document ID is not found in RESP3 format
fn extract_document_id(result: &Value, doc_index: usize) -> Option<String> {
    match result {
        // RESP2 format: Return early without validation
        Value::Array(_) => None,

        // RESP3 format: Map with "results" array containing document maps
        Value::Map(map) => {
            let results_value = map.find("results");

            if let Value::Array(results) = results_value {
                if doc_index >= results.len() {
                    panic!(
                        "Document index {} out of bounds (total documents: {})",
                        doc_index,
                        results.len()
                    );
                }

                if let Value::Map(doc_map) = &results[doc_index] {
                    // The document ID is stored at the top level of the document map
                    let id_value = doc_map.find("id");
                    match id_value {
                        Value::BulkString(s) => Some(String::from_utf8_lossy(s).to_string()),
                        Value::SimpleString(s) => Some(s.clone()),
                        _ => panic!("Expected document ID to be a string, got {:?}", id_value),
                    }
                } else {
                    panic!(
                        "Expected document at index {} to be a Map, got {:?}",
                        doc_index, results[doc_index]
                    );
                }
            } else {
                panic!(
                    "extract_document_id: Expected 'results' to be Array, got {:?}",
                    results_value
                );
            }
        }
        _ => panic!("Unexpected result format: {:?}", result),
    }
}

/// Extract a field value from a specific document in FT.SEARCH results
///
/// # Arguments
/// * `result` - The FT.SEARCH result Value
/// * `doc_index` - The index of the document (0-based)
/// * `field_name` - The name of the field to extract
///
/// # Returns
/// The field value as a String, or None if:
/// - The result is in RESP2 format (returns early without validation)
/// - The field doesn't exist in RESP3 format
///
/// # Panics
/// Panics if the document index is out of bounds or the RESP3 result format is unexpected
fn extract_document_field(result: &Value, doc_index: usize, field_name: &str) -> Option<String> {
    match result {
        // RESP2 format: Return early without validation
        Value::Array(_) => None,

        // RESP3 format: Map with "results" array containing document maps
        Value::Map(map) => {
            let results_value = map.find("results");

            if let Value::Array(results) = results_value {
                if doc_index >= results.len() {
                    panic!(
                        "Document index {} out of bounds (total documents: {})",
                        doc_index,
                        results.len()
                    );
                }

                if let Value::Map(doc_map) = &results[doc_index] {
                    // Check in extra_attributes map for fields
                    if let Some(Value::Map(attrs)) = doc_map.find_opt("extra_attributes") {
                        if let Some(field_value) = attrs.find_opt(field_name) {
                            return match field_value {
                                Value::BulkString(s) => {
                                    Some(String::from_utf8_lossy(s).to_string())
                                }
                                Value::SimpleString(s) => Some(s.clone()),
                                Value::Int(i) => Some(i.to_string()),
                                Value::Double(d) => Some(d.to_string()),
                                _ => None,
                            };
                        }
                    }
                    None
                } else {
                    panic!(
                        "Expected document at index {} to be a Map, got {:?}",
                        doc_index, results[doc_index]
                    );
                }
            } else {
                panic!(
                    "extract_document_field: Expected 'results' to be Array, got {:?}",
                    results_value
                );
            }
        }
        _ => panic!("Unexpected result format: {:?}", result),
    }
}

/// Verify that FT.SEARCH result with NOCONTENT has no document content
fn verify_no_content(result: &Value) {
    // RESP2: Skip validation
    if matches!(result, Value::Array(_)) {
        return;
    }

    // RESP3: Verify extra_attributes is absent or empty
    let Value::Map(map) = result else {
        panic!("Expected Map for RESP3, got {:?}", result);
    };

    let results = map.find("results");
    let Value::Array(results) = results else {
        panic!("Expected 'results' to be Array, got {:?}", results);
    };

    for (i, result_item) in results.iter().enumerate() {
        let Value::Map(result_map) = result_item else {
            panic!("Expected result {} to be a Map, got {:?}", i, result_item);
        };

        if let Some(Value::Map(extra_attributes)) = result_map.find_opt("extra_attributes") {
            assert!(
                extra_attributes.is_empty(),
                "Expected 'extra_attributes' to be empty for result {}, got {:?}",
                i,
                extra_attributes
            );
        }
    }
}

/// Verify that FT.SEARCH result has a specific field in each document
fn verify_field_exists(result: &Value, field_name: &str) {
    // RESP2: Skip validation
    if matches!(result, Value::Array(_)) {
        return;
    }

    // RESP3: Verify each result has the specified field
    let Value::Map(map) = result else {
        panic!("Expected Map for RESP3, got {:?}", result);
    };

    let results = map.find("results");
    let Value::Array(results) = results else {
        panic!("Expected 'results' to be Array, got {:?}", results);
    };

    for (i, result_item) in results.iter().enumerate() {
        let Value::Map(result_map) = result_item else {
            panic!("Expected result {} to be a Map, got {:?}", i, result_item);
        };

        // Verify the field exists - find() will panic if it doesn't
        let _field_value = result_map.find(field_name);
    }
}

/// Verify that FT.SEARCH result with WITHSCORES has "score" field
fn verify_with_scores(result: &Value) {
    verify_field_exists(result, "score");
}

/// Verify that FT.SEARCH result with WITHSORTKEYS has "sortkey" field
fn verify_sortkey(result: &Value) {
    verify_field_exists(result, "sortkey");
}

/// Verify that all document IDs in the result are contained in the expected list
///
/// # Arguments
/// * `result` - The FT.SEARCH result Value
/// * `expected_ids` - Array of expected document IDs
/// * `count` - Optional count of documents. If None, extract_search_count will be called
fn verify_document_ids_contain(result: &Value, expected_ids: &[&str], count: Option<usize>) {
    // RESP2: Skip validation
    if matches!(result, Value::Array(_)) {
        return;
    }

    let count = count.unwrap_or_else(|| extract_search_count(result));
    for i in 0..count {
        if let Some(id) = extract_document_id(result, i) {
            assert!(
                expected_ids.contains(&id.as_str()),
                "Expected document ID '{}' to be in the expected list {:?}",
                id,
                expected_ids
            );
        }
    }
}

/// Verify that FT.SEARCH result contains a warning with the given string
///
/// # Arguments
/// * `result` - The FT.SEARCH result Value
/// * `expected_warning` - The expected warning string (case-insensitive comparison)
///
/// # Panics
/// Panics if:
/// - The warning field is not found in RESP3 format
/// - The warning doesn't contain the expected string
fn verify_warning_presence(result: &Value, expected_warning: &str) {
    // RESP2: Skip validation
    if matches!(result, Value::Array(_)) {
        return;
    }

    // RESP3: Extract and verify warning
    let Value::Map(map) = result else {
        panic!("Expected Map for RESP3, got {:?}", result);
    };

    let warning_value = map.find_opt("warning");
    let warning_value = match warning_value {
        Some(val) => val,
        None => panic!("Expected 'warning' field to be present in the result"),
    };

    // Extract warning string(s) - warnings can be a single string or an array of strings
    let warning_strings: Vec<String> = match warning_value {
        Value::BulkString(s) => vec![String::from_utf8_lossy(s).to_string()],
        Value::SimpleString(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| match v {
                Value::BulkString(s) => Some(String::from_utf8_lossy(s).to_string()),
                Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        other => panic!("Expected warning to be a string or array, got {:?}", other),
    };

    // Check if any warning contains the expected string (case-insensitive)
    let expected_lower = expected_warning.to_lowercase();
    let found = warning_strings
        .iter()
        .any(|w| w.to_lowercase().contains(&expected_lower));

    assert!(
        found,
        "Expected warning to contain '{}', but got: {:?}",
        expected_warning, warning_strings
    );
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_basic_ft_search(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_basic_ft_search - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: Value = con.ft_search(INDEX_NAME, "*").unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, PRODUCTS_COUNT,
        "Expected all products to be returned in the search results."
    );
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_nocontent(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_nocontent - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: Value = con
        .ft_search_options(INDEX_NAME, "*", &SearchOptions::new().nocontent())
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, PRODUCTS_COUNT,
        "Expected all products to be returned in the search results."
    );
    verify_no_content(&result);
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_verbatim(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_verbatim - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result = con
        .ft_search_options(INDEX_NAME, "running", &SearchOptions::new().verbatim())
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 1, "Expected 1 product in search results.");
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, "product:6",
            "Expected product:6 (Running Shoes Pro) to be returned."
        );
    }

    let result = con
        .ft_search_options(INDEX_NAME, "run", &SearchOptions::new().verbatim())
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 0, "Expected no products to be returned.");
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_withscores(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_withscores - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: Value = con
        .ft_search_options(INDEX_NAME, SMART, &SearchOptions::new().withscores())
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results.");
    verify_with_scores(&result);
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_withsortkeys(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_withsortkeys - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: Value = con
        .ft_search_options(INDEX_NAME, SMART, &SearchOptions::new().withsortkeys())
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results.");
    verify_sortkey(&result);
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_numeric_filters(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_numeric_filters - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    const PRICE: &str = "price";
    // Searching from -inf to +inf should return all products
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Unbounded,
                Bound::Unbounded,
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, PRODUCTS_COUNT,
        "Expected all products to be returned from the search."
    );

    let cheapest_product = PRODUCTS
        .iter()
        .min_by(|a, b| a.price.total_cmp(&b.price))
        .unwrap();
    let cheapest_product_id_formatted = format!("product:{}", cheapest_product.id);
    let most_expensive_product = PRODUCTS
        .iter()
        .max_by(|a, b| a.price.total_cmp(&b.price))
        .unwrap();
    let most_expensive_product_id_formatted = format!("product:{}", most_expensive_product.id);

    // Search for products within the range of the cheapest and most expensive products should return all products
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Included(cheapest_product.price),
                Bound::Included(most_expensive_product.price),
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, PRODUCTS_COUNT,
        "Expected all products to be returned from the search."
    );

    // Searching for products within a range should return the correct number of products
    // Exclude the cheapest and most expensive products
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Excluded(cheapest_product.price),
                Bound::Excluded(most_expensive_product.price),
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count,
        PRODUCTS_COUNT - 2,
        "Expected all but the cheapest and most expensive products to be returned from the search."
    );
    for i in 0..count {
        if let Some(id) = extract_document_id(&result, i) {
            assert_ne!(
                id, cheapest_product_id_formatted,
                "Expected the cheapest product to be excluded from the search.",
            );
            assert_ne!(
                id, most_expensive_product_id_formatted,
                "Expected the most expensive product to be excluded from the search.",
            );
        }
    }

    // Exclude the cheapest product
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Excluded(cheapest_product.price),
                Bound::Unbounded,
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count,
        PRODUCTS_COUNT - 1,
        "Expected all but the cheapest product to be returned from the search."
    );
    for i in 0..count {
        if let Some(id) = extract_document_id(&result, i) {
            assert_ne!(
                id, cheapest_product_id_formatted,
                "Expected the cheapest product to be excluded from the search.",
            );
        }
    }

    // Exclude the most expensive product
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Unbounded,
                Bound::Excluded(most_expensive_product.price),
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count,
        PRODUCTS_COUNT - 1,
        "Expected all but the most expensive product to be returned from the search."
    );
    for i in 0..count {
        if let Some(id) = extract_document_id(&result, i) {
            assert_ne!(
                id, most_expensive_product_id_formatted,
                "Expected the most expensive product to be excluded from the search.",
            );
        }
    }

    // Search for a concrete product
    // Only the cheapest product
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Included(cheapest_product.price),
                Bound::Included(cheapest_product.price),
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected only the cheapest product to be returned from the search."
    );
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, cheapest_product_id_formatted,
            "Expected the cheapest product to be returned from the search."
        );
    }
    if let Some(price) = extract_document_field(&result, 0, PRICE) {
        assert_eq!(
            price,
            cheapest_product.price.to_string(),
            "Expected the cheapest product to be returned from the search."
        );
    }
    // Only the most expensive product
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Included(most_expensive_product.price),
                Bound::Included(most_expensive_product.price),
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected only the most expensive product to be returned from the search."
    );
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, most_expensive_product_id_formatted,
            "Expected the most expensive product to be returned from the search."
        );
    }
    if let Some(price) = extract_document_field(&result, 0, PRICE) {
        assert_eq!(
            price,
            most_expensive_product.price.to_string(),
            "Expected the most expensive product to be returned from the search."
        );
    }

    // Searching for products outside of the range should return no products
    // Below the cheapest product
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Unbounded,
                Bound::Excluded(cheapest_product.price),
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 0,
        "Expected no products to be returned from the search."
    );
    // Above the most expensive product
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().filter(NumericFilter::new(
                PRICE,
                Bound::Excluded(most_expensive_product.price),
                Bound::Unbounded,
            )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 0,
        "Expected no products to be returned from the search."
    );

    // Invalid ranges should return an error
    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().filter(NumericFilter::new(
            PRICE,
            Bound::Excluded(most_expensive_product.price),
            Bound::Excluded(cheapest_product.price),
        )),
    );
    assert!(
        result.is_err(),
        "Expected an error to be returned from the search."
    );
    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().filter(NumericFilter::new(
            PRICE,
            Bound::Included(most_expensive_product.price),
            Bound::Included(cheapest_product.price),
        )),
    );
    assert!(
        result.is_err(),
        "Expected an error to be returned from the search."
    );

    // Test with a field that is not a numeric field
    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().filter(NumericFilter::new(
            "title",
            Bound::Unbounded,
            Bound::Unbounded,
        )),
    );
    assert!(
        result.is_err(),
        "Expected an error to be returned from the search."
    );

    // Filter by multiple fields
    // Find all products with rating between 0 and 4.5 from the PRODUCTS array,
    // which are also within the price range of [the cheapest, most expensive) products
    const RATING: &str = "rating";
    let max_rating = 4.5;
    let products_with_rating_up_to_4_5 = PRODUCTS
        .iter()
        .filter(|p| {
            p.rating <= max_rating
                && p.price >= cheapest_product.price
                && p.price < most_expensive_product.price
        })
        .count();

    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new()
                .filter(NumericFilter::new(
                    PRICE,
                    Bound::Included(cheapest_product.price),
                    Bound::Excluded(most_expensive_product.price),
                ))
                .filter(NumericFilter::new(
                    RATING,
                    Bound::Unbounded,
                    Bound::Included(4.5),
                )),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, products_with_rating_up_to_4_5,
        "Expected {} products to be returned from the search.",
        products_with_rating_up_to_4_5
    );
    for i in 0..count {
        if let Some(price) = extract_document_field(&result, i, PRICE) {
            assert!(
                price.parse::<f64>().unwrap() >= cheapest_product.price,
                "Expected the price to be greater than or equal to the cheapest product."
            );
            assert!(
                price.parse::<f64>().unwrap() < most_expensive_product.price,
                "Expected the price to be less than the most expensive product."
            );
        }
        if let Some(rating) = extract_document_field(&result, i, RATING) {
            assert!(
                rating.parse::<f32>().unwrap() <= max_rating,
                "Expected the rating to be less than or equal to {}.",
                max_rating
            );
        }
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_geo_filters(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_geo_filters - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    fn test_geo_search_all_units(
        con: &mut redis::Connection,
        location: (f64, f64),
        radius_km: f64,
        expected_matches: usize,
        expected_ids: &[&str],
    ) {
        let units_of_length = [Unit::Meters, Unit::Kilometers, Unit::Miles, Unit::Feet];

        for unit in units_of_length {
            let radius = match unit {
                Unit::Meters => radius_km * 1000.0,
                Unit::Kilometers => radius_km,
                Unit::Miles => radius_km / 1.60934,
                Unit::Feet => radius_km * 3280.84,
                _ => unreachable!("Unsupported unit"),
            };

            let result = con
                .ft_search_options(
                    INDEX_NAME,
                    "*",
                    &SearchOptions::new().geofilter(GeoFilter::new(
                        "location", location.0, location.1, radius, unit,
                    )),
                )
                .unwrap();
            let count = extract_search_count(&result);
            assert_eq!(
                count, expected_matches,
                "Expected {} products to be returned from the search.",
                expected_matches
            );
            verify_document_ids_contain(&result, &expected_ids, Some(count));
        }
    }

    // Test with valid locations in NYC
    let new_york_city = (-73.935242, 40.730610);
    // Test with 1 km radius - should return 1 product (located in Brooklyn)
    test_geo_search_all_units(&mut con, new_york_city, 1.0, 1, &["product:1"]);
    // Test with 20 km radius - should return 2 products (located in Brooklyn and Queens)
    test_geo_search_all_units(
        &mut con,
        new_york_city,
        20.0,
        2,
        &["product:1", "product:3"],
    );

    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().geofilter(GeoFilter::new(
            "location",
            -200.0, // Invalid longitude
            100.0,  // Invalid latitude
            1.0,
            Unit::Kilometers,
        )),
    );
    assert!(
        result.is_err(),
        "Expected an error to be returned from the search."
    );

    // Test with a field that is not a geo field
    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().geofilter(GeoFilter::new(
            "price",
            -73.935242,
            40.730610,
            1.0,
            Unit::Kilometers,
        )),
    );
    assert!(
        result.is_err(),
        "Expected an error to be returned from the search."
    );
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_inkeys(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_inkeys - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // Verify that INKEYS returns only the specified inkeys, even if there are more matching documents
    // Using only a single inkey
    let result = con
        .ft_search_options(INDEX_NAME, "*", &SearchOptions::new().inkey("product:1"))
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected exactly 1 product to be returned from the search."
    );
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, "product:1",
            "Expected the product to be returned from the search."
        );
    }

    // Using multiple inkeys
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().inkey("product:2").inkey("product:3"),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 2,
        "Expected 2 products to be returned from the search."
    );
    verify_document_ids_contain(&result, &["product:2", "product:3"], Some(count));

    // Verify that non-existent inkeys are ignored if there are other valid inkeys
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new()
                .inkey("non_existent_key")
                .inkey("product:4")
                .inkey("another_non_existent_key"),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected only the valid product to be returned from the search."
    );
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, "product:4",
            "Expected the product to be returned from the search."
        );
    }

    // Verify that non-existent inkeys return no results
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().inkey("non_existent_key"),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 0,
        "Expected no products to be returned from the search."
    );

    // Verify that inkeys require an exact match
    let result = con
        .ft_search_options(INDEX_NAME, "*", &SearchOptions::new().inkey("product:"))
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 0,
        "Expected no products to be returned from the search."
    );
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_infields(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_infields - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // Verify that if no infields are specified, all fields are searched
    let result = con.ft_search(INDEX_NAME, SMART).unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );

    // Verify that if infields are specified, only those fields are searched
    // If all fields are covered, the search is identical to the one above
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().infields(["title", "description"]),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );
    // If only some of the fields are covered, the search returns fewer results
    let result = con
        .ft_search_options(INDEX_NAME, SMART, &SearchOptions::new().infield("title"))
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 2,
        "Expected 2 products to be returned from the search."
    );
    for i in 0..count {
        if let Some(title) = extract_document_field(&result, i, "title") {
            assert!(
                title.to_lowercase().contains(SMART),
                "Expected the title to contain '{}', got: {}",
                SMART,
                title
            );
        }
    }

    // Verify that non-existent infields are ignored if there are other valid infields
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().infields(["non_existent_field", "title", "description"]),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );

    // Verify that non-existent infields return no results
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().infield("non_existent_field"),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 0,
        "Expected no products to be returned from the search."
    );
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_return_fields(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_return_fields - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    const TITLE: &str = "title";
    const PRICE: &str = "price";
    const DESCRIPTION: &str = "description";
    const COST: &str = "cost";
    const NON_EXISTENT_FIELD: &str = "non_existent_field";

    // Verify that return fields work as expected
    // Using a single return field
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().return_field(ReturnField::new(TITLE)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            assert!(
                extract_document_field(&result, i, TITLE).is_some(),
                "Expected the title field to be returned from the search."
            );
        }
    }

    // Using multiple return fields
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().return_fields([
                ReturnField::new(TITLE),
                ReturnField::new(PRICE),
                ReturnField::new(DESCRIPTION),
            ]),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            for field_name in [TITLE, PRICE, DESCRIPTION] {
                assert!(
                    extract_document_field(&result, i, field_name).is_some(),
                    "Expected the {} field to be returned from the search.",
                    field_name
                );
            }
        }
    }

    // Using return fields with aliases
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new()
                .return_fields([ReturnField::new(TITLE), ReturnField::new(PRICE).alias(COST)]),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            assert!(
                extract_document_field(&result, i, TITLE).is_some(),
                "Expected the title field to be returned from the search."
            );
            // Verify that the alias is returned
            assert!(
                extract_document_field(&result, i, COST).is_some(),
                "Expected the price field to be returned from the search."
            );
            // Verify that the original field name is not returned
            assert!(
                extract_document_field(&result, i, PRICE).is_none(),
                "Expected the price field to not be returned from the search."
            );
        }
    }

    // Verify that non-existent return fields are ignored
    let result = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().return_fields([
                ReturnField::new(NON_EXISTENT_FIELD),
                ReturnField::new(TITLE),
            ]),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 products to be returned from the search."
    );
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            assert!(
                extract_document_field(&result, i, TITLE).is_some(),
                "Expected the title field to be returned from the search."
            );
            assert!(
                extract_document_field(&result, i, NON_EXISTENT_FIELD).is_none(),
                "Expected the non_existent_field field to not be returned from the search."
            );
        }
    }
}

const SUMMARIZATION_INDEX_NAME: &str = "summarization_idx";
fn create_index_for_summarization_test(con: &mut redis::Connection) {
    let schema = schema! {
        "txt1" => SchemaTextField::new(),
        "txt2" => SchemaTextField::new(),
    };
    assert_eq!(
        con.ft_create(
            SUMMARIZATION_INDEX_NAME,
            &CreateOptions::new().on(IndexDataType::Hash).prefix("doc:"),
            &schema
        ),
        Ok("OK".to_string())
    );
}

fn setup_data_for_summarization_test(con: &mut redis::Connection) {
    let _: () = con.hset_multiple(
        "doc:1",
        &[
            ("txt1", "Redis is an open-source in-memory database project implementing a networked, in-memory key-value store with optional durability. Redis supports different kinds of abstract data structures, such as strings, lists, maps, sets, sorted sets, hyperloglogs, bitmaps and spatial indexes. The project is mainly developed by Salvatore Sanfilippo and is currently sponsored by Redis Labs.[4] Redis Labs creates and maintains the official Redis Enterprise Pack."),
            ("txt2", "Redis typically holds the whole dataset in memory. Versions up to 2.4 could be configured to use what they refer to as virtual memory[19] in which some of the dataset is stored on disk, but this feature is deprecated. Persistence is now achieved in two different ways: one is called snapshotting, and is a semi-persistent durability mode where the dataset is asynchronously transferred from memory to disk from time to time, written in RDB dump format. Since version 1.1 the safer alternative is AOF, an append-only file (a journal) that is written as operations modifying the dataset in memory are processed. Redis is able to rewrite the append-only file in the background in order to avoid an indefinite growth of the journal.")
        ]
    ).unwrap();
}

#[rstest]
#[case(ProtocolVersion::RESP2)]
#[case(ProtocolVersion::RESP3)]
#[serial]
fn test_ft_search_with_summarize(#[case] protocol: ProtocolVersion) {
    println!(
        "Starting test_ft_search_with_summarize - Protocol: {:?}, DataType: Hash",
        protocol
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = create_connection_with_protocol(&ctx, protocol);
    let _: () = con.flushdb().unwrap();
    create_index_for_summarization_test(&mut con);
    setup_data_for_summarization_test(&mut con);
    // Wait for the data to be indexed.
    // TODO: This can be changed to use FT.INFO to check the index status.
    std::thread::sleep(std::time::Duration::from_millis(500));

    const TXT1: &str = "txt1";
    const TXT2: &str = "txt2";
    const CUSTOM_SEPARATOR: &str = "-|-";
    const SEARCH_TERM: &str = "memory persistence salvatore";
    const SEARCH_TERM_TOKENS: [&str; 3] = ["memory", "persistence", "salvatore"];
    const NUMBER_OF_FRAGMENTS: usize = 3;

    let result: Value = con
        .ft_search_options(
            SUMMARIZATION_INDEX_NAME,
            SEARCH_TERM,
            &SearchOptions::new().summarize(
                SummarizeOptions::new()
                    .fields([TXT1, TXT2])
                    .frags(NUMBER_OF_FRAGMENTS as u32)
                    .len(4)
                    .separator(CUSTOM_SEPARATOR),
            ),
        )
        .unwrap();
    if let Some(text1_summarized) = extract_document_field(&result, 0, TXT1) {
        let text1_summary_tokenized: Vec<&str> = text1_summarized
            .split(CUSTOM_SEPARATOR)
            .filter(|s| !s.is_empty())
            .collect();
        assert!(text1_summary_tokenized.len() <= NUMBER_OF_FRAGMENTS);
        // Verify that each non-empty fragment contains at least one of the search term tokens
        assert!(text1_summary_tokenized.iter().all(|part| {
            SEARCH_TERM_TOKENS
                .iter()
                .any(|token| part.to_lowercase().contains(token))
        }));
    }

    if let Some(text2_summarized) = extract_document_field(&result, 0, TXT2) {
        let text2_summary_tokenized: Vec<&str> = text2_summarized
            .split(CUSTOM_SEPARATOR)
            .filter(|s| !s.is_empty())
            .collect();
        assert!(text2_summary_tokenized.len() <= NUMBER_OF_FRAGMENTS);
        // Verify that each non-empty fragment contains at least one of the search term tokens
        assert!(text2_summary_tokenized.iter().all(|part| {
            SEARCH_TERM_TOKENS
                .iter()
                .any(|token| part.to_lowercase().contains(token))
        }));
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_summarize_is_unsupported_for_json_indices(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_summarize_is_unsupported_for_json_indices - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        SMART,
        &SearchOptions::new().summarize(
            SummarizeOptions::new()
                .field("title")
                .frags(3)
                .len(10)
                .separator("..."),
        ),
    );
    assert!(result.is_err());
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[serial]
fn test_ft_search_with_highlight(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_highlight - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    const SEARCH_TERM: &str = SMART;
    const HIGHLIGHT_FIELDS: &[&str] = &["title", "description"];

    const DEFAULT_OPENING_TAG: &str = "<b>";
    const DEFAULT_CLOSING_TAG: &str = "</b>";
    const CUSTOM_OPENING_TAG: &str = "<start>";
    const CUSTOM_CLOSING_TAG: &str = "</end>";

    // Test 1: Verify that highlight can be specified without fields and tags (uses default <b> tags)
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SEARCH_TERM,
            &SearchOptions::new().highlight(HighlightOptions::new()),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");

    for i in 0..count {
        for field_name in HIGHLIGHT_FIELDS {
            if let Some(field_value) = extract_document_field(&result, i, field_name)
                && field_value.contains(SEARCH_TERM)
            {
                assert!(
                    field_value.contains(DEFAULT_OPENING_TAG)
                        && field_value.contains(DEFAULT_CLOSING_TAG),
                    "Expected field {} to contain default opening {} and closing {} highlight tags, got: {}",
                    field_name,
                    DEFAULT_OPENING_TAG,
                    DEFAULT_CLOSING_TAG,
                    field_value,
                );
            }
        }
    }

    // Test 2: Verify that highlight can be specified with specific fields
    for i in 0..HIGHLIGHT_FIELDS.len() {
        let result: Value = con
            .ft_search_options(
                INDEX_NAME,
                SEARCH_TERM,
                &SearchOptions::new().highlight(HighlightOptions::new().field(HIGHLIGHT_FIELDS[i])),
            )
            .unwrap();

        let count = extract_search_count(&result);
        assert_eq!(count, 3, "Expected 3 products in search results");

        for j in 0..count {
            for field_name in HIGHLIGHT_FIELDS {
                if let Some(field_value) = extract_document_field(&result, j, field_name)
                    && field_value.contains(SEARCH_TERM)
                {
                    if *field_name == HIGHLIGHT_FIELDS[i] {
                        assert!(
                            field_value.contains(DEFAULT_OPENING_TAG)
                                && field_value.contains(DEFAULT_CLOSING_TAG),
                            "Expected field {} to contain default opening {} and closing {} highlight tags, got: {}",
                            field_name,
                            DEFAULT_OPENING_TAG,
                            DEFAULT_CLOSING_TAG,
                            field_value
                        );
                    } else {
                        assert!(
                            !field_value.contains(DEFAULT_OPENING_TAG)
                                && !field_value.contains(DEFAULT_CLOSING_TAG),
                            "Expected field {} to not contain highlight tags, got: {}",
                            field_name,
                            field_value
                        );
                    }
                }
            }
        }
    }

    // Test 3: Verify that highlight can be specified with custom tags
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SEARCH_TERM,
            &SearchOptions::new()
                .highlight(HighlightOptions::new().tags(CUSTOM_OPENING_TAG, CUSTOM_CLOSING_TAG)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    for i in 0..count {
        for field_name in HIGHLIGHT_FIELDS {
            if let Some(field_value) = extract_document_field(&result, i, field_name)
                && field_value.contains(SEARCH_TERM)
            {
                assert!(
                    field_value.contains(CUSTOM_OPENING_TAG)
                        && field_value.contains(CUSTOM_CLOSING_TAG),
                    "Expected field {} to contain custom opening {} and closing {} highlight tags, got: {}",
                    field_name,
                    CUSTOM_OPENING_TAG,
                    CUSTOM_CLOSING_TAG,
                    field_value,
                );
            }
        }
    }

    // Test 4: Verify that highlight can be specified with both fields and custom tags
    for i in 0..HIGHLIGHT_FIELDS.len() {
        let result: Value = con
            .ft_search_options(
                INDEX_NAME,
                SEARCH_TERM,
                &SearchOptions::new().highlight(
                    HighlightOptions::new()
                        .field(HIGHLIGHT_FIELDS[i])
                        .tags(CUSTOM_OPENING_TAG, CUSTOM_CLOSING_TAG),
                ),
            )
            .unwrap();
        let count = extract_search_count(&result);
        assert_eq!(count, 3, "Expected 3 products in search results");
        for j in 0..count {
            for field_name in HIGHLIGHT_FIELDS {
                if let Some(field_value) = extract_document_field(&result, j, field_name)
                    && field_value.contains(SEARCH_TERM)
                {
                    if *field_name == HIGHLIGHT_FIELDS[i] {
                        assert!(
                            field_value.contains(CUSTOM_OPENING_TAG)
                                && field_value.contains(CUSTOM_CLOSING_TAG),
                            "Expected field {} to contain custom opening {} and closing {} highlight tags, got: {}",
                            field_name,
                            CUSTOM_OPENING_TAG,
                            CUSTOM_CLOSING_TAG,
                            field_value,
                        );
                    } else {
                        assert!(
                            !field_value.contains(CUSTOM_OPENING_TAG)
                                && !field_value.contains(CUSTOM_CLOSING_TAG),
                            "Expected field {} to not contain custom opening {} and closing {} highlight tags, got: {}",
                            field_name,
                            CUSTOM_OPENING_TAG,
                            CUSTOM_CLOSING_TAG,
                            field_value,
                        );
                    }
                }
            }
        }
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_highlight_is_unsupported_for_json_indices(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_highlight_is_unsupported_for_json_indices - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().highlight(HighlightOptions::new()),
    );
    assert!(result.is_err());
}

const HELLO_WORLD_INDEX_NAME: &str = "hello_world_idx";

fn create_hello_world_index_for_slop_searches(
    con: &mut redis::Connection,
    data_type: IndexDataType,
) {
    let schema = match data_type {
        IndexDataType::Hash => schema! {
            "phrase" => SchemaTextField::new(),
        },
        IndexDataType::Json => schema! {
            "$.phrase" => SchemaTextField::new().alias("phrase"),
        },
        _ => panic!("Unsupported data type: {:?}", data_type),
    };
    assert_eq!(
        con.ft_create(
            HELLO_WORLD_INDEX_NAME,
            &CreateOptions::new().on(data_type),
            &schema
        ),
        Ok("OK".to_string())
    );
}

fn setup_hello_world_data_for_slop_searches(con: &mut redis::Connection, data_type: IndexDataType) {
    const HELLO_WORLD_PHRASES: &[(&str, &str)] = &[
        ("s1", "hello world"),
        ("s2", "hello simple world"),
        ("s3", "hello somewhat less simple world"),
        (
            "s4",
            "hello complicated yet encouraging problem solving world",
        ),
        (
            "s5",
            "hello complicated yet amazingly encouraging problem solving world",
        ),
    ];
    for (key, phrase) in HELLO_WORLD_PHRASES {
        match data_type {
            IndexDataType::Hash => {
                let _: () = con.hset(key, "phrase", phrase).unwrap();
            }
            IndexDataType::Json => {
                use redis::JsonCommands;
                let json_data = json!({
                    "phrase": phrase
                });
                let _: () = con.json_set(key, "$", &json_data).unwrap();
            }
            _ => panic!("Unsupported data type: {:?}", data_type),
        }
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_slop(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    /*
    This test uses a different index and data from the rest of the tests in this file.
    It is based on the example from the Redis Search documentation:
    https://redis.io/docs/latest/commands/ft.search/
    */
    println!(
        "Starting test_ft_search_with_slop - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = create_connection_with_protocol(&ctx, protocol);
    let _: () = con.flushdb().unwrap();
    // Create the index first
    create_hello_world_index_for_slop_searches(&mut con, data_type);
    // Then insert the data
    setup_hello_world_data_for_slop_searches(&mut con, data_type);
    // Wait for the data to be indexed.
    // TODO: This can be changed to use FT.INFO to check the index status.
    std::thread::sleep(std::time::Duration::from_millis(500));

    const HELLO_WORLD_SEARCH_QUERY: &str = "hello world";
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            HELLO_WORLD_SEARCH_QUERY,
            &SearchOptions::new().nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 5,
        "Expected 5 documents to be returned from the search."
    );

    // Slop 0 should only match the exact phrase.
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            HELLO_WORLD_SEARCH_QUERY,
            &SearchOptions::new().slop(0).nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document to be returned from the search."
    );
    verify_document_ids_contain(&result, &["s1"], Some(count));

    // Slop 1 should match the exact phrase and the one with a single word in between.
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            HELLO_WORLD_SEARCH_QUERY,
            &SearchOptions::new().slop(1).nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 2,
        "Expected 2 documents to be returned from the search."
    );
    verify_document_ids_contain(&result, &["s1", "s2"], Some(count));

    // Slop 3 should match the exact phrase and all documents with three or fewer words between hello and world.
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            HELLO_WORLD_SEARCH_QUERY,
            &SearchOptions::new().slop(3).nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 3,
        "Expected 3 documents to be returned from the search."
    );
    verify_document_ids_contain(&result, &["s1", "s2", "s3"], Some(count));

    // "s5" needs a SLOP of 6 or higher, but all other documents should be returned.
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            HELLO_WORLD_SEARCH_QUERY,
            &SearchOptions::new().slop(5).nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 4,
        "Expected 4 documents to be returned from the search."
    );
    verify_document_ids_contain(&result, &["s1", "s2", "s3", "s4"], Some(count));

    // The order of the words within the search query does not matter by default
    let result1 = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            "hello amazing world",
            &SearchOptions::new().nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result1);
    assert_eq!(
        count, 1,
        "Expected 1 document to be returned from the search."
    );

    let result2 = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            "amazing hello world",
            &SearchOptions::new().nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result2);
    assert_eq!(
        count, 1,
        "Expected 1 document to be returned from the search."
    );

    assert_eq!(result1, result2);

    // Stemming is applied by default, so words like "encouraging" and "encouraged" will match because they share the same stem.
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            "hello encouraged world",
            &SearchOptions::new().slop(4).nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document to be returned from the search."
    );
    let result = con
        .ft_search_options(
            HELLO_WORLD_INDEX_NAME,
            "hello encouraged world",
            &SearchOptions::new().slop(5).nocontent(),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, 2,
        "Expected 2 documents to be returned from the search."
    );
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_in_order(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_in_order - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // Verify that inorder requires an exact match of the order of the terms in the search query
    // IMPORTANT: Slop is required with inorder!
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "smartphone 5G",
            &SearchOptions::new().inorder().nocontent().slop(1),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document to be returned from the search."
    );
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, "product:8",
            "Expected the product to be returned from the search."
        );
    }

    let result = con
        .ft_search_options(
            INDEX_NAME,
            "5G smartphone",
            &SearchOptions::new().inorder().nocontent().slop(1),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 0,
        "Expected no documents to be returned from the search."
    );
}

const LANGUAGE_INDEX_NAME: &str = "language_idx";

fn create_index_for_language_searches(con: &mut redis::Connection, data_type: IndexDataType) {
    let schema = match data_type {
        IndexDataType::Hash => schema! {
            "title" => SchemaTextField::new(),
        },
        IndexDataType::Json => schema! {
            "$.title" => SchemaTextField::new().alias("title"),
        },
        _ => panic!("Unsupported data type: {:?}", data_type),
    };
    assert_eq!(
        con.ft_create(
            LANGUAGE_INDEX_NAME,
            &CreateOptions::new()
                .on(data_type)
                .prefix("doc:")
                .language_field(match data_type {
                    IndexDataType::Hash => "lang",
                    IndexDataType::Json => "$.lang",
                    _ => panic!("Unsupported data type: {:?}", data_type),
                }),
            &schema
        ),
        Ok("OK".to_string())
    );
}

fn setup_data_for_language_dependent_searches(
    con: &mut redis::Connection,
    data_type: IndexDataType,
) {
    const LANGUAGE_DOCUMENTS: &[(&str, &str, &str)] = &[
        ("doc:1", "run fast", "english"),
        ("doc:2", "running in the park", "english"),
        ("doc:3", "correr rapido", "spanish"),
        ("doc:4", "corriendo en el parque", "spanish"),
    ];
    for (key, title, lang) in LANGUAGE_DOCUMENTS {
        match data_type {
            IndexDataType::Hash => {
                let _: () = con
                    .hset_multiple(key, &[("title", title), ("lang", lang)])
                    .unwrap();
            }
            IndexDataType::Json => {
                use redis::JsonCommands;
                let json_data = json!({
                    "title": title,
                    "lang": lang
                });
                let _: () = con.json_set(key, "$", &json_data).unwrap();
            }
            _ => panic!("Unsupported data type: {:?}", data_type),
        }
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_language(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_language - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = create_connection_with_protocol(&ctx, protocol);
    let _: () = con.flushdb().unwrap();
    create_index_for_language_searches(&mut con, data_type);
    setup_data_for_language_dependent_searches(&mut con, data_type);
    // Wait for the data to be indexed.
    // TODO: This can be changed to use FT.INFO to check the index status.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Test 1: English stemming - searching "running" should find "run"
    // With English language, "run" and "running" share the same stem
    let result: Value = con
        .ft_search_options(
            LANGUAGE_INDEX_NAME,
            "running",
            &SearchOptions::new().language(SearchLanguage::English),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 2,
        "Expected 2 documents with English stemming (doc:1 'run fast' and doc:2 'running in the park')"
    );
    verify_document_ids_contain(&result, &["doc:1", "doc:2"], Some(count));

    // Test 2: Spanish stemming - searching "correr" should find "corriendo"
    // With Spanish language, "correr" and "corriendo" share the same stem
    let result: Value = con
        .ft_search_options(
            LANGUAGE_INDEX_NAME,
            "correr",
            &SearchOptions::new().language(SearchLanguage::Spanish),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 2,
        "Expected 2 documents with Spanish stemming (doc:3 'correr rapido' and doc:4 'corriendo en el parque')"
    );
    verify_document_ids_contain(&result, &["doc:3", "doc:4"], Some(count));

    // Test 3: Verbatim mode disables stemming
    // Searching "run" with VERBATIM should only find exact match
    let result: Value = con
        .ft_search_options(
            LANGUAGE_INDEX_NAME,
            "run",
            &SearchOptions::new()
                .language(SearchLanguage::English)
                .verbatim(),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document with VERBATIM (only doc:1 'run fast', not 'running')"
    );
    verify_document_ids_contain(&result, &["doc:1"], Some(count));

    // Test 4: Wrong language doesn't apply correct stemming
    // Searching "running" with Spanish language shouldn't find "run"
    // because Spanish stemmer doesn't know English word stems
    let result: Value = con
        .ft_search_options(
            LANGUAGE_INDEX_NAME,
            "running",
            &SearchOptions::new().language(SearchLanguage::Spanish),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document when using wrong language (only exact match 'running')"
    );
    verify_document_ids_contain(&result, &["doc:2"], Some(count));

    // Test 5: Search for "park" should find both English documents
    let result: Value = con
        .ft_search_options(
            LANGUAGE_INDEX_NAME,
            "park",
            &SearchOptions::new().language(SearchLanguage::English),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document containing 'park' (doc:2 'running in the park')"
    );
    verify_document_ids_contain(&result, &["doc:2"], Some(count));

    // Test 6: Search for "parque" should find Spanish document
    let result: Value = con
        .ft_search_options(
            LANGUAGE_INDEX_NAME,
            "parque",
            &SearchOptions::new().language(SearchLanguage::Spanish),
        )
        .unwrap();

    let count = extract_search_count(&result);
    assert_eq!(
        count, 1,
        "Expected 1 document containing 'parque' (doc:4 'corriendo en el parque')"
    );
    verify_document_ids_contain(&result, &["doc:4"], Some(count));
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_custom_expander_and_scorer(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_custom_expander_and_scorer - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // This test just shows that if the expander doesn't exist, the search will be performed without it.
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new().expander("non_existent_expander"),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(
        count, PRODUCTS_COUNT,
        "Expected {} products in search results",
        PRODUCTS_COUNT
    );
    // However, if the scorer doesn't exist, an error is returned.
    let result: RedisResult<Value> = con.ft_search_options(
        INDEX_NAME,
        "*",
        &SearchOptions::new().scorer("non_existent_scorer"),
    );
    assert!(result.is_err());
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_scoring_function(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_scoring_function - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    // Test all scoring functions against the same index setup
    let scoring_functions = [
        ScoringFunction::Tfidf,
        ScoringFunction::TfidfDocnorm,
        ScoringFunction::Bm25Std,
        ScoringFunction::Bm25StdNorm,
        ScoringFunction::Bm25StdTanh { factor: None },
        ScoringFunction::Bm25StdTanh { factor: Some(12) },
        ScoringFunction::Dismax,
        ScoringFunction::Docscore,
        ScoringFunction::Hamming,
    ];

    for scoring_function in scoring_functions {
        println!("  Testing scoring function: {:?}", scoring_function);

        let result: Value = con
            .ft_search_options(
                INDEX_NAME,
                SMART,
                &SearchOptions::new()
                    .withscores()
                    .scoring_function(scoring_function),
            )
            .unwrap();

        let count = extract_search_count(&result);
        assert_eq!(count, 3, "Expected 3 products for {:?}", scoring_function);
        verify_with_scores(&result);
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_explainscore(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_explainscore - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: Value = con
        .ft_search_options(INDEX_NAME, SMART, &SearchOptions::new().explainscore())
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    verify_with_scores(&result);
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_sortby(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_with_sortby - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let mut prices: Vec<f64> = Vec::new();

    // Verify that sortby sorts in ascending order by default
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().sortby("price", None),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            if let Some(price) = extract_document_field(&result, i, "price") {
                prices.push(price.parse::<f64>().unwrap());
            }
        }
        // Check that all of the collected prices were pushed in an ascending order
        if !prices.is_empty() {
            for i in 0..prices.len() - 1 {
                assert!(
                    prices[i] <= prices[i + 1],
                    "Expected prices to be in ascending order"
                );
            }
        }
    }

    // Verify that sortby with ASC returns the same result as sortby with no direction
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().sortby("price", Some(SortDirection::Asc)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            if let Some(price) = extract_document_field(&result, i, "price") {
                assert_eq!(
                    price,
                    prices[i].to_string(),
                    "Expected prices to be the same when sorted in ascending order"
                );
            }
        }
    }

    prices.clear();

    // Verify sortby with DESC direction
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().sortby("price", Some(SortDirection::Desc)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            if let Some(price) = extract_document_field(&result, i, "price") {
                prices.push(price.parse::<f64>().unwrap());
            }
        }
        // Check that all of the collected prices were pushed in a descending order
        if !prices.is_empty() {
            for i in 0..prices.len() - 1 {
                assert!(
                    prices[i] >= prices[i + 1],
                    "Expected prices to be in descending order"
                );
            }
        }
    }

    let mut titles: Vec<String> = Vec::new();
    // Verify that text fields can also be used for sorting
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().sortby("title", None),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            if let Some(title) = extract_document_field(&result, i, "title") {
                titles.push(title);
            }
        }
        assert!(
            titles.is_sorted(),
            "Expected titles to be in ascending order"
        );
    }
    titles.clear();

    // Verify that sortby with DESC direction works for text fields as well
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().sortby("title", Some(SortDirection::Desc)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            if let Some(title) = extract_document_field(&result, i, "title") {
                titles.push(title);
            }
        }
        assert!(
            titles.is_sorted_by(|a, b| a >= b),
            "Expected titles to be in descending order"
        );
    }

    // Verify that sortby can be applied to fields that were not declared as sortable
    let mut number_of_comments: Vec<u16> = Vec::new();
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new().sortby("number_of_comments", Some(SortDirection::Desc)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");
    if protocol == ProtocolVersion::RESP3 {
        for i in 0..count {
            if let Some(num_of_comments) = extract_document_field(&result, i, "number_of_comments")
            {
                number_of_comments.push(num_of_comments.parse::<u16>().unwrap());
            }
        }
        assert!(
            number_of_comments.is_sorted_by(|a, b| a >= b),
            "Expected number_of_comments to be in descending order"
        );
    }
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_limit(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_with_limit - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result = con.ft_search(INDEX_NAME, SMART).unwrap();
    let total_count = extract_search_count(&result);
    assert_eq!(total_count, 3, "Expected 3 products in search results");

    let mut prices: Vec<f64> = Vec::new();
    for i in 0..total_count {
        let result: Value = con
            .ft_search_options(
                INDEX_NAME,
                SMART,
                &SearchOptions::new()
                    .limit((i, 1))
                    .sortby("price", Some(SortDirection::Desc)),
            )
            .unwrap();

        // Verify that total_results is still 3 (total matching documents)
        let total_results = extract_search_count(&result);
        assert_eq!(
            total_results, 3,
            "Expected total_results to be 3 (total matching documents)"
        );

        // Verify that only 1 result is actually returned in the response (RESP3 only)
        if let Some(returned_count) = extract_returned_results_count(&result) {
            assert_eq!(
                returned_count, 1,
                "Expected 1 result to be returned in the response"
            );
        }

        // Extract the price of each product into a collection
        if let Some(price) = extract_document_field(&result, 0, "price") {
            prices.push(price.parse::<f64>().unwrap());
        }
    }

    // Check that all of the collected prices were pushed in a descending order
    if !prices.is_empty() {
        for i in 0..prices.len() - 1 {
            assert!(
                prices[i] >= prices[i + 1],
                "Expected prices to be in descending order"
            );
        }
    }

    let cheapest_product = PRODUCTS
        .iter()
        .min_by(|a, b| a.price.total_cmp(&b.price))
        .unwrap();
    let cheapest_product_id_formatted = format!("product:{}", cheapest_product.id);
    let most_expensive_product = PRODUCTS
        .iter()
        .max_by(|a, b| a.price.total_cmp(&b.price))
        .unwrap();
    let most_expensive_product_id_formatted = format!("product:{}", most_expensive_product.id);

    // Find the most expensive product in the collection
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new()
                .sortby("price", Some(SortDirection::Desc))
                .limit((0, 1)),
        )
        .unwrap();
    let total_results = extract_search_count(&result);
    assert_eq!(
        total_results, PRODUCTS_COUNT,
        "Expected total_results to be {} (total matching documents)",
        PRODUCTS_COUNT
    );
    if let Some(returned_count) = extract_returned_results_count(&result) {
        assert_eq!(
            returned_count, 1,
            "Expected 1 result to be returned in the response"
        );
    }
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, most_expensive_product_id_formatted,
            "Expected the most expensive product to be returned from the search."
        );
    }

    // Find the cheapest product in the collection
    let result = con
        .ft_search_options(
            INDEX_NAME,
            "*",
            &SearchOptions::new()
                .sortby("price", Some(SortDirection::Asc))
                .limit((0, 1)),
        )
        .unwrap();
    let total_results = extract_search_count(&result);
    assert_eq!(
        total_results, PRODUCTS_COUNT,
        "Expected total_results to be {} (total matching documents)",
        PRODUCTS_COUNT
    );
    if let Some(returned_count) = extract_returned_results_count(&result) {
        assert_eq!(
            returned_count, 1,
            "Expected 1 result to be returned in the response"
        );
    }
    if let Some(id) = extract_document_id(&result, 0) {
        assert_eq!(
            id, cheapest_product_id_formatted,
            "Expected the cheapest product to be returned from the search."
        );
    }

    // Test that limit 0 0 behaves like NOCONTENT
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new()
                .limit((0, 0))
                .sortby("price", Some(SortDirection::Desc)),
        )
        .unwrap();
    let total_results = extract_search_count(&result);
    assert_eq!(
        total_results, 3,
        "Expected total_results to be 3 (total matching documents)"
    );
    verify_no_content(&result);
}

/// Generate a large dataset of products
fn setup_large_products(con: &mut redis::Connection, data_type: IndexDataType, count: usize) {
    const TITLES: &[&str] = &[
        "Wireless Headphones",
        "Gaming Keyboard",
        "Smart TV",
        "Smartphone",
        "Bluetooth Speaker",
        "Laptop",
        "Tablet",
        "Smartwatch",
        "Camera",
    ];
    const DESCRIPTIONS: &[&str] = &[
        "Premium quality product",
        "High performance device",
        "Latest technology",
        "Durable and reliable",
        "Energy efficient",
        "Eco-friendly design",
    ];

    const LOCATIONS: &[&str] = &[
        "-73.935242,40.730610",
        "-118.243683,34.052235",
        "-87.623177,41.881832",
        "139.691711,35.689487",
        "2.352222,48.856613",
        "-0.127758,51.507351",
    ];

    for i in 0..count {
        let id = i + 1;
        let title = TITLES[i % TITLES.len()];
        let description = DESCRIPTIONS[i % DESCRIPTIONS.len()];
        let price = 50.0 + (i % 1000) as f64;
        let location = LOCATIONS[i % LOCATIONS.len()];
        let created_at = 1700000000 + (i as i64 * 1000);

        match data_type {
            IndexDataType::Hash => {
                let _: () = con
                    .hset_multiple(
                        format!("product:{}", id),
                        &[
                            ("title", title),
                            ("description", description),
                            ("price", &price.to_string()),
                            ("location", location),
                            ("created_at", &created_at.to_string()),
                        ],
                    )
                    .unwrap();
            }
            IndexDataType::Json => {
                use redis::JsonCommands;
                let json_data = json!({
                    "title": title,
                    "description": description,
                    "price": price,
                    "location": location,
                    "created_at": created_at,
                });
                let _: bool = con
                    .json_set(format!("product:{}", id), "$", &json_data)
                    .unwrap();
            }
            _ => panic!("Unsupported data type: {:?}", data_type),
        }
    }
}

// This test is only for RESP3, as it checks for warnings in the result.
#[rstest]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_timeout(
    #[case] protocol: ProtocolVersion,
    #[case] data_type: IndexDataType,
) {
    println!(
        "Starting test_ft_search_with_timeout - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = create_connection_with_protocol(&ctx, protocol);
    let _: () = con.flushdb().unwrap();
    create_products_index(&mut con, data_type);
    setup_large_products(&mut con, data_type, 10_000);

    // Let the data be indexed.
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Enforce a timeout of 1ms and make the query slower by applying some extra parameters.
    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            SMART,
            &SearchOptions::new()
                .timeout(1)
                .withsortkeys()
                .explainscore(),
        )
        .unwrap();
    verify_with_scores(&result);
    verify_sortkey(&result);
    verify_warning_presence(&result, "Timeout");
}

#[rstest]
#[case(ProtocolVersion::RESP2, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP2, IndexDataType::Json)]
#[case(ProtocolVersion::RESP3, IndexDataType::Hash)]
#[case(ProtocolVersion::RESP3, IndexDataType::Json)]
#[serial]
fn test_ft_search_with_params(#[case] protocol: ProtocolVersion, #[case] data_type: IndexDataType) {
    println!(
        "Starting test_ft_search_with_params - Protocol: {:?}, DataType: {:?}",
        protocol, data_type
    );

    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
    let mut con = setup_test_env(&ctx, protocol, data_type);

    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            "$term",
            &SearchOptions::new().param(QueryParam::new("term", SMART)),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 3, "Expected 3 products in search results");

    let result: Value = con
        .ft_search_options(
            INDEX_NAME,
            "@title:$term and @price:[$min $max]",
            &SearchOptions::new().params([("term", "smartphone"), ("min", "1"), ("max", "1000")]),
        )
        .unwrap();
    let count = extract_search_count(&result);
    assert_eq!(count, 1, "Expected 1 product in search results");
}
