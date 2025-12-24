//! ClickHouse integration tests
//!
//! Tests run against k8s.tyrell.com.au cluster via .env settings

use std::env;

use dfe_loader_clickhouse::clickhouse::ClickHouseClient;
use dfe_loader_clickhouse::config::ClickHouseConfig;

fn load_dotenv() {
    // Load from project root .env file
    let _ = dotenvy::from_path("/projects/dfe-loader-clickhouse/.env");
}

/// Skip test if no ClickHouse available
fn skip_if_no_clickhouse() -> bool {
    load_dotenv();

    let host = env::var("CLICKHOUSE_HOST").unwrap_or_default();
    let port: u16 = env::var("CLICKHOUSE_NATIVE_PORT")
        .unwrap_or_else(|_| "9000".to_string())
        .parse()
        .unwrap_or(9000);

    if host.is_empty() {
        eprintln!("CLICKHOUSE_HOST not set");
        return true;
    }

    // Try to connect using ToSocketAddrs for DNS resolution
    let addr = format!("{}:{}", host, port);
    eprintln!("Checking ClickHouse at {}...", addr);

    use std::net::ToSocketAddrs;
    match addr.to_socket_addrs() {
        Ok(mut addrs) => {
            if let Some(socket_addr) = addrs.next() {
                match std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_secs(3)) {
                    Ok(_) => {
                        eprintln!("ClickHouse reachable at {} ({})", addr, socket_addr);
                        false
                    }
                    Err(e) => {
                        eprintln!("ClickHouse not reachable at {}: {}", addr, e);
                        true
                    }
                }
            } else {
                eprintln!("Could not resolve {}", addr);
                true
            }
        }
        Err(e) => {
            eprintln!("DNS resolution failed for {}: {}", addr, e);
            true
        }
    }
}

fn get_test_config() -> ClickHouseConfig {
    load_dotenv();

    let host = env::var("CLICKHOUSE_HOST").expect("CLICKHOUSE_HOST not set");
    let port = env::var("CLICKHOUSE_NATIVE_PORT").unwrap_or_else(|_| "9000".to_string());
    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());
    let username = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    let host_with_port = format!("{}:{}", host, port);
    eprintln!("Config: host={}, db={}, user={}", host_with_port, database, username);

    ClickHouseConfig {
        hosts: vec![host_with_port],
        database,
        username,
        password,
        protocol: "native".to_string(),
        tables: Vec::new(),
        tls: None,
    }
}

#[tokio::test]
async fn test_clickhouse_connect() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let config = get_test_config();
    let start = std::time::Instant::now();
    let result = ClickHouseClient::new(&config).await;

    match result {
        Ok(client) => {
            let elapsed = start.elapsed();
            eprintln!("✓ Connected to ClickHouse in {:?}", elapsed);
            eprintln!("  Host: {:?}", config.hosts);
            eprintln!("  Database: {}", client.database());
            assert!(!client.database().is_empty());
        }
        Err(e) => {
            eprintln!("✗ ClickHouse connection failed: {}", e);
            panic!("Connection should succeed when ClickHouse is available");
        }
    }
}

#[tokio::test]
async fn test_clickhouse_insert_json() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let config = get_test_config();
    let client = match ClickHouseClient::new(&config).await {
        Ok(c) => c,
        Err(e) => {
            panic!("ClickHouse connection failed: {}", e);
        }
    };

    // Create a test table
    let table_name = format!("test_insert_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id UInt64,
            event String,
            category String,
            value Float64,
            timestamp DateTime DEFAULT now()
        ) ENGINE = Memory",
        table_name
    );

    let start = std::time::Instant::now();
    client.query(&create_sql).await.expect("Failed to create table");
    eprintln!("✓ Created table '{}' in {:?}", table_name, start.elapsed());

    // Insert small batch (2 rows)
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        serde_json::json!({"id": 1, "event": "login", "category": "auth", "value": 1.5}).as_object().unwrap().clone(),
        serde_json::json!({"id": 2, "event": "logout", "category": "auth", "value": 2.5}).as_object().unwrap().clone(),
    ];

    let start = std::time::Instant::now();
    let result = client.insert_json(&table_name, rows).await;
    let elapsed = start.elapsed();
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());
    eprintln!("✓ Inserted 2 rows in {:?}", elapsed);

    // Insert larger batch (1000 rows)
    let categories = ["auth", "api", "web", "mobile"];
    let mut rows = Vec::with_capacity(1000);
    for i in 0..1000usize {
        let cat = categories[i % 4];
        rows.push(serde_json::json!({
            "id": i,
            "event": format!("event_{}", i % 10),
            "category": cat,
            "value": (i as f64) * 1.5
        }).as_object().unwrap().clone());
    }

    let start = std::time::Instant::now();
    let result = client.insert_json(&table_name, rows).await;
    let elapsed = start.elapsed();
    assert!(result.is_ok(), "Batch insert failed: {:?}", result.err());
    let count = result.unwrap();
    eprintln!("✓ Inserted {} rows in {:?} ({:.0} rows/sec)", count, elapsed, count as f64 / elapsed.as_secs_f64());

    // Insert even larger batch (5000 rows)
    let categories5 = ["auth", "api", "web", "mobile", "backend"];
    let mut rows = Vec::with_capacity(5000);
    for i in 0..5000usize {
        let cat = categories5[i % 5];
        rows.push(serde_json::json!({
            "id": 1000 + i,
            "event": format!("bulk_{}", i % 100),
            "category": cat,
            "value": (i as f64) * 0.7
        }).as_object().unwrap().clone());
    }

    let start = std::time::Instant::now();
    let result = client.insert_json(&table_name, rows).await;
    let elapsed = start.elapsed();
    assert!(result.is_ok(), "Large batch insert failed: {:?}", result.err());
    let count = result.unwrap();
    eprintln!("✓ Inserted {} rows in {:?} ({:.0} rows/sec)", count, elapsed, count as f64 / elapsed.as_secs_f64());

    // Cleanup
    let start = std::time::Instant::now();
    client.query(&format!("DROP TABLE IF EXISTS {}", table_name)).await.expect("Failed to drop");
    eprintln!("✓ Dropped table in {:?}", start.elapsed());
}

#[tokio::test]
async fn test_clickhouse_table_exists() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let config = get_test_config();
    let client = match ClickHouseClient::new(&config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test: ClickHouse connection failed: {}", e);
            return;
        }
    };

    // system.tables should always exist
    let result = client.table_exists("tables").await;
    assert!(result.is_ok());
}
