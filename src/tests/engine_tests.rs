use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde_json::json;
use time_series_ql::{
    QueryEngine,
    parser::{Query, WindowSpec, Aggregation},
    storage::TimeSeriesStore,
    engine::ExecuteQuery,
};
use actix::Actor;

#[actix_rt::test]
async fn test_query_execution() {
    let store = TimeSeriesStore::new().await.unwrap();
    let engine = QueryEngine::new(store);
    
    let query = Query {
        select: vec![Aggregation::Avg("cpu".to_string())],
        from: "metrics".to_string(),
        window: WindowSpec { duration: Duration::from_secs(60) },
        filter: None,
        group_by: None,
    };
    
    let addr = engine.start();
    let result = addr.send(ExecuteQuery(query)).await.unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_data_ingestion_and_query() {
    // Initialize components
    let store = TimeSeriesStore::new().await.unwrap();
    let engine = QueryEngine::new(store);
    let engine_arc = std::sync::Arc::new(engine);

    // Get current timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    // Test data ingestion
    let data = vec![
        json!({
            "timestamp": now,
            "metric": "cpu",
            "value": 0.75,
            "tags": "host=server1"
        }),
        json!({
            "timestamp": now + 1000,
            "metric": "cpu",
            "value": 0.85,
            "tags": "host=server1"
        }),
    ];

    let store = engine_arc.get_store();
    store.append(
        vec![now, now + 1000],
        vec!["cpu".to_string(), "cpu".to_string()],
        vec![0.75, 0.85],
        vec![Some("host=server1".to_string()), Some("host=server1".to_string())],
    ).await.unwrap();

    // Test query execution
    let query = Query {
        select: vec![Aggregation::Avg("cpu".to_string())],
        from: "metrics".to_string(),
        window: WindowSpec { duration: Duration::from_secs(60) },
        filter: None,
        group_by: None,
    };

    let addr = engine_arc.start();
    let result = addr.send(ExecuteQuery(query)).await.unwrap();
    
    assert!(result.is_ok());
    let batches = result.unwrap();
    assert!(!batches.is_empty());

    // Validate aggregation result
    let avg_batch = &batches[0];
    let values = avg_batch.column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    
    assert_eq!(values.value(0), 0.8); // Average of 0.75 and 0.85
}

#[tokio::test]
async fn test_time_window_aggregation() {
    let store = TimeSeriesStore::new().await.unwrap();
    let engine = QueryEngine::new(store);
    let engine_arc = std::sync::Arc::new(engine);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    // Insert data points in different windows
    let store = engine_arc.get_store();
    store.append(
        vec![now, now + 1_000_000_000], // 1 second apart
        vec!["cpu".to_string(), "cpu".to_string()],
        vec![0.5, 0.7],
        vec![Some("host=server1".to_string()), Some("host=server1".to_string())],
    ).await.unwrap();

    // Query with 500ms window
    let query = Query {
        select: vec![Aggregation::Avg("cpu".to_string())],
        from: "metrics".to_string(),
        window: WindowSpec { duration: Duration::from_millis(500) },
        filter: None,
        group_by: None,
    };

    let addr = engine_arc.start();
    let result = addr.send(ExecuteQuery(query)).await.unwrap();
    
    assert!(result.is_ok());
    let batches = result.unwrap();
    assert_eq!(batches.len(), 2); // Should have two separate windows
}