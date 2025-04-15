use axum::{
    routing::{post, get},
    extract::Json,
    Router,
    Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use actix::Actor;
use tower::ServiceExt;
use crate::engine::{QueryEngine, ExecuteQuery};
use arrow::util::display::ArrayFormatter;
use arrow::util::display::FormatOptions;
use tokio::task::LocalSet;

#[derive(Debug, Serialize)]
struct ApiResponse {
    success: bool,
    message: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct DataPoint {
    timestamp: i64,
    metric: String,
    value: f64,
    tags: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    query: String,
    start_time: Option<i64>,
    end_time: Option<i64>,
}

async fn health_check() -> Json<ApiResponse> {
    Json(ApiResponse {
        success: true,
        message: Some("Service is healthy".to_string()),
        data: None,
    })
}

async fn ingest_data(
    Extension(engine): Extension<Arc<QueryEngine>>,
    Json(data): Json<Vec<DataPoint>>,
) -> Json<ApiResponse> {
    let store = engine.get_store();
    
    // Transform data points into batch format
    let mut timestamps = Vec::with_capacity(data.len());
    let mut metrics = Vec::with_capacity(data.len());
    let mut values = Vec::with_capacity(data.len());
    let mut tags = Vec::with_capacity(data.len());

    for point in &data {
        timestamps.push(point.timestamp);
        metrics.push(point.metric.clone());
        values.push(point.value);
        tags.push(point.tags.clone());
    }

    match store.append(timestamps, metrics, values, tags).await {
        Ok(_) => Json(ApiResponse {
            success: true,
            message: Some(format!("Successfully ingested {} data points", data.len())),
            data: None,
        }),
        Err(e) => Json(ApiResponse {
            success: false,
            message: Some(format!("Failed to ingest data: {}", e)),
            data: None,
        }),
    }
}

async fn execute_query(
    Extension(engine): Extension<Arc<QueryEngine>>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    use crate::parser::parse_query;
    
    match parse_query(&request.query) {
        Ok((_, mut query)) => {
            // If time bounds provided, update the query's window
            if let (Some(start), Some(end)) = (request.start_time, request.end_time) {
                query.window.duration = Duration::from_nanos((end - start) as u64);
            }
            
            // Create a LocalSet for actor execution
            let local = LocalSet::new();
            let result = local.run_until(async move {
                let engine_instance = (*engine).clone();
                let addr = engine_instance.start();
                addr.send(ExecuteQuery(query)).await
            }).await;

            match result {
                Ok(result) => match result {
                    Ok(batches) => {
                        // Convert Arrow RecordBatch to JSON
                        let results: Vec<serde_json::Value> = batches.iter()
                        .map(|batch| {
                            let mut json_rows = Vec::new();
                            for i in 0..batch.num_rows() {
                                let mut row = serde_json::Map::new();
                                for (j, field) in batch.schema().fields().iter().enumerate() {
                                    let array = batch.column(j);
                                    // Create formatter and format value using the display module
                                    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default()).unwrap();
                                    let value = serde_json::Value::String(formatter.value(i).to_string());
                                    row.insert(field.name().clone(), value);
                                }
                                json_rows.push(serde_json::Value::Object(row));
                            }
                            serde_json::Value::Array(json_rows)
                        })
                        .collect();
                            
                        (StatusCode::OK, Json(ApiResponse {
                            success: true,
                            message: Some("Query executed successfully".to_string()),
                            data: Some(serde_json::json!({ "results": results })),
                        }))
                    },
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiResponse {
                            success: false,
                            message: Some(format!("Query execution failed: {}", e)),
                            data: None,
                        })
                    )
                },
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse {
                        success: false,
                        message: Some(format!("Failed to execute query: {}", e)),
                        data: None,
                    })
                )
            }
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                message: Some(format!("Failed to parse query: {}", e)),
                data: None,
            })
        )
    }
}

pub async fn serve(engine: QueryEngine) -> Result<()> {
    let shared_engine = Arc::new(engine);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/ingest", post(ingest_data))
        .route("/query", post(execute_query))
        .layer(Extension(shared_engine));

    let addr = "127.0.0.1:8080";
    tracing::info!("Starting server on {}", addr);
    
    axum::Server::bind(&addr.parse()?)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use tower::ServiceExt;
    use crate::storage::TimeSeriesStore;
    
    #[tokio::test]
    async fn test_health_check() {
        let store = TimeSeriesStore::new().await.unwrap();
        let engine = QueryEngine::new(store);
        let app = Router::new()
            .route("/health", get(health_check))
            .layer(Extension(Arc::new(engine)));

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}