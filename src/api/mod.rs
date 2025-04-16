use actix_web::{
    web::{self, Data},
    App, HttpResponse, HttpServer,
    Responder,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use actix::Actor;
use crate::engine::{QueryEngine, ExecuteQuery};
use arrow::util::display::ArrayFormatter;
use arrow::util::display::FormatOptions;
use tokio::task::LocalSet;
use arrow::record_batch::RecordBatch;

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

async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: Some("Service is healthy".to_string()),
        data: None,
    })
}

async fn ingest_data(
    engine: Data<Arc<QueryEngine>>,
    data: web::Json<Vec<DataPoint>>,
) -> impl Responder {
    let store = engine.get_store();
    
    let mut timestamps = Vec::with_capacity(data.len());
    let mut metrics = Vec::with_capacity(data.len());
    let mut values = Vec::with_capacity(data.len());
    let mut tags = Vec::with_capacity(data.len());

    for point in data.iter() {
        timestamps.push(point.timestamp);
        metrics.push(point.metric.clone());
        values.push(point.value);
        tags.push(point.tags.clone());
    }

    match store.append(timestamps, metrics, values, tags).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse {
            success: true,
            message: Some(format!("Successfully ingested {} data points", data.len())),
            data: None,
        }),
        Err(e) => HttpResponse::InternalServerError().json(ApiResponse {
            success: false,
            message: Some(format!("Failed to ingest data: {}", e)),
            data: None,
        }),
    }
}

async fn execute_query(
    engine: web::Data<Arc<QueryEngine>>,
    request: web::Json<QueryRequest>,
) -> HttpResponse {
    use crate::parser::parse_query;

    match parse_query(&request.query) {
        Ok((_, mut query)) => {
            if let (Some(start), Some(end)) = (request.start_time, request.end_time) {
                query.window.duration = Duration::from_nanos((end - start) as u64);
            }

            // Create a static actor instance
            let engine_ref = engine.as_ref();
            let addr = engine_ref.start_actor();
            
            match addr.send(ExecuteQuery(query)).await {
                Ok(Ok(batches)) => {
                    let results = process_batches(&batches);
                    HttpResponse::Ok().json(ApiResponse {
                        success: true,
                        message: Some("Query executed successfully".to_string()),
                        data: Some(serde_json::json!({ "results": results })),
                    })
                }
                Ok(Err(e)) => HttpResponse::InternalServerError().json(ApiResponse {
                    success: false,
                    message: Some(format!("Query execution failed: {}", e)),
                    data: None,
                }),
                Err(e) => HttpResponse::InternalServerError().json(ApiResponse {
                    success: false,
                    message: Some(format!("Failed to send query to engine: {}", e)),
                    data: None,
                }),
            }
        }
        Err(e) => HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            message: Some(format!("Failed to parse query: {}", e)),
            data: None,
        }),
    }
}

// Helper function to process batches
fn process_batches(batches: &[RecordBatch]) -> Vec<serde_json::Value> {
    batches.iter()
        .map(|batch| {
            let mut json_rows = Vec::new();
            for i in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                for (j, field) in batch.schema().fields().iter().enumerate() {
                    let array = batch.column(j);
                    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default()).unwrap();
                    let value = serde_json::Value::String(formatter.value(i).to_string());
                    row.insert(field.name().clone(), value);
                }
                json_rows.push(serde_json::Value::Object(row));
            }
            serde_json::Value::Array(json_rows)
        })
        .collect()
}

pub async fn serve(engine: QueryEngine) -> Result<()> {
    let shared_engine = Arc::new(engine);
    
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(shared_engine.clone()))
            .route("/health", web::get().to(health_check))
            .route("/ingest", web::post().to(ingest_data))
            .route("/query", web::post().to(execute_query))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App};
    use crate::storage::TimeSeriesStore;

    #[actix_web::test]
    async fn test_health_check() {
        let store = TimeSeriesStore::new().await.unwrap();
        let engine = QueryEngine::new(store);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(Arc::new(engine)))
                .route("/health", web::get().to(health_check)),
        )
        .await;

        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    }
}