mod parser;
mod engine;
mod storage;
mod api;

use anyhow::Result;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .compact()
        .init();
    
    info!("Starting TimeSeriesQL engine...");
    
    // Initialize storage engine
    let storage = storage::TimeSeriesStore::new().await?;
    
    // Initialize query engine
    let engine = engine::QueryEngine::new(storage);
    
    // Start API server
    api::serve(engine).await?;
    
    Ok(())
}
