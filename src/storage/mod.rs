use anyhow::Result;
use arrow::{
    array::{Float64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use parquet::{
    arrow::ArrowWriter,
    file::properties::WriterProperties,
};
use std::path::PathBuf;
use tokio::fs::{self, File};
use async_trait::async_trait;

/// Represents a time series data store using Apache Arrow
pub struct TimeSeriesStore {
    // Using RwLock for concurrent access to data
    batches: Arc<RwLock<Vec<RecordBatch>>>,
    schema: Arc<Schema>,
    data_dir: PathBuf,
    max_batches_in_memory: usize,
}

impl TimeSeriesStore {
    pub async fn new(data_dir: PathBuf) -> Result<Self> {
        // Create a new directory for storing data
        fs::create_dir_all(&data_dir).await?;
        // Define schema for time series data
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("tags", DataType::Utf8, true),
        ]));

        Ok(Self {
            batches: Arc::new(RwLock::new(Vec::new())),
            schema,
            data_dir,
            max_batches_in_memory: 1000, // Configurable
        })
    }

    /// Append new time series data
    pub async fn append(&self, 
        timestamps: Vec<i64>,
        metrics: Vec<String>,
        values: Vec<f64>,
        tags: Vec<Option<String>>) -> Result<()> {
        
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(StringArray::from(metrics)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(tags)),
            ],
        )?;

        let mut batches = self.batches.write().await;
        batches.push(batch);

        // Check if we need to persist to disk
        if batches.len() >= self.max_batches_in_memory {
            self.persist_batch(&batch).await?;
            batches.clear(); // Clear memory after persistence
        }

        Ok(())
    }

    /// Query data within a time range
    pub async fn query(&self, start_time: i64, end_time: i64) -> Result<Vec<RecordBatch>> {
        let batches = self.batches.read().await;
        
        // Filter batches based on time range
        let filtered: Vec<RecordBatch> = batches
            .iter()
            .filter(|batch| {
                let timestamps = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                
                timestamps.iter().any(|ts| {
                    ts.map(|ts| ts >= start_time && ts <= end_time)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .collect();

        Ok(filtered)
    }

    pub async fn load_persisted_data(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.data_dir).await?;
        let mut batches = self.batches.write().await;

        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().unwrap_or_default() == "parquet" {
                let file = File::open(entry.path()).await?;
                // Read parquet file into Arrow RecordBatch
                // Implementation depends on specific needs
                // You might want to use ParquetRecordBatchReader here
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_append_and_query() {
        let store = TimeSeriesStore::new().await.unwrap();
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        
        let timestamps = vec![now, now + 1000];
        let metrics = vec!["cpu".to_string(), "cpu".to_string()];
        let values = vec![0.5, 0.7];
        let tags = vec![Some("host=server1".to_string()), Some("host=server1".to_string())];
        
        store.append(timestamps.clone(), metrics, values, tags).await.unwrap();
        
        let results = store.query(now, now + 1000).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = tempdir().unwrap();
        let store = TimeSeriesStore::new(temp_dir.path().to_path_buf()).await.unwrap();
        
        // Create test data
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        
        // Force persistence by adding more than max_batches_in_memory
        for i in 0..1001 {
            let timestamps = vec![now + i];
            let metrics = vec!["cpu".to_string()];
            let values = vec![0.5];
            let tags = vec![Some("host=server1".to_string())];
            
            store.append(timestamps, metrics, values, tags).await.unwrap();
        }

        // Verify files were created
        let files = fs::read_dir(&temp_dir).await.unwrap();
        assert!(files.count().await > 0);
    }
}