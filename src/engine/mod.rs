use actix::prelude::*;
use anyhow::Result;
use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt64Array, StringArray},
    record_batch::RecordBatch,
    compute,
};
use futures::stream::{self, StreamExt};
use std::{sync::Arc, time::Duration};
use crate::{parser::{Query, Aggregation}, storage::TimeSeriesStore};
use crate::parser::WindowSpec;

/// Actor message for query execution
#[derive(Message)]
#[rtype(result = "Result<Vec<RecordBatch>>")]
pub struct ExecuteQuery(pub Query);

/// QueryEngine actor for concurrent query processing
#[derive(Clone)]
pub struct QueryEngine {
    store: Arc<TimeSeriesStore>,
}

impl Actor for QueryEngine {
    type Context = Context<Self>;
}

impl QueryEngine {
    pub fn new(store: TimeSeriesStore) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    pub fn get_store(&self) -> Arc<TimeSeriesStore> {
        self.store.clone()
    }

    pub fn start_actor(&self) -> Addr<Self> 
    where
        Self: Clone,
    {
        Actor::start(self.clone())
    }

    async fn apply_window(&self, batches: Vec<RecordBatch>, window: Duration) -> Result<Vec<RecordBatch>> {
        let window_nanos = window.as_nanos() as i64;
        
        let mut windowed_batches = Vec::new();
        for batch in batches {
            let timestamps = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            
            // Group data points into windows
            let mut current_window: Vec<u64> = Vec::new();
            let mut window_start = timestamps.value(0);
            
            for i in 0..timestamps.len() {
                let ts = timestamps.value(i);
                if ts - window_start > window_nanos {
                    // Create new window batch
                    if !current_window.is_empty() {
                        let indices = UInt64Array::from(current_window.clone());
                        windowed_batches.push(RecordBatch::try_new(
                            batch.schema(),
                            batch.columns().iter()
                                .map(|col| compute::take(col, &indices, None))
                                .collect::<Result<Vec<_>, _>>()?,
                        )?);
                    }
                    current_window = vec![i as u64];
                    window_start = ts;
                } else {
                    current_window.push(i as u64);
                }
            }
            
            // Handle last window
            if !current_window.is_empty() {
                let indices = UInt64Array::from(current_window);
                windowed_batches.push(RecordBatch::try_new(
                    batch.schema(),
                    batch.columns().iter()
                        .map(|col| compute::take(col, &indices, None))
                        .collect::<Result<Vec<_>, _>>()?,
                )?);
            }
        }
        
        Ok(windowed_batches)
    }

    async fn apply_aggregation(&self, batches: Vec<RecordBatch>, agg: Aggregation) -> Result<RecordBatch> {
        match agg {
            Aggregation::Avg(_metric) => {
                let mut sum = 0.0;
                let mut count = 0;
                
                for batch in &batches {
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    
                    sum += compute::sum(values).unwrap();
                    count += values.len();
                }
                
                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                
                Ok(RecordBatch::try_new(
                    batches[0].schema(),
                    vec![
                        batches[0].column(0).clone(), // timestamp
                        batches[0].column(1).clone(), // metric
                        Arc::new(Float64Array::from(vec![avg])), // value
                        Arc::new(StringArray::from(vec![None::<&str>])), // tags
                    ],
                )?)
            },
            Aggregation::Sum(_metric) => {
                let mut sum = 0.0;
                
                for batch in &batches {
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    
                    sum += compute::sum(values).unwrap();
                }
                
                Ok(RecordBatch::try_new(
                    batches[0].schema(),
                    vec![
                        batches[0].column(0).clone(),
                        Arc::new(Float64Array::from(vec![sum])),
                    ],
                )?)
            },
            Aggregation::Count(_metric) => {
                let mut count = 0;
                
                for batch in &batches {
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    
                    count += values.len();
                }
                
                Ok(RecordBatch::try_new(
                    batches[0].schema(),
                    vec![
                        batches[0].column(0).clone(),
                        Arc::new(Float64Array::from(vec![count as f64])),
                    ],
                )?)
            },
            Aggregation::Min(_metric) => {
                let mut min = f64::INFINITY;
                
                for batch in &batches {
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    
                    min = min.min(compute::min(values).unwrap());
                }
                
                Ok(RecordBatch::try_new(
                    batches[0].schema(),
                    vec![
                        batches[0].column(0).clone(),
                        Arc::new(Float64Array::from(vec![min])),
                    ],
                )?)
            },
            Aggregation::Max(_metric) => {
                let mut max = f64::NEG_INFINITY;
                
                for batch in &batches {
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    
                    max = max.max(compute::max(values).unwrap());
                }
                
                Ok(RecordBatch::try_new(
                    batches[0].schema(),
                    vec![
                        batches[0].column(0).clone(),
                        Arc::new(Float64Array::from(vec![max])),
                    ],
                )?)
            },
        }
    }
}

impl Handler<ExecuteQuery> for QueryEngine {
    type Result = ResponseFuture<Result<Vec<RecordBatch>>>;

    fn handle(&mut self, msg: ExecuteQuery, _ctx: &mut Context<Self>) -> Self::Result {
        let store = self.store.clone();
        let engine = self.clone();
        
        Box::pin(async move {
            let Query { window, select, .. } = msg.0;
            
            // Query data for the time window
            let raw_data = store.query(0, i64::MAX).await?;
            
            // Apply time window
            let windowed_data = engine.apply_window(raw_data, window.duration).await?;
            
            // Apply aggregations in parallel using stream
            let aggregated: Vec<RecordBatch> = stream::iter(select)
                .map(|agg| engine.apply_aggregation(windowed_data.clone(), agg.clone()))
                .buffer_unordered(4) // Process up to 4 aggregations concurrently
                .collect::<Vec<Result<RecordBatch>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            Ok(aggregated)
        })
    }
}

// Update the test to use the existing TimeSeriesStore implementation
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
    use std::sync::Arc as ArrowArc;

    #[actix_rt::test]
    async fn test_query_execution() {
        // Create store using existing implementation
        let store = TimeSeriesStore::new().await.unwrap();
        
        // Insert test data
        let timestamps = vec![Duration::from_secs(0).as_nanos() as i64];
        let metrics = vec!["cpu".to_string()];
        let values = vec![42.0];
        let tags = vec![Some("host=test".to_string())];
        
        store.append(timestamps, metrics, values, tags).await.unwrap();

        let engine = QueryEngine::new(store);
        let addr = engine.start_actor();
        
        let query = Query {
            select: vec![Aggregation::Avg("cpu".to_string())],
            from: "metrics".to_string(),
            window: WindowSpec { duration: Duration::from_secs(60) },
            filter: None,
            group_by: None,
        };
        
        let result = addr.send(ExecuteQuery(query)).await.unwrap();
        println!("Query result: {:?}", result);
        
        assert!(result.is_ok());
        
        System::current().stop();
    }
}