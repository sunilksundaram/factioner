use crate::pool::ServicePool;
use crate::types::{OperationPollSnapshot, PoolMetrics, SubmitOptions};
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct PoolRegistry {
    pools: RwLock<HashMap<String, Arc<dyn ErasedPool>>>,
}

#[async_trait::async_trait]
pub trait ErasedPool: Send + Sync {
    fn name(&self) -> &str;
    async fn metrics(&self) -> PoolMetrics;
    async fn drain(&self, timeout_ms: u64);
    async fn execute_json(
        &self,
        payload: serde_json::Value,
        options: SubmitOptions,
    ) -> Result<serde_json::Value, String>;
    async fn stream_sse(
        &self,
        payload: serde_json::Value,
        options: SubmitOptions,
    ) -> Result<BoxStream<'static, Result<String, String>>, String>;
    async fn submit_operation_json(
        &self,
        payload: serde_json::Value,
        options: SubmitOptions,
    ) -> Result<String, String>;
    async fn poll_operation_json(
        &self,
        handle_id: &str,
    ) -> Result<OperationPollSnapshot<serde_json::Value>, String>;
    async fn cancel_operation_json(&self, handle_id: &str) -> Result<bool, String>;
}

#[async_trait::async_trait]
impl<TReq, TRes> ErasedPool for ServicePool<TReq, TRes>
where
    TReq: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
    TRes: Clone + Send + Sync + serde::Serialize + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn metrics(&self) -> PoolMetrics {
        self.get_metrics().await
    }

    async fn drain(&self, timeout_ms: u64) {
        ServicePool::drain(self, timeout_ms).await;
    }

    async fn execute_json(
        &self,
        payload: serde_json::Value,
        options: SubmitOptions,
    ) -> Result<serde_json::Value, String> {
        let typed_req: TReq =
            serde_json::from_value(payload).map_err(|e| format!("Invalid JSON request payload: {}", e))?;
        let res = self.submit(typed_req, options).await?;
        let data_json = serde_json::to_value(&res.data).map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "success": res.success,
            "data": data_json,
            "endpointId": res.endpoint_id,
            "attempts": res.attempts,
            "durationMs": res.duration_ms,
            "queueWaitMs": res.queue_wait_ms,
        }))
    }

    async fn stream_sse(
        &self,
        payload: serde_json::Value,
        options: SubmitOptions,
    ) -> Result<BoxStream<'static, Result<String, String>>, String> {
        let typed_req: TReq =
            serde_json::from_value(payload).map_err(|e| format!("Invalid JSON request payload: {}", e))?;
        let stream = self.submit_stream(typed_req, options).await?;
        Ok(stream)
    }

    async fn submit_operation_json(
        &self,
        payload: serde_json::Value,
        options: SubmitOptions,
    ) -> Result<String, String> {
        let typed_req: TReq =
            serde_json::from_value(payload).map_err(|e| format!("Invalid JSON request payload: {}", e))?;
        self.submit_operation(typed_req, options).await
    }

    async fn poll_operation_json(
        &self,
        handle_id: &str,
    ) -> Result<OperationPollSnapshot<serde_json::Value>, String> {
        let snap = self.poll_operation(handle_id).await?;
        let data = match snap.data {
            Some(d) => Some(serde_json::to_value(&d).map_err(|e| e.to_string())?),
            None => None,
        };
        Ok(OperationPollSnapshot {
            done: snap.done,
            status: snap.status,
            data,
        })
    }

    async fn cancel_operation_json(&self, handle_id: &str) -> Result<bool, String> {
        let (upstream,) = self.cancel_operation(handle_id).await?;
        Ok(upstream)
    }
}

impl PoolRegistry {
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register<TReq, TRes>(&self, pool: Arc<ServicePool<TReq, TRes>>)
    where
        TReq: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
        TRes: Clone + Send + Sync + serde::Serialize + 'static,
    {
        let name = pool.name.clone();
        self.pools.write().await.insert(name, pool);
    }

    pub async fn get(&self, name: &str) -> Option<Arc<dyn ErasedPool>> {
        self.pools.read().await.get(name).cloned()
    }

    pub async fn get_all_metrics(&self) -> HashMap<String, PoolMetrics> {
        let pools = self.pools.read().await;
        let mut out = HashMap::new();
        for (name, pool) in pools.iter() {
            out.insert(name.clone(), pool.metrics().await);
        }
        out
    }

    pub async fn drain_all(&self, timeout_ms: u64) {
        let pools = self.pools.read().await;
        for pool in pools.values() {
            pool.drain(timeout_ms).await;
        }
    }
}

impl Default for PoolRegistry {
    fn default() -> Self {
        Self::new()
    }
}
