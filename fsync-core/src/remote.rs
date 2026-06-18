use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Single remote operation derived from local FS event.
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum RemoteOp {
    Upload { local: PathBuf, remote: String },
    Remove { remote: String },
    MkDir { remote: String },
    Rename { from: String, to: String },
}

#[async_trait]
pub trait RemoteFs: Send + Sync + 'static {
    async fn apply_batch(&self, ops: Vec<RemoteOp>) -> Result<()>;
    async fn apply_batch_cancelled(
        &self,
        ops: Vec<RemoteOp>,
        cancel: CancellationToken,
    ) -> Result<()> {
        if cancel.is_cancelled() {
            return Err(anyhow!("remote operation cancelled"));
        }
        self.apply_batch(ops).await
    }
    async fn ping(&self) -> Result<()>;
}
