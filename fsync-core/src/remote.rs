use anyhow::Result;
use async_trait::async_trait;

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
    async fn ping(&self) -> Result<()>;
}
