use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// Glob pattern (wrapper type for clarity)
/// For now we store as plain String and defer compilation to `globset::Pattern` during runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RemoteCfg {
    /// SFTP remote endpoint
    Sftp {
        host: String,
        user: String,
        password: Option<String>,
        key: Option<PathBuf>,
        #[serde(default)]
        fingerprints: Option<Vec<String>>, // allowed host key fingerprints or base64 keys
    },
    // Future variants: Http { ... }, Grpc { ... }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    pub id:        Uuid,
    pub name:      String,
    pub local:     PathBuf,
    pub remote:    String,
    #[serde(default)]
    pub include:   Vec<Pattern>,
    #[serde(default)]
    pub exclude:   Vec<Pattern>,
    #[serde(default = "TaskConfig::default_scan_ms")]
    pub scan_ms:   u64,
    /// Optional size filter in the form of "..", "..n", "n..", or "m..n" (bytes)
    #[serde(default)]
    pub size:      Option<String>,
    /// Max retry attempts for remote operations
    #[serde(default = "TaskConfig::default_retry_max")]
    pub retry_max: u32,
    /// Initial backoff in ms for retries (exponential)
    #[serde(default = "TaskConfig::default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    pub remote_cfg: RemoteCfg,
}

impl TaskConfig {
    fn default_scan_ms() -> u64 { 300 }
    fn default_retry_max() -> u32 { 3 }
    fn default_retry_backoff_ms() -> u64 { 500 }
}
