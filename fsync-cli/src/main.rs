use anyhow::{anyhow, Result};
use clap::Parser;
use fsync_core::{spawn_task, TaskConfig, RemoteCfg, SyncManager};
use fsync_remote_sftp::SftpRemote;
use std::{fs, path::Path, sync::Arc};

#[derive(Parser)]
#[command(name = "fsync", version, about = "FSync – directory sync CLI")]
struct Cli {
    /// Path to config file (TOML / JSON / YAML)
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let text = fs::read_to_string(&cli.config)
        .map_err(|e| anyhow!("read config {} failed: {e}", cli.config))?;

    // Detect format by extension
    let ext = Path::new(&cli.config).extension().and_then(|s| s.to_str()).unwrap_or("");
    let tasks: Vec<TaskConfig> = match ext {
        "json" => serde_json::from_str(&text)?,
        "yaml" | "yml" => serde_yaml::from_str(&text)?,
        _ => serde_yaml::from_str(&text)?, // default to yaml
    };

    if tasks.is_empty() {
        return Err(anyhow!("no tasks defined in config"));
    }

    // Spawn every task
    let manager = SyncManager::new();
    for cfg in tasks {
        match &cfg.remote_cfg {
            RemoteCfg::Sftp { host, user, password, key: _, fingerprints } => {
                let remote = SftpRemote::connect(host, user, password.as_deref(), fingerprints.clone()).await?;
                spawn_task(cfg.clone(), remote);
            }
        }
    }

    println!("FSync running... press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;
    println!("Stopping");
    Ok(())
}
