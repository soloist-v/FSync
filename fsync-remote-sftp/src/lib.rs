mod ssh_client;
mod utils;

use crate::utils::{create_dir_all, remove_dir_all};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use fsync_core::{RemoteFs, RemoteOp};
use russh::client::AuthResult;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use ssh_client::Client;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{error, info};

pub struct SftpRemote {
    sftp: SftpSession,
}

impl SftpRemote {
    pub async fn connect(host_with_port: &str, user: &str, password: Option<&str>, allowed_fingerprints: Option<Vec<String>>) -> Result<Self> {
        let (host, port) = match host_with_port.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p.parse().map_err(|_| anyhow!("invalid port in host: {host_with_port}"))?;
                (h.to_string(), port)
            }
            None => (host_with_port.to_string(), 22u16),
        };

        let config = russh::client::Config::default();
        let mut session = russh::client::connect(Arc::new(config), (host.as_str(), port), Client { allowed_fingerprints }).await?;
        let res = session
            .authenticate_password(user, password.unwrap_or(""))
            .await?;
        if let AuthResult::Failure {
            remaining_methods,
            partial_success,
        } = res
        {
            return Err(anyhow!(
                "Authentication failed, remaining_methods: {:?}, partial_success: {}",
                remaining_methods,
                partial_success
            ));
        }
        let channel = session.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;
        info!("current path: {:?}", sftp.canonicalize(".").await?);
        Ok(Self { sftp })
    }
}

#[async_trait]
impl RemoteFs for SftpRemote {
    async fn apply_batch(&self, ops: Vec<RemoteOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // Improved batching: execute independent file uploads concurrently with bounded concurrency;
        // directory create/remove/rename kept sequential to preserve order.
        use futures::stream::{self, StreamExt};
        const MAX_CONCURRENCY: usize = 4;
        let mut seq_ops = Vec::new();
        let mut uploads = Vec::new();
        for op in ops {
            match op {
                RemoteOp::Upload { local, remote } => uploads.push((local, remote)),
                other => seq_ops.push(other),
            }
        }
        // Run uploads concurrently
        stream::iter(uploads)
            .map(|(local, remote)| async move {
                let mut reader = tokio::fs::File::open(local).await?;
                let mut remote_file = self.sftp.create(remote).await?;
                tokio::io::copy(&mut reader, &mut remote_file).await?;
                Result::<()>::Ok(())
            })
            .buffer_unordered(MAX_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        // Apply sequential ops
        for op in seq_ops {
            match op {
                RemoteOp::Upload { .. } => unreachable!(),
                RemoteOp::Remove { remote } => {
                    let metadata = self.sftp.metadata(remote.as_str()).await?;
                    if metadata.is_dir() {
                        remove_dir_all(&self.sftp, &remote).await?;
                    } else {
                        self.sftp.remove_file(remote.as_str()).await?;
                    }
                }
                RemoteOp::MkDir { remote } => {
                    create_dir_all(&self.sftp, &remote).await?;
                }
                RemoteOp::Rename { from, to } => {
                    let _ = self.sftp.rename(from, to).await;
                }
            }
        }
        Ok(())
    }

    async fn ping(&self) -> Result<()> {
        let _ = self.sftp.metadata(".").await?;
        Ok(())
    }
}
