mod ssh_client;
mod utils;

use crate::utils::{create_dir_all, remove_dir_all};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use fsync_core::{RemoteFs, RemoteOp};
use russh::client::AuthResult;
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::StatusCode;
use ssh_client::Client;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub struct SftpRemote {
    sftp: SftpSession,
}

impl SftpRemote {
    pub async fn connect(
        host_with_port: &str,
        user: &str,
        password: Option<&str>,
        allowed_fingerprints: Option<Vec<String>>,
    ) -> Result<Self> {
        let (host, port) = match host_with_port.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p
                    .parse()
                    .map_err(|_| anyhow!("invalid port in host: {host_with_port}"))?;
                (h.to_string(), port)
            }
            None => (host_with_port.to_string(), 22u16),
        };

        let config = russh::client::Config::default();
        let mut session = russh::client::connect(
            Arc::new(config),
            (host.as_str(), port),
            Client {
                allowed_fingerprints,
            },
        )
        .await?;
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

        for op in ops {
            match op {
                RemoteOp::Upload { local, remote } => {
                    if let Some(parent) = remote_parent(&remote) {
                        create_dir_all(&self.sftp, parent).await?;
                    }
                    let mut reader = tokio::fs::File::open(local).await?;
                    let mut remote_file = self.sftp.create(remote).await?;
                    tokio::io::copy(&mut reader, &mut remote_file).await?;
                }
                RemoteOp::Remove { remote } => {
                    let metadata = match self.sftp.metadata(remote.as_str()).await {
                        Ok(metadata) => metadata,
                        Err(e) if is_no_such_file(&e) => continue,
                        Err(e) => return Err(e.into()),
                    };
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
                    if let Some(parent) = remote_parent(&to) {
                        create_dir_all(&self.sftp, parent).await?;
                    }
                    if let Err(e) = self.sftp.rename(from, to).await {
                        if !is_no_such_file(&e) {
                            return Err(e.into());
                        }
                    }
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

fn remote_parent(remote: &str) -> Option<String> {
    let parent = Path::new(remote).parent()?;
    let parent = parent.to_string_lossy().replace('\\', "/");
    if parent.is_empty() || parent == "." {
        None
    } else {
        Some(parent)
    }
}

fn is_no_such_file(error: &SftpError) -> bool {
    matches!(
        error,
        SftpError::Status(status) if status.status_code == StatusCode::NoSuchFile
    )
}
