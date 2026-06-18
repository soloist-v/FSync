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
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct SftpRemote {
    sftp: SftpSession,
    ensured_dirs: Mutex<HashSet<String>>,
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
        Ok(Self {
            sftp,
            ensured_dirs: Mutex::new(HashSet::new()),
        })
    }

    async fn ensure_dir_all(&self, remote: &str) -> Result<()> {
        let remote = normalize_remote_dir(remote);
        if remote.is_empty() {
            return Ok(());
        }

        if self.ensured_dirs.lock().await.contains(&remote) {
            return Ok(());
        }

        create_dir_all(&self.sftp, &remote).await?;
        let mut ensured_dirs = self.ensured_dirs.lock().await;
        for dir in remote_dir_chain(&remote) {
            ensured_dirs.insert(dir);
        }
        Ok(())
    }
}

#[async_trait]
impl RemoteFs for SftpRemote {
    async fn apply_batch(&self, ops: Vec<RemoteOp>) -> Result<()> {
        self.apply_batch_cancelled(ops, CancellationToken::new())
            .await
    }

    async fn apply_batch_cancelled(
        &self,
        ops: Vec<RemoteOp>,
        cancel: CancellationToken,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        for op in ops {
            if cancel.is_cancelled() {
                return Err(anyhow!("remote operation cancelled"));
            }
            match op {
                RemoteOp::Upload { local, remote } => {
                    if let Some(parent) = remote_parent(&remote) {
                        self.ensure_dir_all(&parent).await?;
                    }
                    let mut reader = tokio::fs::File::open(local).await?;
                    let tmp_remote = upload_temp_path(&remote);
                    let mut remote_file = self.sftp.create(&tmp_remote).await?;
                    let upload_result = copy_cancelled(&mut reader, &mut remote_file, &cancel)
                        .await
                        .and_then(|_| {
                            if cancel.is_cancelled() {
                                Err(anyhow!("remote operation cancelled"))
                            } else {
                                Ok(())
                            }
                        });
                    if let Err(e) = upload_result {
                        let _ = remote_file.shutdown().await;
                        let _ = self.sftp.remove_file(tmp_remote.as_str()).await;
                        return Err(e);
                    }
                    remote_file.flush().await?;
                    let _ = remote_file.shutdown().await;
                    if let Err(e) = self.sftp.rename(&tmp_remote, &remote).await {
                        if !is_no_such_file(&e) {
                            let _ = self.sftp.remove_file(remote.as_str()).await;
                            self.sftp.rename(&tmp_remote, &remote).await?;
                        }
                    }
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
                    self.ensure_dir_all(&remote).await?;
                }
                RemoteOp::Rename { from, to } => {
                    if let Some(parent) = remote_parent(&to) {
                        self.ensure_dir_all(&parent).await?;
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

fn upload_temp_path(remote: &str) -> String {
    format!("{remote}.fsync.tmp")
}

async fn copy_cancelled<R, W>(
    reader: &mut R,
    writer: &mut W,
    cancel: &CancellationToken,
) -> Result<u64>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    const BUF_SIZE: usize = 1024 * 1024;

    let mut buf = vec![0_u8; BUF_SIZE];
    let mut written = 0_u64;
    loop {
        let n = tokio::select! {
            _ = cancel.cancelled() => return Err(anyhow!("remote operation cancelled")),
            result = reader.read(&mut buf) => result?,
        };
        if n == 0 {
            return Ok(written);
        }

        tokio::select! {
            _ = cancel.cancelled() => return Err(anyhow!("remote operation cancelled")),
            result = writer.write_all(&buf[..n]) => result?,
        }
        written += n as u64;
    }
}

fn normalize_remote_dir(remote: &str) -> String {
    remote.replace('\\', "/").trim_end_matches('/').to_string()
}

fn remote_dir_chain(remote: &str) -> Vec<String> {
    let remote = normalize_remote_dir(remote);
    let mut dirs = Vec::new();
    let mut current = String::new();
    for part in remote.split('/').filter(|part| !part.is_empty()) {
        current.push('/');
        current.push_str(part);
        dirs.push(current.clone());
    }
    if !remote.starts_with('/') && !remote.is_empty() {
        dirs.clear();
        let mut relative = String::new();
        for part in remote.split('/').filter(|part| !part.is_empty()) {
            if !relative.is_empty() {
                relative.push('/');
            }
            relative.push_str(part);
            dirs.push(relative.clone());
        }
    }
    dirs
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
