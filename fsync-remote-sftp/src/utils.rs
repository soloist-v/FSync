use russh_sftp::client::error::Error;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{Status, StatusCode};
use std::path::{Path, PathBuf};

fn to_remote_path<P: AsRef<Path>>(p: P) -> String {
    p.as_ref()
        .to_string_lossy()
        .replace('\\', "/")
}

/// 异步地递归创建目录及其所有父目录。
///
/// # Arguments
///
/// * `sftp` - 一个可变的 `SftpSession` 引用，用于执行 SFTP 命令。
/// * `path` - 需要创建的目录路径。
///
/// # Errors
///
/// 如果在创建目录过程中遇到任何 `russh_sftp::Error`，此函数将返回错误。
/// 如果路径中的某个部分已经存在但是一个文件，此函数将会失败并返回错误。
pub async fn create_dir_all(sftp: &SftpSession, path: impl AsRef<Path>) -> Result<(), Error> {
    // 初始检查：如果最终目标路径存在但不是目录，则提前失败。
    let path = path.as_ref();
    if let Ok(attrs) = sftp.metadata(to_remote_path(path)).await {
        return if attrs.is_dir() {
            Ok(())
        } else {
            // 路径存在，但是一个文件，返回一个描述性错误。
            Err(Error::Status(Status {
                id: 0, // id 是请求ID，这里我们没有，因此使用占位符
                status_code: StatusCode::Failure,
                error_message: format!(
                    "a file with the same name already exists: {}",
                    path.display()
                ),
                language_tag: "en-US".to_string(),
            }))
        };
    }

    let ancestors: Vec<PathBuf> = path.ancestors().map(|p| p.to_path_buf()).collect();

    for p in ancestors.iter().rev() {
        if p.as_os_str().is_empty() || p.as_os_str() == "/" {
            continue;
        }

        match sftp.create_dir(to_remote_path(p)).await {
            Ok(_) => {} // 目录创建成功
            Err(e) => {
                // 检查错误是否为通用的 `Failure`，这可能意味着目录已存在。
                let is_failure = if let Error::Status(status) = &e {
                    status.status_code == StatusCode::Failure
                } else {
                    false
                };
                if is_failure {
                    // `create_dir` 失败，可能是因为目录已存在。我们用 `stat` 来确认。
                    match sftp.metadata(to_remote_path(p)).await {
                        Ok(attrs) => {
                            if !attrs.is_dir() {
                                // 路径上存在同名文件，这是一个不可恢复的错误。
                                return Err(Error::Status(Status {
                                    id: 0,
                                    status_code: StatusCode::Failure,
                                    error_message: format!(
                                        "path component is a file, not a directory: {}",
                                        p.display()
                                    ),
                                    language_tag: "en-US".to_string(),
                                }));
                            }
                            // 如果是目录，则没问题，可以安全地继续。
                        }
                        Err(_stat_err) => {
                            // 如果 `stat` 也失败了，说明确实存在问题，返回 `create_dir` 的原始错误。
                            return Err(e);
                        }
                    }
                } else {
                    // 如果是其他错误 (如 PermissionDenied)，则直接返回。
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}

/// 异步地递归删除一个目录及其所有内容。
///
/// 这个函数会遍历指定路径下的所有文件和子目录，并逐个删除它们，
/// 最后再删除该目录本身。
///
/// # Arguments
///
/// * `sftp` - 一个可变的 `SftpSession` 引用，用于执行 SFTP 命令。
/// * `path` - 需要删除的目录路径。
///
/// # Errors
///
/// - 如果路径存在但不是一个目录，将返回错误。
/// - 如果在删除文件或目录时遇到权限问题或其他 I/O 错误，将返回 `russh_sftp::Error`。
///
/// # Behavior
///
/// - 如果指定的路径不存在，函数会成功返回 `Ok(())`，因为目标状态（路径不存在）已经达成。
/// - 此函数使用 `lstat` 来检查路径属性，不会跟随符号链接。
pub async fn remove_dir_all(sftp: &SftpSession, path: impl AsRef<Path>) -> Result<(), Error> {
    let root = path.as_ref().to_path_buf();
    // 确认存在且为目录
    match sftp.metadata(to_remote_path(&root)).await {
        Ok(attrs) => {
            if !attrs.is_dir() {
                return Err(Error::Status(Status {
                    id: 0,
                    status_code: StatusCode::Failure,
                    error_message: format!("path is not a directory: {}", root.display()),
                    language_tag: "en-US".to_string(),
                }));
            }
        }
        Err(e) => {
            if let Error::Status(status) = &e {
                if status.status_code == StatusCode::NoSuchFile { return Ok(()); }
            }
            return Err(e);
        }
    }

    // 非递归 DFS：后序删除目录
    let mut stack: Vec<(PathBuf, bool)> = vec![(root.clone(), false)];
    while let Some((dir, visited)) = stack.pop() {
        if !visited {
            // 第一次遇到目录，压回已访问标记，然后处理子项
            stack.push((dir.clone(), true));
            let entries = match sftp.read_dir(to_remote_path(&dir)).await {
                Ok(e) => e,
                Err(e) => {
                    // 如果目录刚好被删了，忽略
                    if let Error::Status(status) = &e {
                        if status.status_code == StatusCode::NoSuchFile { continue; }
                    }
                    return Err(e);
                }
            };
            for entry in entries {
                let name = entry.file_name();
                if name == "." || name == ".." { continue; }
                let child = dir.join(name);
                if entry.metadata().is_dir() {
                    stack.push((child, false));
                } else {
                    let _ = sftp.remove_file(to_remote_path(&child)).await;
                }
            }
        } else {
            let _ = sftp.remove_dir(to_remote_path(&dir)).await;
        }
    }
    Ok(())
}
