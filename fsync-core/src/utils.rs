use std::path::PathBuf;
pub fn as_posix_path(path: &PathBuf) -> String {
    path.to_string_lossy().replace("\\", "/")
}
pub fn remote_path(local: &PathBuf, remote: &str, local_path: &PathBuf) -> String {
    let rel = local_path.strip_prefix(local).unwrap_or(local_path);
    let s = PathBuf::from(remote)
        .join(rel)
        .to_string_lossy()
        .to_string();
    s.replace('\\', "/")
}
