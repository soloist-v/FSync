use std::path::{Component, Path};

pub fn as_posix_path(path: &Path) -> String {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                parts.push(prefix.as_os_str().to_string_lossy().replace('\\', "/"));
            }
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => parts.push("..".into()),
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
        }
    }

    strip_windows_extended_prefix(&parts.join("/").replace('\\', "/"))
}

pub fn normalize_posix_path_str(path: &str) -> String {
    strip_windows_extended_prefix(&path.replace('\\', "/"))
}

pub fn normalize_key_path(path: &Path) -> String {
    let path = as_posix_path(path);
    if cfg!(windows) {
        path.to_ascii_lowercase()
    } else {
        path
    }
}

pub fn relative_posix_path(path: &Path, root: &Path) -> Option<String> {
    relative_posix_path_str(&normalize_key_path(path), &normalize_key_path(root))
}

pub fn relative_posix_path_str(path: &str, root: &str) -> Option<String> {
    let path = path.trim_end_matches('/');
    let root = root.trim_end_matches('/');
    if path == root {
        return Some(String::new());
    }
    path.strip_prefix(root)
        .and_then(|rest| rest.strip_prefix('/'))
        .map(str::to_string)
}

pub fn join_posix_path(root: &str, relative: &str) -> String {
    let root = normalize_posix_path_str(root)
        .trim_end_matches('/')
        .to_string();
    let relative = normalize_posix_path_str(relative)
        .trim_start_matches('/')
        .to_string();
    if relative.is_empty() {
        root
    } else {
        format!("{root}/{relative}")
    }
}

pub fn display_posix_path(path: &Path) -> String {
    as_posix_path(path)
}

fn strip_windows_extended_prefix(path: &str) -> String {
    path.strip_prefix("//?/")
        .or_else(|| path.strip_prefix("\\\\?\\"))
        .unwrap_or(path)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posix_path_uses_forward_slashes() {
        assert_eq!(
            normalize_posix_path_str(r"\\?\D:\Workspace\mix\file.txt"),
            "D:/Workspace/mix/file.txt"
        );
    }

    #[test]
    fn relative_path_uses_posix_separators() {
        assert_eq!(
            relative_posix_path(
                Path::new(r"\\?\D:\Workspace\mix\ultralytics\tests\a.py"),
                Path::new(r"D:\Workspace\mix\ultralytics")
            ),
            Some("tests/a.py".into())
        );
    }
}
