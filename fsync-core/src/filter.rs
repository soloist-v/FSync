use crate::utils::{normalize_key_path, relative_posix_path_str};
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::Path;

/// Runtime filter compiled from include / exclude pattern lists.
#[derive(Debug, Clone)]
pub struct PathFilter {
    root: String,
    include: GlobSet,
    exclude: GlobSet,
}

impl PathFilter {
    /// Build a filter from lists. Empty include list means "include all".
    pub fn new<P: AsRef<Path>>(
        root: P,
        include: &[crate::config::Pattern],
        exclude: &[crate::config::Pattern],
    ) -> Self {
        let mut inc_builder = GlobSetBuilder::new();
        let mut exc_builder = GlobSetBuilder::new();
        for pat in include {
            add_pattern(&mut inc_builder, &pat.0);
        }
        for pat in exclude {
            add_pattern(&mut exc_builder, &pat.0);
        }
        Self {
            root: normalize_key_path(root.as_ref()),
            include: inc_builder
                .build()
                .unwrap_or_else(|_| GlobSetBuilder::new().build().unwrap()),
            exclude: exc_builder
                .build()
                .unwrap_or_else(|_| GlobSetBuilder::new().build().unwrap()),
        }
    }

    /// Determine whether a given path should be synced.
    pub fn check<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = self.match_path(path.as_ref());
        let included = self.include.len() == 0 || self.include.is_match(path.as_str());
        let excluded = self.exclude.is_match(path.as_str());
        included && !excluded
    }

    /// Determine whether a directory subtree should be traversed or created.
    ///
    /// Include patterns are intentionally not applied here: a directory can fail
    /// an include rule while still containing files that pass it. Exclude
    /// patterns do apply to the directory itself and to synthetic descendants so
    /// patterns such as `**/__pycache__/**` prune the `__pycache__` directory.
    pub fn check_dir<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = self.match_path(path.as_ref());
        !self.excludes_dir(&path)
    }

    fn match_path(&self, path: &Path) -> String {
        let path = normalize_key_path(path);
        relative_posix_path_str(&path, &self.root).unwrap_or(path)
    }

    fn excludes_dir(&self, path: &str) -> bool {
        if self.exclude.is_match(path) {
            return true;
        }

        let path = path.trim_end_matches('/');
        self.exclude.is_match(format!("{path}/").as_str())
            || self
                .exclude
                .is_match(format!("{path}/__fsync_probe__").as_str())
    }
}

fn add_pattern(builder: &mut GlobSetBuilder, pattern: &str) {
    for pattern in expand_pattern(pattern) {
        if let Ok(glob) = Glob::new(&pattern) {
            builder.add(glob);
        }
    }
}

fn expand_pattern(pattern: &str) -> Vec<String> {
    let pattern = pattern.trim().replace('\\', "/");
    if pattern.is_empty() {
        return Vec::new();
    }

    if !pattern.ends_with('/') {
        return vec![pattern];
    }

    let base = pattern.trim_end_matches('/').trim_start_matches("./");
    if base.is_empty() {
        return Vec::new();
    }

    let mut expanded = vec![base.to_string(), format!("{base}/**")];
    if !base.contains('/') {
        expanded.push(format!("**/{base}"));
        expanded.push(format!("**/{base}/**"));
    }
    expanded.sort();
    expanded.dedup();
    expanded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Pattern;

    #[test]
    fn test_filter_basic() {
        let include = vec![Pattern("**/*.rs".into())];
        let exclude = vec![Pattern("tests/**".into())];
        let filter = PathFilter::new(".", &include, &exclude);
        assert!(filter.check("src/lib.rs"));
        assert!(!filter.check("tests/main.rs"));
        assert!(!filter.check("README.md"));
    }

    #[test]
    fn exclude_matches_relative_path_not_absolute_parent_path() {
        let exclude = vec![Pattern("*data/*".into())];
        let filter = PathFilter::new("D:/Workspace/mix/ultralytics", &[], &exclude);

        assert!(filter.check("D:/Workspace/mix/ultralytics/tests/dasdas.py"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/data/sample.txt"));
    }

    #[test]
    fn excludes_nested_cache_directories_by_relative_path() {
        let exclude = vec![Pattern("*__pycache__/*".into())];
        let filter = PathFilter::new("D:/Workspace/mix/ultralytics", &[], &exclude);

        assert!(filter.check("D:/Workspace/mix/ultralytics/tests/dasdas.py"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/tests/__pycache__/dasdas.pyc"));
    }

    #[test]
    fn extended_length_windows_paths_still_match_relative_root() {
        let exclude = vec![Pattern("data/*".into())];
        let filter = PathFilter::new("D:/Workspace/mix/ultralytics", &[], &exclude);

        assert!(filter.check(r"\\?\D:\Workspace\mix\ultralytics\tests\dasdas.py"));
        assert!(!filter.check(r"\\?\D:\Workspace\mix\ultralytics\data\sample.txt"));
    }

    #[test]
    fn recommended_directory_excludes_match_nested_children() {
        let exclude = vec![
            Pattern(".venv/**".into()),
            Pattern("runs/**".into()),
            Pattern("output/**".into()),
            Pattern("data/**".into()),
            Pattern(".git/**".into()),
            Pattern(".pytest_cache/**".into()),
            Pattern("**/__pycache__/**".into()),
            Pattern("**/dataset/**".into()),
        ];
        let filter = PathFilter::new("D:/Workspace/mix/ultralytics", &[], &exclude);

        assert!(filter.check("D:/Workspace/mix/ultralytics/tests/dasdas.py"));
        assert!(!filter
            .check("D:/Workspace/mix/ultralytics/ultralytics/utils/__pycache__/torch_utils.pyc"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/data/images/a.jpg"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/dataset/images/a.jpg"));
    }

    #[test]
    fn recommended_directory_excludes_prune_directory_itself() {
        let exclude = vec![
            Pattern(".venv/**".into()),
            Pattern("runs/**".into()),
            Pattern("output/**".into()),
            Pattern("data/**".into()),
            Pattern(".git/**".into()),
            Pattern(".pytest_cache/**".into()),
            Pattern("**/__pycache__/**".into()),
            Pattern("**/dataset/**".into()),
            Pattern("**/.git/**".into()),
        ];
        let filter = PathFilter::new("D:/Workspace/mix/ultralytics", &[], &exclude);

        assert!(filter.check_dir("D:/Workspace/mix/ultralytics/tests"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/.venv"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/data"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/tests/__pycache__"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/foo/dataset"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/foo/.git"));
    }

    #[test]
    fn trailing_slash_excludes_directory_and_children() {
        let exclude = vec![
            Pattern(".venv/".into()),
            Pattern("runs/".into()),
            Pattern("output/".into()),
            Pattern("data/".into()),
            Pattern(".git/".into()),
            Pattern(".pytest_cache/".into()),
            Pattern("__pycache__/".into()),
            Pattern("dataset/".into()),
        ];
        let filter = PathFilter::new("D:/Workspace/mix/ultralytics", &[], &exclude);

        assert!(filter.check("D:/Workspace/mix/ultralytics/tests/dfdfd.py"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/.venv/pyvenv.cfg"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/foo/.venv/pyvenv.cfg"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/tests/__pycache__/dfdfd.pyc"));
        assert!(!filter.check("D:/Workspace/mix/ultralytics/foo/dataset/image.jpg"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/tests/__pycache__"));
        assert!(!filter.check_dir("D:/Workspace/mix/ultralytics/foo/dataset"));
    }

    #[test]
    fn trailing_slash_with_nested_path_excludes_relative_subtree() {
        let exclude = vec![Pattern("foo/bar/".into())];
        let filter = PathFilter::new("D:/Workspace/mix/project", &[], &exclude);

        assert!(!filter.check("D:/Workspace/mix/project/foo/bar/a.txt"));
        assert!(!filter.check_dir("D:/Workspace/mix/project/foo/bar"));
        assert!(filter.check("D:/Workspace/mix/project/other/foo/bar/a.txt"));
        assert!(filter.check_dir("D:/Workspace/mix/project/other/foo/bar"));
    }
}
