use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::Path;

/// Runtime filter compiled from include / exclude pattern lists.
#[derive(Debug, Clone)]
pub struct PathFilter {
    include: GlobSet,
    exclude: GlobSet,
}

impl PathFilter {
    /// Build a filter from lists. Empty include list means "include all".
    pub fn new(include: &[crate::config::Pattern], exclude: &[crate::config::Pattern]) -> Self {
        let mut inc_builder = GlobSetBuilder::new();
        let mut exc_builder = GlobSetBuilder::new();
        // compile patterns, ignore compile errors individually
        for pat in include {
            if let Ok(g) = Glob::new(&pat.0) {
                inc_builder.add(g);
            }
        }
        for pat in exclude {
            if let Ok(g) = Glob::new(&pat.0) {
                exc_builder.add(g);
            }
        }
        Self {
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
        let path = path.as_ref();
        let included = self.include.len() == 0 || self.include.is_match(path);
        let excluded = self.exclude.is_match(path);
        included && !excluded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Pattern;

    #[test]
    fn test_filter_basic() {
        let include = vec![Pattern("**/*.rs".into())];
        let exclude = vec![Pattern("tests/**".into())];
        let filter = PathFilter::new(&include, &exclude);
        assert!(filter.check("src/lib.rs"));
        assert!(!filter.check("tests/main.rs"));
        assert!(!filter.check("README.md"));
    }
}
