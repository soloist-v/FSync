//! FSync storage layer – wraps `foyer` for per-task timestamp persistence.

use anyhow::Result;
use foyer::HybridCache;
use std::path::Path;

pub struct FoyerStore {
    inner: HybridCache<String, u64>,
}

impl FoyerStore {
    pub async fn open<P: AsRef<Path>>(memory_size: usize, cache_dir: P) -> Result<Self> {
        let inner = HybridCache::builder()
            .memory(memory_size)
            .storage()
            .build()
            .await?;
        Ok(Self { inner })
    }

    pub async fn get_u64(&self, key: &String) -> Result<Option<u64>> {
        match self.inner.get(key).await? {
            None => Ok(None),
            Some(a) => Ok(Some(*a.value())),
        }
    }

    pub fn put_u64(&self, key: String, val: u64) -> Result<()> {
        self.inner.insert(key, val);
        Ok(())
    }
}
