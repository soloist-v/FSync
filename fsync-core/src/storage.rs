//! SQLite-backed timestamp storage for per-task sync state.

use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

const STATE_DB_FILE: &str = "state.db";

#[derive(Clone)]
pub struct StateStore {
    pool: SqlitePool,
}

impl StateStore {
    pub async fn open<P: AsRef<Path>>(_memory_size: usize, cache_dir: P) -> Result<Self> {
        let cache_dir = cache_dir.as_ref();
        fs::create_dir_all(cache_dir)?;
        let db_path = cache_dir.join(STATE_DB_FILE);
        tracing::info!(db_path = %db_path.display(), "opening sqlite state store");

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&sqlite_url(&db_path))
            .await?;

        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&pool)
            .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS file_states (
                local_path TEXT PRIMARY KEY NOT NULL,
                mtime INTEGER NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&pool)
        .await?;

        tracing::info!(db_path = %db_path.display(), "sqlite state store opened");
        Ok(Self { pool })
    }

    pub async fn get_u64(&self, key: &String) -> Result<Option<u64>> {
        let row =
            sqlx::query_as::<_, (i64,)>("SELECT mtime FROM file_states WHERE local_path = ?1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(mtime,)| mtime as u64))
    }

    pub async fn put_u64(&self, key: String, val: u64) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO file_states (local_path, mtime, updated_at)
            VALUES (?1, ?2, CURRENT_TIMESTAMP)
            ON CONFLICT(local_path) DO UPDATE SET
                mtime = excluded.mtime,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(key)
        .bind(i64::try_from(val)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn remove_u64(&self, key: &String) -> Result<()> {
        sqlx::query("DELETE FROM file_states WHERE local_path = ?1")
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn remove_tree(&self, root: &String) -> Result<usize> {
        let rows = sqlx::query_as::<_, (String,)>("SELECT local_path FROM file_states")
            .fetch_all(&self.pool)
            .await?;
        let stale = rows
            .into_iter()
            .map(|(key,)| key)
            .filter(|key| path_is_self_or_child(key, root))
            .collect::<Vec<_>>();

        if stale.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        for key in &stale {
            sqlx::query("DELETE FROM file_states WHERE local_path = ?1")
                .bind(key)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(stale.len())
    }

    pub async fn cleanup_missing(&self, live_keys: &HashSet<String>) -> Result<usize> {
        let rows = sqlx::query_as::<_, (String,)>("SELECT local_path FROM file_states")
            .fetch_all(&self.pool)
            .await?;
        let stale = rows
            .into_iter()
            .map(|(key,)| key)
            .filter(|key| !live_keys.contains(key))
            .collect::<Vec<_>>();

        if stale.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        for key in &stale {
            sqlx::query("DELETE FROM file_states WHERE local_path = ?1")
                .bind(key)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(stale.len())
    }

    pub async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

fn sqlite_url(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    format!("sqlite://{path}?mode=rwc")
}

fn path_is_self_or_child(path: &str, root: &str) -> bool {
    let path = normalize_state_path(path);
    let root = normalize_state_path(root);
    path == root
        || path
            .strip_prefix(&root)
            .map(|rest| rest.starts_with('/') || rest.starts_with('\\'))
            .unwrap_or(false)
}

fn normalize_state_path(path: &str) -> String {
    let path = path.strip_prefix(r"\\?\").unwrap_or(path);
    if cfg!(windows) {
        path.to_ascii_lowercase()
    } else {
        path.to_string()
    }
}
