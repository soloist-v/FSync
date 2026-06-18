use anyhow::{anyhow, Result};
use fsync_core::{Pattern, TaskConfig, TaskState};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tracing_appender::rolling;
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

use crate::models::{
    absolute_path, cache_dir_for_config, default_task_cache_dir, path_text, placeholder_remote_cfg,
    remote_cfg_from_profile, AppConfig, AppState, LoadedTask, RemoteProfile, TaskView, CONFIG_PATH,
};
use crate::operation_logs::{OperationLogNotifier, OperationLogReader, OperationLogWriter};

#[derive(Clone)]
pub(crate) struct AppStorage {
    pub(crate) pool: SqlitePool,
    pub(crate) config: AppConfig,
    pub(crate) operation_log_writer: OperationLogWriter,
    pub(crate) operation_log_reader: OperationLogReader,
}

pub(crate) fn init_file_logging(config: &AppConfig) -> Result<()> {
    let log_dir = config.log_dir();
    fs::create_dir_all(&log_dir)?;
    let appender = rolling::daily(&log_dir, "fsync.log");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "fsync_core=info,fsync_remote_sftp=info,fsync_ui_egui=info,icu_provider=off,warn",
        )
    });
    let _ = fmt()
        .with_writer(appender)
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .try_init();
    Ok(())
}

pub(crate) fn load_app_config() -> Result<AppConfig> {
    let Ok(text) = fs::read_to_string(CONFIG_PATH) else {
        let mut config = AppConfig::default();
        config.normalize();
        persist_app_config(&config)?;
        return Ok(config);
    };
    let mut config = serde_yaml::from_str::<AppConfig>(&text)?;
    config.normalize();
    persist_app_config(&config)?;
    Ok(config)
}

pub(crate) fn persist_app_config(config: &AppConfig) -> Result<()> {
    fs::write(CONFIG_PATH, serde_yaml::to_string(config)?)?;
    Ok(())
}

pub(crate) async fn init_storage(config: AppConfig) -> Result<AppStorage> {
    if let Some(parent) = config.database_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::create_dir_all(&config.cache_dir)?;
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&sqlite_url(&config.database_path))
        .await?;
    migrate_database(&pool).await?;
    cleanup_orphan_task_caches(&pool, &config.cache_dir).await?;
    let operation_log_notifier = OperationLogNotifier::new();
    let operation_log_writer =
        OperationLogWriter::spawn(pool.clone(), operation_log_notifier.clone());
    let operation_log_reader = OperationLogReader::new(pool.clone());
    Ok(AppStorage {
        pool,
        config,
        operation_log_writer,
        operation_log_reader,
    })
}

pub(crate) fn load_config(
    runtime: &Runtime,
    storage: &AppStorage,
    state: &Arc<Mutex<AppState>>,
) -> Result<()> {
    let remote_profiles = runtime.block_on(read_remote_profiles(&storage.pool))?;
    let tasks = runtime.block_on(read_tasks(
        &storage.pool,
        &storage.config.cache_dir,
        &remote_profiles,
    ))?;
    let mut state = state.lock().unwrap();
    state.remote_profiles = remote_profiles;
    state.tasks = tasks
        .into_iter()
        .map(|task| TaskView {
            cfg: task.cfg,
            remote_profile_id: task.remote_profile_id,
            handle: None,
            log_rx: None,
            logs: task
                .recent_logs
                .iter()
                .map(|log| log.display_message())
                .collect(),
            last_operation_log_id: task
                .recent_logs
                .last()
                .map(|log| log.id)
                .unwrap_or_default(),
            state: TaskState::Idle,
            starting: false,
        })
        .collect();
    state.selected = if state.tasks.is_empty() {
        None
    } else {
        Some(0)
    };
    Ok(())
}

fn sqlite_url(path: &PathBuf) -> String {
    let path = path_text(path);
    format!("sqlite://{path}?mode=rwc")
}

async fn migrate_database(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS remote_profiles (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            remote_kind TEXT NOT NULL,
            sftp_host TEXT NOT NULL,
            sftp_user TEXT NOT NULL,
            sftp_password TEXT,
            sftp_key_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS remote_profile_fingerprints (
            profile_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (profile_id, position),
            FOREIGN KEY (profile_id) REFERENCES remote_profiles(id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_tasks (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            local_path TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            remote_profile_id TEXT,
            cache_dir TEXT,
            scan_ms INTEGER NOT NULL,
            size_filter TEXT,
            retry_max INTEGER NOT NULL,
            retry_backoff_ms INTEGER NOT NULL,
            debounce_ms INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (remote_profile_id) REFERENCES remote_profiles(id) ON DELETE SET NULL
        )
        "#,
    )
    .execute(pool)
    .await?;
    crate::operation_logs::migrate(pool).await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_task_filters (
            task_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('include', 'exclude')),
            pattern TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (task_id, kind, position),
            FOREIGN KEY (task_id) REFERENCES sync_tasks(id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn cleanup_orphan_task_caches(pool: &SqlitePool, cache_root: &PathBuf) -> Result<()> {
    let rows =
        sqlx::query_as::<_, (String, Option<String>)>("SELECT id, cache_dir FROM sync_tasks")
            .fetch_all(pool)
            .await?;
    let cache_root_abs = absolute_path(cache_root)?;
    let mut live_dirs = rows
        .into_iter()
        .filter_map(|(id, cache_dir)| {
            let dir = cache_dir
                .map(PathBuf::from)
                .unwrap_or_else(|| default_task_cache_dir(cache_root, &id));
            absolute_path(&dir).ok()
        })
        .filter(|dir| dir.starts_with(&cache_root_abs))
        .collect::<Vec<_>>();
    live_dirs.sort();
    live_dirs.dedup();

    for entry in fs::read_dir(cache_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let path_abs = absolute_path(&path)?;
        if !live_dirs.iter().any(|dir| dir == &path_abs) {
            tracing::info!(cache_dir = %path_text(&path), "removing orphan task cache");
            fs::remove_dir_all(&path)?;
        }
    }
    Ok(())
}

async fn read_remote_profiles(pool: &SqlitePool) -> Result<Vec<RemoteProfile>> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
        ),
    >(
        r#"
        SELECT id, name, remote_kind, sftp_host, sftp_user, sftp_password, sftp_key_path
        FROM remote_profiles
        ORDER BY rowid
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut profiles = Vec::with_capacity(rows.len());
    for (id, name, remote_kind, host, user, password, key_path) in rows {
        if remote_kind != "sftp" {
            return Err(anyhow!("unsupported remote kind: {remote_kind}"));
        }
        let fingerprints = sqlx::query_as::<_, (String,)>(
            "SELECT fingerprint FROM remote_profile_fingerprints WHERE profile_id = ?1 ORDER BY position",
        )
        .bind(&id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(fingerprint,)| fingerprint)
        .collect::<Vec<_>>();

        profiles.push(RemoteProfile {
            id: Uuid::parse_str(&id)?,
            name,
            host,
            user,
            password,
            key: key_path.map(PathBuf::from),
            fingerprints,
        });
    }
    Ok(profiles)
}

async fn read_tasks(
    pool: &SqlitePool,
    cache_root: &PathBuf,
    remote_profiles: &[RemoteProfile],
) -> Result<Vec<LoadedTask>> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
            Option<String>,
            i64,
            i64,
            i64,
        ),
    >(
        r#"
        SELECT id, name, local_path, remote_path, remote_profile_id, cache_dir, scan_ms,
               size_filter, retry_max, retry_backoff_ms, debounce_ms
        FROM sync_tasks
        ORDER BY rowid
        "#,
    )
    .fetch_all(pool)
    .await?;

    let profiles_by_id = remote_profiles
        .iter()
        .map(|profile| (profile.id, profile.clone()))
        .collect::<HashMap<_, _>>();
    let mut tasks = Vec::with_capacity(rows.len());
    for (
        id,
        name,
        local_path,
        remote_path,
        remote_profile_id,
        cache_dir,
        scan_ms,
        size_filter,
        retry_max,
        retry_backoff_ms,
        debounce_ms,
    ) in rows
    {
        let filter_rows = sqlx::query_as::<_, (String, String)>(
            "SELECT kind, pattern FROM sync_task_filters WHERE task_id = ?1 ORDER BY kind, position",
        )
        .bind(&id)
        .fetch_all(pool)
        .await?;
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        for (kind, pattern) in filter_rows {
            match kind.as_str() {
                "include" => include.push(Pattern(pattern)),
                "exclude" => exclude.push(Pattern(pattern)),
                _ => return Err(anyhow!("unsupported filter kind: {kind}")),
            }
        }
        let remote_profile_id = remote_profile_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()?;
        let remote_cfg = remote_profile_id
            .and_then(|id| profiles_by_id.get(&id).cloned())
            .map(|profile| remote_cfg_from_profile(&profile))
            .unwrap_or_else(placeholder_remote_cfg);
        let cache_dir = cache_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| default_task_cache_dir(cache_root, &id));
        tasks.push(LoadedTask {
            cfg: TaskConfig {
                id: Uuid::parse_str(&id)?,
                name,
                local: PathBuf::from(local_path),
                remote: remote_path,
                cache_dir: Some(cache_dir),
                include,
                exclude,
                scan_ms: scan_ms.try_into()?,
                size: size_filter,
                retry_max: retry_max.try_into()?,
                retry_backoff_ms: retry_backoff_ms.try_into()?,
                debounce_ms: debounce_ms.try_into()?,
                remote_cfg,
            },
            remote_profile_id,
            recent_logs: OperationLogReader::new(pool.clone())
                .read_recent(&id, 1_000)
                .await?,
        });
    }
    Ok(tasks)
}

pub(crate) async fn save_state(
    storage: &AppStorage,
    remote_profiles: &[RemoteProfile],
    tasks: &[LoadedTask],
) -> Result<()> {
    replace_state(
        &storage.pool,
        &storage.config.cache_dir,
        remote_profiles,
        tasks,
    )
    .await
}

async fn replace_state(
    pool: &SqlitePool,
    cache_root: &PathBuf,
    remote_profiles: &[RemoteProfile],
    tasks: &[LoadedTask],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM sync_task_filters")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM sync_tasks")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM remote_profile_fingerprints")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM remote_profiles")
        .execute(&mut *tx)
        .await?;

    for profile in remote_profiles {
        sqlx::query(
            r#"
            INSERT INTO remote_profiles (
                id, name, remote_kind, sftp_host, sftp_user, sftp_password, sftp_key_path, updated_at
            )
            VALUES (?1, ?2, 'sftp', ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP)
            "#,
        )
        .bind(profile.id.to_string())
        .bind(&profile.name)
        .bind(&profile.host)
        .bind(&profile.user)
        .bind(&profile.password)
        .bind(profile.key.as_ref().map(|path| path_text(path)))
        .execute(&mut *tx)
        .await?;

        for (position, fingerprint) in profile.fingerprints.iter().enumerate() {
            sqlx::query(
                "INSERT INTO remote_profile_fingerprints (profile_id, fingerprint, position) VALUES (?1, ?2, ?3)",
            )
            .bind(profile.id.to_string())
            .bind(fingerprint)
            .bind(i64::try_from(position)?)
            .execute(&mut *tx)
            .await?;
        }
    }

    for task in tasks {
        let cfg = &task.cfg;
        sqlx::query(
            r#"
            INSERT INTO sync_tasks (
                id, name, local_path, remote_path, remote_profile_id, cache_dir, scan_ms, size_filter,
                retry_max, retry_backoff_ms, debounce_ms, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, CURRENT_TIMESTAMP)
            "#,
        )
        .bind(cfg.id.to_string())
        .bind(&cfg.name)
        .bind(path_text(&cfg.local))
        .bind(&cfg.remote)
        .bind(task.remote_profile_id.map(|id| id.to_string()))
        .bind(path_text(&cache_dir_for_config(cfg, cache_root)))
        .bind(i64::try_from(cfg.scan_ms)?)
        .bind(&cfg.size)
        .bind(i64::from(cfg.retry_max))
        .bind(i64::try_from(cfg.retry_backoff_ms)?)
        .bind(i64::try_from(cfg.debounce_ms)?)
        .execute(&mut *tx)
        .await?;

        for (position, pattern) in cfg.include.iter().enumerate() {
            sqlx::query(
                "INSERT INTO sync_task_filters (task_id, kind, pattern, position) VALUES (?1, 'include', ?2, ?3)",
            )
            .bind(cfg.id.to_string())
            .bind(&pattern.0)
            .bind(i64::try_from(position)?)
            .execute(&mut *tx)
            .await?;
        }
        for (position, pattern) in cfg.exclude.iter().enumerate() {
            sqlx::query(
                "INSERT INTO sync_task_filters (task_id, kind, pattern, position) VALUES (?1, 'exclude', ?2, ?3)",
            )
            .bind(cfg.id.to_string())
            .bind(&pattern.0)
            .bind(i64::try_from(position)?)
            .execute(&mut *tx)
            .await?;
        }
    }

    sqlx::query("DELETE FROM task_operation_logs WHERE task_id NOT IN (SELECT id FROM sync_tasks)")
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

pub(crate) fn open_local_dir(path: PathBuf) -> Result<()> {
    let path = path.canonicalize().unwrap_or(path);
    if !path.exists() {
        return Err(anyhow!("path does not exist: {}", path_text(&path)));
    }

    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("explorer");
        command.arg(&path);
        command
    };

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(&path);
        command
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(&path);
        command
    };

    command.spawn()?;
    Ok(())
}
