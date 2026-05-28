use anyhow::{anyhow, Result};
use fsync_core::{spawn_task, Pattern, RemoteCfg, SyncTaskHandle, TaskConfig, TaskLog, TaskState};
use fsync_remote_sftp::SftpRemote;
use serde::{Deserialize, Serialize};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

slint::include_modules!();

const CONFIG_PATH: &str = "config.yaml";
const DEFAULT_DATABASE_PATH: &str = "fsync.db";
const DEFAULT_CACHE_DIR: &str = "data/cache";

#[derive(Clone)]
struct AppStorage {
    pool: SqlitePool,
    config: AppConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppConfig {
    #[serde(default = "AppConfig::default_database_path")]
    database_path: PathBuf,
    #[serde(default = "AppConfig::default_cache_dir")]
    cache_dir: PathBuf,
    #[serde(default)]
    theme: Option<String>,
    #[serde(default)]
    font_size: Option<u32>,
    #[serde(default)]
    log_dir: Option<PathBuf>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_path: PathBuf::from(DEFAULT_DATABASE_PATH),
            cache_dir: PathBuf::from(DEFAULT_CACHE_DIR),
            theme: None,
            font_size: None,
            log_dir: None,
        }
    }
}

impl AppConfig {
    fn default_database_path() -> PathBuf {
        PathBuf::from(DEFAULT_DATABASE_PATH)
    }

    fn default_cache_dir() -> PathBuf {
        PathBuf::from(DEFAULT_CACHE_DIR)
    }
}

struct TaskView {
    cfg: TaskConfig,
    handle: Option<Arc<SyncTaskHandle>>,
    log_rx: Option<broadcast::Receiver<TaskLog>>,
    logs: Vec<String>,
    state: TaskState,
    starting: bool,
}

#[derive(Default)]
struct AppState {
    tasks: Vec<TaskView>,
    selected: Option<usize>,
}

#[derive(Clone)]
struct Draft {
    id: Uuid,
    name: String,
    local: String,
    remote: String,
    cache_dir: String,
    include: String,
    exclude: String,
    size: String,
    scan_ms: String,
    debounce_ms: String,
    retry_max: String,
    retry_backoff_ms: String,
    host: String,
    user: String,
    password: String,
}

fn main() -> Result<()> {
    init_console_logging();
    let runtime = Arc::new(Runtime::new()?);
    let ui = MainWindow::new()?;
    let state = Arc::new(Mutex::new(AppState::default()));
    let storage = Arc::new(runtime.block_on(init_storage())?);

    load_config(&ui, &state, &storage, &runtime);
    wire_callbacks(&ui, &state, &runtime, &storage);

    ui.run()?;
    Ok(())
}

fn init_console_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "fsync_core=info,fsync_remote_sftp=info,fsync_ui_slint=info,icu_provider=off,warn",
        )
    });
    let _ = fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .try_init();
}

fn wire_callbacks(
    ui: &MainWindow,
    state: &Arc<Mutex<AppState>>,
    runtime: &Arc<Runtime>,
    storage: &Arc<AppStorage>,
) {
    {
        let ui = ui.as_weak();
        let state = state.clone();
        let runtime = runtime.clone();
        let storage = storage.clone();
        ui.unwrap().on_reload_config(move || {
            if let Some(ui) = ui.upgrade() {
                load_config(&ui, &state, &storage, &runtime);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_select_task(move |idx| {
            if let Some(ui) = ui.upgrade() {
                select_task(&ui, &state, idx as usize, false);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_edit_task(move |idx| {
            if let Some(ui) = ui.upgrade() {
                select_task(&ui, &state, idx as usize, true);
            }
        });
    }

    {
        let ui = ui.as_weak();
        ui.unwrap().on_show_config(move || {
            if let Some(ui) = ui.upgrade() {
                ui.set_logs_tab(false);
            }
        });
    }

    {
        let ui = ui.as_weak();
        ui.unwrap().on_show_logs(move || {
            if let Some(ui) = ui.upgrade() {
                ui.set_logs_tab(true);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        let storage = storage.clone();
        ui.unwrap().on_new_task(move || {
            if let Some(ui) = ui.upgrade() {
                let mut state_ref = state.lock().unwrap();
                state_ref.tasks.push(TaskView {
                    cfg: sample_config(&storage.config.cache_dir),
                    handle: None,
                    log_rx: None,
                    logs: Vec::new(),
                    state: TaskState::Idle,
                    starting: false,
                });
                state_ref.selected = Some(state_ref.tasks.len() - 1);
                drop(state_ref);
                sync_all(&ui, &state);
                ui.set_logs_tab(false);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_duplicate_task(move |idx| {
            if let Some(ui) = ui.upgrade() {
                duplicate_task(&ui, &state, idx as usize);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_delete_task(move |idx| {
            if let Some(ui) = ui.upgrade() {
                delete_task(&ui, &state, idx as usize);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_apply_draft(move || {
            if let Some(ui) = ui.upgrade() {
                apply_draft(&ui, &state);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        let runtime = runtime.clone();
        let storage = storage.clone();
        ui.unwrap().on_save_config(move || {
            if let Some(ui) = ui.upgrade() {
                apply_draft(&ui, &state);
                let configs = state
                    .lock()
                    .unwrap()
                    .tasks
                    .iter()
                    .map(|task| task.cfg.clone())
                    .collect::<Vec<_>>();
                match runtime.block_on(save_config(&storage, &configs)) {
                    Ok(()) => ui.set_toast(
                        format!(
                            "Saved {} task(s) to {}",
                            configs.len(),
                            storage.config.database_path.display()
                        )
                        .into(),
                    ),
                    Err(e) => ui.set_toast(format!("Save failed: {e}").into()),
                }
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        let runtime = runtime.clone();
        ui.unwrap().on_toggle_task(move |idx| {
            if let Some(ui) = ui.upgrade() {
                toggle_task(ui.as_weak(), &state, &runtime, idx as usize);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        let runtime = runtime.clone();
        ui.unwrap().on_start_all(move || {
            if let Some(ui) = ui.upgrade() {
                let len = state.lock().unwrap().tasks.len();
                for idx in 0..len {
                    toggle_task(ui.as_weak(), &state, &runtime, idx);
                }
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_stop_all(move || {
            if let Some(ui) = ui.upgrade() {
                for task in &mut state.lock().unwrap().tasks {
                    if let Some(handle) = &task.handle {
                        handle.stop();
                    }
                    task.starting = false;
                }
                sync_all(&ui, &state);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        ui.unwrap().on_open_local(move |idx| {
            if let Some(ui) = ui.upgrade() {
                open_task_local(&ui, &state, idx as usize);
            }
        });
    }

    {
        let ui = ui.as_weak();
        let state = state.clone();
        let runtime = runtime.clone();
        ui.unwrap().on_task_menu_action(move |idx, action| {
            if let Some(ui) = ui.upgrade() {
                let idx = idx as usize;
                match action.as_str() {
                    "open" => open_task_local(&ui, &state, idx),
                    "duplicate" => duplicate_task(&ui, &state, idx),
                    "delete" => delete_task(&ui, &state, idx),
                    "toggle" => toggle_task(ui.as_weak(), &state, &runtime, idx),
                    other => ui.set_toast(format!("Unknown task menu action: {other}").into()),
                }
            }
        });
    }

    start_log_pump(ui.as_weak(), state.clone());
}

fn load_config(
    ui: &MainWindow,
    state: &Arc<Mutex<AppState>>,
    storage: &AppStorage,
    runtime: &Runtime,
) {
    match runtime.block_on(read_config(storage)) {
        Ok(configs) => {
            let mut state_ref = state.lock().unwrap();
            state_ref.tasks = configs
                .into_iter()
                .map(|cfg| TaskView {
                    cfg,
                    handle: None,
                    log_rx: None,
                    logs: Vec::new(),
                    state: TaskState::Idle,
                    starting: false,
                })
                .collect();
            let task_count = state_ref.tasks.len();
            state_ref.selected = if state_ref.tasks.is_empty() {
                None
            } else {
                Some(0)
            };
            drop(state_ref);
            sync_all(ui, state);
            tracing::info!(
                task_count,
                database_path = %storage.config.database_path.display(),
                "tasks loaded"
            );
        }
        Err(e) => {
            state.lock().unwrap().tasks.clear();
            sync_all(ui, state);
            ui.set_toast(format!("Load failed: {e}").into());
        }
    }
}

async fn init_storage() -> Result<AppStorage> {
    let config = load_app_config()?;
    if let Some(parent) = config.database_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::create_dir_all(&config.cache_dir)?;
    let database_url = sqlite_url(&config.database_path);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    migrate_database(&pool).await?;
    cleanup_orphan_task_caches(&pool, &config.cache_dir).await?;

    Ok(AppStorage { pool, config })
}

fn load_app_config() -> Result<AppConfig> {
    let Ok(text) = fs::read_to_string(CONFIG_PATH) else {
        let config = AppConfig::default();
        fs::write(CONFIG_PATH, serde_yaml::to_string(&config)?)?;
        return Ok(config);
    };

    let config = serde_yaml::from_str::<AppConfig>(&text)?;
    fs::write(CONFIG_PATH, serde_yaml::to_string(&config)?)?;
    Ok(config)
}

fn sqlite_url(path: &PathBuf) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    format!("sqlite://{path}?mode=rwc")
}

async fn migrate_database(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            local_path TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            cache_dir TEXT,
            scan_ms INTEGER NOT NULL,
            size_filter TEXT,
            retry_max INTEGER NOT NULL,
            retry_backoff_ms INTEGER NOT NULL,
            debounce_ms INTEGER NOT NULL,
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

    let _ = sqlx::query("ALTER TABLE tasks ADD COLUMN cache_dir TEXT")
        .execute(pool)
        .await;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS task_filters (
            task_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('include', 'exclude')),
            pattern TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (task_id, kind, position),
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS task_fingerprints (
            task_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (task_id, position),
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn cleanup_orphan_task_caches(pool: &SqlitePool, cache_root: &PathBuf) -> Result<()> {
    let rows = sqlx::query_as::<_, (String, Option<String>)>("SELECT id, cache_dir FROM tasks")
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
            tracing::info!(cache_dir = %path.display(), "removing orphan task cache");
            fs::remove_dir_all(&path)?;
        }
    }

    Ok(())
}

async fn read_config(storage: &AppStorage) -> Result<Vec<TaskConfig>> {
    read_tasks(&storage.pool, &storage.config.cache_dir).await
}

async fn read_tasks(pool: &SqlitePool, cache_root: &PathBuf) -> Result<Vec<TaskConfig>> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            Option<String>,
            i64,
            Option<String>,
            i64,
            i64,
            i64,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
        ),
    >(
        r#"
        SELECT
            id,
            name,
            local_path,
            remote_path,
            cache_dir,
            scan_ms,
            size_filter,
            retry_max,
            retry_backoff_ms,
            debounce_ms,
            remote_kind,
            sftp_host,
            sftp_user,
            sftp_password,
            sftp_key_path
        FROM tasks
        ORDER BY rowid
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut tasks = Vec::with_capacity(rows.len());
    for (
        id,
        name,
        local_path,
        remote_path,
        cache_dir,
        scan_ms,
        size_filter,
        retry_max,
        retry_backoff_ms,
        debounce_ms,
        remote_kind,
        sftp_host,
        sftp_user,
        sftp_password,
        sftp_key_path,
    ) in rows
    {
        if remote_kind != "sftp" {
            return Err(anyhow!("unsupported remote kind: {remote_kind}"));
        }

        let filter_rows = sqlx::query_as::<_, (String, String)>(
            "SELECT kind, pattern FROM task_filters WHERE task_id = ?1 ORDER BY kind, position",
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

        let fingerprints = sqlx::query_as::<_, (String,)>(
            "SELECT fingerprint FROM task_fingerprints WHERE task_id = ?1 ORDER BY position",
        )
        .bind(&id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(fingerprint,)| fingerprint)
        .collect::<Vec<_>>();

        let cache_dir = cache_dir
            .map(PathBuf::from)
            .map(|path| migrate_legacy_default_cache_dir(cache_root, &id, path))
            .unwrap_or_else(|| default_task_cache_dir(cache_root, &id));

        tasks.push(TaskConfig {
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
            remote_cfg: RemoteCfg::Sftp {
                host: sftp_host,
                user: sftp_user,
                password: sftp_password,
                key: sftp_key_path.map(PathBuf::from),
                fingerprints: if fingerprints.is_empty() {
                    None
                } else {
                    Some(fingerprints)
                },
            },
        });
    }

    Ok(tasks)
}

fn sync_all(ui: &MainWindow, state: &Arc<Mutex<AppState>>) {
    sync_tasks(ui, state);
    sync_selected(ui, state);
}

fn sync_tasks(ui: &MainWindow, state: &Arc<Mutex<AppState>>) {
    let state = state.lock().unwrap();
    let selected = state.selected;
    let rows = state
        .tasks
        .iter()
        .enumerate()
        .map(|(idx, task)| TaskItem {
            name: task.cfg.name.clone().into(),
            local: task.cfg.local.display().to_string().into(),
            remote: task.cfg.remote.clone().into(),
            status: if task.starting && task.handle.is_none() {
                "Connecting to SFTP".into()
            } else {
                state_label(&task.state).into()
            },
            selected: selected == Some(idx),
            running: matches!(task.state, TaskState::Running),
            starting: task.starting || matches!(task.state, TaskState::Starting(_)),
        })
        .collect::<Vec<_>>();
    ui.set_tasks(ModelRc::new(VecModel::from(rows)));
    ui.set_selected_index(selected.map(|idx| idx as i32).unwrap_or(-1));
}

fn sync_selected(ui: &MainWindow, state: &Arc<Mutex<AppState>>) {
    let state_ref = state.lock().unwrap();
    let Some(idx) = state_ref.selected else {
        ui.set_logs(ModelRc::new(VecModel::from(Vec::<SharedString>::new())));
        return;
    };
    let Some(task) = state_ref.tasks.get(idx) else {
        return;
    };
    let draft = Draft::from_config(&task.cfg);
    set_draft(ui, &draft);
    ui.set_logs(ModelRc::new(VecModel::from(
        task.logs
            .iter()
            .cloned()
            .map(Into::into)
            .collect::<Vec<_>>(),
    )));
}

fn select_task(ui: &MainWindow, state: &Arc<Mutex<AppState>>, idx: usize, config_tab: bool) {
    state.lock().unwrap().selected = Some(idx);
    sync_all(ui, state);
    ui.set_logs_tab(!config_tab);
}

fn apply_draft(ui: &MainWindow, state: &Arc<Mutex<AppState>>) {
    let Some(idx) = state.lock().unwrap().selected else {
        return;
    };
    let draft = get_draft(ui);
    match draft.to_config() {
        Ok(cfg) => {
            state.lock().unwrap().tasks[idx].cfg = cfg;
            sync_all(ui, state);
            ui.set_toast("Task updated in memory".into());
        }
        Err(e) => ui.set_toast(format!("Invalid task: {e}").into()),
    }
}

fn duplicate_task(ui: &MainWindow, state: &Arc<Mutex<AppState>>, idx: usize) {
    let mut state_ref = state.lock().unwrap();
    if idx < state_ref.tasks.len() {
        let mut cfg = state_ref.tasks[idx].cfg.clone();
        cfg.id = Uuid::new_v4();
        cfg.name = format!("{} Copy", cfg.name);
        state_ref.tasks.push(TaskView {
            cfg,
            handle: None,
            log_rx: None,
            logs: Vec::new(),
            state: TaskState::Idle,
            starting: false,
        });
        state_ref.selected = Some(state_ref.tasks.len() - 1);
    }
    drop(state_ref);
    sync_all(ui, state);
    ui.set_logs_tab(false);
    ui.set_toast("Task duplicated".into());
}

fn delete_task(ui: &MainWindow, state: &Arc<Mutex<AppState>>, idx: usize) {
    let mut state_ref = state.lock().unwrap();
    if idx >= state_ref.tasks.len() {
        return;
    }
    if state_ref.tasks[idx].handle.is_some() || state_ref.tasks[idx].starting {
        ui.set_toast("Stop the task before deleting it".into());
        return;
    }
    state_ref.tasks.remove(idx);
    state_ref.selected = if state_ref.tasks.is_empty() {
        None
    } else {
        Some(idx.min(state_ref.tasks.len() - 1))
    };
    drop(state_ref);
    sync_all(ui, state);
    ui.set_toast("Task deleted".into());
}

fn open_task_local(ui: &MainWindow, state: &Arc<Mutex<AppState>>, idx: usize) {
    let Some(task) = state.lock().unwrap().tasks.get(idx).map(|t| t.cfg.clone()) else {
        return;
    };
    if let Err(e) = open_local_dir(task.local) {
        ui.set_toast(format!("Open local folder failed: {e}").into());
    } else {
        ui.set_toast("Opened local folder".into());
    }
}

fn toggle_task(
    ui: Weak<MainWindow>,
    state: &Arc<Mutex<AppState>>,
    runtime: &Arc<Runtime>,
    idx: usize,
) {
    let mut state_ref = state.lock().unwrap();
    let Some(task) = state_ref.tasks.get_mut(idx) else {
        return;
    };
    if task.handle.is_some() || task.starting {
        tracing::info!(
            task_id = %task.cfg.id,
            task_name = %task.cfg.name,
            "stopping task from UI"
        );
        if let Some(handle) = &task.handle {
            handle.stop();
        }
        task.starting = false;
        task.handle = None;
        task.state = TaskState::Idle;
        task.logs.push("Stop requested".into());
        state_ref.selected = Some(idx);
        drop(state_ref);
        if let Some(ui) = ui.upgrade() {
            sync_all(&ui, state);
        }
        return;
    }

    let cfg = task.cfg.clone();
    tracing::info!(
        task_id = %cfg.id,
        task_name = %cfg.name,
        "starting task from UI"
    );
    task.starting = true;
    task.logs.push("Connecting to SFTP".into());
    state_ref.selected = Some(idx);
    drop(state_ref);
    if let Some(ui) = ui.upgrade() {
        ui.set_logs_tab(true);
        sync_all(&ui, state);
    }

    let state_for_ui = state.clone();
    let ui_for_task = ui.clone();
    runtime.spawn(async move {
        let result = start_remote_task(cfg).await;
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(ui) = ui_for_task.upgrade() {
                let mut state_ref = state_for_ui.lock().unwrap();
                if let Some(task) = state_ref.tasks.get_mut(idx) {
                    task.starting = false;
                    match result {
                        Ok(handle) => {
                            let next_state = (*handle.state()).clone();
                            task.log_rx = Some(handle.subscribe_logs());
                            task.logs.push("Task spawned".into());
                            task.starting = matches!(next_state, TaskState::Starting(_));
                            task.state = next_state;
                            task.handle = Some(Arc::new(handle));
                        }
                        Err(e) => {
                            task.state = TaskState::Error(e.clone());
                            task.logs.push(format!("Start failed: {e}"));
                            ui.set_toast(format!("Start failed: {e}").into());
                        }
                    }
                }
                drop(state_ref);
                sync_all(&ui, &state_for_ui);
            }
        });
    });
}

async fn start_remote_task(cfg: TaskConfig) -> Result<SyncTaskHandle, String> {
    let RemoteCfg::Sftp {
        host,
        user,
        password,
        fingerprints,
        ..
    } = cfg.remote_cfg.clone();
    let mut attempt = 0u32;
    let max = cfg.retry_max;
    let mut backoff = cfg.retry_backoff_ms;
    loop {
        tracing::info!(
            task_id = %cfg.id,
            task_name = %cfg.name,
            host = %host,
            attempt = attempt + 1,
            "connecting to SFTP"
        );
        match SftpRemote::connect(&host, &user, password.as_deref(), fingerprints.clone()).await {
            Ok(remote) => {
                tracing::info!(
                    task_id = %cfg.id,
                    task_name = %cfg.name,
                    host = %host,
                    "SFTP connected, spawning sync task"
                );
                return Ok(spawn_task(cfg, remote));
            }
            Err(e) => {
                attempt += 1;
                tracing::warn!(
                    task_id = %cfg.id,
                    task_name = %cfg.name,
                    host = %host,
                    attempt,
                    error = %e,
                    "SFTP connect failed"
                );
                if attempt > max {
                    return Err(e.to_string());
                }
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                backoff = backoff.saturating_mul(2);
            }
        }
    }
}

fn start_log_pump(ui: Weak<MainWindow>, state: Arc<Mutex<AppState>>) {
    let timer = Box::leak(Box::new(slint::Timer::default()));
    timer.start(
        slint::TimerMode::Repeated,
        std::time::Duration::from_millis(500),
        move || {
            let Some(ui) = ui.upgrade() else {
                return;
            };
            let mut logs_changed = false;
            let mut tasks_changed = false;
            {
                let mut state_ref = state.lock().unwrap();
                for task in &mut state_ref.tasks {
                    if let Some(rx) = &mut task.log_rx {
                        loop {
                            match rx.try_recv() {
                                Ok(log) => {
                                    task.logs.push(log.message);
                                    logs_changed = true;
                                }
                                Err(broadcast::error::TryRecvError::Empty) => break,
                                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                                    task.logs
                                        .push(format!("Skipped {skipped} old log message(s)"));
                                    logs_changed = true;
                                }
                                Err(broadcast::error::TryRecvError::Closed) => break,
                            }
                        }
                    }
                    if let Some(handle) = &task.handle {
                        let next = (*handle.state()).clone();
                        let current_label = state_label(&task.state);
                        let next_label = state_label(&next);
                        if current_label != next_label {
                            task.logs.push(format!("State: {next_label}"));
                            task.starting = matches!(next, TaskState::Starting(_));
                            task.state = next;
                            logs_changed = true;
                            tasks_changed = true;
                        }
                    }
                    if task.logs.len() > 500 {
                        let remove_count = task.logs.len() - 500;
                        task.logs.drain(0..remove_count);
                        logs_changed = true;
                    }
                }
            }
            if tasks_changed {
                sync_all(&ui, &state);
            } else if logs_changed {
                sync_selected(&ui, &state);
            }
        },
    );
}

async fn save_config(storage: &AppStorage, configs: &[TaskConfig]) -> Result<()> {
    replace_tasks(&storage.pool, &storage.config.cache_dir, configs).await
}

async fn replace_tasks(
    pool: &SqlitePool,
    cache_root: &PathBuf,
    configs: &[TaskConfig],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM task_fingerprints")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM task_filters")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM tasks").execute(&mut *tx).await?;

    for cfg in configs {
        let RemoteCfg::Sftp {
            host,
            user,
            password,
            key,
            fingerprints,
        } = &cfg.remote_cfg;
        sqlx::query(
            r#"
            INSERT INTO tasks (
                id,
                name,
                local_path,
                remote_path,
                cache_dir,
                scan_ms,
                size_filter,
                retry_max,
                retry_backoff_ms,
                debounce_ms,
                remote_kind,
                sftp_host,
                sftp_user,
                sftp_password,
                sftp_key_path,
                updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'sftp', ?11, ?12, ?13, ?14, CURRENT_TIMESTAMP)
            "#,
        )
        .bind(cfg.id.to_string())
        .bind(&cfg.name)
        .bind(cfg.local.to_string_lossy().to_string())
        .bind(&cfg.remote)
        .bind(cache_dir_for_config(cfg, cache_root).to_string_lossy().to_string())
        .bind(i64::try_from(cfg.scan_ms)?)
        .bind(&cfg.size)
        .bind(i64::from(cfg.retry_max))
        .bind(i64::try_from(cfg.retry_backoff_ms)?)
        .bind(i64::try_from(cfg.debounce_ms)?)
        .bind(host)
        .bind(user)
        .bind(password)
        .bind(key.as_ref().map(|path| path.to_string_lossy().to_string()))
        .execute(&mut *tx)
        .await?;

        for (position, pattern) in cfg.include.iter().enumerate() {
            sqlx::query(
                "INSERT INTO task_filters (task_id, kind, pattern, position) VALUES (?1, 'include', ?2, ?3)",
            )
            .bind(cfg.id.to_string())
            .bind(&pattern.0)
            .bind(i64::try_from(position)?)
            .execute(&mut *tx)
            .await?;
        }

        for (position, pattern) in cfg.exclude.iter().enumerate() {
            sqlx::query(
                "INSERT INTO task_filters (task_id, kind, pattern, position) VALUES (?1, 'exclude', ?2, ?3)",
            )
            .bind(cfg.id.to_string())
            .bind(&pattern.0)
            .bind(i64::try_from(position)?)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(fingerprints) = fingerprints {
            for (position, fingerprint) in fingerprints.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO task_fingerprints (task_id, fingerprint, position) VALUES (?1, ?2, ?3)",
                )
                .bind(cfg.id.to_string())
                .bind(fingerprint)
                .bind(i64::try_from(position)?)
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    tx.commit().await?;
    Ok(())
}

fn open_local_dir(path: PathBuf) -> Result<()> {
    let path = path.canonicalize().unwrap_or(path);
    if !path.exists() {
        return Err(anyhow!("path does not exist: {}", path.display()));
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

impl Draft {
    fn from_config(cfg: &TaskConfig) -> Self {
        let RemoteCfg::Sftp {
            host,
            user,
            password,
            ..
        } = &cfg.remote_cfg;
        Self {
            id: cfg.id,
            name: cfg.name.clone(),
            local: cfg.local.display().to_string(),
            remote: cfg.remote.clone(),
            cache_dir: cfg
                .cache_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CACHE_DIR).join(cfg.id.to_string()))
                .display()
                .to_string(),
            include: cfg
                .include
                .iter()
                .map(|p| p.0.as_str())
                .collect::<Vec<_>>()
                .join(";"),
            exclude: cfg
                .exclude
                .iter()
                .map(|p| p.0.as_str())
                .collect::<Vec<_>>()
                .join(";"),
            size: cfg.size.clone().unwrap_or_default(),
            scan_ms: cfg.scan_ms.to_string(),
            debounce_ms: cfg.debounce_ms.to_string(),
            retry_max: cfg.retry_max.to_string(),
            retry_backoff_ms: cfg.retry_backoff_ms.to_string(),
            host: host.clone(),
            user: user.clone(),
            password: password.clone().unwrap_or_default(),
        }
    }

    fn to_config(&self) -> Result<TaskConfig> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("task name is required"));
        }
        if self.local.trim().is_empty() {
            return Err(anyhow!("local path is required"));
        }
        if self.remote.trim().is_empty() {
            return Err(anyhow!("remote path is required"));
        }
        if self.host.trim().is_empty() {
            return Err(anyhow!("SFTP host is required"));
        }
        if self.user.trim().is_empty() {
            return Err(anyhow!("SFTP user is required"));
        }
        Ok(TaskConfig {
            id: self.id,
            name: self.name.trim().to_string(),
            local: PathBuf::from(self.local.trim()),
            remote: self.remote.trim().to_string(),
            cache_dir: if self.cache_dir.trim().is_empty() {
                None
            } else {
                Some(PathBuf::from(self.cache_dir.trim()))
            },
            include: split_patterns(&self.include),
            exclude: split_patterns(&self.exclude),
            scan_ms: parse_u64(&self.scan_ms, "scan interval")?,
            size: blank_to_none(&self.size),
            retry_max: parse_u32(&self.retry_max, "retry max")?,
            retry_backoff_ms: parse_u64(&self.retry_backoff_ms, "retry backoff")?,
            debounce_ms: parse_u64(&self.debounce_ms, "debounce")?,
            remote_cfg: RemoteCfg::Sftp {
                host: self.host.trim().to_string(),
                user: self.user.trim().to_string(),
                password: blank_to_none(&self.password),
                key: None,
                fingerprints: None,
            },
        })
    }
}

fn get_draft(ui: &MainWindow) -> Draft {
    Draft {
        id: Uuid::parse_str(ui.get_draft_id().trim()).unwrap_or_else(|_| Uuid::new_v4()),
        name: ui.get_draft_name().to_string(),
        local: ui.get_draft_local().to_string(),
        remote: ui.get_draft_remote().to_string(),
        cache_dir: ui.get_draft_cache_dir().to_string(),
        include: ui.get_draft_include().to_string(),
        exclude: ui.get_draft_exclude().to_string(),
        size: ui.get_draft_size().to_string(),
        scan_ms: ui.get_draft_scan_ms().to_string(),
        debounce_ms: ui.get_draft_debounce_ms().to_string(),
        retry_max: ui.get_draft_retry_max().to_string(),
        retry_backoff_ms: ui.get_draft_retry_backoff_ms().to_string(),
        host: ui.get_draft_host().to_string(),
        user: ui.get_draft_user().to_string(),
        password: ui.get_draft_password().to_string(),
    }
}

fn set_draft(ui: &MainWindow, draft: &Draft) {
    ui.set_draft_id(draft.id.to_string().into());
    ui.set_draft_name(draft.name.clone().into());
    ui.set_draft_local(draft.local.clone().into());
    ui.set_draft_remote(draft.remote.clone().into());
    ui.set_draft_cache_dir(draft.cache_dir.clone().into());
    ui.set_draft_include(draft.include.clone().into());
    ui.set_draft_exclude(draft.exclude.clone().into());
    ui.set_draft_size(draft.size.clone().into());
    ui.set_draft_scan_ms(draft.scan_ms.clone().into());
    ui.set_draft_debounce_ms(draft.debounce_ms.clone().into());
    ui.set_draft_retry_max(draft.retry_max.clone().into());
    ui.set_draft_retry_backoff_ms(draft.retry_backoff_ms.clone().into());
    ui.set_draft_host(draft.host.clone().into());
    ui.set_draft_user(draft.user.clone().into());
    ui.set_draft_password(draft.password.clone().into());
}

fn split_patterns(value: &str) -> Vec<Pattern> {
    value
        .split(';')
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| Pattern(line.to_string()))
        .collect()
}

fn blank_to_none(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn cache_dir_for_config(cfg: &TaskConfig, cache_root: &PathBuf) -> PathBuf {
    cfg.cache_dir
        .clone()
        .unwrap_or_else(|| default_task_cache_dir(cache_root, &cfg.id.to_string()))
}

fn default_task_cache_dir(cache_root: &PathBuf, id: &str) -> PathBuf {
    cache_root.join(id)
}

fn migrate_legacy_default_cache_dir(cache_root: &PathBuf, id: &str, path: PathBuf) -> PathBuf {
    if path == PathBuf::from("cache").join(id) {
        default_task_cache_dir(cache_root, id)
    } else {
        path
    }
}

fn absolute_path(path: &PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.clone())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn parse_u64(value: &str, label: &str) -> Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| anyhow!("{label} must be a non-negative integer"))
}

fn parse_u32(value: &str, label: &str) -> Result<u32> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| anyhow!("{label} must be a non-negative integer"))
}

fn state_label(state: &TaskState) -> String {
    match state {
        TaskState::Idle => "Idle".into(),
        TaskState::Starting(stage) => stage.clone(),
        TaskState::Running => "Running".into(),
        TaskState::Error(e) => format!("Error: {e}"),
    }
}

fn sample_config(cache_root: &PathBuf) -> TaskConfig {
    let id = Uuid::new_v4();
    TaskConfig {
        id,
        name: "New Task".into(),
        local: PathBuf::from("."),
        remote: "/tmp/fsync".into(),
        cache_dir: Some(default_task_cache_dir(cache_root, &id.to_string())),
        include: Vec::new(),
        exclude: Vec::new(),
        scan_ms: 300,
        size: None,
        retry_max: 3,
        retry_backoff_ms: 500,
        debounce_ms: 150,
        remote_cfg: RemoteCfg::Sftp {
            host: "localhost:22".into(),
            user: String::new(),
            password: None,
            key: None,
            fingerprints: None,
        },
    }
}
