use anyhow::{anyhow, Result};
use eframe::egui::ThemePreference;
use fsync_core::{Pattern, RemoteCfg, SyncTaskHandle, TaskConfig, TaskLog, TaskState};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::operation_logs::OperationLogRecord;

pub(crate) const CONFIG_PATH: &str = "config.yaml";
pub(crate) const DEFAULT_DATABASE_PATH: &str = "fsync.db";
pub(crate) const DEFAULT_CACHE_DIR: &str = "data/cache";
pub(crate) const DEFAULT_LOG_DIR: &str = "data/logs";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AppConfig {
    #[serde(default = "AppConfig::default_database_path")]
    pub(crate) database_path: PathBuf,
    #[serde(default = "AppConfig::default_cache_dir")]
    pub(crate) cache_dir: PathBuf,
    #[serde(default)]
    pub(crate) theme: Option<String>,
    #[serde(default)]
    pub(crate) font_size: Option<u32>,
    #[serde(default)]
    pub(crate) log_dir: Option<PathBuf>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_path: PathBuf::from(DEFAULT_DATABASE_PATH),
            cache_dir: PathBuf::from(DEFAULT_CACHE_DIR),
            theme: None,
            font_size: None,
            log_dir: Some(PathBuf::from(DEFAULT_LOG_DIR)),
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

    pub(crate) fn normalize(&mut self) {
        if self.log_dir.is_none() {
            self.log_dir = Some(PathBuf::from(DEFAULT_LOG_DIR));
        }
    }

    pub(crate) fn log_dir(&self) -> PathBuf {
        self.log_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_DIR))
    }

    pub(crate) fn theme_mode(&self) -> ThemeMode {
        ThemeMode::from_config_value(self.theme.as_deref())
    }

    pub(crate) fn set_theme_mode(&mut self, mode: ThemeMode) {
        self.theme = Some(mode.as_config_value().to_string());
    }
}

pub(crate) struct TaskView {
    pub(crate) cfg: TaskConfig,
    pub(crate) remote_profile_id: Option<Uuid>,
    pub(crate) handle: Option<Arc<SyncTaskHandle>>,
    pub(crate) log_rx: Option<broadcast::Receiver<TaskLog>>,
    pub(crate) logs: Vec<String>,
    pub(crate) last_operation_log_id: i64,
    pub(crate) state: TaskState,
    pub(crate) starting: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteProfile {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) host: String,
    pub(crate) user: String,
    pub(crate) password: Option<String>,
    pub(crate) key: Option<PathBuf>,
    pub(crate) fingerprints: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct LoadedTask {
    pub(crate) cfg: TaskConfig,
    pub(crate) remote_profile_id: Option<Uuid>,
    pub(crate) recent_logs: Vec<OperationLogRecord>,
}

#[derive(Default)]
pub(crate) struct AppState {
    pub(crate) tasks: Vec<TaskView>,
    pub(crate) remote_profiles: Vec<RemoteProfile>,
    pub(crate) selected: Option<usize>,
}

#[derive(Clone, Default)]
pub(crate) struct Draft {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) local: String,
    pub(crate) remote: String,
    pub(crate) cache_dir: String,
    pub(crate) include: String,
    pub(crate) exclude: String,
    pub(crate) size: String,
    pub(crate) scan_ms: String,
    pub(crate) debounce_ms: String,
    pub(crate) retry_max: String,
    pub(crate) retry_backoff_ms: String,
    pub(crate) remote_profile_id: Option<Uuid>,
}

#[derive(Clone, Default)]
pub(crate) struct RemoteProfileDraft {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) host: String,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) key_path: String,
    pub(crate) fingerprints: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PanelTab {
    Dashboard,
    Settings,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ThemeMode {
    #[default]
    System,
    Light,
    Dark,
}

impl ThemeMode {
    pub(crate) fn from_config_value(value: Option<&str>) -> Self {
        match value
            .unwrap_or("system")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "light" => Self::Light,
            "dark" => Self::Dark,
            _ => Self::System,
        }
    }

    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }

    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }

    pub(crate) fn as_preference(self) -> ThemePreference {
        match self {
            Self::System => ThemePreference::System,
            Self::Light => ThemePreference::Light,
            Self::Dark => ThemePreference::Dark,
        }
    }
}

impl Draft {
    pub(crate) fn from_task(task: &TaskView) -> Self {
        let cfg = &task.cfg;
        Self {
            id: cfg.id,
            name: cfg.name.clone(),
            local: path_text(&cfg.local),
            remote: cfg.remote.clone(),
            cache_dir: path_text(
                &cfg.cache_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(DEFAULT_CACHE_DIR).join(cfg.id.to_string())),
            ),
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
            remote_profile_id: task.remote_profile_id,
        }
    }

    pub(crate) fn to_config(&self, remote_profile: Option<&RemoteProfile>) -> Result<TaskConfig> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("task name is required"));
        }
        if self.local.trim().is_empty() {
            return Err(anyhow!("local path is required"));
        }
        if self.remote.trim().is_empty() {
            return Err(anyhow!("remote path is required"));
        }
        Ok(TaskConfig {
            id: self.id,
            name: self.name.trim().to_string(),
            local: PathBuf::from(self.local.trim()),
            remote: self.remote.trim().to_string(),
            cache_dir: blank_to_none(&self.cache_dir).map(PathBuf::from),
            include: split_patterns(&self.include),
            exclude: split_patterns(&self.exclude),
            scan_ms: parse_u64(&self.scan_ms, "scan interval")?,
            size: blank_to_none(&self.size),
            retry_max: parse_u32(&self.retry_max, "retry max")?,
            retry_backoff_ms: parse_u64(&self.retry_backoff_ms, "retry backoff")?,
            debounce_ms: parse_u64(&self.debounce_ms, "debounce")?,
            remote_cfg: remote_profile
                .map(remote_cfg_from_profile)
                .unwrap_or_else(placeholder_remote_cfg),
        })
    }
}

impl RemoteProfileDraft {
    pub(crate) fn new_empty() -> Self {
        Self {
            id: Uuid::new_v4(),
            ..Default::default()
        }
    }

    pub(crate) fn from_profile(profile: &RemoteProfile) -> Self {
        Self {
            id: profile.id,
            name: profile.name.clone(),
            host: profile.host.clone(),
            user: profile.user.clone(),
            password: profile.password.clone().unwrap_or_default(),
            key_path: profile
                .key
                .as_ref()
                .map(|path| path_text(path))
                .unwrap_or_default(),
            fingerprints: profile.fingerprints.join(";"),
        }
    }

    pub(crate) fn to_profile(&self) -> Result<RemoteProfile> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("profile name is required"));
        }
        if self.host.trim().is_empty() {
            return Err(anyhow!("host is required"));
        }
        if self.user.trim().is_empty() {
            return Err(anyhow!("user is required"));
        }
        Ok(RemoteProfile {
            id: self.id,
            name: self.name.trim().to_string(),
            host: self.host.trim().to_string(),
            user: self.user.trim().to_string(),
            password: blank_to_none(&self.password),
            key: blank_to_none(&self.key_path).map(PathBuf::from),
            fingerprints: self
                .fingerprints
                .split(';')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_string)
                .collect(),
        })
    }
}

pub(crate) fn selected_draft(state: &Arc<Mutex<AppState>>) -> Option<Draft> {
    let state = state.lock().unwrap();
    let idx = state.selected?;
    state.tasks.get(idx).map(Draft::from_task)
}

pub(crate) fn selected_profile_draft(
    state: &Arc<Mutex<AppState>>,
    selected_profile: Option<usize>,
) -> Option<RemoteProfileDraft> {
    let state = state.lock().unwrap();
    selected_profile
        .and_then(|idx| state.remote_profiles.get(idx))
        .map(RemoteProfileDraft::from_profile)
}

pub(crate) fn patterns_text(patterns: &[Pattern]) -> String {
    if patterns.is_empty() {
        "-".into()
    } else {
        patterns
            .iter()
            .map(|pattern| pattern.0.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

pub(crate) fn split_patterns(value: &str) -> Vec<Pattern> {
    value
        .split(';')
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| Pattern(line.to_string()))
        .collect()
}

pub(crate) fn blank_to_none(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

pub(crate) fn cache_dir_for_config(cfg: &TaskConfig, cache_root: &PathBuf) -> PathBuf {
    cfg.cache_dir
        .clone()
        .unwrap_or_else(|| default_task_cache_dir(cache_root, &cfg.id.to_string()))
}

pub(crate) fn default_task_cache_dir(cache_root: &PathBuf, id: &str) -> PathBuf {
    cache_root.join(id)
}

pub(crate) fn absolute_path(path: &PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.clone())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

pub(crate) fn path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub(crate) fn parse_u64(value: &str, label: &str) -> Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| anyhow!("{label} must be a non-negative integer"))
}

pub(crate) fn parse_u32(value: &str, label: &str) -> Result<u32> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| anyhow!("{label} must be a non-negative integer"))
}

pub(crate) fn state_label(state: &TaskState) -> String {
    match state {
        TaskState::Idle => "Idle".into(),
        TaskState::Starting(stage) => stage.clone(),
        TaskState::Running => "Running".into(),
        TaskState::Error(e) => format!("Error: {e}"),
    }
}

pub(crate) fn sample_task(
    cache_root: &PathBuf,
    remote_profile: Option<&RemoteProfile>,
) -> TaskView {
    let id = Uuid::new_v4();
    TaskView {
        cfg: TaskConfig {
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
            remote_cfg: remote_profile
                .map(remote_cfg_from_profile)
                .unwrap_or_else(placeholder_remote_cfg),
        },
        remote_profile_id: remote_profile.map(|profile| profile.id),
        handle: None,
        log_rx: None,
        logs: Vec::new(),
        last_operation_log_id: 0,
        state: TaskState::Idle,
        starting: false,
    }
}

pub(crate) fn find_remote_profile(
    profiles: &[RemoteProfile],
    remote_profile_id: Option<Uuid>,
) -> Option<&RemoteProfile> {
    let remote_profile_id = remote_profile_id?;
    profiles
        .iter()
        .find(|profile| profile.id == remote_profile_id)
}

pub(crate) fn remote_cfg_from_profile(profile: &RemoteProfile) -> RemoteCfg {
    RemoteCfg::Sftp {
        host: profile.host.clone(),
        user: profile.user.clone(),
        password: profile.password.clone(),
        key: profile.key.clone(),
        fingerprints: if profile.fingerprints.is_empty() {
            None
        } else {
            Some(profile.fingerprints.clone())
        },
    }
}

pub(crate) fn placeholder_remote_cfg() -> RemoteCfg {
    RemoteCfg::Sftp {
        host: String::new(),
        user: String::new(),
        password: None,
        key: None,
        fingerprints: None,
    }
}

pub(crate) fn refresh_tasks_for_profile(tasks: &mut [TaskView], profile: &RemoteProfile) {
    for task in tasks {
        if task.remote_profile_id == Some(profile.id) {
            task.cfg.remote_cfg = remote_cfg_from_profile(profile);
        }
    }
}

pub(crate) fn clear_remote_profile_from_tasks(tasks: &mut [TaskView], profile_id: Uuid) {
    for task in tasks {
        if task.remote_profile_id == Some(profile_id) {
            task.remote_profile_id = None;
            task.cfg.remote_cfg = placeholder_remote_cfg();
        }
    }
}
