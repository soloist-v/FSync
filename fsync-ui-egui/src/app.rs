mod profiles_ui;
mod shell;
mod tasks_ui;

use anyhow::Result;
use fsync_core::{spawn_task, RemoteCfg, RemoteOpLog, SyncTaskHandle, TaskConfig, TaskState};
use fsync_remote_sftp::SftpRemote;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

use crate::models::{
    default_task_cache_dir, find_remote_profile, path_text, sample_task, selected_draft,
    selected_profile_draft, state_label, AppConfig, AppState, Draft, LoadedTask, PanelTab,
    RemoteProfileDraft, TaskView, ThemeMode,
};
use crate::operation_logs::OperationLogNotification;
use crate::storage::{load_config, persist_app_config, save_state, AppStorage};
use crate::theme::{configure_style, install_chinese_fonts};

pub(crate) struct FSyncApp {
    runtime: Arc<Runtime>,
    storage: Arc<AppStorage>,
    config: AppConfig,
    state: Arc<Mutex<AppState>>,
    draft: Draft,
    tab: PanelTab,
    theme_mode: ThemeMode,
    toast: Option<(String, Instant)>,
    show_profiles_modal: bool,
    selected_profile: Option<usize>,
    profile_draft: RemoteProfileDraft,
    profile_password_visible: bool,
    pattern_editor: Option<PatternEditorKind>,
    pattern_draft: Vec<String>,
    new_pattern: String,
    operation_log_rx: broadcast::Receiver<OperationLogNotification>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PatternEditorKind {
    Include,
    Exclude,
}

impl PatternEditorKind {
    fn title(self) -> &'static str {
        match self {
            Self::Include => "Include Patterns",
            Self::Exclude => "Exclude Patterns",
        }
    }
}

impl FSyncApp {
    pub(crate) fn new(
        cc: &eframe::CreationContext<'_>,
        runtime: Arc<Runtime>,
        storage: Arc<AppStorage>,
        state: Arc<Mutex<AppState>>,
    ) -> Self {
        install_chinese_fonts(&cc.egui_ctx);
        let config = storage.config.clone();
        let theme_mode = config.theme_mode();
        configure_style(&cc.egui_ctx, theme_mode);
        let draft = selected_draft(&state).unwrap_or_default();
        let selected_profile = {
            let state = state.lock().unwrap();
            if state.remote_profiles.is_empty() {
                None
            } else {
                Some(0)
            }
        };
        let profile_draft = selected_profile_draft(&state, selected_profile).unwrap_or_default();
        let operation_log_rx = storage.operation_log_writer.subscribe();
        Self {
            runtime,
            storage,
            config,
            state,
            draft,
            tab: PanelTab::Dashboard,
            theme_mode,
            toast: None,
            show_profiles_modal: false,
            selected_profile,
            profile_draft,
            profile_password_visible: false,
            pattern_editor: None,
            pattern_draft: Vec::new(),
            new_pattern: String::new(),
            operation_log_rx,
        }
    }

    fn set_theme_mode(&mut self, ctx: &eframe::egui::Context, theme_mode: ThemeMode) {
        self.theme_mode = theme_mode;
        self.config.set_theme_mode(theme_mode);
        configure_style(ctx, theme_mode);
        match persist_app_config(&self.config) {
            Ok(()) => self.toast(format!("Theme: {}", theme_mode.as_label())),
            Err(e) => self.toast(format!("Theme save failed: {e}")),
        }
    }

    fn reload(&mut self) {
        match load_config(&self.runtime, &self.storage, &self.state) {
            Ok(()) => {
                self.draft = selected_draft(&self.state).unwrap_or_default();
                self.sync_profile_modal_state();
                self.toast("Reloaded tasks");
            }
            Err(e) => self.toast(format!("Reload failed: {e}")),
        }
    }

    fn save(&mut self) {
        if self.tab == PanelTab::Settings {
            if let Err(e) = self.apply_draft() {
                self.toast(format!("Invalid task: {e}"));
                return;
            }
        }
        match self.persist_state() {
            Ok(task_count) => self.toast(format!(
                "Saved {} task(s) to {}",
                task_count,
                path_text(&self.storage.config.database_path)
            )),
            Err(e) => self.toast(format!("Save failed: {e}")),
        }
    }

    fn new_task(&mut self) {
        let default_profile = {
            let state = self.state.lock().unwrap();
            state.remote_profiles.first().cloned()
        };
        let task = sample_task(&self.storage.config.cache_dir, default_profile.as_ref());
        let mut state = self.state.lock().unwrap();
        state.tasks.push(task);
        state.selected = Some(state.tasks.len() - 1);
        drop(state);
        self.draft = selected_draft(&self.state).unwrap_or_default();
        self.tab = PanelTab::Settings;
    }

    fn select_task(&mut self, idx: usize) {
        self.state.lock().unwrap().selected = Some(idx);
        self.draft = selected_draft(&self.state).unwrap_or_default();
        self.tab = PanelTab::Dashboard;
    }

    fn duplicate_task(&mut self, idx: usize) {
        let mut state = self.state.lock().unwrap();
        if let Some((remote_profile_id, mut cfg)) = state
            .tasks
            .get(idx)
            .map(|source| (source.remote_profile_id, source.cfg.clone()))
        {
            cfg.id = uuid::Uuid::new_v4();
            cfg.name = format!("{} Copy", cfg.name);
            cfg.cache_dir = Some(default_task_cache_dir(
                &self.storage.config.cache_dir,
                &cfg.id.to_string(),
            ));
            state.tasks.push(TaskView {
                cfg,
                remote_profile_id,
                handle: None,
                log_rx: None,
                logs: Vec::new(),
                last_operation_log_id: 0,
                state: TaskState::Idle,
                starting: false,
            });
            state.selected = Some(state.tasks.len() - 1);
            drop(state);
            self.draft = selected_draft(&self.state).unwrap_or_default();
            self.tab = PanelTab::Settings;
        }
    }

    fn delete_task(&mut self, idx: usize) {
        let mut state = self.state.lock().unwrap();
        if idx >= state.tasks.len() {
            return;
        }
        if state.tasks[idx].handle.is_some() || state.tasks[idx].starting {
            drop(state);
            self.toast("Stop the task before deleting it");
            return;
        }
        state.tasks.remove(idx);
        state.selected = if state.tasks.is_empty() {
            None
        } else {
            Some(idx.min(state.tasks.len() - 1))
        };
        drop(state);
        self.draft = selected_draft(&self.state).unwrap_or_default();
    }

    fn apply_draft(&mut self) -> Result<()> {
        let Some(idx) = self.state.lock().unwrap().selected else {
            return Ok(());
        };
        let profile = {
            let state = self.state.lock().unwrap();
            find_remote_profile(&state.remote_profiles, self.draft.remote_profile_id).cloned()
        };
        let cfg = self.draft.to_config(profile.as_ref())?;
        if let Some(task) = self.state.lock().unwrap().tasks.get_mut(idx) {
            task.cfg = cfg;
            task.remote_profile_id = self.draft.remote_profile_id;
        }
        self.toast("Task updated in memory");
        Ok(())
    }

    fn toggle_task(&mut self, idx: usize) {
        let mut state = self.state.lock().unwrap();
        let Some(task) = state.tasks.get_mut(idx) else {
            return;
        };
        if task.handle.is_some() || task.starting {
            if let Some(handle) = &task.handle {
                handle.stop();
            }
            task.starting = false;
            task.logs.push("Stop requested".into());
            return;
        }

        if task.remote_profile_id.is_none() {
            drop(state);
            self.toast("Select a remote profile first");
            return;
        }

        let cfg = task.cfg.clone();
        task.starting = true;
        task.logs.push("Connecting to SFTP".into());
        state.selected = Some(idx);
        drop(state);
        self.tab = PanelTab::Dashboard;

        let state = self.state.clone();
        self.runtime.spawn(async move {
            let result = start_remote_task(cfg).await;
            let mut state = state.lock().unwrap();
            if let Some(task) = state.tasks.get_mut(idx) {
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
                    }
                }
            }
        });
    }

    fn start_all(&mut self) {
        let len = self.state.lock().unwrap().tasks.len();
        for idx in 0..len {
            let running = self
                .state
                .lock()
                .unwrap()
                .tasks
                .get(idx)
                .map(|task| task.handle.is_some() || task.starting)
                .unwrap_or(false);
            if !running {
                self.toggle_task(idx);
            }
        }
    }

    fn stop_all(&mut self) {
        let mut state = self.state.lock().unwrap();
        for task in &mut state.tasks {
            if let Some(handle) = &task.handle {
                handle.stop();
            }
            task.starting = false;
            task.logs.push("Stop requested".into());
        }
    }

    fn poll_task_events(&mut self) {
        let mut operation_logs: Vec<(String, RemoteOpLog)> = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            for task in &mut state.tasks {
                if let Some(rx) = &mut task.log_rx {
                    loop {
                        match rx.try_recv() {
                            Ok(log) => {
                                if let Some(remote_op) = log.remote_op.clone() {
                                    operation_logs.push((task.cfg.id.to_string(), remote_op));
                                } else {
                                    task.logs.push(log.message);
                                }
                            }
                            Err(broadcast::error::TryRecvError::Empty) => break,
                            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                                task.logs
                                    .push(format!("Skipped {skipped} old log message(s)"));
                            }
                            Err(broadcast::error::TryRecvError::Closed) => break,
                        }
                    }
                }
                if let Some(handle) = &task.handle {
                    let next = (*handle.state()).clone();
                    if state_label(&task.state) != state_label(&next) {
                        task.state = next;
                        task.starting = matches!(task.state, TaskState::Starting(_));
                    }
                    if matches!(task.state, TaskState::Idle | TaskState::Error(_)) {
                        task.handle = None;
                        task.log_rx = None;
                        task.starting = false;
                    }
                }
                if task.logs.len() > 1_000 {
                    let remove_count = task.logs.len() - 1_000;
                    task.logs.drain(0..remove_count);
                }
            }
        }

        if !operation_logs.is_empty() {
            if let Err(e) = self
                .storage
                .operation_log_writer
                .enqueue_many(operation_logs)
            {
                tracing::warn!(error = %e, "failed to enqueue task operation logs");
                self.toast(format!("Operation log enqueue failed: {e}"));
            }
        }

        self.poll_operation_log_notifications();
    }

    fn poll_operation_log_notifications(&mut self) {
        let mut changed = Vec::<OperationLogNotification>::new();
        let mut reload_all = false;

        loop {
            match self.operation_log_rx.try_recv() {
                Ok(notification) => changed.push(notification),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        skipped,
                        "operation log notifications lagged; refreshing all task logs"
                    );
                    reload_all = true;
                    break;
                }
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }

        if !reload_all && changed.is_empty() {
            return;
        }

        let queries = {
            let state = self.state.lock().unwrap();
            state
                .tasks
                .iter()
                .enumerate()
                .filter_map(|(idx, task)| {
                    let task_id = task.cfg.id.to_string();
                    let latest_id = changed
                        .iter()
                        .filter(|notification| notification.task_id == task_id)
                        .map(|notification| notification.latest_id)
                        .max();
                    let should_read = reload_all
                        || latest_id
                            .map(|latest_id| latest_id > task.last_operation_log_id)
                            .unwrap_or(false);
                    should_read.then_some((idx, task_id, task.last_operation_log_id, latest_id))
                })
                .collect::<Vec<_>>()
        };

        for (idx, task_id, last_seen_id, latest_id) in queries {
            match self.read_operation_logs_until(&task_id, last_seen_id, latest_id) {
                Ok(records) if records.is_empty() => {}
                Ok(records) => {
                    let mut state = self.state.lock().unwrap();
                    let Some(task) = state.tasks.get_mut(idx) else {
                        continue;
                    };
                    if task.cfg.id.to_string() != task_id {
                        continue;
                    }
                    for record in records {
                        task.last_operation_log_id = task.last_operation_log_id.max(record.id);
                        task.logs.push(record.display_message());
                    }
                    if task.logs.len() > 1_000 {
                        let remove_count = task.logs.len() - 1_000;
                        task.logs.drain(0..remove_count);
                    }
                }
                Err(e) => {
                    tracing::warn!(task_id, error = %e, "failed to read task operation logs");
                    self.toast(format!("Operation log read failed: {e}"));
                }
            }
        }
    }

    fn read_operation_logs_until(
        &self,
        task_id: &str,
        last_seen_id: i64,
        latest_id: Option<i64>,
    ) -> Result<Vec<crate::operation_logs::OperationLogRecord>> {
        const PAGE_SIZE: i64 = 2_000;

        let mut cursor = last_seen_id;
        let mut all = Vec::new();
        loop {
            let records = self.runtime.block_on(
                self.storage
                    .operation_log_reader
                    .read_after(task_id, cursor, PAGE_SIZE),
            )?;
            if records.is_empty() {
                break;
            }

            cursor = records.last().map(|record| record.id).unwrap_or(cursor);
            let is_short_page = records.len() < PAGE_SIZE as usize;
            all.extend(records);

            if latest_id
                .map(|latest_id| cursor >= latest_id)
                .unwrap_or(false)
                || is_short_page
            {
                break;
            }
        }

        Ok(all)
    }

    fn toast(&mut self, message: impl Into<String>) {
        self.toast = Some((message.into(), Instant::now()));
    }

    fn sync_profile_modal_state(&mut self) {
        let next_selected = {
            let state = self.state.lock().unwrap();
            match self.selected_profile {
                Some(idx) if idx < state.remote_profiles.len() => Some(idx),
                Some(_) | None if !state.remote_profiles.is_empty() => Some(0),
                _ => None,
            }
        };
        self.selected_profile = next_selected;
        self.profile_draft = selected_profile_draft(&self.state, self.selected_profile)
            .unwrap_or_else(RemoteProfileDraft::new_empty);
    }

    fn restore_remote_profile_draft(&mut self) {
        self.profile_draft = selected_profile_draft(&self.state, self.selected_profile)
            .unwrap_or_else(RemoteProfileDraft::new_empty);
    }

    fn open_pattern_editor(&mut self, kind: PatternEditorKind) {
        self.pattern_draft = match kind {
            PatternEditorKind::Include => patterns_from_text(&self.draft.include),
            PatternEditorKind::Exclude => patterns_from_text(&self.draft.exclude),
        };
        self.new_pattern.clear();
        self.pattern_editor = Some(kind);
    }

    fn apply_pattern_editor(&mut self) {
        let Some(kind) = self.pattern_editor else {
            return;
        };
        let value = self
            .pattern_draft
            .iter()
            .map(|pattern| pattern.trim())
            .filter(|pattern| !pattern.is_empty())
            .collect::<Vec<_>>()
            .join("; ");
        match kind {
            PatternEditorKind::Include => self.draft.include = value,
            PatternEditorKind::Exclude => self.draft.exclude = value,
        }
        self.pattern_editor = None;
        self.new_pattern.clear();
    }

    fn persist_state(&self) -> Result<usize> {
        let (tasks, remote_profiles) = {
            let state = self.state.lock().unwrap();
            (
                state
                    .tasks
                    .iter()
                    .map(|task| LoadedTask {
                        cfg: task.cfg.clone(),
                        remote_profile_id: task.remote_profile_id,
                        recent_logs: Vec::new(),
                    })
                    .collect::<Vec<_>>(),
                state.remote_profiles.clone(),
            )
        };
        self.runtime
            .block_on(save_state(&self.storage, &remote_profiles, &tasks))?;
        Ok(tasks.len())
    }
}

fn patterns_from_text(value: &str) -> Vec<String> {
    value
        .split(';')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_string)
        .collect()
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
