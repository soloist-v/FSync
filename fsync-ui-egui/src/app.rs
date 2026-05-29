mod profiles_ui;
mod shell;
mod tasks_ui;

use anyhow::Result;
use fsync_core::{spawn_task, RemoteCfg, SyncTaskHandle, TaskConfig, TaskState};
use fsync_remote_sftp::SftpRemote;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

use crate::models::{
    default_task_cache_dir, find_remote_profile, sample_task, selected_draft,
    selected_profile_draft, state_label, AppConfig, AppState, Draft, LoadedTask, PanelTab,
    RemoteProfileDraft, TaskView, ThemeMode,
};
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
                self.storage.config.database_path.display()
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
            task.handle = None;
            task.state = TaskState::Idle;
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
            task.handle = None;
            task.starting = false;
            task.state = TaskState::Idle;
            task.logs.push("Stop requested".into());
        }
    }

    fn poll_task_events(&mut self) {
        let mut state = self.state.lock().unwrap();
        for task in &mut state.tasks {
            if let Some(rx) = &mut task.log_rx {
                loop {
                    match rx.try_recv() {
                        Ok(log) => task.logs.push(log.message),
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
            }
            if task.logs.len() > 1_000 {
                let remove_count = task.logs.len() - 1_000;
                task.logs.drain(0..remove_count);
            }
        }
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
