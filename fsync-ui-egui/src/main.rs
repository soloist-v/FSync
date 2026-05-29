#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

use anyhow::{anyhow, Result};
use eframe::egui::{self, FontData, FontDefinitions, FontFamily, Theme, ThemePreference};
use fsync_core::{spawn_task, Pattern, RemoteCfg, SyncTaskHandle, TaskConfig, TaskLog, TaskState};
use fsync_remote_sftp::SftpRemote;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tracing_appender::rolling;
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

const CONFIG_PATH: &str = "config.yaml";
const DEFAULT_DATABASE_PATH: &str = "fsync.db";
const DEFAULT_CACHE_DIR: &str = "data/cache";
const DEFAULT_LOG_DIR: &str = "data/logs";

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

    fn normalize(&mut self) {
        if self.log_dir.is_none() {
            self.log_dir = Some(PathBuf::from(DEFAULT_LOG_DIR));
        }
    }

    fn log_dir(&self) -> PathBuf {
        self.log_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_DIR))
    }

    fn theme_mode(&self) -> ThemeMode {
        ThemeMode::from_config_value(self.theme.as_deref())
    }

    fn set_theme_mode(&mut self, mode: ThemeMode) {
        self.theme = Some(mode.as_config_value().to_string());
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

#[derive(Clone, Default)]
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum PanelTab {
    Dashboard,
    Settings,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ThemeMode {
    #[default]
    System,
    Light,
    Dark,
}

impl ThemeMode {
    fn from_config_value(value: Option<&str>) -> Self {
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

    fn as_config_value(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }

    fn as_preference(self) -> ThemePreference {
        match self {
            Self::System => ThemePreference::System,
            Self::Light => ThemePreference::Light,
            Self::Dark => ThemePreference::Dark,
        }
    }
}

struct FSyncApp {
    runtime: Arc<Runtime>,
    storage: Arc<AppStorage>,
    config: AppConfig,
    state: Arc<Mutex<AppState>>,
    draft: Draft,
    tab: PanelTab,
    theme_mode: ThemeMode,
    toast: Option<(String, Instant)>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("FSync failed: {e:?}");
    }
}

fn run() -> Result<()> {
    let config = load_app_config()?;
    init_file_logging(&config)?;
    let runtime = Arc::new(Runtime::new()?);
    let storage = Arc::new(runtime.block_on(init_storage(config))?);
    let state = Arc::new(Mutex::new(AppState::default()));
    load_config(&runtime, &storage, &state)?;

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("FSync")
            .with_inner_size([1120.0, 760.0])
            .with_min_inner_size([900.0, 560.0]),
        ..Default::default()
    };
    eframe::run_native(
        "FSync",
        options,
        Box::new(move |cc| Ok(Box::new(FSyncApp::new(cc, runtime, storage, state)))),
    )
    .map_err(|e| anyhow!("{e}"))
}

impl FSyncApp {
    fn new(
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
        Self {
            runtime,
            storage,
            config,
            state,
            draft,
            tab: PanelTab::Dashboard,
            theme_mode,
            toast: None,
        }
    }

    fn set_theme_mode(&mut self, ctx: &egui::Context, theme_mode: ThemeMode) {
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
                self.toast("Reloaded tasks");
            }
            Err(e) => self.toast(format!("Reload failed: {e}")),
        }
    }

    fn save(&mut self) {
        if self.tab == PanelTab::Settings {
            self.apply_draft();
        }
        let configs = self
            .state
            .lock()
            .unwrap()
            .tasks
            .iter()
            .map(|task| task.cfg.clone())
            .collect::<Vec<_>>();
        match self.runtime.block_on(save_config(&self.storage, &configs)) {
            Ok(()) => self.toast(format!(
                "Saved {} task(s) to {}",
                configs.len(),
                self.storage.config.database_path.display()
            )),
            Err(e) => self.toast(format!("Save failed: {e}")),
        }
    }

    fn new_task(&mut self) {
        let cfg = sample_config(&self.storage.config.cache_dir);
        let mut state = self.state.lock().unwrap();
        state.tasks.push(TaskView {
            cfg: cfg.clone(),
            handle: None,
            log_rx: None,
            logs: Vec::new(),
            state: TaskState::Idle,
            starting: false,
        });
        state.selected = Some(state.tasks.len() - 1);
        drop(state);
        self.draft = Draft::from_config(&cfg);
        self.tab = PanelTab::Settings;
    }

    fn select_task(&mut self, idx: usize) {
        self.state.lock().unwrap().selected = Some(idx);
        self.draft = selected_draft(&self.state).unwrap_or_default();
        self.tab = PanelTab::Dashboard;
    }

    fn duplicate_task(&mut self, idx: usize) {
        let mut state = self.state.lock().unwrap();
        if let Some(source) = state.tasks.get(idx) {
            let mut cfg = source.cfg.clone();
            cfg.id = Uuid::new_v4();
            cfg.name = format!("{} Copy", cfg.name);
            cfg.cache_dir = Some(default_task_cache_dir(
                &self.storage.config.cache_dir,
                &cfg.id.to_string(),
            ));
            state.tasks.push(TaskView {
                cfg: cfg.clone(),
                handle: None,
                log_rx: None,
                logs: Vec::new(),
                state: TaskState::Idle,
                starting: false,
            });
            state.selected = Some(state.tasks.len() - 1);
            self.draft = Draft::from_config(&cfg);
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

    fn apply_draft(&mut self) {
        let Some(idx) = self.state.lock().unwrap().selected else {
            return;
        };
        match self.draft.to_config() {
            Ok(cfg) => {
                if let Some(task) = self.state.lock().unwrap().tasks.get_mut(idx) {
                    task.cfg = cfg;
                }
                self.toast("Task updated in memory");
            }
            Err(e) => self.toast(format!("Invalid task: {e}")),
        }
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

    fn render_left_panel(&mut self, ui: &mut egui::Ui) {
        let mut next_theme = None;
        ui.horizontal(|ui| {
            ui.heading("Tasks");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_sized([76.0, 28.0], egui::Button::new("New"))
                    .clicked()
                {
                    self.new_task();
                }
                if ui
                    .add_sized([28.0, 28.0], egui::Button::new("↻"))
                    .on_hover_text("Reload tasks")
                    .clicked()
                {
                    self.reload();
                }
                ui.menu_button(self.theme_mode.as_label(), |ui| {
                    for mode in [ThemeMode::System, ThemeMode::Light, ThemeMode::Dark] {
                        if ui
                            .selectable_label(self.theme_mode == mode, mode.as_label())
                            .clicked()
                        {
                            next_theme = Some(mode);
                            ui.close();
                        }
                    }
                });
            });
        });
        if let Some(mode) = next_theme {
            self.set_theme_mode(ui.ctx(), mode);
        }
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(
                    [ui.available_width() * 0.5 - 4.0, 28.0],
                    egui::Button::new("Start All"),
                )
                .clicked()
            {
                self.start_all();
            }
            if ui
                .add_sized([ui.available_width(), 28.0], egui::Button::new("Stop All"))
                .clicked()
            {
                self.stop_all();
            }
        });
        ui.add_space(8.0);

        let rows = {
            let state = self.state.lock().unwrap();
            state
                .tasks
                .iter()
                .enumerate()
                .map(|(idx, task)| {
                    (
                        idx,
                        state.selected == Some(idx),
                        task.cfg.name.clone(),
                        state_label(&task.state),
                        task.handle.is_some() || task.starting,
                        task.state.clone(),
                        task.starting,
                    )
                })
                .collect::<Vec<_>>()
        };

        if rows.is_empty() {
            ui.add_space(10.0);
            ui.label(egui::RichText::new("No sync tasks").strong());
            ui.label(egui::RichText::new("Use New to create a task.").weak());
            return;
        }

        egui::ScrollArea::vertical()
            .id_salt("tasks_list_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                for (idx, _selected, name, status, running, state, starting) in rows {
                    let mut toggle_clicked = false;
                    let row_width = ui.available_width();
                    let row = ui.allocate_ui_with_layout(
                        egui::vec2(row_width, 40.0),
                        egui::Layout::top_down(egui::Align::Min),
                        |ui| {
                            egui::Frame::group(ui.style())
                                .corner_radius(6.0)
                                .inner_margin(egui::Margin::symmetric(10, 8))
                                .show(ui, |ui| {
                                    ui.set_min_width(ui.available_width());
                                    let button_width = 58.0;

                                    ui.horizontal(|ui| {
                                        ui.spacing_mut().item_spacing.x = 8.0;
                                        status_dot(ui, status_color(ui, &state, starting), &status);
                                        ui.label(egui::RichText::new(name).strong().size(14.0));
                                        let spacer = (ui.available_width() - button_width).max(0.0);
                                        if spacer > 0.0 {
                                            ui.add_space(spacer);
                                        }

                                        if ui
                                            .add_sized(
                                                [button_width, 24.0],
                                                egui::Button::new(if running {
                                                    "Stop"
                                                } else {
                                                    "Start"
                                                }),
                                            )
                                            .clicked()
                                        {
                                            toggle_clicked = true;
                                        }
                                    });
                                });
                        },
                    );
                    let response = row.response.interact(egui::Sense::click());
                    if toggle_clicked {
                        self.toggle_task(idx);
                    } else if response.clicked() {
                        self.select_task(idx);
                    }
                    response.context_menu(|ui| {
                        if ui.button("Open Local Folder").clicked() {
                            self.open_local(idx);
                            ui.close();
                        }
                        if ui.button("Duplicate").clicked() {
                            self.duplicate_task(idx);
                            ui.close();
                        }
                        if ui.button("Delete").clicked() {
                            self.delete_task(idx);
                            ui.close();
                        }
                    });
                    ui.add_space(6.0);
                }
            });
    }

    fn render_right_panel(&mut self, ui: &mut egui::Ui) {
        let selected = self.state.lock().unwrap().selected;
        let Some(idx) = selected else {
            ui.heading("Ready when you are");
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Create or select a task to begin.").weak());
            return;
        };
        if idx >= self.state.lock().unwrap().tasks.len() {
            return;
        }

        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.tab, PanelTab::Dashboard, "Dashboard");
            ui.selectable_value(&mut self.tab, PanelTab::Settings, "Task Settings");
        });
        ui.separator();

        match self.tab {
            PanelTab::Dashboard => self.render_dashboard(ui, idx),
            PanelTab::Settings => self.render_settings(ui),
        }
    }

    fn render_dashboard(&mut self, ui: &mut egui::Ui, idx: usize) {
        let (cfg, logs) = {
            let state = self.state.lock().unwrap();
            let task = &state.tasks[idx];
            (task.cfg.clone(), task.logs.clone())
        };
        let remote_cfg = cfg.remote_cfg.clone();
        ui.columns(2, |columns| {
            info_tile(&mut columns[0], "Local", &cfg.local.display().to_string());
            info_tile(&mut columns[1], "Remote", &cfg.remote);

            info_tile(
                &mut columns[0],
                "Cache",
                &cfg.cache_dir
                    .clone()
                    .unwrap_or_else(|| {
                        default_task_cache_dir(&self.storage.config.cache_dir, &cfg.id.to_string())
                    })
                    .display()
                    .to_string(),
            );
            info_tile(&mut columns[1], "Task ID", &cfg.id.to_string());

            info_tile(&mut columns[0], "Include", &patterns_text(&cfg.include));
            info_tile(&mut columns[1], "Exclude", &patterns_text(&cfg.exclude));

            let RemoteCfg::Sftp { host, user, .. } = &remote_cfg;
            info_tile(&mut columns[0], "Host", host);
            info_tile(&mut columns[1], "User", user);
        });

        ui.add_space(8.0);
        ui.heading("Logs");
        ui.add_space(4.0);
        let height = (ui.available_height() - 4.0).max(180.0);
        egui::Frame::new()
            .corner_radius(6.0)
            .inner_margin(egui::Margin::same(10))
            .show(ui, |ui| {
                ui.set_min_height(height);
                egui::ScrollArea::vertical()
                    .id_salt("task_logs_scroll")
                    .stick_to_bottom(true)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Extend);
                        if logs.is_empty() {
                            ui.label(egui::RichText::new("No log entries yet").monospace().weak());
                        } else {
                            for log in logs {
                                ui.label(egui::RichText::new(log).monospace());
                            }
                        }
                    });
            });
    }

    fn render_settings(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .id_salt("task_settings_scroll")
            .show(ui, |ui| {
                ui.columns(2, |columns| {
                    edit_field(&mut columns[0], "Name", &mut self.draft.name);
                    edit_field(&mut columns[1], "Cache", &mut self.draft.cache_dir);
                });
                ui.columns(2, |columns| {
                    edit_field(&mut columns[0], "Local", &mut self.draft.local);
                    edit_field(&mut columns[1], "Remote", &mut self.draft.remote);
                });
                ui.columns(2, |columns| {
                    edit_field(&mut columns[0], "Include", &mut self.draft.include);
                    edit_field(&mut columns[1], "Exclude", &mut self.draft.exclude);
                });
                ui.columns(3, |columns| {
                    edit_field(&mut columns[0], "Host", &mut self.draft.host);
                    edit_field(&mut columns[1], "User", &mut self.draft.user);
                    edit_password(&mut columns[2], "Password", &mut self.draft.password);
                });
                ui.columns(4, |columns| {
                    edit_field(&mut columns[0], "Scan ms", &mut self.draft.scan_ms);
                    edit_field(&mut columns[1], "Debounce ms", &mut self.draft.debounce_ms);
                    edit_field(&mut columns[2], "Retry max", &mut self.draft.retry_max);
                    edit_field(
                        &mut columns[3],
                        "Backoff ms",
                        &mut self.draft.retry_backoff_ms,
                    );
                });
                edit_field(ui, "Size", &mut self.draft.size);
                ui.add_space(12.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_sized([72.0, 28.0], egui::Button::new("Apply"))
                        .clicked()
                    {
                        self.apply_draft();
                    }
                    if ui
                        .add_sized([72.0, 28.0], egui::Button::new("Save"))
                        .clicked()
                    {
                        self.save();
                    }
                });
            });
    }

    fn open_local(&mut self, idx: usize) {
        let path = self
            .state
            .lock()
            .unwrap()
            .tasks
            .get(idx)
            .map(|task| task.cfg.local.clone());
        if let Some(path) = path {
            match open_local_dir(path) {
                Ok(()) => self.toast("Opened local folder"),
                Err(e) => self.toast(format!("Open local folder failed: {e}")),
            }
        }
    }
}

impl eframe::App for FSyncApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_task_events();
        if self
            .state
            .lock()
            .unwrap()
            .tasks
            .iter()
            .any(|task| task.handle.is_some() || task.starting)
        {
            ctx.request_repaint_after(Duration::from_millis(250));
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let root_size = ui.available_size();
        egui::Frame::central_panel(ui.style()).show(ui, |ui| {
            ui.set_min_size(root_size);
            let full_size = ui.available_size();
            let left_width = 380.0;
            let gap = 10.0;
            let right_width = (full_size.x - left_width - gap).max(320.0);
            let panel_height = full_size.y;

            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(left_width, panel_height),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        panel_frame(ui).show(ui, |ui| {
                            ui.set_min_height((panel_height - 32.0).max(0.0));
                            self.render_left_panel(ui);
                        });
                    },
                );

                ui.add_space(gap);

                ui.allocate_ui_with_layout(
                    egui::vec2(right_width, panel_height),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        panel_frame(ui).show(ui, |ui| {
                            ui.set_min_height((panel_height - 32.0).max(0.0));
                            self.render_right_panel(ui);
                        });
                    },
                );
            });
        });

        if let Some((message, created_at)) = &self.toast {
            if created_at.elapsed() < Duration::from_secs(4) {
                egui::Area::new("toast".into())
                    .anchor(egui::Align2::RIGHT_BOTTOM, [-18.0, -18.0])
                    .show(&ctx, |ui| {
                        egui::Frame::popup(ui.style())
                            .corner_radius(6.0)
                            .inner_margin(egui::Margin::symmetric(12, 8))
                            .show(ui, |ui| {
                                ui.label(message);
                            });
                    });
            }
        }
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

fn configure_style(ctx: &egui::Context, theme_mode: ThemeMode) {
    ctx.options_mut(|opt| {
        opt.theme_preference = theme_mode.as_preference();
    });

    for theme in [Theme::Light, Theme::Dark] {
        ctx.style_mut_of(theme, |style| {
            style.spacing.item_spacing = egui::vec2(8.0, 8.0);
            style.spacing.button_padding = egui::vec2(10.0, 5.0);
        });
    }
}

fn install_chinese_fonts(ctx: &egui::Context) {
    let mut fonts = FontDefinitions::default();
    let mut fallback_fonts = Vec::new();

    for (name, path) in [
        ("msyh", r"C:\Windows\Fonts\msyh.ttc"),
        ("simhei", r"C:\Windows\Fonts\simhei.ttf"),
        ("simsun", r"C:\Windows\Fonts\simsun.ttc"),
        ("noto_sans_sc", r"C:\Windows\Fonts\NotoSansSC-VF.ttf"),
    ] {
        if let Ok(bytes) = fs::read(path) {
            fonts
                .font_data
                .insert(name.to_owned(), Arc::new(FontData::from_owned(bytes)));
            fallback_fonts.push(name.to_owned());
        }
    }

    if fallback_fonts.is_empty() {
        return;
    }

    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        let entries = fonts.families.entry(family).or_default();
        entries.extend(fallback_fonts.iter().cloned());
    }

    ctx.set_fonts(fonts);
}

fn panel_frame(ui: &egui::Ui) -> egui::Frame {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().window_fill())
        .inner_margin(egui::Margin::same(12))
        .outer_margin(egui::Margin::same(8))
        .corner_radius(8.0)
}

fn info_tile(ui: &mut egui::Ui, label: &str, value: &str) {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(egui::Margin::symmetric(10, 7))
        .show(ui, |ui| {
            ui.set_min_height(44.0);
            ui.label(egui::RichText::new(label).small().weak());
            ui.label(value);
        });
}

fn edit_field(ui: &mut egui::Ui, label: &str, value: &mut String) {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(egui::Margin::symmetric(10, 7))
        .show(ui, |ui| {
            ui.set_min_height(58.0);
            ui.label(egui::RichText::new(label).small().weak());
            ui.add_sized(
                [ui.available_width(), 28.0],
                egui::TextEdit::singleline(value),
            );
        });
}

fn edit_password(ui: &mut egui::Ui, label: &str, value: &mut String) {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(egui::Margin::symmetric(10, 7))
        .show(ui, |ui| {
            ui.set_min_height(58.0);
            ui.label(egui::RichText::new(label).small().weak());
            ui.add_sized(
                [ui.available_width(), 28.0],
                egui::TextEdit::singleline(value).password(true),
            );
        });
}

fn status_dot(ui: &mut egui::Ui, color: egui::Color32, hover_text: &str) {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(10.0, 10.0), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 3.5, color);
    response.on_hover_text(hover_text);
}

fn status_color(ui: &egui::Ui, state: &TaskState, starting: bool) -> egui::Color32 {
    let visuals = ui.visuals();
    if starting || matches!(state, TaskState::Starting(_)) {
        visuals.warn_fg_color
    } else {
        match state {
            TaskState::Idle => visuals.widgets.noninteractive.fg_stroke.color,
            TaskState::Starting(_) => visuals.warn_fg_color,
            TaskState::Running => visuals.hyperlink_color,
            TaskState::Error(_) => visuals.error_fg_color,
        }
    }
}

fn init_file_logging(config: &AppConfig) -> Result<()> {
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

fn load_app_config() -> Result<AppConfig> {
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

fn persist_app_config(config: &AppConfig) -> Result<()> {
    fs::write(CONFIG_PATH, serde_yaml::to_string(config)?)?;
    Ok(())
}

async fn init_storage(config: AppConfig) -> Result<AppStorage> {
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
    Ok(AppStorage { pool, config })
}

fn load_config(
    runtime: &Runtime,
    storage: &AppStorage,
    state: &Arc<Mutex<AppState>>,
) -> Result<()> {
    let configs = runtime.block_on(read_tasks(&storage.pool, &storage.config.cache_dir))?;
    let mut state = state.lock().unwrap();
    state.tasks = configs
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
    state.selected = if state.tasks.is_empty() {
        None
    } else {
        Some(0)
    };
    Ok(())
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
        SELECT id, name, local_path, remote_path, cache_dir, scan_ms, size_filter,
               retry_max, retry_backoff_ms, debounce_ms, remote_kind, sftp_host,
               sftp_user, sftp_password, sftp_key_path
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
                id, name, local_path, remote_path, cache_dir, scan_ms, size_filter,
                retry_max, retry_backoff_ms, debounce_ms, remote_kind, sftp_host,
                sftp_user, sftp_password, sftp_key_path, updated_at
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
            cache_dir: blank_to_none(&self.cache_dir).map(PathBuf::from),
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

fn selected_draft(state: &Arc<Mutex<AppState>>) -> Option<Draft> {
    let state = state.lock().unwrap();
    let idx = state.selected?;
    state
        .tasks
        .get(idx)
        .map(|task| Draft::from_config(&task.cfg))
}

fn patterns_text(patterns: &[Pattern]) -> String {
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
