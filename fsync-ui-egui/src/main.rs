#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod app;
mod models;
mod operation_logs;
mod storage;
mod theme;
mod widgets;

use anyhow::{anyhow, Result};
use eframe::egui;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use crate::app::FSyncApp;
use crate::models::AppState;
use crate::storage::{init_file_logging, init_storage, load_app_config, load_config};
use crate::theme::load_app_icon;

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
    let app_icon = load_app_icon()?;

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("FSync")
            .with_inner_size([1120.0, 760.0])
            .with_min_inner_size([900.0, 560.0])
            .with_icon(app_icon),
        ..Default::default()
    };
    eframe::run_native(
        "FSync",
        options,
        Box::new(move |cc| Ok(Box::new(FSyncApp::new(cc, runtime, storage, state)))),
    )
    .map_err(|e| anyhow!("{e}"))
}
