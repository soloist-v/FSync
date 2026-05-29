use eframe::egui;

use crate::app::FSyncApp;
use crate::models::{find_remote_profile, patterns_text, state_label, PanelTab};
use crate::widgets::{
    edit_field, edit_remote_profile_selector, info_tile, status_color, status_dot,
};

impl FSyncApp {
    pub(super) fn render_left_panel(&mut self, ui: &mut egui::Ui) {
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
                    for mode in [
                        crate::models::ThemeMode::System,
                        crate::models::ThemeMode::Light,
                        crate::models::ThemeMode::Dark,
                    ] {
                        if ui
                            .selectable_label(self.theme_mode == mode, mode.as_label())
                            .clicked()
                        {
                            next_theme = Some(mode);
                            ui.close();
                        }
                    }
                });
                if ui
                    .add_sized([76.0, 28.0], egui::Button::new("Profiles"))
                    .clicked()
                {
                    self.show_profiles_modal = true;
                    self.sync_profile_modal_state();
                }
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
                for (idx, selected, name, status, running, state, starting) in rows {
                    let mut toggle_clicked = false;
                    let row_width = ui.available_width();
                    let row = ui.allocate_ui_with_layout(
                        egui::vec2(row_width, 40.0),
                        egui::Layout::top_down(egui::Align::Min),
                        |ui| {
                            egui::Frame::group(ui.style())
                                .fill(if selected {
                                    ui.visuals().selection.bg_fill.linear_multiply(0.12)
                                } else {
                                    ui.visuals().window_fill()
                                })
                                .corner_radius(6.0)
                                .inner_margin(egui::Margin::symmetric(10, 8))
                                .show(ui, |ui| {
                                    ui.set_min_width(ui.available_width());
                                    let button_width = 58.0;

                                    ui.horizontal(|ui| {
                                        ui.spacing_mut().item_spacing.x = 8.0;
                                        status_dot(ui, status_color(ui, &state, starting), &status);
                                        let name_text = if selected {
                                            egui::RichText::new(name)
                                                .strong()
                                                .size(14.0)
                                                .color(ui.visuals().selection.stroke.color)
                                        } else {
                                            egui::RichText::new(name).strong().size(14.0)
                                        };
                                        ui.label(name_text);
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

    pub(super) fn render_right_panel(&mut self, ui: &mut egui::Ui) {
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

    pub(super) fn render_dashboard(&mut self, ui: &mut egui::Ui, idx: usize) {
        let (cfg, logs, profile) = {
            let state = self.state.lock().unwrap();
            let task = &state.tasks[idx];
            (
                task.cfg.clone(),
                task.logs.clone(),
                find_remote_profile(&state.remote_profiles, task.remote_profile_id).cloned(),
            )
        };
        ui.columns(2, |columns| {
            info_tile(&mut columns[0], "Local", &cfg.local.display().to_string());
            info_tile(&mut columns[1], "Remote", &cfg.remote);

            info_tile(
                &mut columns[0],
                "Profile",
                profile
                    .as_ref()
                    .map(|profile| profile.name.as_str())
                    .unwrap_or("Unassigned"),
            );
            info_tile(&mut columns[1], "Task ID", &cfg.id.to_string());

            info_tile(
                &mut columns[0],
                "User",
                profile
                    .as_ref()
                    .map(|profile| profile.user.as_str())
                    .unwrap_or("-"),
            );
            info_tile(
                &mut columns[1],
                "Host",
                profile
                    .as_ref()
                    .map(|profile| profile.host.as_str())
                    .unwrap_or("-"),
            );

            info_tile(&mut columns[0], "Include", &patterns_text(&cfg.include));
            info_tile(&mut columns[1], "Exclude", &patterns_text(&cfg.exclude));
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

    pub(super) fn render_settings(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .id_salt("task_settings_scroll")
            .show(ui, |ui| {
                let profiles = {
                    let state = self.state.lock().unwrap();
                    state.remote_profiles.clone()
                };
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
                edit_remote_profile_selector(
                    ui,
                    &profiles,
                    &mut self.draft.remote_profile_id,
                    &mut self.show_profiles_modal,
                );
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
                        if let Err(e) = self.apply_draft() {
                            self.toast(format!("Invalid task: {e}"));
                        }
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

    pub(super) fn open_local(&mut self, idx: usize) {
        let path = self
            .state
            .lock()
            .unwrap()
            .tasks
            .get(idx)
            .map(|task| task.cfg.local.clone());
        if let Some(path) = path {
            match crate::storage::open_local_dir(path) {
                Ok(()) => self.toast("Opened local folder"),
                Err(e) => self.toast(format!("Open local folder failed: {e}")),
            }
        }
    }
}
