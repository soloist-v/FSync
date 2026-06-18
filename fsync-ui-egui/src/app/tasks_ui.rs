use eframe::egui;

use crate::app::{FSyncApp, PatternEditorKind};
use crate::models::{find_remote_profile, path_text, patterns_text, state_label, PanelTab};
use crate::widgets::{
    edit_field, edit_remote_profile_selector, info_tile_sized, status_color, status_dot,
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
        dashboard_info_row(
            ui,
            "Local",
            &path_text(&cfg.local),
            "Remote",
            &cfg.remote,
            58.0,
        );
        dashboard_info_row(
            ui,
            "Profile",
            profile
                .as_ref()
                .map(|profile| profile.name.as_str())
                .unwrap_or("Unassigned"),
            "Task ID",
            &cfg.id.to_string(),
            58.0,
        );
        dashboard_info_row(
            ui,
            "User",
            profile
                .as_ref()
                .map(|profile| profile.user.as_str())
                .unwrap_or("-"),
            "Host",
            profile
                .as_ref()
                .map(|profile| profile.host.as_str())
                .unwrap_or("-"),
            58.0,
        );
        dashboard_info_row(
            ui,
            "Include",
            &patterns_text(&cfg.include),
            "Exclude",
            &patterns_text(&cfg.exclude),
            78.0,
        );

        ui.add_space(8.0);
        ui.heading("Logs");
        ui.add_space(4.0);
        let height = ui.available_height().max(180.0);
        egui::Frame::new()
            .corner_radius(6.0)
            .inner_margin(egui::Margin::symmetric(10, 8))
            .show(ui, |ui| {
                let scroll_height = (height - 20.0).max(120.0);
                ui.set_min_height(scroll_height);
                egui::ScrollArea::vertical()
                    .id_salt("task_logs_scroll")
                    .stick_to_bottom(true)
                    .auto_shrink([false, false])
                    .max_height(scroll_height)
                    .show(ui, |ui| {
                        ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Extend);
                        if logs.is_empty() {
                            ui.label(egui::RichText::new("No log entries yet").monospace().weak());
                        } else {
                            for log in logs {
                                ui.label(egui::RichText::new(log).monospace());
                            }
                        }
                        ui.add_space(8.0);
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
                let include = self.draft.include.clone();
                let exclude = self.draft.exclude.clone();
                let mut edit_include = false;
                let mut edit_exclude = false;
                ui.columns(2, |columns| {
                    edit_include = pattern_preview(
                        &mut columns[0],
                        "Include",
                        &include,
                        "No include patterns",
                    );
                    edit_exclude = pattern_preview(
                        &mut columns[1],
                        "Exclude",
                        &exclude,
                        "No exclude patterns",
                    );
                });
                if edit_include {
                    self.open_pattern_editor(PatternEditorKind::Include);
                }
                if edit_exclude {
                    self.open_pattern_editor(PatternEditorKind::Exclude);
                }
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

    pub(super) fn render_pattern_modal(&mut self, ctx: &egui::Context) {
        let Some(kind) = self.pattern_editor else {
            return;
        };

        const MODAL_WIDTH: f32 = 680.0;
        const MODAL_HEIGHT: f32 = 430.0;
        const CONTENT_WIDTH: f32 = MODAL_WIDTH - 24.0;

        let mut open = true;
        egui::Window::new(kind.title())
            .open(&mut open)
            .fixed_size(egui::vec2(MODAL_WIDTH, MODAL_HEIGHT))
            .resizable(false)
            .collapsible(false)
            .constrain(true)
            .show(ctx, |ui| {
                ui.set_width(CONTENT_WIDTH);
                ui.horizontal(|ui| {
                    let edit_width = CONTENT_WIDTH - 80.0;
                    ui.add_sized(
                        [edit_width, 28.0],
                        egui::TextEdit::singleline(&mut self.new_pattern).hint_text("Pattern"),
                    );
                    if ui
                        .add_sized([72.0, 28.0], egui::Button::new("Add"))
                        .clicked()
                    {
                        let pattern = self.new_pattern.trim();
                        if !pattern.is_empty() {
                            self.pattern_draft.push(pattern.to_string());
                            self.new_pattern.clear();
                        }
                    }
                });
                ui.add_space(8.0);

                egui::Frame::group(ui.style())
                    .fill(ui.visuals().faint_bg_color)
                    .inner_margin(egui::Margin::same(8))
                    .show(ui, |ui| {
                        ui.set_width(CONTENT_WIDTH);
                        let height = 300.0;
                        egui::ScrollArea::vertical()
                            .id_salt("pattern_editor_list")
                            .max_height(height)
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                ui.set_width(CONTENT_WIDTH - 16.0);
                                let mut remove_idx = None;
                                let mut move_up_idx = None;
                                let mut move_down_idx = None;
                                let len = self.pattern_draft.len();

                                if len == 0 {
                                    ui.label(egui::RichText::new("No patterns").weak());
                                }

                                for idx in 0..len {
                                    ui.horizontal(|ui| {
                                        ui.set_width(CONTENT_WIDTH - 16.0);
                                        ui.label(
                                            egui::RichText::new(format!("{:02}", idx + 1))
                                                .small()
                                                .weak(),
                                        );
                                        let edit_width = CONTENT_WIDTH - 188.0;
                                        ui.add_sized(
                                            [edit_width, 26.0],
                                            egui::TextEdit::singleline(
                                                &mut self.pattern_draft[idx],
                                            ),
                                        );
                                        if ui
                                            .add_enabled(
                                                idx > 0,
                                                egui::Button::new("↑")
                                                    .min_size(egui::vec2(28.0, 24.0)),
                                            )
                                            .on_hover_text("Move up")
                                            .clicked()
                                        {
                                            move_up_idx = Some(idx);
                                        }
                                        if ui
                                            .add_enabled(
                                                idx + 1 < len,
                                                egui::Button::new("↓")
                                                    .min_size(egui::vec2(28.0, 24.0)),
                                            )
                                            .on_hover_text("Move down")
                                            .clicked()
                                        {
                                            move_down_idx = Some(idx);
                                        }
                                        if ui
                                            .add_sized([56.0, 24.0], egui::Button::new("Delete"))
                                            .clicked()
                                        {
                                            remove_idx = Some(idx);
                                        }
                                    });
                                    ui.add_space(4.0);
                                }

                                if let Some(idx) = move_up_idx {
                                    self.pattern_draft.swap(idx, idx - 1);
                                }
                                if let Some(idx) = move_down_idx {
                                    self.pattern_draft.swap(idx, idx + 1);
                                }
                                if let Some(idx) = remove_idx {
                                    self.pattern_draft.remove(idx);
                                }
                            });
                    });

                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_sized([72.0, 28.0], egui::Button::new("Apply"))
                        .clicked()
                    {
                        self.apply_pattern_editor();
                    }
                    if ui
                        .add_sized([72.0, 28.0], egui::Button::new("Cancel"))
                        .clicked()
                    {
                        self.pattern_editor = None;
                        self.new_pattern.clear();
                    }
                });
            });

        if !open {
            self.pattern_editor = None;
            self.new_pattern.clear();
        }
    }
}

fn pattern_preview(ui: &mut egui::Ui, label: &str, value: &str, empty_text: &str) -> bool {
    let mut modify_clicked = false;
    egui::Frame::group(ui.style())
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(egui::Margin::symmetric(10, 7))
        .show(ui, |ui| {
            ui.set_min_height(58.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(label).small().weak());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add_sized([72.0, 24.0], egui::Button::new("Modify"))
                        .clicked()
                    {
                        modify_clicked = true;
                    }
                });
            });
            let preview = if value.trim().is_empty() {
                empty_text
            } else {
                value.trim()
            };
            ui.add(
                egui::Label::new(egui::RichText::new(preview).monospace())
                    .truncate()
                    .selectable(false),
            );
        });
    modify_clicked
}

fn dashboard_info_row(
    ui: &mut egui::Ui,
    left_label: &str,
    left_value: &str,
    right_label: &str,
    right_value: &str,
    height: f32,
) {
    let spacing = ui.spacing().item_spacing.x;
    let width = ui.available_width();
    let column_width = ((width - spacing) / 2.0).max(120.0);

    ui.horizontal_top(|ui| {
        ui.spacing_mut().item_spacing.x = spacing;
        let tile_size = egui::vec2(column_width, height);
        info_tile_sized(ui, left_label, left_value, tile_size);
        info_tile_sized(ui, right_label, right_value, tile_size);
    });
    ui.add_space(spacing);
}
