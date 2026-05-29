use eframe::egui;

use crate::app::FSyncApp;
use crate::models::{
    clear_remote_profile_from_tasks, selected_draft, selected_profile_draft, RemoteProfileDraft,
};
use crate::widgets::{edit_field, edit_password};

impl FSyncApp {
    pub(super) fn save_remote_profile(&mut self) {
        match self.profile_draft.to_profile() {
            Ok(profile) => {
                {
                    let mut state = self.state.lock().unwrap();
                    let index = state
                        .remote_profiles
                        .iter()
                        .position(|existing| existing.id == profile.id);
                    let selected = if let Some(index) = index {
                        state.remote_profiles[index] = profile.clone();
                        index
                    } else {
                        state.remote_profiles.push(profile.clone());
                        state.remote_profiles.len() - 1
                    };
                    crate::models::refresh_tasks_for_profile(&mut state.tasks, &profile);
                    self.selected_profile = Some(selected);
                }
                self.draft = selected_draft(&self.state).unwrap_or_default();
                self.profile_draft = RemoteProfileDraft::from_profile(&profile);
                match self.persist_state() {
                    Ok(_) => self.toast("Remote profile saved"),
                    Err(e) => self.toast(format!("Save profile failed: {e}")),
                }
            }
            Err(e) => self.toast(format!("Invalid profile: {e}")),
        }
    }

    pub(super) fn delete_remote_profile(&mut self) {
        let Some(selected_profile) = self.selected_profile else {
            return;
        };
        let removed_profile = {
            let mut state = self.state.lock().unwrap();
            if selected_profile >= state.remote_profiles.len() {
                return;
            }
            let removed = state.remote_profiles.remove(selected_profile);
            clear_remote_profile_from_tasks(&mut state.tasks, removed.id);
            removed
        };
        self.selected_profile = {
            let state = self.state.lock().unwrap();
            if state.remote_profiles.is_empty() {
                None
            } else {
                Some(selected_profile.min(state.remote_profiles.len() - 1))
            }
        };
        if self.draft.remote_profile_id == Some(removed_profile.id) {
            self.draft.remote_profile_id = None;
            let _ = self.apply_draft();
        }
        self.profile_draft = selected_profile_draft(&self.state, self.selected_profile)
            .unwrap_or_else(RemoteProfileDraft::new_empty);
        match self.persist_state() {
            Ok(_) => self.toast("Remote profile deleted"),
            Err(e) => self.toast(format!("Delete profile failed: {e}")),
        }
    }

    pub(super) fn render_profiles_modal(&mut self, ctx: &egui::Context) {
        if !self.show_profiles_modal {
            return;
        }

        let profiles = {
            let state = self.state.lock().unwrap();
            state.remote_profiles.clone()
        };

        let mut open = self.show_profiles_modal;
        egui::Window::new("Remote Profiles")
            .open(&mut open)
            .default_size(egui::vec2(760.0, 420.0))
            .resizable(true)
            .show(ctx, |ui| {
                let list_height = ui.available_height().max(280.0);
                ui.horizontal_top(|ui| {
                    ui.vertical(|ui| {
                        ui.set_width(240.0);
                        egui::Frame::group(ui.style())
                            .fill(ui.visuals().faint_bg_color)
                            .inner_margin(egui::Margin::same(10))
                            .show(ui, |ui| {
                                if ui.button("New").clicked() {
                                    self.selected_profile = None;
                                    self.profile_draft = RemoteProfileDraft::new_empty();
                                }
                                ui.add_space(8.0);
                                egui::ScrollArea::vertical()
                                    .max_height((list_height - 56.0).max(220.0))
                                    .auto_shrink([false, false])
                                    .id_salt("remote_profiles_list")
                                    .show(ui, |ui| {
                                        for (idx, profile) in profiles.iter().enumerate() {
                                            let mut delete_clicked = false;
                                            let mut select_clicked = false;
                                            let selected = self.selected_profile == Some(idx);
                                            egui::Frame::group(ui.style())
                                                .fill(if selected {
                                                    ui.visuals()
                                                        .selection
                                                        .bg_fill
                                                        .linear_multiply(0.12)
                                                } else {
                                                    ui.visuals().window_fill()
                                                })
                                                .inner_margin(egui::Margin::symmetric(8, 6))
                                                .show(ui, |ui| {
                                                    ui.set_min_width(ui.available_width());
                                                    ui.horizontal(|ui| {
                                                        let delete_width = 52.0;
                                                        let spacing = ui.spacing().item_spacing.x;
                                                        let content_width = (ui.available_width()
                                                            - delete_width
                                                            - spacing)
                                                            .max(80.0);
                                                        let name = if selected {
                                                            egui::RichText::new(&profile.name)
                                                                .strong()
                                                                .color(
                                                                    ui.visuals()
                                                                        .selection
                                                                        .stroke
                                                                        .color,
                                                                )
                                                        } else {
                                                            egui::RichText::new(&profile.name)
                                                                .strong()
                                                        };
                                                        ui.allocate_ui_with_layout(
                                                            egui::vec2(content_width, 24.0),
                                                            egui::Layout::left_to_right(
                                                                egui::Align::Center,
                                                            ),
                                                            |ui| {
                                                                if ui
                                                                    .add(
                                                                        egui::Label::new(name)
                                                                            .sense(
                                                                                egui::Sense::click(
                                                                                ),
                                                                            ),
                                                                    )
                                                                    .on_hover_cursor(
                                                                        egui::CursorIcon::PointingHand,
                                                                    )
                                                                    .clicked()
                                                                {
                                                                    select_clicked = true;
                                                                }
                                                                let host_width =
                                                                    ui.available_width().max(24.0);
                                                                if ui
                                                                    .add_sized(
                                                                        [host_width, 24.0],
                                                                        egui::Label::new(
                                                                            egui::RichText::new(
                                                                                &profile.host,
                                                                            )
                                                                            .small()
                                                                            .weak(),
                                                                        )
                                                                        .truncate()
                                                                        .sense(
                                                                            egui::Sense::click(),
                                                                        ),
                                                                    )
                                                                    .on_hover_cursor(
                                                                        egui::CursorIcon::PointingHand,
                                                                    )
                                                                    .clicked()
                                                                {
                                                                    select_clicked = true;
                                                                }
                                                            },
                                                        );
                                                        ui.with_layout(
                                                            egui::Layout::right_to_left(
                                                                egui::Align::Center,
                                                            ),
                                                            |ui| {
                                                                if ui
                                                                    .add_sized(
                                                                        [52.0, 24.0],
                                                                        egui::Button::new("Delete"),
                                                                    )
                                                                    .clicked()
                                                                {
                                                                    delete_clicked = true;
                                                                }
                                                            },
                                                        );
                                                    });
                                                });
                                            ui.add_space(6.0);
                                            if delete_clicked {
                                                self.selected_profile = Some(idx);
                                                self.delete_remote_profile();
                                                break;
                                            }
                                            if select_clicked {
                                                self.selected_profile = Some(idx);
                                                self.profile_draft =
                                                    RemoteProfileDraft::from_profile(profile);
                                            }
                                        }
                                    });
                            });
                    });

                    ui.add_space(12.0);

                    ui.vertical(|ui| {
                        egui::Frame::group(ui.style())
                            .fill(ui.visuals().faint_bg_color)
                            .inner_margin(egui::Margin::same(10))
                            .show(ui, |ui| {
                                edit_field(ui, "Name", &mut self.profile_draft.name);
                                edit_field(ui, "Host", &mut self.profile_draft.host);
                                edit_field(ui, "User", &mut self.profile_draft.user);
                                edit_password(
                                    ui,
                                    "Password",
                                    &mut self.profile_draft.password,
                                    &mut self.profile_password_visible,
                                );
                                edit_field(ui, "Key Path", &mut self.profile_draft.key_path);
                                edit_field(
                                    ui,
                                    "Fingerprints (; separated)",
                                    &mut self.profile_draft.fingerprints,
                                );
                                ui.add_space(12.0);
                                ui.horizontal(|ui| {
                                    if ui
                                        .add_sized([72.0, 28.0], egui::Button::new("Save"))
                                        .clicked()
                                    {
                                        self.save_remote_profile();
                                    }
                                    if ui
                                        .add_sized([72.0, 28.0], egui::Button::new("Restore"))
                                        .clicked()
                                    {
                                        self.restore_remote_profile_draft();
                                    }
                                });
                            });
                    });
                });
            });
        self.show_profiles_modal = open;
    }
}
