use eframe::egui;
use fsync_core::TaskState;
use uuid::Uuid;

use crate::models::{find_remote_profile, RemoteProfile};

pub(crate) fn panel_frame(ui: &egui::Ui) -> egui::Frame {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().window_fill())
        .inner_margin(egui::Margin::same(12))
        .outer_margin(egui::Margin::same(8))
        .corner_radius(8.0)
}

pub(crate) fn info_tile_sized(ui: &mut egui::Ui, label: &str, value: &str, size: egui::Vec2) {
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let visuals = ui.visuals();
    ui.painter().rect(
        rect,
        4.0,
        visuals.faint_bg_color,
        visuals.widgets.noninteractive.bg_stroke,
        egui::StrokeKind::Inside,
    );

    let content = rect.shrink2(egui::vec2(10.0, 7.0));
    let label_font = egui::TextStyle::Small.resolve(ui.style());
    let value_font = egui::TextStyle::Body.resolve(ui.style());
    let label_color = visuals.weak_text_color();
    let value_color = visuals.text_color();
    let painter = ui.painter().with_clip_rect(content);

    painter.text(
        content.left_top(),
        egui::Align2::LEFT_TOP,
        label,
        label_font,
        label_color,
    );
    painter.text(
        content.left_top() + egui::vec2(0.0, 20.0),
        egui::Align2::LEFT_TOP,
        value,
        value_font,
        value_color,
    );

    response.on_hover_text(value);
}

pub(crate) fn edit_field(ui: &mut egui::Ui, label: &str, value: &mut String) {
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

pub(crate) fn edit_password(
    ui: &mut egui::Ui,
    label: &str,
    value: &mut String,
    visible: &mut bool,
) {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(egui::Margin::symmetric(10, 7))
        .show(ui, |ui| {
            ui.set_min_height(58.0);
            ui.label(egui::RichText::new(label).small().weak());
            ui.horizontal(|ui| {
                let button_width = 56.0;
                let spacing = ui.spacing().item_spacing.x;
                let edit_width = (ui.available_width() - button_width - spacing).max(80.0);
                ui.add_sized(
                    [edit_width, 28.0],
                    egui::TextEdit::singleline(value).password(!*visible),
                );
                if ui
                    .add_sized(
                        [button_width, 28.0],
                        egui::Button::new(if *visible { "Hide" } else { "Show" }),
                    )
                    .clicked()
                {
                    *visible = !*visible;
                }
            });
        });
}

pub(crate) fn edit_remote_profile_selector(
    ui: &mut egui::Ui,
    profiles: &[RemoteProfile],
    selected: &mut Option<Uuid>,
    show_profiles_modal: &mut bool,
) {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().faint_bg_color)
        .inner_margin(egui::Margin::symmetric(10, 7))
        .show(ui, |ui| {
            ui.set_min_height(58.0);
            ui.label(egui::RichText::new("Remote Profile").small().weak());
            ui.horizontal(|ui| {
                egui::ComboBox::from_id_salt("task_remote_profile")
                    .selected_text(
                        find_remote_profile(profiles, *selected)
                            .map(|profile| profile.name.as_str())
                            .unwrap_or("Unassigned"),
                    )
                    .width((ui.available_width() - 84.0).max(120.0))
                    .show_ui(ui, |ui| {
                        ui.selectable_value(selected, None, "Unassigned");
                        for profile in profiles {
                            ui.selectable_value(selected, Some(profile.id), &profile.name);
                        }
                    });
                if ui
                    .add_sized([72.0, 28.0], egui::Button::new("Manage"))
                    .clicked()
                {
                    *show_profiles_modal = true;
                }
            });
        });
}

pub(crate) fn status_dot(ui: &mut egui::Ui, color: egui::Color32, hover_text: &str) {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(10.0, 10.0), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 3.5, color);
    response.on_hover_text(hover_text);
}

pub(crate) fn status_color(ui: &egui::Ui, state: &TaskState, starting: bool) -> egui::Color32 {
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
