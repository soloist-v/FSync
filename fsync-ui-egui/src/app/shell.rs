use eframe::egui;

use crate::app::FSyncApp;
use crate::widgets::panel_frame;

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
            ctx.request_repaint_after(std::time::Duration::from_millis(250));
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

        self.render_profiles_modal(&ctx);

        if let Some((message, created_at)) = &self.toast {
            if created_at.elapsed() < std::time::Duration::from_secs(4) {
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
