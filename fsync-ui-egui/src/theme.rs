use anyhow::{Context, Result};
use eframe::egui::{self, FontData, FontDefinitions, FontFamily, Theme};
use std::fs;
use std::sync::Arc;

use crate::models::ThemeMode;

const APP_ICON_SVG: &str = include_str!("../assets/icon.svg");

pub(crate) fn configure_style(ctx: &egui::Context, theme_mode: ThemeMode) {
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

pub(crate) fn load_app_icon() -> Result<egui::IconData> {
    let tree =
        resvg::usvg::Tree::from_data(APP_ICON_SVG.as_bytes(), &resvg::usvg::Options::default())
            .context("failed to parse app icon svg")?;
    let icon_size = 128u32;
    let mut pixmap = resvg::tiny_skia::Pixmap::new(icon_size, icon_size)
        .context("failed to allocate app icon pixmap")?;
    let size = tree.size();
    let scale = (icon_size as f32 / size.width()).min(icon_size as f32 / size.height());
    let translate_x = (icon_size as f32 - size.width() * scale) * 0.5;
    let translate_y = (icon_size as f32 - size.height() * scale) * 0.5;
    let transform = resvg::tiny_skia::Transform::from_scale(scale, scale)
        .post_translate(translate_x, translate_y);

    resvg::render(&tree, transform, &mut pixmap.as_mut());

    Ok(egui::IconData {
        rgba: pixmap.take(),
        width: icon_size,
        height: icon_size,
    })
}

pub(crate) fn install_chinese_fonts(ctx: &egui::Context) {
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
