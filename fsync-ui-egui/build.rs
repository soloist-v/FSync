use anyhow::{Context, Result};
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=assets/icon.svg");

    #[cfg(target_os = "windows")]
    if let Err(error) = compile_windows_icon() {
        panic!("failed to build Windows icon resource: {error:#}");
    }
}

#[cfg(target_os = "windows")]
fn compile_windows_icon() -> Result<()> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let svg_path = manifest_dir.join("assets").join("icon.svg");
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let ico_path = out_dir.join("fsync-icon.ico");

    write_ico_from_svg(&svg_path, &ico_path)?;

    winresource::WindowsResource::new()
        .set_icon(ico_path.to_string_lossy().as_ref())
        .compile()
        .context("failed to compile Windows resources")?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn write_ico_from_svg(svg_path: &Path, ico_path: &Path) -> Result<()> {
    let svg_bytes = std::fs::read(svg_path)
        .with_context(|| format!("failed to read svg icon at {}", svg_path.display()))?;
    let tree = resvg::usvg::Tree::from_data(&svg_bytes, &resvg::usvg::Options::default())
        .context("failed to parse svg icon")?;

    let mut icon_dir = ico::IconDir::new(ico::ResourceType::Icon);
    for size in [16u32, 24, 32, 48, 64, 128, 256] {
        let image = render_icon_image(&tree, size)?;
        icon_dir.add_entry(ico::IconDirEntry::encode(&image)?);
    }

    let file = File::create(ico_path)
        .with_context(|| format!("failed to create ico file at {}", ico_path.display()))?;
    let mut writer = BufWriter::new(file);
    icon_dir
        .write(&mut writer)
        .context("failed to write ico file")?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn render_icon_image(tree: &resvg::usvg::Tree, size: u32) -> Result<ico::IconImage> {
    let mut pixmap = resvg::tiny_skia::Pixmap::new(size, size)
        .with_context(|| format!("failed to allocate {size}x{size} icon pixmap"))?;
    let svg_size = tree.size();
    let scale = (size as f32 / svg_size.width()).min(size as f32 / svg_size.height());
    let translate_x = (size as f32 - svg_size.width() * scale) * 0.5;
    let translate_y = (size as f32 - svg_size.height() * scale) * 0.5;
    let transform = resvg::tiny_skia::Transform::from_scale(scale, scale)
        .post_translate(translate_x, translate_y);

    resvg::render(tree, transform, &mut pixmap.as_mut());

    ico::IconImage::from_rgba_data(size, size, pixmap.take()).pipe(Ok)
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
