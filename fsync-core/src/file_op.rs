use std::path::{PathBuf, Path};
use notify::{EventKind, event::{ModifyKind, RemoveKind, CreateKind}};

#[derive(Debug, Clone)]
pub enum FileOp {
    Create(PathBuf),
    Modify(PathBuf),
    Remove(PathBuf),
    Rename(PathBuf, PathBuf),
    MkDir(PathBuf),
}

impl FileOp {
    pub fn path(&self) -> &Path {
        match self {
            FileOp::Create(p) | FileOp::Modify(p) | FileOp::Remove(p) | FileOp::MkDir(p) => p,
            FileOp::Rename(from, _to) => from,
        }
    }
}

/// Convert a notify::Event into zero or more FileOp.
pub fn event_to_ops(event: notify::Event) -> Vec<FileOp> {
    let mut ops = Vec::new();
    match event.kind {
        EventKind::Create(CreateKind::File) => {
            for p in event.paths { ops.push(FileOp::Create(p)); }
        }
        EventKind::Create(CreateKind::Folder) => {
            for p in event.paths { ops.push(FileOp::MkDir(p)); }
        }
        EventKind::Modify(ModifyKind::Data(_)) | EventKind::Modify(ModifyKind::Metadata(_)) => {
            for p in event.paths { ops.push(FileOp::Modify(p)); }
        }
        EventKind::Modify(ModifyKind::Name(_)) => {
            // rename move event contains two paths (from, to)
            if event.paths.len() == 2 {
                ops.push(FileOp::Rename(event.paths[0].clone(), event.paths[1].clone()));
            }
        }
        EventKind::Remove(RemoveKind::File) | EventKind::Remove(RemoveKind::Folder) => {
            for p in event.paths { ops.push(FileOp::Remove(p)); }
        }
        _ => {}
    }
    ops
}
