use notify::{
    event::{CreateKind, ModifyKind, RemoveKind},
    EventKind,
};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FsEvent {
    Create(PathBuf),
    Modify(PathBuf),
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
    MkDir(PathBuf),
}

// #[derive(Debug, Clone)]
// pub enum FsEvent {
//     Create(PathBuf),
//     Modify(PathBuf),
//     Remove(PathBuf),
//     Rename(PathBuf, PathBuf),
//     MkDir(PathBuf),
// }

impl FsEvent {
    pub fn path(&self) -> &std::path::Path {
        match self {
            FsEvent::Create(p) | FsEvent::Modify(p) | FsEvent::Remove(p) | FsEvent::MkDir(p) => p,
            FsEvent::Rename(_from, to) => to,
        }
    }
}

/// Convert a notify::Event into zero or more FileOp.
pub fn event_to_ops(event: notify::Event) -> Vec<FsEvent> {
    let mut ops = Vec::new();
    match event.kind {
        EventKind::Create(CreateKind::File) => {
            for p in event.paths {
                ops.push(FsEvent::Create(p));
            }
        }
        EventKind::Create(CreateKind::Folder) => {
            for p in event.paths {
                ops.push(FsEvent::MkDir(p));
            }
        }
        EventKind::Modify(ModifyKind::Data(_)) | EventKind::Modify(ModifyKind::Metadata(_)) => {
            for p in event.paths {
                ops.push(FsEvent::Modify(p));
            }
        }
        EventKind::Modify(ModifyKind::Name(_)) => {
            // rename move event contains two paths (from, to)
            if event.paths.len() == 2 {
                let [from, to]: [PathBuf; 2] =
                    event.paths.try_into().expect("expected exactly 2 paths");
                ops.push(FsEvent::Rename(from, to));
            }
        }
        EventKind::Remove(RemoveKind::File) | EventKind::Remove(RemoveKind::Folder) => {
            for p in event.paths {
                ops.push(FsEvent::Remove(p));
            }
        }
        _ => {}
    }
    ops
}
