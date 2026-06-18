use notify::{
    event::{CreateKind, ModifyKind, RemoveKind, RenameMode},
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
        EventKind::Create(CreateKind::Any) => {
            for p in event.paths {
                if p.is_dir() {
                    ops.push(FsEvent::MkDir(p));
                } else {
                    ops.push(FsEvent::Create(p));
                }
            }
        }
        EventKind::Create(CreateKind::Folder) => {
            for p in event.paths {
                ops.push(FsEvent::MkDir(p));
            }
        }
        EventKind::Modify(ModifyKind::Any)
        | EventKind::Modify(ModifyKind::Data(_))
        | EventKind::Modify(ModifyKind::Metadata(_))
        | EventKind::Modify(ModifyKind::Other) => {
            for p in event.paths {
                ops.push(FsEvent::Modify(p));
            }
        }
        EventKind::Modify(ModifyKind::Name(mode)) => match (mode, event.paths.len()) {
            (RenameMode::Both, 2) | (RenameMode::Any, 2) | (RenameMode::Other, 2) => {
                let mut paths = event.paths.into_iter();
                let from = paths.next().expect("len checked");
                let to = paths.next().expect("len checked");
                ops.push(FsEvent::Rename(from, to));
            }
            (RenameMode::From, _) => {
                for p in event.paths {
                    ops.push(FsEvent::Remove(p));
                }
            }
            (RenameMode::To, _) | (RenameMode::Any, _) | (RenameMode::Other, _) => {
                for p in event.paths {
                    if p.is_dir() {
                        ops.push(FsEvent::MkDir(p));
                    } else {
                        ops.push(FsEvent::Create(p));
                    }
                }
            }
            (RenameMode::Both, _) => {}
        },
        EventKind::Remove(RemoveKind::File)
        | EventKind::Remove(RemoveKind::Folder)
        | EventKind::Remove(RemoveKind::Any) => {
            for p in event.paths {
                ops.push(FsEvent::Remove(p));
            }
        }
        _ => {}
    }
    ops
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::{
        event::{CreateKind, DataChange, RenameMode},
        Event,
    };
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn create_any_detects_existing_directory() {
        let dir = std::env::temp_dir().join(format!(
            "fsync-file-op-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();

        let event = Event {
            kind: EventKind::Create(CreateKind::Any),
            paths: vec![dir.clone()],
            attrs: Default::default(),
        };

        assert_eq!(event_to_ops(event), vec![FsEvent::MkDir(dir.clone())]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn single_path_rename_to_is_treated_as_create() {
        let file = p("new.txt");
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            paths: vec![file.clone()],
            attrs: Default::default(),
        };

        assert_eq!(event_to_ops(event), vec![FsEvent::Create(file)]);
    }

    #[test]
    fn single_path_rename_from_is_treated_as_remove() {
        let file = p("old.txt");
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::From)),
            paths: vec![file.clone()],
            attrs: Default::default(),
        };

        assert_eq!(event_to_ops(event), vec![FsEvent::Remove(file)]);
    }

    #[test]
    fn two_path_name_event_is_treated_as_rename() {
        let from = p("old.txt");
        let to = p("new.txt");
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            paths: vec![from.clone(), to.clone()],
            attrs: Default::default(),
        };

        assert_eq!(event_to_ops(event), vec![FsEvent::Rename(from, to)]);
    }

    #[test]
    fn unknown_modify_kind_still_triggers_upload_check() {
        let file = p("changed.txt");
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            paths: vec![file.clone()],
            attrs: Default::default(),
        };

        assert_eq!(event_to_ops(event), vec![FsEvent::Modify(file)]);
    }

    fn p(path: &str) -> PathBuf {
        PathBuf::from(path)
    }
}
