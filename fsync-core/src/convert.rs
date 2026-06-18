use crate::file_op::FsEvent;
use crate::utils::as_posix_path;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EntityId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathKind {
    File,
    Dir,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentState {
    Clean,
    Dirty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileIntent {
    Create,
    Modify,
}

#[derive(Debug, Clone)]
struct EntityState {
    id: EntityId,
    original_path: Option<PathBuf>,
    final_path: Option<PathBuf>,
    kind: PathKind,
    content: ContentState,
    file_intent: FileIntent,
    created_locally: bool,
    had_prior_remove: bool,
    first_seen: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoveStage {
    BeforeRenames,
    AfterParentRenames,
}

#[derive(Debug, Clone)]
struct RemovedPath {
    path: PathBuf,
    stage: RemoveStage,
    order: usize,
}

#[derive(Debug, Default)]
struct FinalStateBuilder {
    path_to_entity: HashMap<PathBuf, EntityId>,
    entities: Vec<EntityState>,
    removed_paths: Vec<RemovedPath>,
    preserve_remove_on_create: HashSet<PathBuf>,
    suppressed_removes: HashSet<PathBuf>,
    removed_final_to_remote: HashMap<PathBuf, PathBuf>,
    order: usize,
}

pub fn collapse_ops(events: Vec<FsEvent>) -> Vec<FsEvent> {
    FinalStateBuilder::new().apply_all(events).emit()
}

impl FinalStateBuilder {
    fn new() -> Self {
        Self::default()
    }

    fn apply_all(mut self, events: Vec<FsEvent>) -> Self {
        for event in events {
            self.apply(event);
        }
        self
    }

    fn apply(&mut self, event: FsEvent) {
        match event {
            FsEvent::Create(path) => self.create(path, PathKind::File),
            FsEvent::MkDir(path) => self.create(path, PathKind::Dir),
            FsEvent::Modify(path) => self.modify(path),
            FsEvent::Rename(from, to) => self.rename(from, to),
            FsEvent::Remove(path) => self.remove(path),
        }
        self.order += 1;
    }

    fn create(&mut self, path: PathBuf, kind: PathKind) {
        let path = self.rewrite_event_path_from_original_parent(&path);
        if let Some(existing) = self.path_to_entity.get(&path).copied() {
            self.remove_entity_for_local_create(existing);
        }

        let had_prior_remove = self.has_pending_remove(&path);
        if kind == PathKind::File && !self.preserve_remove_on_create.contains(&path) {
            self.suppress_pending_remove(&path);
            if let Some(remove_path) = self.removed_final_to_remote.remove(&path) {
                self.suppress_pending_remove(&remove_path);
            }
        } else if kind == PathKind::Dir {
            self.preserve_remove_on_create.insert(path.clone());
        }

        let id = self.push_entity(EntityState {
            id: EntityId(self.entities.len()),
            original_path: None,
            final_path: Some(path.clone()),
            kind,
            content: ContentState::Dirty,
            file_intent: FileIntent::Create,
            created_locally: true,
            had_prior_remove,
            first_seen: self.order,
        });
        self.path_to_entity.insert(path, id);
    }

    fn modify(&mut self, path: PathBuf) {
        let path = self.rewrite_event_path_from_original_parent(&path);
        let id = match self.path_to_entity.get(&path).copied() {
            Some(id) => id,
            None => {
                let original = self
                    .original_path_for_final_path(&path)
                    .unwrap_or_else(|| path.clone());
                self.infer_remote_entity(original, path.clone())
            }
        };

        let entity = self.entity_mut(id);
        entity.content = ContentState::Dirty;
        if entity.kind == PathKind::Unknown {
            entity.kind = PathKind::File;
        }
        entity.file_intent = FileIntent::Modify;
    }

    fn rename(&mut self, from: PathBuf, to: PathBuf) {
        let from = self.rewrite_event_path_from_original_parent(&from);
        let to = self.rewrite_event_path_from_original_parent(&to);
        if from == to {
            return;
        }

        let moved = match self.path_to_entity.remove(&from) {
            Some(id) => id,
            None => {
                let id = self.infer_remote_entity(from.clone(), from.clone());
                self.path_to_entity.remove(&from);
                id
            }
        };

        if let Some(overwritten) = self.path_to_entity.get(&to).copied() {
            let restore_target_remove = self.entities[overwritten.0].created_locally
                && self.suppressed_removes.contains(&to);
            self.remove_entity_for_overwrite(overwritten, RemoveStage::BeforeRenames);
            if restore_target_remove {
                self.record_remove(to.clone(), RemoveStage::BeforeRenames);
            }
        }

        self.rewrite_descendants(&from, &to);
        if self.entity_mut(moved).content == ContentState::Dirty {
            self.entity_mut(moved).file_intent = FileIntent::Create;
        }
        self.path_to_entity.insert(to.clone(), moved);
        self.entity_mut(moved).final_path = Some(to);
    }

    fn remove(&mut self, path: PathBuf) {
        let path = self.rewrite_event_path_from_original_parent(&path);
        self.remove_descendants(&path);

        let Some(id) = self.path_to_entity.remove(&path) else {
            let stage = self.remove_stage_for_unknown_path(&path);
            self.record_remove(path, stage);
            return;
        };

        let entity = self.entity_mut(id);
        let should_remove_remote =
            !entity.created_locally || entity.original_path.is_some() || entity.had_prior_remove;
        let remove_path = entity
            .original_path
            .clone()
            .or_else(|| entity.final_path.clone());
        let final_path = entity.final_path.clone();
        entity.final_path = None;

        if should_remove_remote {
            if let Some(path) = remove_path {
                if let Some(final_path) = final_path {
                    self.removed_final_to_remote
                        .insert(final_path, path.clone());
                }
                self.record_remove(path, RemoveStage::BeforeRenames);
            }
        }
    }

    fn emit(&self) -> Vec<FsEvent> {
        let parent_renames = self.parent_renames();
        let mut seen_removes = HashSet::new();
        let mut before = Vec::new();
        let mut after_parent = Vec::new();

        for remove in &self.removed_paths {
            let path = rewrite_descendant_path(&remove.path, &parent_renames);
            if !seen_removes.insert(path.clone()) {
                continue;
            }
            match remove.stage {
                RemoveStage::BeforeRenames => before.push((remove.order, FsEvent::Remove(path))),
                RemoveStage::AfterParentRenames => {
                    after_parent.push((remove.order, FsEvent::Remove(path)))
                }
            }
        }

        let mut parent_ops = Vec::new();
        let mut ops = Vec::new();

        for entity in self.live_entities() {
            let Some(final_path) = entity.final_path.as_ref() else {
                continue;
            };
            let final_path = rewrite_descendant_path(final_path, &parent_renames);

            if self.is_subsumed_by_local_directory(entity, &final_path) {
                continue;
            }

            let event = if entity.created_locally {
                Some(match entity.kind {
                    PathKind::Dir => FsEvent::MkDir(final_path),
                    PathKind::File | PathKind::Unknown => match entity.file_intent {
                        FileIntent::Create => FsEvent::Create(final_path),
                        FileIntent::Modify => FsEvent::Modify(final_path),
                    },
                })
            } else {
                match entity.original_path.as_ref() {
                    Some(original) if *original != final_path => {
                        if entity.content == ContentState::Clean {
                            Some(FsEvent::Rename(original.clone(), final_path))
                        } else {
                            let covered_by_parent_rename =
                                rewrite_descendant_path(original, &parent_renames) == final_path;
                            if !covered_by_parent_rename && seen_removes.insert(original.clone()) {
                                before.push((
                                    entity.first_seen + 1,
                                    FsEvent::Remove(original.clone()),
                                ));
                            }
                            Some(match entity.kind {
                                PathKind::Dir => FsEvent::MkDir(final_path),
                                PathKind::File => match entity.file_intent {
                                    FileIntent::Create => FsEvent::Create(final_path),
                                    FileIntent::Modify => FsEvent::Modify(final_path),
                                },
                                PathKind::Unknown => FsEvent::Create(final_path),
                            })
                        }
                    }
                    Some(_) | None if entity.content == ContentState::Dirty => {
                        Some(match entity.kind {
                            PathKind::Dir => FsEvent::MkDir(final_path),
                            PathKind::File => match entity.file_intent {
                                FileIntent::Create => FsEvent::Create(final_path),
                                FileIntent::Modify => FsEvent::Modify(final_path),
                            },
                            PathKind::Unknown => FsEvent::Modify(final_path),
                        })
                    }
                    _ => None,
                }
            };

            let Some(event) = event else {
                continue;
            };

            match &event {
                FsEvent::Rename(_, _) => parent_ops.push((entity.first_seen, event)),
                _ => ops.push((entity.first_seen, event)),
            }
        }

        before.sort_by_key(|(order, _)| *order);
        parent_ops.sort_by_key(|(_, event)| match event {
            FsEvent::Rename(_, to) => as_posix_path(to),
            _ => String::new(),
        });
        after_parent.sort_by_key(|(order, _)| *order);
        ops.sort_by_key(|(order, event)| match event {
            FsEvent::Rename(_, to) => (1usize, as_posix_path(to), *order),
            _ => (0usize, String::new(), *order),
        });

        let mut output = Vec::new();
        output.extend(before.into_iter().map(|(_, event)| event));
        output.extend(parent_ops.into_iter().map(|(_, event)| event));
        output.extend(after_parent.into_iter().map(|(_, event)| event));
        output.extend(ops.into_iter().map(|(_, event)| event));
        output
    }

    fn push_entity(&mut self, mut entity: EntityState) -> EntityId {
        let id = EntityId(self.entities.len());
        entity.id = id;
        self.entities.push(entity);
        id
    }

    fn infer_remote_entity(&mut self, original_path: PathBuf, final_path: PathBuf) -> EntityId {
        let id = self.push_entity(EntityState {
            id: EntityId(self.entities.len()),
            original_path: Some(original_path),
            final_path: Some(final_path.clone()),
            kind: PathKind::Unknown,
            content: ContentState::Clean,
            file_intent: FileIntent::Create,
            created_locally: false,
            had_prior_remove: false,
            first_seen: self.order,
        });
        self.path_to_entity.insert(final_path, id);
        id
    }

    fn entity_mut(&mut self, id: EntityId) -> &mut EntityState {
        &mut self.entities[id.0]
    }

    fn remove_entity_for_overwrite(&mut self, id: EntityId, stage: RemoveStage) {
        let entity = self.entity_mut(id);
        let path = entity
            .original_path
            .clone()
            .or_else(|| entity.final_path.clone());
        let should_remove_remote = !entity.created_locally || entity.original_path.is_some();
        if let Some(final_path) = entity.final_path.take() {
            self.path_to_entity.remove(&final_path);
        }
        if should_remove_remote {
            if let Some(path) = path {
                self.record_remove(path, stage);
            }
        }
    }

    fn remove_entity_for_local_create(&mut self, id: EntityId) {
        let entity = self.entity_mut(id);
        let should_remove_remote = entity.original_path.as_ref().is_some_and(|original| {
            Some(original) != entity.final_path.as_ref() && entity.content == ContentState::Dirty
        });
        let path = entity.original_path.clone();
        if let Some(final_path) = entity.final_path.take() {
            self.path_to_entity.remove(&final_path);
        }
        if should_remove_remote {
            if let Some(path) = path {
                self.record_remove(path, RemoveStage::BeforeRenames);
            }
        }
    }

    fn record_remove(&mut self, path: PathBuf, stage: RemoveStage) {
        self.removed_paths.push(RemovedPath {
            path,
            stage,
            order: self.order,
        });
    }

    fn remove_stage_for_unknown_path(&self, path: &Path) -> RemoveStage {
        let should_run_after_parent_rename = self.live_entities().into_iter().any(|entity| {
            let (Some(original), Some(final_path)) =
                (entity.original_path.as_ref(), entity.final_path.as_ref())
            else {
                return false;
            };
            entity.content == ContentState::Clean
                && original != final_path
                && is_descendant(path, final_path)
        });

        if should_run_after_parent_rename {
            RemoveStage::AfterParentRenames
        } else {
            RemoveStage::BeforeRenames
        }
    }

    fn suppress_pending_remove(&mut self, path: &Path) {
        let mut suppressed = false;
        self.removed_paths.retain(|remove| {
            let keep = remove.path != path;
            suppressed |= !keep;
            keep
        });
        if suppressed {
            self.suppressed_removes.insert(path.to_path_buf());
        }
    }

    fn has_pending_remove(&self, path: &Path) -> bool {
        self.removed_paths.iter().any(|remove| remove.path == path)
    }

    fn remove_descendants(&mut self, parent: &Path) {
        let descendants = self
            .path_to_entity
            .iter()
            .filter_map(|(path, id)| {
                if is_descendant(path, parent) {
                    Some((path.clone(), *id))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for (path, id) in descendants {
            self.path_to_entity.remove(&path);
            self.entity_mut(id).final_path = None;
        }
    }

    fn rewrite_descendants(&mut self, from: &Path, to: &Path) {
        let descendants = self
            .path_to_entity
            .iter()
            .filter_map(|(path, id)| {
                if is_descendant(path, from) {
                    Some((path.clone(), *id))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for (old_path, id) in descendants {
            let new_path = replace_prefix(&old_path, from, to);
            self.path_to_entity.remove(&old_path);
            self.path_to_entity.insert(new_path.clone(), id);
            self.entity_mut(id).final_path = Some(new_path);
        }

        for remove in &mut self.removed_paths {
            if is_descendant(&remove.path, from) {
                remove.path = replace_prefix(&remove.path, from, to);
                remove.stage = RemoveStage::AfterParentRenames;
            }
        }
        self.removed_paths.retain(|remove| {
            !(remove.stage == RemoveStage::BeforeRenames && is_descendant(&remove.path, to))
        });
    }

    fn live_entities(&self) -> Vec<&EntityState> {
        let mut entities = self
            .entities
            .iter()
            .filter(|entity| entity.final_path.is_some())
            .collect::<Vec<_>>();
        entities.sort_by_key(|entity| entity.first_seen);
        entities
    }

    fn parent_renames(&self) -> Vec<(PathBuf, PathBuf)> {
        let mut renames = Vec::new();
        for entity in self.live_entities() {
            let (Some(original), Some(final_path)) =
                (entity.original_path.as_ref(), entity.final_path.as_ref())
            else {
                continue;
            };
            if original == final_path || entity.content != ContentState::Clean {
                continue;
            }
            let affects_child_entity = self.entities.iter().any(|child| {
                child.id != entity.id
                    && (child.final_path.as_ref().is_some_and(|path| {
                        is_descendant(path, original) || is_descendant(path, final_path)
                    }) || child.original_path.as_ref().is_some_and(|path| {
                        is_descendant(path, original) || is_descendant(path, final_path)
                    }))
            });
            let affects_child_remove = self.removed_paths.iter().any(|remove| {
                is_descendant(&remove.path, original)
                    || (remove.stage == RemoveStage::AfterParentRenames
                        && is_descendant(&remove.path, final_path))
            });
            if affects_child_entity || affects_child_remove {
                renames.push((original.clone(), final_path.clone()));
            }
        }
        renames
    }

    fn original_path_for_final_path(&self, path: &Path) -> Option<PathBuf> {
        self.live_entities()
            .into_iter()
            .filter_map(|entity| {
                let original = entity.original_path.as_ref()?;
                let final_path = entity.final_path.as_ref()?;
                if entity.content == ContentState::Clean && is_descendant(path, final_path) {
                    Some(replace_prefix(path, final_path, original))
                } else {
                    None
                }
            })
            .max_by_key(|path| path.components().count())
    }

    fn rewrite_event_path_from_original_parent(&self, path: &Path) -> PathBuf {
        self.live_entities()
            .into_iter()
            .filter_map(|entity| {
                let original = entity.original_path.as_ref()?;
                let final_path = entity.final_path.as_ref()?;
                if entity.content == ContentState::Clean
                    && original != final_path
                    && is_descendant(path, original)
                {
                    Some(replace_prefix(path, original, final_path))
                } else {
                    None
                }
            })
            .max_by_key(|path| path.components().count())
            .unwrap_or_else(|| path.to_path_buf())
    }

    fn is_subsumed_by_local_directory(&self, entity: &EntityState, final_path: &Path) -> bool {
        entity.kind != PathKind::Dir
            && self.live_entities().into_iter().any(|parent| {
                parent.id != entity.id
                    && parent.created_locally
                    && !parent.had_prior_remove
                    && parent.kind == PathKind::Dir
                    && parent
                        .final_path
                        .as_ref()
                        .is_some_and(|parent_path| is_descendant(final_path, parent_path))
            })
    }
}

fn is_descendant(path: &Path, ancestor: &Path) -> bool {
    path != ancestor && path.starts_with(ancestor)
}

fn replace_prefix(path: &Path, from: &Path, to: &Path) -> PathBuf {
    match path.strip_prefix(from) {
        Ok(rest) => to.join(rest),
        Err(_) => path.to_path_buf(),
    }
}

fn rewrite_descendant_path(path: &Path, renames: &[(PathBuf, PathBuf)]) -> PathBuf {
    let mut rewritten = path.to_path_buf();
    for _ in 0..renames.len() {
        let Some((from, to)) = renames
            .iter()
            .filter(|(from, _)| is_descendant(&rewritten, from))
            .max_by_key(|(from, _)| from.components().count())
        else {
            break;
        };
        let next = replace_prefix(&rewritten, from, to);
        if next == rewritten {
            break;
        }
        rewritten = next;
    }
    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(path: &str) -> PathBuf {
        PathBuf::from(path)
    }

    #[test]
    fn collapses_duplicate_modifies() {
        assert_eq!(
            collapse_ops(vec![FsEvent::Modify(p("A")), FsEvent::Modify(p("A"))]),
            vec![FsEvent::Modify(p("A"))]
        );
    }

    #[test]
    fn collapses_pure_rename_chain() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Rename(p("A"), p("B")),
                FsEvent::Rename(p("B"), p("C")),
            ]),
            vec![FsEvent::Rename(p("A"), p("C"))]
        );
    }

    #[test]
    fn create_then_rename_uploads_final_path_only() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Create(p("A")),
                FsEvent::Rename(p("A"), p("B")),
            ]),
            vec![FsEvent::Create(p("B"))]
        );
    }

    #[test]
    fn modify_then_rename_removes_old_path_and_uploads_new_path() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Modify(p("A")),
                FsEvent::Rename(p("A"), p("B")),
            ]),
            vec![FsEvent::Remove(p("A")), FsEvent::Create(p("B"))]
        );
    }

    #[test]
    fn rename_then_modify_removes_old_path_and_uploads_new_path() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Rename(p("A"), p("B")),
                FsEvent::Modify(p("B")),
            ]),
            vec![FsEvent::Remove(p("A")), FsEvent::Modify(p("B"))]
        );
    }

    #[test]
    fn rename_then_remove_deletes_original_path() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Rename(p("A"), p("B")),
                FsEvent::Remove(p("B")),
            ]),
            vec![FsEvent::Remove(p("A"))]
        );
    }

    #[test]
    fn create_then_remove_is_noop() {
        assert_eq!(
            collapse_ops(vec![FsEvent::Create(p("A")), FsEvent::Remove(p("A"))]),
            Vec::<FsEvent>::new()
        );
    }

    #[test]
    fn remove_then_create_uploads_final_file() {
        assert_eq!(
            collapse_ops(vec![FsEvent::Remove(p("A")), FsEvent::Create(p("A"))]),
            vec![FsEvent::Create(p("A"))]
        );
    }

    #[test]
    fn rename_then_recreate_source_keeps_source_and_target() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Rename(p("A"), p("B")),
                FsEvent::Create(p("A")),
            ]),
            vec![FsEvent::Rename(p("A"), p("B")), FsEvent::Create(p("A"))]
        );
    }

    #[test]
    fn modified_rename_then_recreate_source_deletes_before_recreate() {
        assert_eq!(
            collapse_ops(vec![
                FsEvent::Modify(p("A")),
                FsEvent::Rename(p("A"), p("B")),
                FsEvent::Create(p("A")),
            ]),
            vec![
                FsEvent::Remove(p("A")),
                FsEvent::Create(p("B")),
                FsEvent::Create(p("A")),
            ]
        );
    }

    #[test]
    fn mkdir_keeps_latest_directory_event() {
        assert_eq!(
            collapse_ops(vec![FsEvent::MkDir(p("D")), FsEvent::MkDir(p("D"))]),
            vec![FsEvent::MkDir(p("D"))]
        );
    }
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod convert_tests;
