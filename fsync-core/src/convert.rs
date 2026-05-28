use crate::file_op::FsEvent;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
#[derive(Debug, Clone)]
struct EventNode {
    // 当前节点所代表的路径（对于 Rename 是目标路径）
    target_path: PathBuf,
    // 指向链上的上一个相关节点（索引）
    prev: Option<usize>,
    // 事件类型
    kind: FsEvent,
    // 是否为该路径的最新状态（尾节点）
    is_latest: bool,
}

// 回溯链，提取最早来源路径和沿途出现的关键信号
fn summarize_lineage(
    nodes: &[EventNode],
    start: Option<usize>,
) -> (Option<PathBuf>, bool, bool, bool) {
    // (origin_path, saw_modify, saw_delete, saw_create)
    let mut origin_path: Option<PathBuf> = None;
    let mut saw_modify = false;
    let mut saw_delete = false;
    let mut saw_create = false;

    let mut cur = start;
    while let Some(i) = cur {
        let node = &nodes[i];
        match node.kind {
            FsEvent::Rename(ref from, _) => {
                origin_path = Some(from.clone());
            }
            FsEvent::Modify(_) => saw_modify = true,
            FsEvent::Remove(_) => saw_delete = true,
            FsEvent::Create(_) => saw_create = true,
            FsEvent::MkDir(_) => {}
        }
        cur = node.prev;
    }

    (origin_path, saw_modify, saw_delete, saw_create)
}

pub fn collapse_ops(events: Vec<FsEvent>) -> Vec<FsEvent> {
    let mut nodes: Vec<EventNode> = Vec::with_capacity(events.len());
    let mut latest_by_path: HashMap<PathBuf, usize> = HashMap::new();
    // 构建事件节点链表（每条路径维持一条链）
    for event in events {
        match event {
            FsEvent::Create(ref path)
            | FsEvent::Modify(ref path)
            | FsEvent::Remove(ref path)
            | FsEvent::MkDir(ref path) => {
                let path = path.clone();
                let prev = latest_by_path.get(&path).cloned();
                if let Some(idx) = prev {
                    nodes[idx].is_latest = false;
                }
                let node = EventNode {
                    target_path: path.clone(),
                    prev,
                    kind: event,
                    is_latest: true,
                };
                let idx = nodes.len();
                nodes.push(node);
                latest_by_path.insert(path, idx);
            }
            FsEvent::Rename(ref from, ref to) => {
                let from = from.clone();
                let to = to.clone();
                let prev = latest_by_path.remove(&from);
                if let Some(idx) = prev {
                    nodes[idx].is_latest = false;
                }
                let node = EventNode {
                    target_path: to.clone(),
                    prev,
                    kind: event,
                    is_latest: true,
                };
                let idx = nodes.len();
                nodes.push(node);
                latest_by_path.insert(to, idx);
            }
        }
    }

    // 生成同步操作
    let mut prefix_removes: Vec<FsEvent> = Vec::new();
    let mut ops: Vec<FsEvent> = Vec::new();
    let mut deleted_once: HashSet<PathBuf> = HashSet::new();

    for i in 0..nodes.len() {
        let node = &nodes[i];
        if !node.is_latest {
            continue;
        }
        match node.kind {
            FsEvent::Create(_) | FsEvent::Modify(_) => {
                // 若链上存在重命名，应删除最早来源，但如果沿途有 Delete/Create 则不需要
                let (origin, _saw_modify, saw_delete, saw_create) =
                    summarize_lineage(&nodes, node.prev);
                if let Some(from) = origin {
                    if from != node.target_path
                        && !saw_delete
                        && !saw_create
                        && !deleted_once.contains(&from)
                    {
                        prefix_removes.push(FsEvent::Remove(from.clone()));
                        deleted_once.insert(from);
                    }
                }
                ops.push(node.kind.clone());
            }
            FsEvent::Rename(ref from, ref _to) => {
                // 压缩重命名链（并检测内容变更/删除/创建）这里的关键是，服务器是起点的状态，所以无需管本地的中间状态，所以本质上我们需要考虑的是服务器的起始状态（也就是本地的起始状态）和本地的最终状态（当前）
                // 所以说对于链式的rename过程中，只有最开始的rename才需要从服务器删除文件，中间的rename都是本地的历史，服务器是从当前开始执行的（也就是本地的起始状态），mod是不存在删除的，但是rename就有
                // 另外删除动作本身也是需要的，刚好结果也是有顺序的，不用担心顺序的问题
                let (origin, saw_modify, saw_delete, saw_create) =
                    summarize_lineage(&nodes, node.prev);
                let to = node.target_path.clone();
                if saw_modify || saw_delete {
                    let from_final = origin.unwrap_or_else(|| from.clone());
                    if !saw_create && from_final != to && !deleted_once.contains(&from_final) {
                        prefix_removes.push(FsEvent::Remove(from_final.clone()));
                        deleted_once.insert(from_final);
                    }
                    ops.push(FsEvent::Create(to));
                } else if saw_create {
                    ops.push(FsEvent::Create(to));
                } else {
                    // 纯重命名链：使用最早来源；若缺失则退回当前 rename 的 from
                    let from_final = match origin {
                        None => from.clone(),
                        Some(a) => a,
                    };
                    ops.push(FsEvent::Rename(from_final, to));
                }
            }
            FsEvent::Remove(ref path) => {
                let (origin, _saw_modify, _saw_delete, saw_create) =
                    summarize_lineage(&nodes, node.prev);
                if saw_create {
                    continue;
                }
                let remove_path = origin.unwrap_or_else(|| path.clone());
                if !deleted_once.contains(&remove_path) {
                    prefix_removes.push(FsEvent::Remove(remove_path.clone()));
                    deleted_once.insert(remove_path);
                }
            }
            FsEvent::MkDir(ref _path) => {
                ops.push(FsEvent::MkDir(node.target_path.clone()));
            }
        }
    }
    prefix_removes.extend(ops);
    let ops = prefix_removes;
    ops
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
