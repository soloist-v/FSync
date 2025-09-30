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
    nodes: &Vec<EventNode>,
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

// fn plan_sync_ops(
//     events: &[FsEvent],
//     local_map_path: &PathBuf,
//     remote_map_path: &str,
// ) -> Vec<RemoteOp> {
//     let mut nodes: Vec<EventNode> = Vec::with_capacity(events.len());
//     let mut latest_by_path: HashMap<PathBuf, usize> = HashMap::new();
//     // 构建事件节点链表（每条路径维持一条链）
//     for event in events.iter().cloned() {
//         match event {
//             FsEvent::Create(ref path)
//             | FsEvent::Modify(ref path)
//             | FsEvent::Remove(ref path)
//             | FsEvent::MkDir(ref path) => {
//                 let prev = latest_by_path.get(path).cloned();
//                 if let Some(idx) = prev {
//                     nodes[idx].is_latest = false;
//                 }
//                 let node = EventNode {
//                     target_path: path.clone(),
//                     prev,
//                     kind: event.clone(),
//                     is_latest: true,
//                 };
//                 let idx = nodes.len();
//                 nodes.push(node);
//                 latest_by_path.insert(path.clone(), idx);
//             }
//             FsEvent::Rename(ref from, ref to) => {
//                 let prev = latest_by_path.get(from).cloned();
//                 if let Some(idx) = prev {
//                     nodes[idx].is_latest = false;
//                 }
//                 let node = EventNode {
//                     target_path: to.clone(),
//                     prev,
//                     kind: event.clone(),
//                     is_latest: true,
//                 };
//                 let idx = nodes.len();
//                 nodes.push(node);
//                 latest_by_path.insert(to.clone(), idx);
//             }
//         }
//     }
//
//     // 生成同步操作
//     let mut ops: Vec<RemoteOp> = Vec::new();
//     let mut deleted_once: HashSet<PathBuf> = HashSet::new();
//
//     for i in 0..nodes.len() {
//         let node = &nodes[i];
//         if !node.is_latest {
//             continue;
//         }
//         match node.kind {
//             FsEvent::Create(ref path) | FsEvent::Modify(ref path) => {
//                 // 若链上存在重命名，应删除最早来源，但如果沿途有 Delete/Create 则不需要
//                 let (origin, _saw_modify, saw_delete, saw_create) =
//                     summarize_lineage(&nodes, node.prev);
//                 if let Some(from) = origin {
//                     if from != node.target_path
//                         && !saw_delete
//                         && !saw_create
//                         && !deleted_once.contains(&from)
//                     {
//                         ops.push(RemoteOp::Remove {
//                             remote: remote_path(local_map_path, remote_map_path, &from),
//                         });
//                         deleted_once.insert(from);
//                     }
//                 }
//                 let r = remote_path(local_map_path, remote_map_path, &node.target_path);
//                 ops.push(
//                     (RemoteOp::Upload {
//                         local: node.target_path,
//                         remote: r,
//                     }),
//                 );
//             }
//             FsEvent::Rename(ref from, ref _to) => {
//                 // 压缩重命名链（并检测内容变更/删除/创建）这里的关键是，服务器是起点的状态，所以无需管本地的中间状态，所以本质上我们需要考虑的是服务器的起始状态（也就是本地的起始状态）和本地的最终状态（当前）
//                 // 所以说对于链式的rename过程中，只有最开始的rename才需要从服务器删除文件，中间的rename都是本地的历史，服务器是从当前开始执行的（也就是本地的起始状态），mod是不存在删除的，但是rename就有
//                 // 另外删除动作本身也是需要的，刚好结果也是有顺序的，不用担心顺序的问题
//                 let (origin, saw_modify, saw_delete, saw_create) =
//                     summarize_lineage(&nodes, node.prev);
//                 let to = node.target_path.clone();
//                 if saw_modify || saw_delete {
//                     if let Some(from) = origin.clone() {
//                         if !saw_delete && !saw_create && from != to && !deleted_once.contains(&from)
//                         {
//                             ops.push(RemoteOp::Remove {
//                                 remote: remote_path(local_map_path, remote_map_path, &from),
//                             });
//                             deleted_once.insert(from);
//                         }
//                     }
//                     let remote = remote_path(local_map_path, remote_map_path, &to);
//                     ops.push(RemoteOp::Upload { local: to, remote });
//                 } else if saw_create {
//                     let remote = remote_path(local_map_path, remote_map_path, &to);
//                     ops.push(RemoteOp::Upload { local: to, remote });
//                 } else {
//                     // 纯重命名链：使用最早来源；若缺失则退回当前 rename 的 from
//                     let from_final = match origin {
//                         None => from.clone(),
//                         Some(a) => a,
//                     };
//                     ops.push(RemoteOp::Rename {
//                         from: remote_path(local_map_path, remote_map_path, &from_final),
//                         to: remote_path(local_map_path, remote_map_path, &to),
//                     });
//                 }
//             }
//             FsEvent::Remove(ref path) => {
//                 if !deleted_once.contains(path) {
//                     ops.push(RemoteOp::Remove {
//                         remote: remote_path(local_map_path, remote_map_path, &path),
//                     });
//                     deleted_once.insert(path.clone());
//                 }
//             }
//             FsEvent::MkDir(ref _path) => {
//                 ops.push(RemoteOp::MkDir {
//                     remote: remote_path(local_map_path, remote_map_path, &node.target_path),
//                 });
//             }
//         }
//     }
//     ops
// }

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
                let prev = latest_by_path.get(path).cloned();
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
                latest_by_path.insert(path.clone(), idx);
            }
            FsEvent::Rename(ref from, ref to) => {
                let prev = latest_by_path.get(from).cloned();
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
                latest_by_path.insert(to.clone(), idx);
            }
        }
    }

    // 生成同步操作
    let mut ops: Vec<FsEvent> = Vec::new();
    let mut deleted_once: HashSet<PathBuf> = HashSet::new();

    for i in 0..nodes.len() {
        let node = &nodes[i];
        if !node.is_latest {
            continue;
        }
        match node.kind {
            FsEvent::Create(ref path) | FsEvent::Modify(ref path) => {
                // 若链上存在重命名，应删除最早来源，但如果沿途有 Delete/Create 则不需要
                let (origin, _saw_modify, saw_delete, saw_create) =
                    summarize_lineage(&nodes, node.prev);
                if let Some(from) = origin {
                    if from != node.target_path
                        && !saw_delete
                        && !saw_create
                        && !deleted_once.contains(&from)
                    {
                        ops.push(FsEvent::Remove(from.clone()));
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
                    if let Some(from) = origin.clone() {
                        if !saw_delete && !saw_create && from != to && !deleted_once.contains(&from)
                        {
                            ops.push(FsEvent::Remove(from.clone()));
                            deleted_once.insert(from);
                        }
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
                if !deleted_once.contains(path) {
                    ops.push(FsEvent::Remove(path.clone()));
                    deleted_once.insert(path.clone());
                }
            }
            FsEvent::MkDir(ref _path) => {
                ops.push(FsEvent::MkDir(node.target_path.clone()));
            }
        }
    }
    ops
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn main() {
        let evts = vec![
            FsEvent::Create("A".into()),
            FsEvent::Create("S".into()),
            FsEvent::Modify("A".into()),
            FsEvent::Modify("S".into()),
            FsEvent::Rename("A".into(), "B".into()),
            FsEvent::Rename("X".into(), "A".into()),
            FsEvent::Modify("A".into()),
            FsEvent::Remove("C".into()),
            FsEvent::MkDir("D".into()),
            FsEvent::Rename("A".into(), "D/A".into()),
            FsEvent::Rename("F".into(), "A".into()),
            FsEvent::Modify("A".into()),
            FsEvent::Modify("A".into()),
            FsEvent::Rename("A1".into(), "B1".into()),
            FsEvent::Rename("B1".into(), "C1".into()),
            FsEvent::Rename("C1".into(), "D1".into()),
        ];
        let result = collapse_ops(&evts);
        for op in result {
            println!("{:?}", op);
        }
    }
}
