//! Core library for FSync – file/directory synchronisation engine.

mod config;
mod file_op;
mod filter;
mod manager;
mod remote;
mod storage;
mod task;

pub use config::{Pattern, RemoteCfg, TaskConfig};
pub use file_op::{event_to_ops, FileOp};
pub use filter::PathFilter;
pub use manager::SyncManager;
pub use remote::{RemoteFs, RemoteOp};
pub use storage::FoyerStore;
pub use task::{spawn_task, SyncTaskHandle, TaskCommand, TaskState};
