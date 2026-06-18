//! Core library for FSync – file/directory synchronisation engine.

mod config;
mod convert;
mod file_op;
mod filter;
mod manager;
mod remote;
mod storage;
mod task;
mod utils;

pub use config::{Pattern, RemoteCfg, TaskConfig};
pub use file_op::{event_to_ops, FsEvent};
pub use filter::PathFilter;
pub use manager::SyncManager;
pub use remote::{RemoteFs, RemoteOp};
pub use storage::StateStore;
pub use task::{
    spawn_task, RemoteOpLog, RemoteOpStatus, SyncTaskHandle, TaskCommand, TaskEvent,
    TaskEventHandler, TaskLog, TaskState,
};

pub use tracing::{debug, error, info, warn};
