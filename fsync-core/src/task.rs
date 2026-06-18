use crate::convert::collapse_ops;
use crate::{
    config::TaskConfig,
    file_op::{event_to_ops, FsEvent},
    filter::PathFilter,
    remote::{RemoteFs, RemoteOp},
    utils::{display_posix_path, join_posix_path, normalize_key_path, relative_posix_path},
    StateStore,
};
use anyhow::{anyhow, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::Path;
use std::time::UNIX_EPOCH;
use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch::Ref;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{sleep, Sleep};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

/// Public handle returned to callers for controlling a running sync task.
pub struct SyncTaskHandle {
    cfg: TaskConfig,
    ctrl_tx: mpsc::Sender<TaskCommand>,
    state_rx: watch::Receiver<TaskState>,
    log_tx: broadcast::Sender<TaskLog>,
    initial_log_rx: Mutex<Option<broadcast::Receiver<TaskLog>>>,
    stop_token: CancellationToken,
}

impl fmt::Debug for SyncTaskHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTaskHandle")
            .field("cfg", &self.cfg)
            .field("ctrl_tx", &self.ctrl_tx)
            .field("state_rx", &self.state_rx)
            .field("log_tx", &self.log_tx)
            .finish_non_exhaustive()
    }
}

impl SyncTaskHandle {
    pub fn config(&self) -> &TaskConfig {
        &self.cfg
    }

    pub fn stop(&self) {
        self.stop_token.cancel();
        let _ = self.ctrl_tx.try_send(TaskCommand::Stop);
    }
    pub fn state(&self) -> Ref<'_, TaskState> {
        self.state_rx.borrow()
    }

    pub fn subscribe_logs(&self) -> broadcast::Receiver<TaskLog> {
        self.initial_log_rx
            .lock()
            .ok()
            .and_then(|mut rx| rx.take())
            .unwrap_or_else(|| self.log_tx.subscribe())
    }
}

impl Drop for SyncTaskHandle {
    fn drop(&mut self) {
        self.stop_token.cancel();
        let _ = self.ctrl_tx.try_send(TaskCommand::Stop);
    }
}

#[derive(Debug, Clone)]
pub enum TaskCommand {
    Stop,
}

#[derive(Debug, Clone)]
pub enum TaskState {
    Idle,
    Starting(String),
    Running,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct TaskLog {
    pub message: String,
    pub remote_op: Option<RemoteOpLog>,
}

#[derive(Debug, Clone, Copy)]
pub enum RemoteOpStatus {
    Applied,
    Failed,
}

#[derive(Debug, Clone)]
pub struct RemoteOpLog {
    pub status: RemoteOpStatus,
    pub op: RemoteOp,
    pub message: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TaskEvent {
    State(TaskState),
    Log(TaskLog),
    RemoteOp(RemoteOpLog),
}

pub trait TaskEventHandler: Send + Sync + 'static {
    fn emit(&self, event: TaskEvent);
}

#[derive(Clone)]
enum StateUpdate {
    Put(String, u64),
    RemoveTree(String),
}

struct PlannedRemoteOp {
    op: RemoteOp,
    state_updates: Vec<StateUpdate>,
}

#[derive(Clone)]
struct BroadcastTaskEventHandler {
    state_tx: watch::Sender<TaskState>,
    log_tx: broadcast::Sender<TaskLog>,
}

impl BroadcastTaskEventHandler {
    fn new(state_tx: watch::Sender<TaskState>, log_tx: broadcast::Sender<TaskLog>) -> Self {
        Self { state_tx, log_tx }
    }
}

impl TaskEventHandler for BroadcastTaskEventHandler {
    fn emit(&self, event: TaskEvent) {
        match event {
            TaskEvent::State(state) => {
                let _ = self.log_tx.send(TaskLog {
                    message: format!("State: {}", state_label(&state)),
                    remote_op: None,
                });
                let _ = self.state_tx.send(state);
            }
            TaskEvent::Log(log) => {
                let _ = self.log_tx.send(log);
            }
            TaskEvent::RemoteOp(log) => {
                let message = log.message.clone();
                let _ = self.log_tx.send(TaskLog {
                    message,
                    remote_op: Some(log),
                });
            }
        }
    }
}

pub(crate) struct SyncTask {
    cfg: TaskConfig,
    filter: Arc<PathFilter>,
    size_min: Option<u64>,
    size_max: Option<u64>,
}

impl SyncTask {
    pub fn new(mut cfg: TaskConfig) -> Self {
        if let Ok(local) = cfg.local.canonicalize() {
            cfg.local = local;
        }
        let filter = Arc::new(PathFilter::new(&cfg.local, &cfg.include, &cfg.exclude));
        let (size_min, size_max) = parse_size_filter(cfg.size.as_deref());
        Self {
            cfg,
            filter,
            size_min,
            size_max,
        }
    }

    pub async fn run(
        self,
        remote: impl RemoteFs,
        mut ctrl_rx: mpsc::Receiver<TaskCommand>,
        event_handler: Arc<dyn TaskEventHandler>,
        stop_token: CancellationToken,
    ) {
        let (op_tx, mut op_rx) = mpsc::unbounded_channel::<FsEvent>();
        emit_state(&event_handler, TaskState::Starting("Opening cache".into()));
        let cache_dir = self
            .cfg
            .cache_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("cache/{}", self.cfg.id)));
        tracing::info!(
            task_id = %self.cfg.id,
            task_name = %self.cfg.name,
            cache_dir = %display_posix_path(&cache_dir),
            "opening task cache"
        );
        let store = match StateStore::open(1024 * 1024 * 4, &cache_dir).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    task_id = %self.cfg.id,
                    cache_dir = %display_posix_path(&cache_dir),
                    error = %e,
                    "failed to open task cache"
                );
                emit_state(
                    &event_handler,
                    TaskState::Error(format!("state store open error: {e}")),
                );
                return;
            }
        };
        tracing::info!(task_id = %self.cfg.id, "task cache opened");

        emit_state(
            &event_handler,
            TaskState::Starting("Starting watcher".into()),
        );
        let mut watcher_guard = match self.spawn_watcher(op_tx.clone()) {
            Ok(guard) => guard,
            Err(e) => {
                emit_state(
                    &event_handler,
                    TaskState::Error(format!("watch error: {e}")),
                );
                return;
            }
        };

        emit_state(
            &event_handler,
            TaskState::Starting("Scanning local tree".into()),
        );
        {
            let mut initial_ops: Vec<FsEvent> = Vec::new();
            let mut live_cache_keys: HashSet<String> = HashSet::new();
            let mut migrated_cache_entries = 0usize;
            let mut state_snapshot = match store.load_all_u64().await {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    emit_state(
                        &event_handler,
                        TaskState::Error(format!("state store read error: {e}")),
                    );
                    return;
                }
            };
            for entry in WalkDir::new(&self.cfg.local)
                .into_iter()
                .filter_entry(|entry| {
                    entry.depth() == 0
                        || !entry.file_type().is_dir()
                        || self.filter.check_dir(entry.path())
                })
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let path = entry.into_path();
                    if self.filter.check(&path) {
                        let key = self.state_key(&path);
                        if !state_snapshot.contains_key(&key) {
                            if let Some(ts) =
                                self.migrate_legacy_state_key(&path, &key, &store).await
                            {
                                migrated_cache_entries += 1;
                                state_snapshot.insert(key.clone(), ts);
                            }
                        }
                        let last = state_snapshot.get(&key);
                        live_cache_keys.insert(key);
                        if should_queue_entry(&path, self.size_min, self.size_max, last) {
                            initial_ops.push(FsEvent::Modify(path));
                        }
                    }
                }
            }
            if migrated_cache_entries > 0 {
                emit_log(
                    &event_handler,
                    format!("Migrated {migrated_cache_entries} cache entrie(s) to relative keys"),
                );
            }
            let cleaned_cache_entries = match store.cleanup_missing(&live_cache_keys).await {
                Ok(removed) if removed > 0 => {
                    emit_log(
                        &event_handler,
                        format!(
                            "Cleaned {removed} stale cache entries, {} live entries remain",
                            live_cache_keys.len()
                        ),
                    );
                    removed
                }
                Ok(_) => 0,
                Err(e) => {
                    emit_log(&event_handler, format!("Cache cleanup failed: {e}"));
                    0
                }
            };
            if cleaned_cache_entries > 0 {
                if let Err(e) = store.flush().await {
                    emit_log(&event_handler, format!("Cache cleanup flush failed: {e}"));
                }
            }
            emit_log(
                &event_handler,
                format!("Initial scan found {} candidate file(s)", initial_ops.len()),
            );
            emit_state(
                &event_handler,
                TaskState::Starting("Applying initial sync".into()),
            );
            if let Err(e) = self
                .flush_batch(&remote, initial_ops, &store, &event_handler, &stop_token)
                .await
            {
                if let Some(stop_watcher) = watcher_guard.take() {
                    stop_watcher();
                }
                if stop_token.is_cancelled() {
                    emit_state(&event_handler, TaskState::Idle);
                } else {
                    emit_state(
                        &event_handler,
                        TaskState::Error(format!("initial sync error: {e}")),
                    );
                }
                return;
            }
        }

        let scan_cancel = CancellationToken::new();
        let scan_handle = {
            let scan_cancel = scan_cancel.clone();
            self.spawn_scanner(scan_cancel, op_tx.clone(), store.clone())
        };

        emit_state(&event_handler, TaskState::Running);
        // batching variables
        let debounce = Duration::from_millis(self.cfg.debounce_ms);
        let mut batch: Vec<FsEvent> = Vec::new();
        let mut sleeper: Option<std::pin::Pin<Box<Sleep>>> = None;
        let mut stopped_by_command = false;
        loop {
            tokio::select! {
                Some(cmd) = ctrl_rx.recv() => {
                    match cmd {
                        TaskCommand::Stop => {
                            stop_token.cancel();
                            stopped_by_command = true;
                            break;
                        }
                    }
                }
                _ = stop_token.cancelled() => {
                    stopped_by_command = true;
                    break;
                }
                Some(op) = op_rx.recv() => {
                    batch.push(op);
                    // if sleeper.is_none() {
                    sleeper = Some(Box::pin(sleep(debounce)));
                    // }
                }
                _ = async { if let Some(ref mut s) = sleeper { s.as_mut().await } }, if sleeper.is_some() => {
                    if let Err(e) = self.flush_batch(&remote, std::mem::take(&mut batch), &store, &event_handler, &stop_token).await {
                        if stop_token.is_cancelled() {
                            stopped_by_command = true;
                        } else {
                            emit_state(&event_handler, TaskState::Error(format!("batch error: {e}")));
                        }
                        break;
                    }
                    sleeper = None;
                }
            }
        }
        if !batch.is_empty() {
            let _ = self
                .flush_batch(&remote, batch, &store, &event_handler, &stop_token)
                .await;
        }
        scan_cancel.cancel();
        let _ = scan_handle.await;
        if let Some(stop_watcher) = watcher_guard {
            stop_watcher();
        }
        if stopped_by_command {
            emit_state(&event_handler, TaskState::Idle);
        }
    }

    fn spawn_scanner(
        &self,
        cancel: CancellationToken,
        scan_tx: mpsc::UnboundedSender<FsEvent>,
        store: StateStore,
    ) -> tokio::task::JoinHandle<()> {
        let scan_interval = Duration::from_millis(self.cfg.scan_ms).max(Duration::from_secs(30));
        let scan_path = self.cfg.local.clone();
        let filter = self.filter.clone();
        let size_min = self.size_min;
        let size_max = self.size_max;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {}
                }

                let state_snapshot = match store.load_all_u64().await {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        crate::warn!("scan cache snapshot error: {e}");
                        continue;
                    }
                };
                for entry in WalkDir::new(&scan_path)
                    .into_iter()
                    .filter_entry(|entry| {
                        entry.depth() == 0
                            || !entry.file_type().is_dir()
                            || filter.check_dir(entry.path())
                    })
                    .filter_map(|e| e.ok())
                {
                    if cancel.is_cancelled() {
                        break;
                    }
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let path = entry.into_path();
                    if !filter.check(&path) {
                        continue;
                    }
                    let key = relative_posix_path(&path, &scan_path)
                        .unwrap_or_else(|| normalize_key_path(&path));
                    if should_queue_entry(&path, size_min, size_max, state_snapshot.get(&key)) {
                        tracing::debug!(path = %display_posix_path(&path), "scanner queued modified file");
                        if let Err(e) = scan_tx.send(FsEvent::Modify(path)) {
                            crate::warn!("{:?}", e);
                            break;
                        }
                    }
                }
            }
        })
    }

    async fn flush_batch(
        &self,
        remote: &impl RemoteFs,
        ops: Vec<FsEvent>,
        store: &StateStore,
        event_handler: &Arc<dyn TaskEventHandler>,
        stop_token: &CancellationToken,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // collapse only consecutive Modify operations for the same path; keep order for others
        let ops = collapse_ops(ops);
        crate::debug!("collapsed fs events: {:?}", ops);
        emit_log(event_handler, summarize_fs_events(&ops));
        if self.has_directory_like_event(&ops).await {
            let settle_ms = self.cfg.debounce_ms.saturating_mul(6).clamp(750, 3_000);
            tokio::time::sleep(Duration::from_millis(settle_ms)).await;
        }
        let mut planned_ops = Vec::new();
        let mut queued_uploads = HashSet::new();
        for op in ops {
            match &op {
                FsEvent::Create(p) | FsEvent::Modify(p) => {
                    let Ok(meta) = tokio::fs::metadata(p).await else {
                        tracing::debug!(path = %display_path(p), "skip missing path");
                        continue;
                    };
                    if meta.is_dir() {
                        self.queue_directory_tree(
                            p,
                            store,
                            event_handler,
                            &mut planned_ops,
                            &mut queued_uploads,
                        )
                        .await?;
                    } else {
                        self.queue_file_upload(
                            p,
                            Some(meta),
                            store,
                            &mut planned_ops,
                            &mut queued_uploads,
                        )
                        .await?;
                    }
                }
                FsEvent::Remove(p) => {
                    let remote = self.remote_path(p);
                    tracing::debug!(remote = %remote, "queued remove");
                    planned_ops.push(PlannedRemoteOp {
                        op: RemoteOp::Remove { remote },
                        state_updates: vec![StateUpdate::RemoveTree(self.state_key(p))],
                    });
                }
                FsEvent::MkDir(p) => {
                    self.queue_directory_tree(
                        p,
                        store,
                        event_handler,
                        &mut planned_ops,
                        &mut queued_uploads,
                    )
                    .await?;
                }
                FsEvent::Rename(from, to) => {
                    let from_remote = self.remote_path(from);
                    let to_remote = self.remote_path(to);
                    tracing::debug!(from = %from_remote, to = %to_remote, "queued rename");
                    // Inherit timestamp from source to avoid unnecessary upload on pure rename
                    let from_key = self.state_key(from);
                    let last = store.get_u64(&from_key).await?;
                    let mut state_updates = Vec::new();
                    if let Some(ts) = last {
                        state_updates.push(StateUpdate::Put(self.state_key(to), ts));
                    }
                    // Clear the source path record
                    state_updates.push(StateUpdate::RemoveTree(from_key));
                    planned_ops.push(PlannedRemoteOp {
                        op: RemoteOp::Rename {
                            from: from_remote,
                            to: to_remote,
                        },
                        state_updates,
                    });
                }
            }
        }
        if !planned_ops.is_empty() {
            crate::debug!(
                "applying remote ops: {:?}",
                planned_ops
                    .iter()
                    .map(|planned| &planned.op)
                    .collect::<Vec<_>>()
            );
            emit_log(event_handler, summarize_planned_remote_ops(&planned_ops));
            self.apply_remote_ops(remote, &planned_ops, store, event_handler, stop_token)
                .await?;
        } else {
            emit_log(event_handler, "No remote ops after cache/filter checks");
        }
        store.flush().await?;
        Ok(())
    }

    async fn queue_directory_tree(
        &self,
        dir: &PathBuf,
        store: &StateStore,
        event_handler: &Arc<dyn TaskEventHandler>,
        planned_ops: &mut Vec<PlannedRemoteOp>,
        queued_uploads: &mut HashSet<String>,
    ) -> Result<()> {
        if !self.filter.check_dir(dir) {
            tracing::debug!(path = %display_path(dir), "skip filtered directory");
            return Ok(());
        }

        let remote = self.remote_path(dir);
        tracing::debug!(remote = %remote, "queued mkdir");
        planned_ops.push(PlannedRemoteOp {
            op: RemoteOp::MkDir { remote },
            state_updates: Vec::new(),
        });

        let mut files_seen = 0usize;
        let mut files_queued = 0usize;
        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_entry(|entry| {
                entry.depth() == 0
                    || !entry.file_type().is_dir()
                    || self.filter.check_dir(entry.path())
            })
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                if entry.depth() > 0 && entry.file_type().is_dir() {
                    let path = entry.path().to_path_buf();
                    let remote = self.remote_path(&path);
                    tracing::debug!(remote = %remote, "queued mkdir");
                    planned_ops.push(PlannedRemoteOp {
                        op: RemoteOp::MkDir { remote },
                        state_updates: Vec::new(),
                    });
                }
                continue;
            }

            let path = entry.into_path();
            files_seen += 1;
            if self
                .queue_file_upload(&path, None, store, planned_ops, queued_uploads)
                .await?
            {
                files_queued += 1;
            }
        }

        emit_log(
            event_handler,
            format!(
                "Directory scan: {} ready file(s), {} queued upload(s): {}",
                files_seen,
                files_queued,
                display_path(dir)
            ),
        );

        Ok(())
    }

    async fn queue_file_upload(
        &self,
        path: &PathBuf,
        meta: Option<std::fs::Metadata>,
        store: &StateStore,
        planned_ops: &mut Vec<PlannedRemoteOp>,
        queued_uploads: &mut HashSet<String>,
    ) -> Result<bool> {
        if !self.filter.check(path) {
            tracing::debug!(path = %display_path(path), "skip filtered file");
            return Ok(false);
        }

        let meta = if let Some(meta) = meta {
            meta
        } else {
            match tokio::fs::metadata(path).await {
                Ok(meta) if meta.is_file() => meta,
                _ => {
                    tracing::debug!(path = %display_path(path), "skip non-file path");
                    return Ok(false);
                }
            }
        };

        if let Some(min) = self.size_min {
            if meta.len() < min {
                tracing::debug!(path = %display_path(path), "skip file below size filter");
                return Ok(false);
            }
        }
        if let Some(max) = self.size_max {
            if meta.len() > max {
                tracing::debug!(path = %display_path(path), "skip file above size filter");
                return Ok(false);
            }
        }

        let Ok(modified) = meta.modified() else {
            return Ok(false);
        };
        let Ok(dur) = modified.duration_since(UNIX_EPOCH) else {
            return Ok(false);
        };

        let mtime = dur.as_secs();
        let key = self.state_key(path);
        if !queued_uploads.insert(key.clone()) {
            return Ok(false);
        }
        let last = match store.get_u64(&key).await? {
            Some(ts) => Some(ts),
            None => self.migrate_legacy_state_key(path, &key, store).await,
        };
        if last == Some(mtime) {
            tracing::debug!(path = %display_path(path), "skip unchanged file");
            return Ok(false);
        }

        let remote = self.remote_path(path);
        tracing::debug!(local = %display_path(path), remote = %remote, "queued upload");
        planned_ops.push(PlannedRemoteOp {
            op: RemoteOp::Upload {
                local: path.clone(),
                remote,
            },
            state_updates: vec![StateUpdate::Put(key, mtime)],
        });

        Ok(true)
    }

    async fn has_directory_like_event(&self, ops: &[FsEvent]) -> bool {
        for op in ops {
            match op {
                FsEvent::MkDir(_) => return true,
                FsEvent::Create(path) | FsEvent::Modify(path) => {
                    if tokio::fs::metadata(path)
                        .await
                        .map(|meta| meta.is_dir())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
                FsEvent::Rename(_, to) => {
                    if tokio::fs::metadata(to)
                        .await
                        .map(|meta| meta.is_dir())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
                FsEvent::Remove(_) => {}
            }
        }
        false
    }

    async fn apply_remote_ops(
        &self,
        remote: &impl RemoteFs,
        ops: &[PlannedRemoteOp],
        store: &StateStore,
        event_handler: &Arc<dyn TaskEventHandler>,
        stop_token: &CancellationToken,
    ) -> Result<()> {
        let mut pending_state_updates = Vec::new();
        for planned in ops {
            if stop_token.is_cancelled() {
                self.apply_state_updates(store, &pending_state_updates, event_handler)
                    .await;
                return Err(anyhow!("task stopped"));
            }
            let op = &planned.op;
            let mut attempt: u32 = 0;
            let mut backoff = self.cfg.retry_backoff_ms;
            let max = self.cfg.retry_max;

            loop {
                match remote
                    .apply_batch_cancelled(vec![op.clone()], stop_token.clone())
                    .await
                {
                    Ok(_) => {
                        let detail = describe_remote_op(op);
                        tracing::debug!(task_id = %self.cfg.id, task_name = %self.cfg.name, operation = %detail, "remote op applied");
                        emit_remote_op_applied(event_handler, op.clone(), detail);
                        pending_state_updates.extend_from_slice(&planned.state_updates);
                        if pending_state_updates.len() >= 128 {
                            self.apply_state_updates(store, &pending_state_updates, event_handler)
                                .await;
                            pending_state_updates.clear();
                        }
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        if attempt > max {
                            self.apply_state_updates(store, &pending_state_updates, event_handler)
                                .await;
                            let detail = format!("Remote op failed: {}", describe_remote_op(op));
                            emit_remote_op_failed(event_handler, op.clone(), detail, e.to_string());
                            return Err(e);
                        }
                        emit_log(
                            event_handler,
                            format!(
                                "Remote op failed, retry {attempt}/{max}: {}: {e}",
                                describe_remote_op(op)
                            ),
                        );
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        backoff = backoff.saturating_mul(2);
                    }
                }
            }
        }

        self.apply_state_updates(store, &pending_state_updates, event_handler)
            .await;
        Ok(())
    }

    async fn apply_state_updates(
        &self,
        store: &StateStore,
        updates: &[StateUpdate],
        event_handler: &Arc<dyn TaskEventHandler>,
    ) {
        let puts = updates
            .iter()
            .filter_map(|update| match update {
                StateUpdate::Put(key, ts) => Some((key.clone(), *ts)),
                StateUpdate::RemoveTree(_) => None,
            })
            .collect::<Vec<_>>();
        if let Err(e) = store.put_many_u64(&puts).await {
            emit_log(event_handler, format!("Cache update failed: {e}"));
        }

        for update in updates {
            if let StateUpdate::RemoveTree(key) = update {
                match store.remove_tree(key).await {
                    Ok(removed) if removed > 0 => {
                        tracing::debug!(key, removed, "removed cache entries");
                    }
                    Ok(_) => {}
                    Err(e) => emit_log(event_handler, format!("Cache remove failed: {e}")),
                }
            }
        }
    }

    fn spawn_watcher(
        &self,
        op_tx: mpsc::UnboundedSender<FsEvent>,
    ) -> Result<Option<impl FnOnce()>> {
        let path = self.cfg.local.clone();
        let filter = self.filter.clone();
        let mut watcher: RecommendedWatcher = RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    tracing::debug!(kind = ?event.kind, paths = ?event.paths, "watch event received");
                    for op in event_to_ops(event) {
                        tracing::debug!(op = ?op, "watch event converted");
                        let pass = match &op {
                            FsEvent::MkDir(path) => filter.check_dir(path),
                            FsEvent::Rename(from, to) => {
                                filter.check(from)
                                    || filter.check(to)
                                    || filter.check_dir(from)
                                    || filter.check_dir(to)
                            }
                            _ => filter.check(op.path()),
                        };
                        if pass {
                            let _ = op_tx.send(op);
                        } else {
                            tracing::debug!(op = ?op, "watch event ignored by filter");
                        }
                    }
                }
                Err(e) => eprintln!("watch error: {e}"),
            },
            notify::Config::default(),
        )
        .map_err(|e| anyhow!(e))?;
        watcher
            .watch(&path, RecursiveMode::Recursive)
            .map_err(|e| anyhow!(e))?;
        // Leak the watcher to keep it alive.
        // std::mem::forget(watcher);
        let f = {
            let path = self.cfg.local.clone();
            move || {
                if let Err(e) = watcher.unwatch(&path) {
                    tracing::error!("{e}");
                }
            }
        };
        Ok(Some(f))
    }
}

impl SyncTask {
    fn remote_path(&self, local: &PathBuf) -> String {
        let rel = self
            .relative_local_path(local)
            .unwrap_or_else(|| normalize_key_path(local));
        join_posix_path(&self.cfg.remote, &rel)
    }

    fn relative_local_path(&self, local: &Path) -> Option<String> {
        relative_posix_path(local, &self.cfg.local)
    }

    fn state_key(&self, local: &Path) -> String {
        self.relative_local_path(local)
            .unwrap_or_else(|| normalize_key_path(local))
    }

    async fn migrate_legacy_state_key(
        &self,
        local: &Path,
        key: &str,
        store: &StateStore,
    ) -> Option<u64> {
        let key = key.to_string();
        if let Ok(Some(ts)) = store.get_u64(&key).await {
            return Some(ts);
        }

        for legacy_key in self.legacy_state_keys(local) {
            if legacy_key == key {
                continue;
            }
            let Ok(Some(ts)) = store.get_u64(&legacy_key).await else {
                continue;
            };
            if store.put_u64(key.clone(), ts).await.is_ok() {
                let _ = store.remove_u64(&legacy_key).await;
                return Some(ts);
            }
        }

        None
    }

    fn legacy_state_keys(&self, local: &Path) -> Vec<String> {
        let mut keys = Vec::new();
        let raw = display_posix_path(local);
        keys.push(raw.clone());
        if let Some(stripped) = raw.strip_prefix("//?/") {
            keys.push(stripped.to_string());
        }
        keys.push(normalize_key_path(local));
        keys.sort();
        keys.dedup();
        keys
    }
}

pub fn spawn_task<R: RemoteFs>(cfg: TaskConfig, remote: R) -> SyncTaskHandle {
    let (ctrl_tx, ctrl_rx) = mpsc::channel(4);
    let (state_tx, state_rx) = watch::channel(TaskState::Starting("Task spawned".into()));
    let (log_tx, _) = broadcast::channel(65_536);
    let initial_log_rx = Mutex::new(Some(log_tx.subscribe()));
    let stop_token = CancellationToken::new();
    let event_handler = Arc::new(BroadcastTaskEventHandler::new(
        state_tx.clone(),
        log_tx.clone(),
    ));
    let task = SyncTask::new(cfg.clone());
    tokio::spawn(task.run(remote, ctrl_rx, event_handler, stop_token.clone()));
    SyncTaskHandle {
        cfg,
        ctrl_tx,
        state_rx,
        log_tx,
        initial_log_rx,
        stop_token,
    }
}

fn parse_size_filter(input: Option<&str>) -> (Option<u64>, Option<u64>) {
    if let Some(s) = input {
        let s = s.trim();
        if s.is_empty() {
            return (None, None);
        }
        if let Some((a, b)) = s.split_once("..") {
            let min = if a.is_empty() {
                None
            } else {
                a.parse::<u64>().ok()
            };
            let max = if b.is_empty() {
                None
            } else {
                b.parse::<u64>().ok()
            };
            return (min, max);
        }
        if let Ok(n) = s.parse::<u64>() {
            return (Some(n), None);
        }
    }
    (None, None)
}

fn emit_state(event_handler: &Arc<dyn TaskEventHandler>, state: TaskState) {
    event_handler.emit(TaskEvent::State(state));
}

fn emit_log(event_handler: &Arc<dyn TaskEventHandler>, message: impl Into<String>) {
    event_handler.emit(TaskEvent::Log(TaskLog {
        message: message.into(),
        remote_op: None,
    }));
}

fn emit_remote_op_applied(
    event_handler: &Arc<dyn TaskEventHandler>,
    op: RemoteOp,
    message: impl Into<String>,
) {
    event_handler.emit(TaskEvent::RemoteOp(RemoteOpLog {
        status: RemoteOpStatus::Applied,
        op,
        message: message.into(),
        error: None,
    }));
}

fn emit_remote_op_failed(
    event_handler: &Arc<dyn TaskEventHandler>,
    op: RemoteOp,
    message: impl Into<String>,
    error: impl Into<String>,
) {
    event_handler.emit(TaskEvent::RemoteOp(RemoteOpLog {
        status: RemoteOpStatus::Failed,
        op,
        message: message.into(),
        error: Some(error.into()),
    }));
}

fn summarize_fs_events(events: &[FsEvent]) -> String {
    let mut create = 0usize;
    let mut modify = 0usize;
    let mut rename = 0usize;
    let mut remove = 0usize;
    let mut mkdir = 0usize;

    for event in events {
        match event {
            FsEvent::Create(_) => create += 1,
            FsEvent::Modify(_) => modify += 1,
            FsEvent::Rename(_, _) => rename += 1,
            FsEvent::Remove(_) => remove += 1,
            FsEvent::MkDir(_) => mkdir += 1,
        }
    }

    format!(
        "Collapsed events: {} total (create {create}, modify {modify}, rename {rename}, remove {remove}, mkdir {mkdir})",
        events.len()
    )
}

fn summarize_planned_remote_ops(ops: &[PlannedRemoteOp]) -> String {
    let mut upload = 0usize;
    let mut remove = 0usize;
    let mut rename = 0usize;
    let mut mkdir = 0usize;

    for planned in ops {
        let op = &planned.op;
        match op {
            RemoteOp::Upload { .. } => upload += 1,
            RemoteOp::Remove { .. } => remove += 1,
            RemoteOp::Rename { .. } => rename += 1,
            RemoteOp::MkDir { .. } => mkdir += 1,
        }
    }

    format!(
        "Remote ops: {} total (upload {upload}, remove {remove}, rename {rename}, mkdir {mkdir})",
        ops.len()
    )
}

fn describe_remote_op(op: &RemoteOp) -> String {
    match op {
        RemoteOp::Upload { local, remote } => {
            format!("Applied upload: {} -> {remote}", display_path(local))
        }
        RemoteOp::Remove { remote } => format!("Applied remove: {remote}"),
        RemoteOp::Rename { from, to } => format!("Applied rename: {from} -> {to}"),
        RemoteOp::MkDir { remote } => format!("Applied mkdir: {remote}"),
    }
}

fn display_path(path: &std::path::Path) -> String {
    display_posix_path(path)
}

fn state_label(state: &TaskState) -> String {
    match state {
        TaskState::Idle => "Idle".into(),
        TaskState::Starting(stage) => format!("Starting - {stage}"),
        TaskState::Running => "Running".into(),
        TaskState::Error(e) => format!("Error - {e}"),
    }
}

fn should_queue_entry(
    path: &PathBuf,
    size_min: Option<u64>,
    size_max: Option<u64>,
    last_mtime: Option<&u64>,
) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if let Some(min) = size_min {
        if meta.len() < min {
            return false;
        }
    }
    if let Some(max) = size_max {
        if meta.len() > max {
            return false;
        }
    }

    let Ok(modified) = meta.modified() else {
        return false;
    };
    let Ok(dur) = modified.duration_since(UNIX_EPOCH) else {
        return false;
    };
    let mtime = dur.as_secs();
    last_mtime.copied() != Some(mtime)
}
