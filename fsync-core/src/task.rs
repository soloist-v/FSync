use crate::convert::collapse_ops;
use crate::{
    config::TaskConfig,
    file_op::{event_to_ops, FsEvent},
    filter::PathFilter,
    remote::{RemoteFs, RemoteOp},
    StateStore,
};
use anyhow::{anyhow, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::time::UNIX_EPOCH;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::watch::Ref;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{sleep, Sleep};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

/// Public handle returned to callers for controlling a running sync task.
#[derive(Debug)]
pub struct SyncTaskHandle {
    cfg: TaskConfig,
    ctrl_tx: mpsc::Sender<TaskCommand>,
    state_rx: watch::Receiver<TaskState>,
    log_tx: broadcast::Sender<TaskLog>,
}

impl SyncTaskHandle {
    pub fn config(&self) -> &TaskConfig {
        &self.cfg
    }

    pub fn stop(&self) {
        let _ = self.ctrl_tx.try_send(TaskCommand::Stop);
    }
    pub fn state(&self) -> Ref<'_, TaskState> {
        self.state_rx.borrow()
    }

    pub fn subscribe_logs(&self) -> broadcast::Receiver<TaskLog> {
        self.log_tx.subscribe()
    }
}

impl Drop for SyncTaskHandle {
    fn drop(&mut self) {
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
}

#[derive(Debug, Clone)]
pub enum TaskEvent {
    State(TaskState),
    Log(TaskLog),
    RemoteOp(RemoteOp),
}

pub trait TaskEventHandler: Send + Sync + 'static {
    fn emit(&self, event: TaskEvent);
}

enum StateUpdate {
    Put(PathBuf, u64),
    RemoveTree(PathBuf),
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
                });
                let _ = self.state_tx.send(state);
            }
            TaskEvent::Log(log) => {
                let _ = self.log_tx.send(log);
            }
            TaskEvent::RemoteOp(op) => {
                let _ = self.log_tx.send(TaskLog {
                    message: describe_remote_op(&op),
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
        let filter = Arc::new(PathFilter::new(&cfg.include, &cfg.exclude));
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
            cache_dir = %cache_dir.display(),
            "opening task cache"
        );
        let store = match StateStore::open(1024 * 1024 * 4, &cache_dir).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    task_id = %self.cfg.id,
                    cache_dir = %cache_dir.display(),
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
            TaskState::Starting("Scanning local tree".into()),
        );
        {
            let mut initial_ops: Vec<FsEvent> = Vec::new();
            let mut live_cache_keys: HashSet<String> = HashSet::new();
            for entry in WalkDir::new(&self.cfg.local)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let path = entry.into_path();
                    if self.filter.check(&path) {
                        live_cache_keys.insert(path.to_string_lossy().to_string());
                        initial_ops.push(FsEvent::Modify(path));
                    }
                }
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
                .flush_batch(&remote, initial_ops, &store, &event_handler)
                .await
            {
                emit_state(
                    &event_handler,
                    TaskState::Error(format!("initial sync error: {e}")),
                );
                return;
            }
        }

        emit_state(
            &event_handler,
            TaskState::Starting("Starting watcher".into()),
        );
        let watcher_guard = match self.spawn_watcher(op_tx.clone()) {
            Ok(guard) => guard,
            Err(e) => {
                emit_state(
                    &event_handler,
                    TaskState::Error(format!("watch error: {e}")),
                );
                return;
            }
        };

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
                            stopped_by_command = true;
                            break;
                        }
                    }
                }
                Some(op) = op_rx.recv() => {
                    batch.push(op);
                    // if sleeper.is_none() {
                    sleeper = Some(Box::pin(sleep(debounce)));
                    // }
                }
                _ = async { if let Some(ref mut s) = sleeper { s.as_mut().await } }, if sleeper.is_some() => {
                    if let Err(e) = self.flush_batch(&remote, std::mem::take(&mut batch), &store, &event_handler).await {
                        emit_state(&event_handler, TaskState::Error(format!("batch error: {e}")));
                        break;
                    }
                    sleeper = None;
                }
            }
        }
        if !batch.is_empty() {
            let _ = self
                .flush_batch(&remote, batch, &store, &event_handler)
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
        let scan_interval = Duration::from_millis(self.cfg.scan_ms);
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

                for entry in WalkDir::new(&scan_path).into_iter().filter_map(|e| e.ok()) {
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
                    match should_scan_file(&path, size_min, size_max, &store).await {
                        Ok(true) => {
                            if let Err(e) = scan_tx.send(FsEvent::Modify(path)) {
                                crate::warn!("{:?}", e);
                                break;
                            }
                        }
                        Ok(false) => {}
                        Err(e) => crate::warn!("scan error: {e}"),
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
        let mut remote_ops = Vec::new();
        // Gather the timestamps of the updated files
        let mut state_updates = Vec::new();
        let mut queued_uploads = HashSet::new();
        for op in ops {
            match &op {
                FsEvent::Create(p) | FsEvent::Modify(p) => {
                    if let Ok(meta) = tokio::fs::metadata(p).await {
                        if meta.is_dir() {
                            self.queue_directory_tree(
                                p,
                                store,
                                event_handler,
                                &mut remote_ops,
                                &mut state_updates,
                                &mut queued_uploads,
                            )
                            .await?;
                        } else {
                            self.queue_file_upload(
                                p,
                                store,
                                event_handler,
                                &mut remote_ops,
                                &mut state_updates,
                                &mut queued_uploads,
                            )
                            .await?;
                        }
                    }
                }
                FsEvent::Remove(p) => {
                    let remote = self.remote_path(p);
                    emit_log(event_handler, format!("Remove: {remote}"));
                    remote_ops.push(RemoteOp::Remove { remote });
                    state_updates.push(StateUpdate::RemoveTree(p.clone()));
                }
                FsEvent::MkDir(p) => {
                    self.queue_directory_tree(
                        p,
                        store,
                        event_handler,
                        &mut remote_ops,
                        &mut state_updates,
                        &mut queued_uploads,
                    )
                    .await?;
                }
                FsEvent::Rename(from, to) => {
                    let from_remote = self.remote_path(from);
                    let to_remote = self.remote_path(to);
                    emit_log(
                        event_handler,
                        format!("Rename: {from_remote} -> {to_remote}"),
                    );
                    remote_ops.push(RemoteOp::Rename {
                        from: from_remote,
                        to: to_remote,
                    });
                    // Inherit timestamp from source to avoid unnecessary upload on pure rename
                    let from_key = from.to_string_lossy().to_string();
                    let last = store.get_u64(&from_key).await?;
                    if let Some(ts) = last {
                        state_updates.push(StateUpdate::Put(to.clone(), ts));
                    }
                    // Clear the source path record
                    state_updates.push(StateUpdate::RemoveTree(from.clone()));
                }
            }
        }
        if !remote_ops.is_empty() {
            crate::debug!("applying remote ops: {:?}", remote_ops);
            emit_log(event_handler, summarize_remote_ops(&remote_ops));
            self.apply_remote_ops(remote, &remote_ops, event_handler)
                .await?;
        } else {
            emit_log(event_handler, "No remote ops after cache/filter checks");
        }
        for update in state_updates {
            match update {
                StateUpdate::Put(path, ts) => {
                    let key = path.to_string_lossy().to_string();
                    let _ = store.put_u64(key, ts).await;
                }
                StateUpdate::RemoveTree(path) => {
                    let key = path.to_string_lossy().to_string();
                    match store.remove_tree(&key).await {
                        Ok(removed) if removed > 0 => {
                            emit_log(
                                event_handler,
                                format!(
                                    "Removed {removed} cache entrie(s): {}",
                                    display_path(&path)
                                ),
                            );
                        }
                        Ok(_) => {}
                        Err(e) => emit_log(event_handler, format!("Cache remove failed: {e}")),
                    }
                }
            }
        }
        store.flush().await?;
        Ok(())
    }

    async fn queue_directory_tree(
        &self,
        dir: &PathBuf,
        store: &StateStore,
        event_handler: &Arc<dyn TaskEventHandler>,
        remote_ops: &mut Vec<RemoteOp>,
        state_updates: &mut Vec<StateUpdate>,
        queued_uploads: &mut HashSet<String>,
    ) -> Result<()> {
        let remote = self.remote_path(dir);
        emit_log(event_handler, format!("MkDir: {remote}"));
        remote_ops.push(RemoteOp::MkDir { remote });

        let mut files_seen = 0usize;
        let mut files_queued = 0usize;
        for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                if entry.depth() > 0 && entry.file_type().is_dir() {
                    let path = entry.path().to_path_buf();
                    let remote = self.remote_path(&path);
                    emit_log(event_handler, format!("MkDir: {remote}"));
                    remote_ops.push(RemoteOp::MkDir { remote });
                }
                continue;
            }

            let path = entry.into_path();
            files_seen += 1;
            if self
                .queue_file_upload(
                    &path,
                    store,
                    event_handler,
                    remote_ops,
                    state_updates,
                    queued_uploads,
                )
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
        store: &StateStore,
        event_handler: &Arc<dyn TaskEventHandler>,
        remote_ops: &mut Vec<RemoteOp>,
        state_updates: &mut Vec<StateUpdate>,
        queued_uploads: &mut HashSet<String>,
    ) -> Result<bool> {
        if !self.filter.check(path) {
            return Ok(false);
        }

        let meta = match tokio::fs::metadata(path).await {
            Ok(meta) if meta.is_file() => meta,
            _ => return Ok(false),
        };

        if let Some(min) = self.size_min {
            if meta.len() < min {
                return Ok(false);
            }
        }
        if let Some(max) = self.size_max {
            if meta.len() > max {
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
        let key = path.to_string_lossy().to_string();
        if !queued_uploads.insert(key.clone()) {
            return Ok(false);
        }
        if store.get_u64(&key).await? == Some(mtime) {
            return Ok(false);
        }

        let remote = self.remote_path(path);
        emit_log(
            event_handler,
            format!("Upload: {} -> {remote}", display_path(path)),
        );
        remote_ops.push(RemoteOp::Upload {
            local: path.clone(),
            remote,
        });
        state_updates.push(StateUpdate::Put(path.clone(), mtime));

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
        ops: &[RemoteOp],
        event_handler: &Arc<dyn TaskEventHandler>,
    ) -> Result<()> {
        for op in ops {
            let mut attempt: u32 = 0;
            let mut backoff = self.cfg.retry_backoff_ms;
            let max = self.cfg.retry_max;

            loop {
                match remote.apply_batch(vec![op.clone()]).await {
                    Ok(_) => {
                        let detail = describe_remote_op(op);
                        tracing::debug!(task_id = %self.cfg.id, task_name = %self.cfg.name, operation = %detail, "remote op applied");
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        if attempt > max {
                            emit_log(
                                event_handler,
                                format!("Remote op failed: {}: {e}", describe_remote_op(op)),
                            );
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

        Ok(())
    }

    fn spawn_watcher(
        &self,
        op_tx: mpsc::UnboundedSender<FsEvent>,
    ) -> Result<Option<impl FnOnce()>> {
        let path = self.cfg.local.clone();
        let filter = self.filter.clone();
        let mut watcher: RecommendedWatcher = RecommendedWatcher::new(
            move |res| match res {
                Ok(event) => {
                    for op in event_to_ops(event) {
                        let pass = match &op {
                            FsEvent::Rename(from, to) => filter.check(from) || filter.check(to),
                            _ => filter.check(op.path()),
                        };
                        if pass {
                            let _ = op_tx.send(op);
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
            .unwrap_or_else(|| local.as_path());
        PathBuf::from(&self.cfg.remote)
            .join(rel)
            .to_string_lossy()
            .replace('\\', "/")
    }

    fn relative_local_path<'a>(&'a self, local: &'a PathBuf) -> Option<&'a std::path::Path> {
        if let Ok(rel) = local.strip_prefix(&self.cfg.local) {
            return Some(rel);
        }
        None
    }
}

pub fn spawn_task<R: RemoteFs>(cfg: TaskConfig, remote: R) -> SyncTaskHandle {
    let (ctrl_tx, ctrl_rx) = mpsc::channel(4);
    let (state_tx, state_rx) = watch::channel(TaskState::Starting("Task spawned".into()));
    let (log_tx, _) = broadcast::channel(512);
    let event_handler = Arc::new(BroadcastTaskEventHandler::new(
        state_tx.clone(),
        log_tx.clone(),
    ));
    let task = SyncTask::new(cfg.clone());
    tokio::spawn(task.run(remote, ctrl_rx, event_handler));
    SyncTaskHandle {
        cfg,
        ctrl_tx,
        state_rx,
        log_tx,
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

fn summarize_remote_ops(ops: &[RemoteOp]) -> String {
    let mut upload = 0usize;
    let mut remove = 0usize;
    let mut rename = 0usize;
    let mut mkdir = 0usize;

    for op in ops {
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
    let display = path.to_string_lossy();
    display
        .strip_prefix(r"\\?\")
        .unwrap_or(display.as_ref())
        .to_string()
}

fn state_label(state: &TaskState) -> String {
    match state {
        TaskState::Idle => "Idle".into(),
        TaskState::Starting(stage) => format!("Starting - {stage}"),
        TaskState::Running => "Running".into(),
        TaskState::Error(e) => format!("Error - {e}"),
    }
}

async fn should_scan_file(
    path: &PathBuf,
    size_min: Option<u64>,
    size_max: Option<u64>,
    store: &StateStore,
) -> Result<bool> {
    let meta = tokio::fs::metadata(path).await?;
    if let Some(min) = size_min {
        if meta.len() < min {
            return Ok(false);
        }
    }
    if let Some(max) = size_max {
        if meta.len() > max {
            return Ok(false);
        }
    }

    let modified = meta.modified()?;
    let dur = modified.duration_since(UNIX_EPOCH)?;
    let mtime = dur.as_secs();
    let key = path.to_string_lossy().to_string();
    let last = store.get_u64(&key).await?;
    Ok(last != Some(mtime))
}
