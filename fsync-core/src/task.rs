use crate::{
    config::TaskConfig,
    file_op::{event_to_ops, FsEvent},
    filter::PathFilter,
    remote::{RemoteFs, RemoteOp},
    FoyerStore,
};
use anyhow::{anyhow, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::watch::Ref;
use tokio::sync::{mpsc, watch};
use walkdir::WalkDir;
use crate::convert::collapse_ops;

/// Public handle returned to callers for controlling a running sync task.
#[derive(Debug)]
pub struct SyncTaskHandle {
    cfg: TaskConfig,
    ctrl_tx: mpsc::Sender<TaskCommand>,
    state_rx: watch::Receiver<TaskState>,
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
}

#[derive(Debug, Clone)]
pub enum TaskCommand {
    Stop,
}

#[derive(Debug, Clone)]
pub enum TaskState {
    Idle,
    Running,
    Error(String),
}

pub(crate) struct SyncTask {
    cfg: TaskConfig,
    filter: Arc<PathFilter>,
    size_min: Option<u64>,
    size_max: Option<u64>,
}

impl SyncTask {
    pub fn new(cfg: TaskConfig) -> Self {
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
        state_tx: watch::Sender<TaskState>,
    ) {
        let (op_tx, mut op_rx) = mpsc::channel::<FsEvent>(1024);
        // open or create cache dir
        let cache_dir = PathBuf::from(format!("cache/{}", self.cfg.id));
        let store = match FoyerStore::open(1024 * 1024 * 4, &cache_dir).await {
            Ok(s) => s,
            Err(e) => {
                let _ = state_tx.send(TaskState::Error(format!("foyer open error: {e}")));
                return;
            }
        };

        // Initial incremental sync before watching
        {
            let mut initial_ops: Vec<FsEvent> = Vec::new();
            for entry in WalkDir::new(&self.cfg.local)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let path = entry.into_path();
                    if self.filter.check(&path) {
                        initial_ops.push(FsEvent::Modify(path));
                    }
                }
            }
            if let Err(e) = self.flush_batch(&remote, initial_ops, &store).await {
                let _ = state_tx.send(TaskState::Error(format!("initial sync error: {e}")));
            }
        }

        // spawn watcher
        if let Err(e) = self.spawn_watcher(op_tx.clone()) {
            let _ = state_tx.send(TaskState::Error(format!("watch error: {e}")));
            return;
        }

        // spawn scanner
        let scan_interval = Duration::from_millis(self.cfg.scan_ms);
        let scan_path = self.cfg.local.clone();
        let filter_clone = self.filter.clone();
        let scan_tx = op_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                interval.tick().await;
                for entry in WalkDir::new(&scan_path).into_iter().filter_map(|e| e.ok()) {
                    if entry.file_type().is_file() {
                        let path = entry.into_path();
                        if filter_clone.check(&path) {
                            let _ = scan_tx.try_send(FsEvent::Modify(path));
                        }
                    }
                }
            }
        });

        let _ = state_tx.send(TaskState::Running);
        // batching variables
        use tokio::time::{sleep, Sleep};
        let debounce = Duration::from_millis(150);
        let mut batch: Vec<FsEvent> = Vec::new();
        let mut sleeper: Option<std::pin::Pin<Box<Sleep>>> = None;
        loop {
            tokio::select! {
                Some(cmd) = ctrl_rx.recv() => {
                    match cmd {
                        TaskCommand::Stop => {
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
                    if let Err(e) = self.flush_batch(&remote, std::mem::take(&mut batch), &store).await {
                        let _ = state_tx.send(TaskState::Error(format!("batch error: {e}")));
                    }
                    sleeper = None;
                }
            }
        }
        if !batch.is_empty() {
            let _ = self.flush_batch(&remote, batch, &store).await;
        }
        let _ = state_tx.send(TaskState::Idle);
    }

    async fn flush_batch(
        &self,
        remote: &impl RemoteFs,
        ops: Vec<FsEvent>,
        store: &FoyerStore,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // collapse only consecutive Modify operations for the same path; keep order for others
        let ops = collapse_ops(ops);
        let mut remote_ops = Vec::new();
        // Gather the timestamps of the updated files
        let mut ts_updates = Vec::new();
        let remote_path = |local: &PathBuf| {
            let rel = local.strip_prefix(&self.cfg.local).unwrap_or(local);
            let s = PathBuf::from(&self.cfg.remote)
                .join(rel)
                .to_string_lossy()
                .to_string();
            s.replace('\\', "/")
        };
        for op in ops {
            match &op {
                FsEvent::Create(p) | FsEvent::Modify(p) => {
                    // size filter and mtime compare
                    if let Ok(meta) = tokio::fs::metadata(p).await {
                        if let (Some(min), Some(sz)) = (self.size_min, Some(meta.len())) {
                            if sz < min {
                                continue;
                            }
                        }
                        if let (Some(max), Some(sz)) = (self.size_max, Some(meta.len())) {
                            if sz > max {
                                continue;
                            }
                        }
                        if let Ok(modified) = meta.modified() {
                            if let Ok(dur) = modified.duration_since(UNIX_EPOCH) {
                                let mtime = dur.as_secs();
                                let key = p.to_string_lossy().to_string();
                                let last = store.get_u64(&key).await?;
                                if last.map(|v| v >= mtime).unwrap_or(false) {
                                    continue;
                                }
                                let r = remote_path(p);
                                remote_ops.push(RemoteOp::Upload {
                                    local: p.clone(),
                                    remote: r,
                                });
                                ts_updates.push((p.clone(), mtime));
                            }
                        }
                    }
                }
                FsEvent::Remove(p) => {
                    remote_ops.push(RemoteOp::Remove {
                        remote: remote_path(p),
                    });
                    ts_updates.push((p.clone(), 0));
                }
                FsEvent::MkDir(p) => {
                    remote_ops.push(RemoteOp::MkDir {
                        remote: remote_path(p),
                    });
                }
                FsEvent::Rename(from, to) => {
                    remote_ops.push(RemoteOp::Rename {
                        from: remote_path(from),
                        to: remote_path(to),
                    });
                    // Inherit timestamp from source to avoid unnecessary upload on pure rename
                    let from_key = from.to_string_lossy().to_string();
                    let last = store.get_u64(&from_key).await?;
                    if let Some(ts) = last {
                        ts_updates.push((to.clone(), ts));
                    }
                    // Clear the source path record
                    ts_updates.push((from.clone(), 0));
                }
            }
        }
        if !remote_ops.is_empty() {
            // simple retry with backoff
            let mut attempt: u32 = 0;
            let mut backoff = self.cfg.retry_backoff_ms;
            let max = self.cfg.retry_max;
            loop {
                match remote.apply_batch(remote_ops.clone()).await {
                    Ok(_) => break,
                    Err(e) => {
                        attempt += 1;
                        if attempt > max {
                            return Err(e);
                        }
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        backoff = backoff.saturating_mul(2);
                    }
                }
            }
        }
        for (p, ts) in ts_updates {
            let _ = store.put_u64(p.to_string_lossy().to_string(), ts);
        }
        Ok(())
    }

    fn spawn_watcher(&self, op_tx: mpsc::Sender<FsEvent>) -> Result<Option<impl FnOnce()>> {
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
                        if pass { let _ = op_tx.blocking_send(op); }
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

pub fn spawn_task<R: RemoteFs>(cfg: TaskConfig, remote: R) -> SyncTaskHandle {
    let (ctrl_tx, ctrl_rx) = mpsc::channel(4);
    let (state_tx, state_rx) = watch::channel(TaskState::Idle);
    let task = SyncTask::new(cfg.clone());
    tokio::spawn(task.run(remote, ctrl_rx, state_tx));
    SyncTaskHandle {
        cfg,
        ctrl_tx,
        state_rx,
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

impl SyncTask {
    // Merge only consecutive Modify ops for the same path, breaking on any
    // Rename/Remove/Create/MkDir for that path. Keep original order otherwise.
    fn collapse_ops(&self, ops: Vec<FsEvent>) -> Vec<FsEvent> {
        let mut out: Vec<FsEvent> = Vec::with_capacity(ops.len());
        let mut last_modify_idx: HashMap<PathBuf, usize> = HashMap::new();

        for op in ops {
            match &op {
                FsEvent::Modify(p) => {
                    if let Some(&idx) = last_modify_idx.get(p) {
                        out[idx] = FsEvent::Modify(p.clone());
                    } else {
                        last_modify_idx.insert(p.clone(), out.len());
                        out.push(op);
                    }
                }
                FsEvent::Remove(p) => {
                    last_modify_idx.remove(p);
                    out.push(op);
                }
                FsEvent::Rename(from, to) => {
                    last_modify_idx.remove(from);
                    last_modify_idx.remove(to);
                    out.push(op);
                }
                FsEvent::Create(p) | FsEvent::MkDir(p) => {
                    last_modify_idx.remove(p);
                    out.push(op);
                }
            }
        }
        out
    }
}
