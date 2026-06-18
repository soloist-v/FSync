use anyhow::{anyhow, Result};
use fsync_core::{RemoteOp, RemoteOpLog, RemoteOpStatus};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

use crate::models::path_text;

#[derive(Debug, Clone)]
pub(crate) struct OperationLogNotification {
    pub(crate) task_id: String,
    pub(crate) latest_id: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct OperationLogRecord {
    pub(crate) id: i64,
    pub(crate) message: String,
    pub(crate) error: Option<String>,
}

impl OperationLogRecord {
    pub(crate) fn display_message(&self) -> String {
        match &self.error {
            Some(error) if !error.is_empty() => format!("{}: {error}", self.message),
            _ => self.message.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct OperationLogNotifier {
    tx: broadcast::Sender<OperationLogNotification>,
}

impl OperationLogNotifier {
    pub(crate) fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { tx }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<OperationLogNotification> {
        self.tx.subscribe()
    }

    fn notify_changed(&self, task_id: String, latest_id: i64) {
        let _ = self
            .tx
            .send(OperationLogNotification { task_id, latest_id });
    }
}

#[derive(Clone)]
pub(crate) struct OperationLogWriter {
    tx: mpsc::UnboundedSender<OperationLogCommand>,
    notifier: OperationLogNotifier,
}

impl OperationLogWriter {
    pub(crate) fn spawn(pool: SqlitePool, notifier: OperationLogNotifier) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_operation_log_actor(pool, notifier.clone(), rx));
        Self { tx, notifier }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<OperationLogNotification> {
        self.notifier.subscribe()
    }

    pub(crate) fn enqueue_many(&self, logs: Vec<(String, RemoteOpLog)>) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        self.tx
            .send(OperationLogCommand::InsertMany(logs))
            .map_err(|_| anyhow!("operation log writer stopped"))
    }
}

enum OperationLogCommand {
    InsertMany(Vec<(String, RemoteOpLog)>),
}

async fn run_operation_log_actor(
    pool: SqlitePool,
    notifier: OperationLogNotifier,
    mut rx: mpsc::UnboundedReceiver<OperationLogCommand>,
) {
    const FLUSH_DELAY: Duration = Duration::from_millis(25);
    const MAX_COMMANDS_PER_FLUSH: usize = 64;

    while let Some(command) = rx.recv().await {
        let mut logs = Vec::new();
        append_command_logs(command, &mut logs);

        tokio::time::sleep(FLUSH_DELAY).await;
        for _ in 0..MAX_COMMANDS_PER_FLUSH {
            match rx.try_recv() {
                Ok(command) => append_command_logs(command, &mut logs),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        match insert_logs(&pool, &logs).await {
            Ok(latest_by_task) => {
                for (task_id, latest_id) in latest_by_task {
                    notifier.notify_changed(task_id, latest_id);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, count = logs.len(), "failed to persist task operation logs");
            }
        }
    }
}

fn append_command_logs(command: OperationLogCommand, logs: &mut Vec<(String, RemoteOpLog)>) {
    match command {
        OperationLogCommand::InsertMany(mut batch) => logs.append(&mut batch),
    }
}

async fn insert_logs(
    pool: &SqlitePool,
    logs: &[(String, RemoteOpLog)],
) -> Result<HashMap<String, i64>> {
    let mut latest_by_task = HashMap::<String, i64>::new();
    let mut tx = pool.begin().await?;
    for (task_id, log) in logs {
        let fields = operation_log_fields(log);
        let result = sqlx::query(
            r#"
            INSERT INTO task_operation_logs (
                task_id, status, op_kind, local_path, remote_path, remote_from, remote_to,
                message, error
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
        )
        .bind(task_id)
        .bind(fields.status)
        .bind(fields.op_kind)
        .bind(fields.local_path)
        .bind(fields.remote_path)
        .bind(fields.remote_from)
        .bind(fields.remote_to)
        .bind(&log.message)
        .bind(&log.error)
        .execute(&mut *tx)
        .await?;
        latest_by_task.insert(task_id.clone(), result.last_insert_rowid());
    }
    tx.commit().await?;
    Ok(latest_by_task)
}

#[derive(Clone)]
pub(crate) struct OperationLogReader {
    pool: SqlitePool,
}

impl OperationLogReader {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn read_recent(
        &self,
        task_id: &str,
        limit: i64,
    ) -> Result<Vec<OperationLogRecord>> {
        let rows = sqlx::query_as::<_, (i64, String, Option<String>)>(
            r#"
            SELECT id, message, error
            FROM (
                SELECT id, message, error
                FROM task_operation_logs
                WHERE task_id = ?1
                ORDER BY id DESC
                LIMIT ?2
            )
            ORDER BY id ASC
            "#,
        )
        .bind(task_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(id, message, error)| OperationLogRecord { id, message, error })
            .collect())
    }

    pub(crate) async fn read_after(
        &self,
        task_id: &str,
        last_seen_id: i64,
        limit: i64,
    ) -> Result<Vec<OperationLogRecord>> {
        let rows = sqlx::query_as::<_, (i64, String, Option<String>)>(
            r#"
            SELECT id, message, error
            FROM task_operation_logs
            WHERE task_id = ?1 AND id > ?2
            ORDER BY id ASC
            LIMIT ?3
            "#,
        )
        .bind(task_id)
        .bind(last_seen_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(id, message, error)| OperationLogRecord { id, message, error })
            .collect())
    }
}

pub(crate) async fn migrate(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS task_operation_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL CHECK (status IN ('applied', 'failed')),
            op_kind TEXT NOT NULL CHECK (op_kind IN ('upload', 'remove', 'rename', 'mkdir')),
            local_path TEXT,
            remote_path TEXT,
            remote_from TEXT,
            remote_to TEXT,
            message TEXT NOT NULL,
            error TEXT
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_task_operation_logs_task_id_id
        ON task_operation_logs(task_id, id)
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

struct OperationLogFields {
    status: &'static str,
    op_kind: &'static str,
    local_path: Option<String>,
    remote_path: Option<String>,
    remote_from: Option<String>,
    remote_to: Option<String>,
}

fn operation_log_fields(log: &RemoteOpLog) -> OperationLogFields {
    let status = match log.status {
        RemoteOpStatus::Applied => "applied",
        RemoteOpStatus::Failed => "failed",
    };
    match &log.op {
        RemoteOp::Upload { local, remote } => OperationLogFields {
            status,
            op_kind: "upload",
            local_path: Some(path_text(local)),
            remote_path: Some(remote.clone()),
            remote_from: None,
            remote_to: None,
        },
        RemoteOp::Remove { remote } => OperationLogFields {
            status,
            op_kind: "remove",
            local_path: None,
            remote_path: Some(remote.clone()),
            remote_from: None,
            remote_to: None,
        },
        RemoteOp::MkDir { remote } => OperationLogFields {
            status,
            op_kind: "mkdir",
            local_path: None,
            remote_path: Some(remote.clone()),
            remote_from: None,
            remote_to: None,
        },
        RemoteOp::Rename { from, to } => OperationLogFields {
            status,
            op_kind: "rename",
            local_path: None,
            remote_path: None,
            remote_from: Some(from.clone()),
            remote_to: Some(to.clone()),
        },
    }
}
