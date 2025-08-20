use iced::widget::{button, column, row, scrollable, text, Container};
use iced::{Element, Length, Task};
use iced::Subscription;
use anyhow::{Result, anyhow};
use fsync_core::{TaskConfig, RemoteCfg, spawn_task, TaskState, SyncTaskHandle, TaskCommand};
use fsync_remote_sftp::SftpRemote;
use std::fs;
use std::sync::Arc;

pub fn main() -> Result<()> {
    iced::application("FSync UI", update, view)
        .subscription(subscription)
        .run_with(|| {
            let initial = FSyncApp { tasks: vec![], toast: None };
            let task = Task::perform(async {
                let txt = fs::read_to_string("config.yaml").map_err(|e| anyhow!(e))?;
                let tasks: Vec<TaskConfig> = serde_yaml::from_str(&txt)?;
                Ok::<_, anyhow::Error>(tasks)
            }, |res| match res { Ok(v) => Message::Loaded(v), Err(_) => Message::Loaded(vec![]) });
            (initial, task)
        })
        .map_err(|e| anyhow!(e))?;
    Ok(())
}

#[derive(Debug, Clone)]
enum Message {
    Loaded(Vec<TaskConfig>),
    LoadedHandle(usize, Arc<SyncTaskHandle>),
    ToggleTask(usize),
    Tick,
    StartAll,
    StopAll,
    Toast(String),
}

struct TaskView {
    cfg: TaskConfig,
    handle: Option<Arc<SyncTaskHandle>>,
    state: TaskState,
}

struct FSyncApp {
    tasks: Vec<TaskView>,
    toast: Option<(String, std::time::Instant)>,
}

fn update(state: &mut FSyncApp, message: Message) -> Task<Message> {
    match message {
        Message::Loaded(list) => {
            state.tasks = list.into_iter().map(|cfg| TaskView { cfg, handle: None, state: TaskState::Idle }).collect();
            Task::none()
        }
        Message::LoadedHandle(idx, handle) => {
            if let Some(task) = state.tasks.get_mut(idx) {
                task.handle = Some(handle);
            }
            Task::none()
        }
        Message::ToggleTask(idx) => {
            if let Some(task) = state.tasks.get_mut(idx) {
                match task.state {
                    TaskState::Idle | TaskState::Error(_) => {
                        if let RemoteCfg::Sftp { host, user, password, fingerprints, .. } = &task.cfg.remote_cfg {
                            let cfg_clone = task.cfg.clone();
                            let host = host.clone();
                            let user = user.clone();
                            let pwd = password.clone();
                            let fp = fingerprints.clone();
                            return Task::perform(async move {
                                let mut attempt = 0u32;
                                let max = cfg_clone.retry_max;
                                let mut backoff = cfg_clone.retry_backoff_ms;
                                loop {
                                    match SftpRemote::connect(&host, &user, pwd.as_deref(), fp.clone()).await {
                                        Ok(remote) => {
                                            let handle = spawn_task(cfg_clone.clone(), remote);
                                            break Ok::<SyncTaskHandle, anyhow::Error>(handle);
                                        }
                                        Err(e) => {
                                            attempt += 1;
                                            if attempt > max { break Err(anyhow!(e)); }
                                            tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                                            backoff = backoff.saturating_mul(2);
                                        }
                                    }
                                }
                            }, move |res| match res { Ok(h) => Message::LoadedHandle(idx, Arc::new(h)), Err(e) => Message::Toast(format!("Start failed: {}", e)) });
                        }
                    }
                    TaskState::Running => {
                        if let Some(h) = &task.handle { let _ = h.ctrl_tx.try_send(TaskCommand::Stop); }
                    }
                }
            }
            Task::none()
        }
        Message::StartAll => {
            let mut tasks: Vec<Task<Message>> = Vec::new();
            for idx in 0..state.tasks.len() {
                if let Some(task) = state.tasks.get(idx) {
                    if matches!(task.state, TaskState::Idle | TaskState::Error(_)) {
                        if let RemoteCfg::Sftp { host, user, password, fingerprints,.. } = &task.cfg.remote_cfg {
                            let cfg_clone = task.cfg.clone();
                            let host = host.clone();
                            let user = user.clone();
                            let pwd = password.clone();
                            let fp = fingerprints.clone();
                            tasks.push(Task::perform(async move {
                                let mut attempt = 0u32;
                                let max = cfg_clone.retry_max;
                                let mut backoff = cfg_clone.retry_backoff_ms;
                                loop {
                                    match SftpRemote::connect(&host, &user, pwd.as_deref(), fp.clone()).await {
                                        Ok(remote) => {
                                            let handle = spawn_task(cfg_clone.clone(), remote);
                                            break Ok::<(usize, SyncTaskHandle), anyhow::Error>((idx, handle));
                                        }
                                        Err(e) => {
                                            attempt += 1;
                                            if attempt > max { break Err(anyhow!(e)); }
                                            tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                                            backoff = backoff.saturating_mul(2);
                                        }
                                    }
                                }
                            }, |res| match res { Ok((i, h)) => Message::LoadedHandle(i, Arc::new(h)), Err(e) => Message::Toast(format!("StartAll failed: {}", e)) }));
                        }
                    }
                }
            }
            Task::batch(tasks)
        }
        Message::StopAll => {
            for tv in &mut state.tasks { if let Some(h) = &tv.handle { let _ = h.ctrl_tx.try_send(TaskCommand::Stop); } }
            Task::none()
        }
        Message::Toast(msg) => { state.toast = Some((msg, std::time::Instant::now())); Task::none() }
        Message::Tick => {
            for tv in &mut state.tasks { if let Some(h) = &tv.handle { let st = (*h.state_rx.borrow()).clone(); tv.state = st; } }
            if let Some((_, t0)) = &state.toast { if t0.elapsed() > std::time::Duration::from_secs(3) { state.toast = None; } }
            Task::none()
        }
    }
}

fn subscription(_state: &FSyncApp) -> Subscription<Message> {
    iced::time::every(std::time::Duration::from_millis(500)).map(|_| Message::Tick)
}

fn view(state: &FSyncApp) -> Element<Message> {
    let header_actions = row![
        button("Start All").on_press_with(|| Message::StartAll),
        button("Stop All").on_press_with(|| Message::StopAll),
    ].spacing(10);
    let header = row![ header_actions, text("Task"), text("State").width(Length::Fill), text("Action") ].spacing(20);
    let mut col = column![ header ].spacing(10);
    if let Some((msg, _)) = &state.toast { col = col.push(text(msg)); }
    for (i, t) in state.tasks.iter().enumerate() {
        let state_str:String = match &t.state { TaskState::Idle => "Idle".into(), TaskState::Running => "Running".into(), TaskState::Error(e) => format!("Error: {e}") };
        let button_label = if matches!(t.state, TaskState::Running) { "Stop" } else { "Start" };
        let row = row![
            text(&t.cfg.name).width(Length::FillPortion(2)),
            text(state_str).width(Length::FillPortion(2)),
            button(button_label).on_press_with(move || Message::ToggleTask(i))
        ].spacing(20);
        col = col.push(row);
    }
    scrollable(Container::new(col).padding(20)).into()
}
