use crate::{
    config::TaskConfig,
    remote::RemoteFs,
    task::{spawn_task, SyncTaskHandle},
};
use std::collections::HashMap;

pub struct SyncManager {
    tasks: HashMap<String, SyncTaskHandle>, // key by id string
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub fn start(&mut self, cfg: TaskConfig, remote: impl RemoteFs) {
        let id = cfg.id.to_string();
        if self.tasks.contains_key(&id) {
            return;
        }
        let handle = spawn_task(cfg, remote);
        self.tasks.insert(id, handle);
    }

    pub fn stop(&mut self, id: &str) {
        if let Some(h) = self.tasks.get(id) {
            let _ = h.stop();
        }
    }

    pub fn stop_all(&mut self) {
        for (_, h) in &self.tasks {
            let _ = h.stop();
        }
    }
}
