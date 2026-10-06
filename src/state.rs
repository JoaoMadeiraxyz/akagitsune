use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::registry::TopicRegistry;

pub struct AppState {
    pub registry: Arc<TopicRegistry>,
    pub connections: AtomicUsize,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(TopicRegistry::new()),
            connections: AtomicUsize::new(0),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
