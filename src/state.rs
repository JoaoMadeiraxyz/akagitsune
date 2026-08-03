use std::sync::atomic::AtomicUsize;

use axum::extract::ws::Message;
use tokio::sync::broadcast;
use uuid::Uuid;

pub const BROADCAST_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub struct BroadcastMessage {
    pub source_id: Uuid,
    pub payload: Message,
}

pub struct AppState {
    pub tx: broadcast::Sender<BroadcastMessage>,
    pub connections: AtomicUsize,
}

impl AppState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            connections: AtomicUsize::new(0),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
