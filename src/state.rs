use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::auth::Tokens;
use crate::registry::TopicRegistry;

pub struct SessionState {
    pub registry: Arc<TopicRegistry>,
    pub connections: AtomicUsize,
}

pub struct AppState {
    tokens: Tokens,
    session: Arc<SessionState>,
}

impl AppState {
    pub fn new(tokens: Tokens) -> Self {
        Self {
            tokens,
            session: Arc::new(SessionState {
                registry: Arc::new(TopicRegistry::new()),
                connections: AtomicUsize::new(0),
            }),
        }
    }

    pub fn tokens(&self) -> &Tokens {
        &self.tokens
    }

    pub fn session(&self) -> Arc<SessionState> {
        Arc::clone(&self.session)
    }
}
