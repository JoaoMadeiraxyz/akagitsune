use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::ws::Message;
use papaya::{Compute, HashMap, Operation};
use tokio::sync::broadcast;
use uuid::Uuid;

pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 64;

#[derive(Clone, Debug)]
pub struct Subscriber {
    pub id: Uuid,
    pub inbox: broadcast::Sender<Message>,
}

#[derive(Default)]
pub struct TopicRegistry {
    topics: HashMap<Box<str>, Arc<[Subscriber]>>,
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, topic: &str, subscriber: &Subscriber) {
        let _ = self
            .topics
            .pin()
            .compute(topic.into(), |entry| match entry {
                None => Operation::Insert(Arc::from([subscriber.clone()])),
                Some((_, members)) if members.iter().any(|m| m.id == subscriber.id) => {
                    Operation::Abort(())
                }
                Some((_, members)) => {
                    let mut next = Vec::with_capacity(members.len() + 1);
                    next.extend_from_slice(members);
                    next.push(subscriber.clone());
                    Operation::Insert(Arc::from(next))
                }
            });
    }

    pub fn unsubscribe(&self, topic: &str, id: Uuid) {
        let _: Compute<'_, _, _, ()> =
            self.topics
                .pin()
                .compute(topic.into(), |entry| match entry {
                    None => Operation::Abort(()),
                    Some((_, members)) if members.iter().all(|m| m.id != id) => {
                        Operation::Abort(())
                    }
                    Some((_, members)) if members.len() == 1 => Operation::Remove,
                    Some((_, members)) => {
                        let next: Arc<[Subscriber]> =
                            members.iter().filter(|m| m.id != id).cloned().collect();
                        Operation::Insert(next)
                    }
                });
    }

    pub fn fanout(&self, topic: &str, from: Uuid, build: impl FnOnce() -> Message) {
        let topics = self.topics.pin();
        let Some(members) = topics.get(topic) else {
            return;
        };
        let mut targets = members.iter().filter(|m| m.id != from);
        let Some(first) = targets.next() else {
            return;
        };
        let frame = build();
        for member in targets {
            let _ = member.inbox.send(frame.clone());
        }
        let _ = first.inbox.send(frame);
    }

    pub fn topic_count(&self) -> usize {
        self.topics.pin().len()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SubscriptionLimitReached;

pub struct Subscriptions {
    registry: Arc<TopicRegistry>,
    subscriber: Subscriber,
    topics: HashSet<Box<str>>,
}

impl Subscriptions {
    pub fn new(registry: Arc<TopicRegistry>, subscriber: Subscriber) -> Self {
        Self {
            registry,
            subscriber,
            topics: HashSet::new(),
        }
    }

    pub fn subscribe(&mut self, topic: &str) -> Result<(), SubscriptionLimitReached> {
        if self.topics.contains(topic) {
            return Ok(());
        }
        if self.topics.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
            return Err(SubscriptionLimitReached);
        }
        self.registry.subscribe(topic, &self.subscriber);
        self.topics.insert(topic.into());
        Ok(())
    }

    pub fn unsubscribe(&mut self, topic: &str) {
        if self.topics.remove(topic) {
            self.registry.unsubscribe(topic, self.subscriber.id);
        }
    }

    pub fn id(&self) -> Uuid {
        self.subscriber.id
    }

    pub fn fanout(&self, topic: &str, build: impl FnOnce() -> Message) {
        self.registry.fanout(topic, self.subscriber.id, build);
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        for topic in self.topics.drain() {
            self.registry.unsubscribe(&topic, self.subscriber.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriber() -> (Subscriber, broadcast::Receiver<Message>) {
        let (inbox, rx) = broadcast::channel(8);
        (
            Subscriber {
                id: Uuid::new_v4(),
                inbox,
            },
            rx,
        )
    }

    #[test]
    fn last_unsubscribe_removes_the_topic() {
        let registry = TopicRegistry::new();
        let (a, _ra) = subscriber();
        let (b, _rb) = subscriber();
        registry.subscribe("k", &a);
        registry.subscribe("k", &b);
        registry.subscribe("k", &b);
        assert_eq!(registry.topic_count(), 1);
        registry.unsubscribe("k", a.id);
        assert_eq!(registry.topic_count(), 1);
        registry.unsubscribe("k", b.id);
        assert_eq!(registry.topic_count(), 0);
    }

    #[test]
    fn fanout_skips_the_publisher_and_missing_topics() {
        let registry = TopicRegistry::new();
        let (a, mut ra) = subscriber();
        let (b, mut rb) = subscriber();
        registry.subscribe("k", &a);
        registry.subscribe("k", &b);
        registry.fanout("k", a.id, || Message::Text("x".into()));
        registry.fanout("missing", a.id, || panic!("built for a missing topic"));
        assert!(ra.try_recv().is_err());
        assert!(rb.try_recv().is_ok());
        assert_eq!(registry.topic_count(), 1);
    }

    #[test]
    fn subscriptions_enforce_the_limit_and_clean_up_on_drop() {
        let registry = Arc::new(TopicRegistry::new());
        let (a, _ra) = subscriber();
        let mut subscriptions = Subscriptions::new(Arc::clone(&registry), a);
        for i in 0..MAX_SUBSCRIPTIONS_PER_CONNECTION {
            assert_eq!(subscriptions.subscribe(&format!("t{i}")), Ok(()));
        }
        assert_eq!(subscriptions.subscribe("t0"), Ok(()));
        assert_eq!(
            subscriptions.subscribe("extra"),
            Err(SubscriptionLimitReached)
        );
        subscriptions.unsubscribe("t0");
        assert_eq!(subscriptions.subscribe("extra"), Ok(()));
        assert_eq!(registry.topic_count(), MAX_SUBSCRIPTIONS_PER_CONNECTION);
        drop(subscriptions);
        assert_eq!(registry.topic_count(), 0);
    }
}
