use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use tokio::sync::broadcast::{self, error::TryRecvError};

use crate::{FrontendEvent, FrontendMessage};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DrainReport {
    pub applied: usize,
    pub lagged: u64,
    pub closed: bool,
}

struct ChannelTimeline {
    messages: VecDeque<Arc<FrontendEvent>>,
    touched: u64,
}

impl ChannelTimeline {
    fn new(touched: u64) -> Self {
        Self {
            messages: VecDeque::new(),
            touched,
        }
    }
}

pub struct FrontendState {
    channels: HashMap<Box<str>, ChannelTimeline>,
    max_channels: usize,
    max_messages_per_channel: usize,
    clock: u64,
    gateway_ready: bool,
    dropped_gateway_events: u64,
}

impl FrontendState {
    pub fn new(max_channels: usize, max_messages_per_channel: usize) -> Self {
        Self {
            channels: HashMap::with_capacity(max_channels.max(1).min(64)),
            max_channels: max_channels.max(1),
            max_messages_per_channel: max_messages_per_channel.max(1),
            clock: 0,
            gateway_ready: false,
            dropped_gateway_events: 0,
        }
    }

    pub fn gateway_ready(&self) -> bool {
        self.gateway_ready
    }

    pub fn cached_channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn dropped_gateway_events(&self) -> u64 {
        self.dropped_gateway_events
    }

    pub fn channel_message_count(&self, channel_id: &str) -> usize {
        self.channels
            .get(channel_id)
            .map(|timeline| timeline.messages.len())
            .unwrap_or(0)
    }

    pub fn latest_message(&self, channel_id: &str) -> Option<&FrontendMessage> {
        self.channels
            .get(channel_id)
            .and_then(|timeline| timeline.messages.back())
            .and_then(|event| match event.as_ref() {
                FrontendEvent::Message(message) => Some(message),
                FrontendEvent::GatewayReady | FrontendEvent::GatewayResumed => None,
            })
    }

    pub fn messages<'a>(
        &'a self,
        channel_id: &'a str,
    ) -> impl Iterator<Item = &'a FrontendMessage> + 'a {
        self.channels
            .get(channel_id)
            .into_iter()
            .flat_map(|timeline| timeline.messages.iter())
            .filter_map(|event| match event.as_ref() {
                FrontendEvent::Message(message) => Some(message),
                FrontendEvent::GatewayReady | FrontendEvent::GatewayResumed => None,
            })
    }

    pub fn apply(&mut self, event: Arc<FrontendEvent>) {
        match event.as_ref() {
            FrontendEvent::GatewayReady | FrontendEvent::GatewayResumed => {
                self.gateway_ready = true;
            }
            FrontendEvent::Message(message) => {
                self.clock = self.clock.saturating_add(1);
                let touched = self.clock;
                let channel_id = message.channel_id.as_ref();

                if !self.channels.contains_key(channel_id) {
                    if self.channels.len() >= self.max_channels {
                        self.evict_least_recently_used_channel();
                    }
                    self.channels
                        .insert(Box::<str>::from(channel_id), ChannelTimeline::new(touched));
                }

                let timeline = self
                    .channels
                    .get_mut(channel_id)
                    .expect("channel timeline was inserted before lookup");
                timeline.touched = touched;

                if timeline.messages.len() >= self.max_messages_per_channel {
                    timeline.messages.pop_front();
                }
                timeline.messages.push_back(Arc::clone(&event));
            }
        }
    }

    pub fn drain(
        &mut self,
        receiver: &mut broadcast::Receiver<Arc<FrontendEvent>>,
        budget: usize,
    ) -> DrainReport {
        let mut report = DrainReport::default();

        while report.applied < budget {
            match receiver.try_recv() {
                Ok(event) => {
                    self.apply(event);
                    report.applied += 1;
                }
                Err(TryRecvError::Lagged(skipped)) => {
                    report.lagged = report.lagged.saturating_add(skipped);
                    self.dropped_gateway_events =
                        self.dropped_gateway_events.saturating_add(skipped);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => {
                    report.closed = true;
                    break;
                }
            }
        }

        report
    }

    fn evict_least_recently_used_channel(&mut self) {
        let Some(oldest) = self.channels.values().map(|timeline| timeline.touched).min() else {
            return;
        };

        let mut removed = false;
        self.channels.retain(|_, timeline| {
            if !removed && timeline.touched == oldest {
                removed = true;
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(channel_id: &str, content: &str) -> Arc<FrontendEvent> {
        Arc::new(FrontendEvent::Message(FrontendMessage {
            channel_id: Box::<str>::from(channel_id),
            author_username: Box::<str>::from("tester"),
            content: Box::<str>::from(content),
        }))
    }

    #[test]
    fn message_history_is_bounded_per_channel() {
        let mut state = FrontendState::new(8, 2);
        state.apply(message("channel", "one"));
        state.apply(message("channel", "two"));
        state.apply(message("channel", "three"));

        let content: Vec<&str> = state
            .messages("channel")
            .map(|message| message.content.as_ref())
            .collect();
        assert_eq!(content, vec!["two", "three"]);
    }

    #[test]
    fn least_recently_used_channel_is_evicted_at_capacity() {
        let mut state = FrontendState::new(2, 4);
        state.apply(message("a", "one"));
        state.apply(message("b", "two"));
        state.apply(message("a", "three"));
        state.apply(message("c", "four"));

        assert_eq!(state.cached_channel_count(), 2);
        assert_eq!(state.channel_message_count("a"), 2);
        assert_eq!(state.channel_message_count("b"), 0);
        assert_eq!(state.channel_message_count("c"), 1);
    }

    #[test]
    fn drain_never_waits_for_more_events() {
        let (sender, mut receiver) = broadcast::channel(8);
        sender.send(message("a", "one")).unwrap();
        sender.send(message("a", "two")).unwrap();

        let mut state = FrontendState::new(8, 8);
        let report = state.drain(&mut receiver, 32);

        assert_eq!(report.applied, 2);
        assert_eq!(report.lagged, 0);
        assert!(!report.closed);
        assert_eq!(state.channel_message_count("a"), 2);
    }

    #[test]
    fn lagged_receiver_is_accounted_without_blocking() {
        let (sender, mut receiver) = broadcast::channel(2);
        sender.send(message("a", "one")).unwrap();
        sender.send(message("a", "two")).unwrap();
        sender.send(message("a", "three")).unwrap();

        let mut state = FrontendState::new(8, 8);
        let report = state.drain(&mut receiver, 8);

        assert!(report.lagged >= 1);
        assert_eq!(state.dropped_gateway_events(), report.lagged);
        assert!(report.applied >= 1);
    }

    #[test]
    fn ready_and_resumed_mark_the_frontend_connected() {
        let mut state = FrontendState::new(1, 1);
        assert!(!state.gateway_ready());
        state.apply(Arc::new(FrontendEvent::GatewayReady));
        assert!(state.gateway_ready());
    }
}
