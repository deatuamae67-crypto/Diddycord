use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use tokio::sync::broadcast::{self, error::TryRecvError};

use crate::{
    topology::TopologyState, FrontendEvent, FrontendMessage, FrontendMessageDelete,
    FrontendMessageUpdate, RestEvent, RestMessage,
};

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
    dropped_rest_events: u64,
    topology: TopologyState,
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
            dropped_rest_events: 0,
            topology: TopologyState::default(),
        }
    }

    pub fn with_topology_limits(
        max_channels: usize,
        max_messages_per_channel: usize,
        max_guilds: usize,
        max_channels_per_guild: usize,
    ) -> Self {
        let mut state = Self::new(max_channels, max_messages_per_channel);
        state.topology = TopologyState::new(max_guilds, max_channels_per_guild);
        state
    }

    pub fn gateway_ready(&self) -> bool {
        self.gateway_ready
    }

    pub fn topology(&self) -> &TopologyState {
        &self.topology
    }

    pub fn cached_channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn dropped_gateway_events(&self) -> u64 {
        self.dropped_gateway_events
    }

    pub fn dropped_rest_events(&self) -> u64 {
        self.dropped_rest_events
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
            .and_then(frontend_message)
    }

    pub fn messages<'a>(
        &'a self,
        channel_id: &'a str,
    ) -> impl Iterator<Item = &'a FrontendMessage> + 'a {
        self.channels
            .get(channel_id)
            .into_iter()
            .flat_map(|timeline| timeline.messages.iter())
            .filter_map(frontend_message)
    }

    pub fn apply(&mut self, event: Arc<FrontendEvent>) {
        if self.topology.apply(event.as_ref()) {
            return;
        }

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

                if let Some(slot) = timeline.messages.iter_mut().rev().find(|cached| {
                    frontend_message(cached)
                        .map(|existing| existing.id.as_ref() == message.id.as_ref())
                        .unwrap_or(false)
                }) {
                    *slot = Arc::clone(&event);
                    return;
                }

                if timeline.messages.len() >= self.max_messages_per_channel {
                    timeline.messages.pop_front();
                }
                timeline.messages.push_back(Arc::clone(&event));
            }
            FrontendEvent::MessageUpdate(update) => {
                self.clock = self.clock.saturating_add(1);
                let touched = self.clock;
                let Some(timeline) = self.channels.get_mut(update.channel_id.as_ref()) else {
                    return;
                };
                timeline.touched = touched;

                let Some(slot) = timeline.messages.iter_mut().rev().find(|event| {
                    frontend_message(event)
                        .map(|message| message.id.as_ref() == update.id.as_ref())
                        .unwrap_or(false)
                }) else {
                    return;
                };
                let Some(existing) = frontend_message(slot) else {
                    return;
                };

                let replacement = FrontendMessage {
                    id: existing.id.clone(),
                    channel_id: existing.channel_id.clone(),
                    author_username: update
                        .author_username
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| existing.author_username.clone()),
                    content: update
                        .content
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| existing.content.clone()),
                };
                *slot = Arc::new(FrontendEvent::Message(replacement));
            }
            FrontendEvent::MessageDelete(delete) => {
                self.clock = self.clock.saturating_add(1);
                let touched = self.clock;
                let Some(timeline) = self.channels.get_mut(delete.channel_id.as_ref()) else {
                    return;
                };
                timeline.touched = touched;
                timeline.messages.retain(|event| {
                    frontend_message(event)
                        .map(|message| message.id.as_ref() != delete.id.as_ref())
                        .unwrap_or(true)
                });
            }
            _ => {}
        }
    }

    pub fn apply_rest(&mut self, event: &RestEvent) {
        match event {
            RestEvent::MessageSent { message, .. } => {
                let Some(author_username) = message.author_username.as_ref() else {
                    return;
                };

                self.apply(Arc::new(FrontendEvent::Message(FrontendMessage {
                    id: message.id.clone(),
                    channel_id: message.channel_id.clone(),
                    author_username: author_username.clone(),
                    content: message.content.clone(),
                })));
            }
            RestEvent::MessageEdited { message, .. } => {
                if let Some(author_username) = message.author_username.as_ref() {
                    self.apply(Arc::new(FrontendEvent::Message(FrontendMessage {
                        id: message.id.clone(),
                        channel_id: message.channel_id.clone(),
                        author_username: author_username.clone(),
                        content: message.content.clone(),
                    })));
                } else {
                    self.apply(Arc::new(FrontendEvent::MessageUpdate(
                        FrontendMessageUpdate {
                            id: message.id.clone(),
                            channel_id: message.channel_id.clone(),
                            author_username: None,
                            content: Some(message.content.clone()),
                        },
                    )));
                }
            }
            RestEvent::MessageDeleted {
                channel_id,
                message_id,
                ..
            } => {
                self.apply(Arc::new(FrontendEvent::MessageDelete(
                    FrontendMessageDelete {
                        id: message_id.clone(),
                        channel_id: channel_id.clone(),
                    },
                )));
            }
            RestEvent::MessagesFetched {
                channel_id,
                messages,
                ..
            } => self.apply_history(channel_id.as_ref(), messages),
            RestEvent::Failed { .. } => {}
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

    pub fn drain_rest(
        &mut self,
        receiver: &mut broadcast::Receiver<Arc<RestEvent>>,
        budget: usize,
    ) -> DrainReport {
        let mut report = DrainReport::default();

        while report.applied < budget {
            match receiver.try_recv() {
                Ok(event) => {
                    self.apply_rest(event.as_ref());
                    report.applied += 1;
                }
                Err(TryRecvError::Lagged(skipped)) => {
                    report.lagged = report.lagged.saturating_add(skipped);
                    self.dropped_rest_events = self.dropped_rest_events.saturating_add(skipped);
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

    fn apply_history(&mut self, channel_id: &str, messages: &[RestMessage]) {
        if !messages.iter().any(|message| {
            message.channel_id.as_ref() == channel_id && message.author_username.is_some()
        }) {
            return;
        }

        self.clock = self.clock.saturating_add(1);
        let touched = self.clock;
        let max_messages = self.max_messages_per_channel;

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
            .expect("channel timeline was inserted before history merge");
        timeline.touched = touched;

        for message in messages {
            if message.channel_id.as_ref() != channel_id {
                continue;
            }
            let Some(author_username) = message.author_username.as_ref() else {
                continue;
            };

            if timeline.messages.iter().any(|cached| {
                frontend_message(cached)
                    .map(|existing| existing.id.as_ref() == message.id.as_ref())
                    .unwrap_or(false)
            }) {
                continue;
            }

            let insert_at = timeline
                .messages
                .iter()
                .position(|cached| {
                    frontend_message(cached)
                        .map(|existing| {
                            snowflake_cmp(message.id.as_ref(), existing.id.as_ref())
                                == Ordering::Less
                        })
                        .unwrap_or(false)
                })
                .unwrap_or(timeline.messages.len());

            timeline.messages.insert(
                insert_at,
                Arc::new(FrontendEvent::Message(FrontendMessage {
                    id: message.id.clone(),
                    channel_id: message.channel_id.clone(),
                    author_username: author_username.clone(),
                    content: message.content.clone(),
                })),
            );

            if timeline.messages.len() > max_messages {
                timeline.messages.pop_front();
            }
        }
    }

    fn evict_least_recently_used_channel(&mut self) {
        let Some(oldest) = self
            .channels
            .values()
            .map(|timeline| timeline.touched)
            .min()
        else {
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

fn frontend_message(event: &Arc<FrontendEvent>) -> Option<&FrontendMessage> {
    match event.as_ref() {
        FrontendEvent::Message(message) => Some(message),
        _ => None,
    }
}

fn snowflake_cmp(left: &str, right: &str) -> Ordering {
    match left.len().cmp(&right.len()) {
        Ordering::Equal => left.cmp(right),
        ordering => ordering,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FrontendGuildSnapshot, RestOperation};

    fn message(id: &str, channel_id: &str, content: &str) -> Arc<FrontendEvent> {
        Arc::new(FrontendEvent::Message(FrontendMessage {
            id: Box::<str>::from(id),
            channel_id: Box::<str>::from(channel_id),
            author_username: Box::<str>::from("tester"),
            content: Box::<str>::from(content),
        }))
    }

    fn rest_message(id: &str, channel_id: &str, content: &str) -> RestMessage {
        RestMessage {
            id: Box::<str>::from(id),
            channel_id: Box::<str>::from(channel_id),
            author_username: Some(Box::<str>::from("tester")),
            content: Box::<str>::from(content),
        }
    }

    #[test]
    fn message_history_is_bounded_per_channel() {
        let mut state = FrontendState::new(8, 2);
        state.apply(message("1", "channel", "one"));
        state.apply(message("2", "channel", "two"));
        state.apply(message("3", "channel", "three"));

        let content: Vec<&str> = state
            .messages("channel")
            .map(|message| message.content.as_ref())
            .collect();
        assert_eq!(content, vec!["two", "three"]);
    }

    #[test]
    fn duplicate_create_upserts_instead_of_appending() {
        let mut state = FrontendState::new(8, 8);
        state.apply(message("1", "channel", "rest copy"));
        state.apply(message("1", "channel", "gateway copy"));

        assert_eq!(state.channel_message_count("channel"), 1);
        assert_eq!(
            state.latest_message("channel").unwrap().content.as_ref(),
            "gateway copy"
        );
    }

    #[test]
    fn least_recently_used_channel_is_evicted_at_capacity() {
        let mut state = FrontendState::new(2, 4);
        state.apply(message("1", "a", "one"));
        state.apply(message("2", "b", "two"));
        state.apply(message("3", "a", "three"));
        state.apply(message("4", "c", "four"));

        assert_eq!(state.cached_channel_count(), 2);
        assert_eq!(state.channel_message_count("a"), 2);
        assert_eq!(state.channel_message_count("b"), 0);
        assert_eq!(state.channel_message_count("c"), 1);
    }

    #[test]
    fn drain_never_waits_for_more_events() {
        let (sender, mut receiver) = broadcast::channel(8);
        sender.send(message("1", "a", "one")).unwrap();
        sender.send(message("2", "a", "two")).unwrap();

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
        sender.send(message("1", "a", "one")).unwrap();
        sender.send(message("2", "a", "two")).unwrap();
        sender.send(message("3", "a", "three")).unwrap();

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

    #[test]
    fn topology_events_flow_through_the_same_gateway_drain() {
        let (sender, mut receiver) = broadcast::channel(8);
        sender
            .send(Arc::new(FrontendEvent::GuildCreate(
                FrontendGuildSnapshot {
                    id: Box::<str>::from("guild"),
                    name: Box::<str>::from("name"),
                    unavailable: false,
                    channels: Vec::new(),
                },
            )))
            .unwrap();

        let mut state = FrontendState::new(8, 8);
        let report = state.drain(&mut receiver, 8);
        assert_eq!(report.applied, 1);
        assert_eq!(state.topology().guild_count(), 1);
        assert_eq!(state.topology().guild("guild").unwrap().name, "name");
    }

    #[test]
    fn message_update_replaces_only_present_fields() {
        let mut state = FrontendState::new(4, 8);
        state.apply(message("55", "channel", "before"));
        state.apply(Arc::new(FrontendEvent::MessageUpdate(
            FrontendMessageUpdate {
                id: Box::<str>::from("55"),
                channel_id: Box::<str>::from("channel"),
                author_username: None,
                content: Some(Box::<str>::from("after")),
            },
        )));

        let updated = state.latest_message("channel").unwrap();
        assert_eq!(updated.id.as_ref(), "55");
        assert_eq!(updated.author_username.as_ref(), "tester");
        assert_eq!(updated.content.as_ref(), "after");
    }

    #[test]
    fn message_delete_removes_cached_message() {
        let mut state = FrontendState::new(4, 8);
        state.apply(message("55", "channel", "keep?"));
        state.apply(message("56", "channel", "keep"));
        state.apply(Arc::new(FrontendEvent::MessageDelete(
            FrontendMessageDelete {
                id: Box::<str>::from("55"),
                channel_id: Box::<str>::from("channel"),
            },
        )));

        assert_eq!(state.channel_message_count("channel"), 1);
        assert_eq!(state.latest_message("channel").unwrap().id.as_ref(), "56");
    }

    #[test]
    fn rest_send_and_gateway_echo_converge_without_duplicates() {
        let mut state = FrontendState::new(4, 8);
        state.apply_rest(&RestEvent::MessageSent {
            request_id: 7,
            message: rest_message("55", "channel", "from REST"),
        });
        state.apply(message("55", "channel", "from Gateway"));

        assert_eq!(state.channel_message_count("channel"), 1);
        assert_eq!(
            state.latest_message("channel").unwrap().content.as_ref(),
            "from Gateway"
        );
    }

    #[test]
    fn rest_edit_and_delete_apply_to_cache() {
        let mut state = FrontendState::new(4, 8);
        state.apply(message("55", "channel", "before"));
        state.apply_rest(&RestEvent::MessageEdited {
            request_id: 8,
            message: rest_message("55", "channel", "after"),
        });
        assert_eq!(
            state.latest_message("channel").unwrap().content.as_ref(),
            "after"
        );

        state.apply_rest(&RestEvent::MessageDeleted {
            request_id: 9,
            channel_id: Box::<str>::from("channel"),
            message_id: Box::<str>::from("55"),
        });
        assert_eq!(state.channel_message_count("channel"), 0);
    }

    #[test]
    fn history_bootstrap_merges_older_messages_in_snowflake_order() {
        let mut state = FrontendState::new(4, 8);
        state.apply(message("200", "22", "live"));
        state.apply_rest(&RestEvent::MessagesFetched {
            request_id: 10,
            channel_id: Box::<str>::from("22"),
            messages: vec![
                rest_message("190", "22", "newer history"),
                rest_message("180", "22", "older history"),
            ],
        });

        let ids: Vec<&str> = state
            .messages("22")
            .map(|message| message.id.as_ref())
            .collect();
        assert_eq!(ids, vec!["180", "190", "200"]);
    }

    #[test]
    fn history_bootstrap_never_overwrites_a_live_cached_copy() {
        let mut state = FrontendState::new(4, 8);
        state.apply(message("190", "22", "edited live copy"));
        state.apply_rest(&RestEvent::MessagesFetched {
            request_id: 11,
            channel_id: Box::<str>::from("22"),
            messages: vec![rest_message("190", "22", "stale history copy")],
        });

        assert_eq!(state.channel_message_count("22"), 1);
        assert_eq!(
            state.latest_message("22").unwrap().content.as_ref(),
            "edited live copy"
        );
    }

    #[test]
    fn history_bootstrap_keeps_the_newest_messages_at_capacity() {
        let mut state = FrontendState::new(4, 3);
        state.apply(message("200", "22", "live"));
        state.apply_rest(&RestEvent::MessagesFetched {
            request_id: 12,
            channel_id: Box::<str>::from("22"),
            messages: vec![
                rest_message("190", "22", "three"),
                rest_message("180", "22", "two"),
                rest_message("170", "22", "one"),
            ],
        });

        let ids: Vec<&str> = state
            .messages("22")
            .map(|message| message.id.as_ref())
            .collect();
        assert_eq!(ids, vec!["180", "190", "200"]);
    }

    #[test]
    fn failed_rest_operation_does_not_mutate_cache() {
        let mut state = FrontendState::new(4, 8);
        state.apply(message("55", "channel", "keep"));
        state.apply_rest(&RestEvent::Failed {
            request_id: 13,
            operation: RestOperation::EditMessage,
            status: Some(500),
            retryable: true,
            message: Box::<str>::from("server error"),
        });

        assert_eq!(state.channel_message_count("channel"), 1);
        assert_eq!(
            state.latest_message("channel").unwrap().content.as_ref(),
            "keep"
        );
    }

    #[test]
    fn rest_drain_tracks_lag_separately() {
        let (sender, mut receiver) = broadcast::channel(2);
        for request_id in 1..=3 {
            sender
                .send(Arc::new(RestEvent::MessageSent {
                    request_id,
                    message: rest_message(&request_id.to_string(), "channel", "x"),
                }))
                .unwrap();
        }

        let mut state = FrontendState::new(4, 8);
        let report = state.drain_rest(&mut receiver, 8);

        assert!(report.lagged >= 1);
        assert_eq!(state.dropped_rest_events(), report.lagged);
        assert_eq!(state.dropped_gateway_events(), 0);
    }
}
