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
    retired_channels: VecDeque<Box<str>>,
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
            retired_channels: VecDeque::new(),
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
        self.invalidate_topology_timelines(event.as_ref());
        if self.topology.apply(event.as_ref()) {
            self.restore_topology_timelines(event.as_ref());
            return;
        }

        match event.as_ref() {
            FrontendEvent::GatewayReady | FrontendEvent::GatewayResumed => {
                self.gateway_ready = true;
            }
            FrontendEvent::DirectChannelCreate(channel)
            | FrontendEvent::DirectChannelUpdate(channel) => {
                self.unretire_channel(channel.id.as_ref());
            }
            FrontendEvent::DirectChannelDelete(_) => {}
            FrontendEvent::Message(message) => {
                let channel_id = message.channel_id.as_ref();
                if self.is_retired_channel(channel_id) {
                    return;
                }

                self.clock = self.clock.saturating_add(1);
                let touched = self.clock;

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
                if self.is_retired_channel(update.channel_id.as_ref()) {
                    return;
                }

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

    fn invalidate_topology_timelines(&mut self, event: &FrontendEvent) {
        let mut retired = Vec::<Box<str>>::new();

        match event {
            FrontendEvent::GuildCreate(snapshot) if !snapshot.unavailable => {
                for channel in self.topology.channels(snapshot.id.as_ref()) {
                    let still_exists = snapshot
                        .channels
                        .iter()
                        .any(|incoming| incoming.id.as_ref() == channel.id.as_ref());
                    if still_exists {
                        continue;
                    }

                    retired.push(channel.id.clone());
                    retired.extend(
                        self.topology
                            .threads_for_parent(snapshot.id.as_ref(), channel.id.as_ref())
                            .map(|thread| thread.id.clone()),
                    );
                }
            }
            FrontendEvent::GuildDelete(delete) if !delete.unavailable => {
                retired.extend(
                    self.topology
                        .channels(delete.id.as_ref())
                        .map(|channel| channel.id.clone()),
                );
                retired.extend(
                    self.topology
                        .threads(delete.id.as_ref())
                        .map(|thread| thread.id.clone()),
                );
            }
            FrontendEvent::ChannelDelete(delete) => {
                retired.push(delete.channel_id.clone());
                retired.extend(
                    self.topology
                        .threads_for_parent(delete.guild_id.as_ref(), delete.channel_id.as_ref())
                        .map(|thread| thread.id.clone()),
                );
            }
            FrontendEvent::DirectChannelDelete(delete) => {
                retired.push(delete.channel_id.clone());
            }
            FrontendEvent::ThreadUpdate(thread) if thread.archived => {
                retired.push(thread.id.clone());
            }
            FrontendEvent::ThreadDelete(delete) => {
                retired.push(delete.id.clone());
            }
            FrontendEvent::ThreadListSync(sync) => {
                for cached in self.topology.threads(sync.guild_id.as_ref()) {
                    let in_scope = match sync.parent_channel_ids.as_ref() {
                        Some(parent_ids) => cached
                            .parent_id
                            .as_deref()
                            .map(|parent_id| parent_ids.iter().any(|id| id.as_ref() == parent_id))
                            .unwrap_or(false),
                        None => true,
                    };
                    if !in_scope {
                        continue;
                    }

                    let remains_active = sync.threads.iter().any(|incoming| {
                        !incoming.archived
                            && incoming.guild_id.as_ref() == sync.guild_id.as_ref()
                            && incoming.id.as_ref() == cached.id.as_ref()
                    });
                    if !remains_active {
                        retired.push(cached.id.clone());
                    }
                }
            }
            _ => {}
        }

        for channel_id in retired {
            self.retire_channel(channel_id.as_ref());
        }
    }

    fn restore_topology_timelines(&mut self, event: &FrontendEvent) {
        match event {
            FrontendEvent::GuildCreate(snapshot) if !snapshot.unavailable => {
                for channel in &snapshot.channels {
                    self.unretire_channel(channel.id.as_ref());
                }
            }
            FrontendEvent::ChannelCreate(change) => {
                self.unretire_channel(change.channel.id.as_ref());
            }
            FrontendEvent::ThreadCreate(thread) | FrontendEvent::ThreadUpdate(thread)
                if !thread.archived =>
            {
                self.unretire_channel(thread.id.as_ref());
            }
            FrontendEvent::ThreadListSync(sync) => {
                for thread in sync.threads.iter().filter(|thread| {
                    !thread.archived && thread.guild_id.as_ref() == sync.guild_id.as_ref()
                }) {
                    self.unretire_channel(thread.id.as_ref());
                }
            }
            _ => {}
        }
    }

    fn retire_channel(&mut self, channel_id: &str) {
        self.channels.remove(channel_id);
        if self
            .retired_channels
            .iter()
            .any(|retired| retired.as_ref() == channel_id)
        {
            return;
        }

        let max_retired = self.max_channels.max(8);
        if self.retired_channels.len() >= max_retired {
            self.retired_channels.pop_front();
        }
        self.retired_channels
            .push_back(Box::<str>::from(channel_id));
    }

    fn unretire_channel(&mut self, channel_id: &str) {
        self.retired_channels
            .retain(|retired| retired.as_ref() != channel_id);
    }

    fn is_retired_channel(&self, channel_id: &str) -> bool {
        self.retired_channels
            .iter()
            .any(|retired| retired.as_ref() == channel_id)
    }

    fn apply_history(&mut self, channel_id: &str, messages: &[RestMessage]) {
        if self.is_retired_channel(channel_id) {
            return;
        }
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
    use crate::{
        FrontendChannel, FrontendChannelDelete, FrontendDirectChannel, FrontendDirectChannelDelete,
        FrontendGuildDelete, FrontendGuildSnapshot, FrontendThread, FrontendThreadListSync,
        RestOperation,
    };

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

    fn topology_channel(id: &str, name: &str) -> FrontendChannel {
        FrontendChannel {
            id: Box::<str>::from(id),
            name: Some(Box::<str>::from(name)),
            kind: 0,
            position: 0,
            parent_id: None,
        }
    }

    fn topology_thread(id: &str, guild_id: &str, parent_id: &str) -> FrontendThread {
        FrontendThread {
            id: Box::<str>::from(id),
            guild_id: Box::<str>::from(guild_id),
            parent_id: Some(Box::<str>::from(parent_id)),
            name: Some(Box::<str>::from("thread")),
            kind: 11,
            archived: false,
            locked: false,
        }
    }

    fn guild_snapshot(guild_id: &str, channel_ids: &[&str]) -> Arc<FrontendEvent> {
        Arc::new(FrontendEvent::GuildCreate(FrontendGuildSnapshot {
            id: Box::<str>::from(guild_id),
            name: Box::<str>::from("guild"),
            unavailable: false,
            channels: channel_ids
                .iter()
                .map(|id| topology_channel(id, id))
                .collect(),
        }))
    }

    fn direct_channel(id: &str) -> FrontendDirectChannel {
        FrontendDirectChannel {
            id: Box::<str>::from(id),
            recipient_id: Some(Box::<str>::from("42")),
            recipient_username: Some(Box::<str>::from("alice")),
            recipient_global_name: None,
            recipient_avatar_hash: None,
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

    #[test]
    fn channel_delete_purges_parent_and_child_thread_timelines() {
        let mut state = FrontendState::new(8, 8);
        state.apply(guild_snapshot("1", &["10"]));
        state.apply(Arc::new(FrontendEvent::ThreadCreate(topology_thread(
            "30", "1", "10",
        ))));
        state.apply(message("100", "10", "parent"));
        state.apply(message("101", "30", "thread"));

        state.apply(Arc::new(FrontendEvent::ChannelDelete(
            FrontendChannelDelete {
                guild_id: Box::<str>::from("1"),
                channel_id: Box::<str>::from("10"),
            },
        )));

        assert_eq!(state.channel_message_count("10"), 0);
        assert_eq!(state.channel_message_count("30"), 0);
        assert_eq!(state.cached_channel_count(), 0);

        state.apply_rest(&RestEvent::MessagesFetched {
            request_id: 20,
            channel_id: Box::<str>::from("10"),
            messages: vec![rest_message("99", "10", "late history")],
        });
        assert_eq!(state.channel_message_count("10"), 0);
    }

    #[test]
    fn temporary_guild_unavailability_preserves_cache_but_true_delete_purges_it() {
        let mut state = FrontendState::new(8, 8);
        state.apply(guild_snapshot("1", &["10"]));
        state.apply(Arc::new(FrontendEvent::ThreadCreate(topology_thread(
            "30", "1", "10",
        ))));
        state.apply(message("100", "10", "parent"));
        state.apply(message("101", "30", "thread"));

        state.apply(Arc::new(FrontendEvent::GuildDelete(FrontendGuildDelete {
            id: Box::<str>::from("1"),
            unavailable: true,
        })));
        assert_eq!(state.channel_message_count("10"), 1);
        assert_eq!(state.channel_message_count("30"), 1);

        state.apply(Arc::new(FrontendEvent::GuildDelete(FrontendGuildDelete {
            id: Box::<str>::from("1"),
            unavailable: false,
        })));
        assert_eq!(state.channel_message_count("10"), 0);
        assert_eq!(state.channel_message_count("30"), 0);
    }

    #[test]
    fn thread_sync_purges_only_threads_removed_from_the_sync_scope() {
        let mut state = FrontendState::new(8, 8);
        state.apply(guild_snapshot("1", &["10"]));
        let first = topology_thread("30", "1", "10");
        let second = topology_thread("31", "1", "10");
        state.apply(Arc::new(FrontendEvent::ThreadCreate(first.clone())));
        state.apply(Arc::new(FrontendEvent::ThreadCreate(second.clone())));
        state.apply(message("100", "30", "old"));
        state.apply(message("101", "31", "keep"));

        state.apply(Arc::new(FrontendEvent::ThreadListSync(
            FrontendThreadListSync {
                guild_id: Box::<str>::from("1"),
                parent_channel_ids: None,
                threads: vec![second],
            },
        )));

        assert_eq!(state.channel_message_count("30"), 0);
        assert_eq!(state.channel_message_count("31"), 1);
    }

    #[test]
    fn archived_thread_is_retired_until_it_becomes_active_again() {
        let mut state = FrontendState::new(8, 8);
        state.apply(guild_snapshot("1", &["10"]));
        let thread = topology_thread("30", "1", "10");
        state.apply(Arc::new(FrontendEvent::ThreadCreate(thread.clone())));
        state.apply(message("100", "30", "before archive"));

        let mut archived = thread.clone();
        archived.archived = true;
        state.apply(Arc::new(FrontendEvent::ThreadUpdate(archived)));
        assert_eq!(state.channel_message_count("30"), 0);

        state.apply_rest(&RestEvent::MessagesFetched {
            request_id: 21,
            channel_id: Box::<str>::from("30"),
            messages: vec![rest_message("90", "30", "late history")],
        });
        assert_eq!(state.channel_message_count("30"), 0);

        state.apply(Arc::new(FrontendEvent::ThreadUpdate(thread)));
        state.apply(message("101", "30", "active again"));
        assert_eq!(state.channel_message_count("30"), 1);
    }

    #[test]
    fn refreshed_guild_snapshot_retires_channels_that_disappeared() {
        let mut state = FrontendState::new(8, 8);
        state.apply(guild_snapshot("1", &["10", "11"]));
        state.apply(message("100", "10", "removed"));
        state.apply(message("101", "11", "kept"));

        state.apply(guild_snapshot("1", &["11"]));

        assert_eq!(state.channel_message_count("10"), 0);
        assert_eq!(state.channel_message_count("11"), 1);
    }

    #[test]
    fn direct_channel_delete_retires_timeline_and_blocks_late_history() {
        let mut state = FrontendState::new(8, 8);
        state.apply(Arc::new(FrontendEvent::DirectChannelCreate(
            direct_channel("77"),
        )));
        state.apply(message("100", "77", "dm"));
        assert_eq!(state.channel_message_count("77"), 1);

        state.apply(Arc::new(FrontendEvent::DirectChannelDelete(
            FrontendDirectChannelDelete {
                channel_id: Box::<str>::from("77"),
            },
        )));
        assert_eq!(state.channel_message_count("77"), 0);

        state.apply_rest(&RestEvent::MessagesFetched {
            request_id: 30,
            channel_id: Box::<str>::from("77"),
            messages: vec![rest_message("90", "77", "late history")],
        });
        state.apply(message("101", "77", "late gateway"));
        assert_eq!(state.channel_message_count("77"), 0);
    }

    #[test]
    fn recreated_direct_channel_clears_retirement_tombstone() {
        let mut state = FrontendState::new(8, 8);
        state.apply(message("100", "77", "before delete"));
        state.apply(Arc::new(FrontendEvent::DirectChannelDelete(
            FrontendDirectChannelDelete {
                channel_id: Box::<str>::from("77"),
            },
        )));
        state.apply(message("101", "77", "blocked"));
        assert_eq!(state.channel_message_count("77"), 0);

        state.apply(Arc::new(FrontendEvent::DirectChannelUpdate(
            direct_channel("77"),
        )));
        state.apply(message("102", "77", "active again"));
        assert_eq!(state.channel_message_count("77"), 1);
        assert_eq!(
            state.latest_message("77").unwrap().content.as_ref(),
            "active again"
        );
    }
}
