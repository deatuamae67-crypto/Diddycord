use std::{fmt, sync::Arc};

use serde::{
    de::{IgnoredAny, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::value::RawValue;
use tokio::sync::broadcast::{self, error::TryRecvError};

use super::events::{emit, FrontendEvent};

const DM_CHANNEL_KIND: u8 = 1;
pub const DEFAULT_MAX_DIRECT_CHANNELS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendDirectChannel {
    pub id: Box<str>,
    pub recipient_id: Option<Box<str>>,
    pub recipient_username: Option<Box<str>>,
    pub recipient_global_name: Option<Box<str>>,
    pub recipient_avatar_hash: Option<Box<str>>,
}

impl FrontendDirectChannel {
    pub fn display_name(&self) -> Option<&str> {
        self.recipient_global_name
            .as_deref()
            .or(self.recipient_username.as_deref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendDirectChannelDelete {
    pub channel_id: Box<str>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectDrainReport {
    pub processed: usize,
    pub direct_events: usize,
    pub lagged: u64,
    pub closed: bool,
}

struct CachedDirectChannel {
    channel: FrontendDirectChannel,
    touched: u64,
}

pub struct DirectTopologyState {
    channels: Vec<CachedDirectChannel>,
    max_channels: usize,
    clock: u64,
    dropped_items: u64,
    dropped_gateway_events: u64,
}

impl Default for DirectTopologyState {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_DIRECT_CHANNELS)
    }
}

impl DirectTopologyState {
    pub fn new(max_channels: usize) -> Self {
        let max_channels = max_channels.max(1);
        Self {
            channels: Vec::with_capacity(max_channels.min(16)),
            max_channels,
            clock: 0,
            dropped_items: 0,
            dropped_gateway_events: 0,
        }
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn dropped_items(&self) -> u64 {
        self.dropped_items
    }

    pub fn dropped_gateway_events(&self) -> u64 {
        self.dropped_gateway_events
    }

    pub fn channels(&self) -> impl Iterator<Item = &FrontendDirectChannel> {
        self.channels.iter().map(|cached| &cached.channel)
    }

    pub fn channel(&self, channel_id: &str) -> Option<&FrontendDirectChannel> {
        self.channels
            .iter()
            .find(|cached| cached.channel.id.as_ref() == channel_id)
            .map(|cached| &cached.channel)
    }

    pub fn apply(&mut self, event: &FrontendEvent) -> bool {
        match event {
            FrontendEvent::DirectChannelCreate(channel)
            | FrontendEvent::DirectChannelUpdate(channel) => {
                self.upsert(channel);
                true
            }
            FrontendEvent::DirectChannelDelete(delete) => {
                self.channels
                    .retain(|cached| cached.channel.id.as_ref() != delete.channel_id.as_ref());
                true
            }
            FrontendEvent::Message(message) => self.touch(message.channel_id.as_ref()),
            FrontendEvent::MessageUpdate(message) => self.touch(message.channel_id.as_ref()),
            FrontendEvent::MessageDelete(message) => self.touch(message.channel_id.as_ref()),
            _ => false,
        }
    }

    pub fn drain(
        &mut self,
        receiver: &mut broadcast::Receiver<Arc<FrontendEvent>>,
        budget: usize,
    ) -> DirectDrainReport {
        let mut report = DirectDrainReport::default();

        while report.processed < budget {
            match receiver.try_recv() {
                Ok(event) => {
                    report.processed += 1;
                    if self.apply(event.as_ref()) {
                        report.direct_events += 1;
                    }
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

    fn upsert(&mut self, channel: &FrontendDirectChannel) {
        self.clock = self.clock.saturating_add(1);
        let touched = self.clock;

        if let Some(cached) = self
            .channels
            .iter_mut()
            .find(|cached| cached.channel.id.as_ref() == channel.id.as_ref())
        {
            cached.channel = channel.clone();
            cached.touched = touched;
            return;
        }

        if self.channels.len() >= self.max_channels {
            self.evict_least_recently_used();
        }
        self.channels.push(CachedDirectChannel {
            channel: channel.clone(),
            touched,
        });
    }

    fn touch(&mut self, channel_id: &str) -> bool {
        let Some(cached) = self
            .channels
            .iter_mut()
            .find(|cached| cached.channel.id.as_ref() == channel_id)
        else {
            return false;
        };

        self.clock = self.clock.saturating_add(1);
        cached.touched = self.clock;
        true
    }

    fn evict_least_recently_used(&mut self) {
        let Some((index, _)) = self
            .channels
            .iter()
            .enumerate()
            .min_by_key(|(_, cached)| cached.touched)
        else {
            return;
        };
        self.channels.remove(index);
        self.dropped_items = self.dropped_items.saturating_add(1);
    }
}

#[derive(Deserialize)]
struct DirectChannel<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(default, borrow)]
    guild_id: Option<&'a str>,
    #[serde(rename = "type")]
    kind: u8,
    #[serde(
        default,
        borrow,
        rename = "recipients",
        deserialize_with = "deserialize_first_recipient"
    )]
    recipient: Option<DirectRecipient<'a>>,
}

#[derive(Deserialize)]
struct DirectRecipient<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    username: &'a str,
    #[serde(default, borrow)]
    global_name: Option<&'a str>,
    #[serde(default, borrow)]
    avatar: Option<&'a str>,
}

#[derive(Deserialize)]
struct DirectChannelDelete<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(default, borrow)]
    guild_id: Option<&'a str>,
}

pub(super) fn emit_direct_channel_create(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    emit_direct_channel_change(frontend, raw, false);
}

pub(super) fn emit_direct_channel_update(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    emit_direct_channel_change(frontend, raw, true);
}

fn emit_direct_channel_change(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
    update: bool,
) {
    let Ok(channel) = serde_json::from_str::<DirectChannel<'_>>(raw.get()) else {
        return;
    };
    if channel.guild_id.is_some() || channel.kind != DM_CHANNEL_KIND || !is_snowflake(channel.id) {
        return;
    }

    let recipient = channel
        .recipient
        .filter(|recipient| is_snowflake(recipient.id) && !recipient.username.is_empty());
    let event = FrontendDirectChannel {
        id: Box::<str>::from(channel.id),
        recipient_id: recipient
            .as_ref()
            .map(|recipient| Box::<str>::from(recipient.id)),
        recipient_username: recipient
            .as_ref()
            .map(|recipient| Box::<str>::from(recipient.username)),
        recipient_global_name: recipient
            .as_ref()
            .and_then(|recipient| recipient.global_name)
            .map(Box::<str>::from),
        recipient_avatar_hash: recipient
            .as_ref()
            .and_then(|recipient| recipient.avatar)
            .map(Box::<str>::from),
    };

    emit(
        frontend,
        if update {
            FrontendEvent::DirectChannelUpdate(event)
        } else {
            FrontendEvent::DirectChannelCreate(event)
        },
    );
}

pub(super) fn emit_direct_channel_delete(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(channel) = serde_json::from_str::<DirectChannelDelete<'_>>(raw.get()) else {
        return;
    };
    if channel.guild_id.is_some() || !is_snowflake(channel.id) {
        return;
    }

    emit(
        frontend,
        FrontendEvent::DirectChannelDelete(FrontendDirectChannelDelete {
            channel_id: Box::<str>::from(channel.id),
        }),
    );
}

fn deserialize_first_recipient<'de, D>(
    deserializer: D,
) -> Result<Option<DirectRecipient<'de>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct FirstRecipientVisitor;

    impl<'de> Visitor<'de> for FirstRecipientVisitor {
        type Value = Option<DirectRecipient<'de>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a Discord DM recipient array")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let first = sequence.next_element::<DirectRecipient<'de>>()?;
            while sequence.next_element::<IgnoredAny>()?.is_some() {}
            Ok(first)
        }
    }

    deserializer.deserialize_seq(FirstRecipientVisitor)
}

fn is_snowflake(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FrontendMessage, FrontendMessageDelete};

    fn direct(id: &str, username: &str) -> FrontendDirectChannel {
        FrontendDirectChannel {
            id: Box::<str>::from(id),
            recipient_id: Some(Box::<str>::from("42")),
            recipient_username: Some(Box::<str>::from(username)),
            recipient_global_name: None,
            recipient_avatar_hash: None,
        }
    }

    #[test]
    fn direct_channel_parser_retains_only_first_recipient_navigation_fields() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "id":"777",
                "type":1,
                "last_message_id":"999",
                "recipients":[
                    {"id":"42","username":"alice","global_name":"Alice","avatar":"hash","public_flags":123},
                    {"id":"43","username":"ignored","global_name":"Ignored","avatar":"other"}
                ]
            }"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_channel_create(&sender, raw);

        let event = receiver.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::DirectChannelCreate(channel) => {
                assert_eq!(channel.id.as_ref(), "777");
                assert_eq!(channel.recipient_id.as_deref(), Some("42"));
                assert_eq!(channel.recipient_username.as_deref(), Some("alice"));
                assert_eq!(channel.display_name(), Some("Alice"));
                assert_eq!(channel.recipient_avatar_hash.as_deref(), Some("hash"));
            }
            _ => panic!("unexpected event"),
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn guild_channel_is_ignored_by_direct_parser() {
        let raw: &RawValue =
            serde_json::from_str(r#"{"id":"777","guild_id":"9","type":0,"name":"general"}"#)
                .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_channel_create(&sender, raw);

        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn direct_channel_delete_requires_no_guild_id() {
        let raw: &RawValue = serde_json::from_str(r#"{"id":"777","type":1}"#).unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_channel_delete(&sender, raw);

        let event = receiver.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::DirectChannelDelete(delete) => {
                assert_eq!(delete.channel_id.as_ref(), "777");
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn direct_channel_without_recipient_is_still_navigable() {
        let raw: &RawValue = serde_json::from_str(r#"{"id":"777","type":1}"#).unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_channel_create(&sender, raw);

        let event = receiver.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::DirectChannelCreate(channel) => {
                assert_eq!(channel.id.as_ref(), "777");
                assert_eq!(channel.display_name(), None);
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn direct_topology_upserts_deletes_and_bounds_with_lru() {
        let mut state = DirectTopologyState::new(2);
        state.apply(&FrontendEvent::DirectChannelCreate(direct("10", "one")));
        state.apply(&FrontendEvent::DirectChannelCreate(direct("11", "two")));
        state.apply(&FrontendEvent::Message(FrontendMessage {
            id: Box::<str>::from("100"),
            channel_id: Box::<str>::from("10"),
            author_username: Box::<str>::from("tester"),
            content: Box::<str>::from("touch"),
        }));
        state.apply(&FrontendEvent::DirectChannelCreate(direct("12", "three")));

        assert_eq!(state.channel_count(), 2);
        assert!(state.channel("10").is_some());
        assert!(state.channel("11").is_none());
        assert!(state.channel("12").is_some());
        assert_eq!(state.dropped_items(), 1);

        state.apply(&FrontendEvent::DirectChannelUpdate(direct("10", "renamed")));
        assert_eq!(
            state.channel("10").unwrap().recipient_username.as_deref(),
            Some("renamed")
        );

        state.apply(&FrontendEvent::DirectChannelDelete(
            FrontendDirectChannelDelete {
                channel_id: Box::<str>::from("10"),
            },
        ));
        assert!(state.channel("10").is_none());
    }

    #[test]
    fn direct_drain_is_budgeted_and_accounts_lag() {
        let (sender, mut receiver) = broadcast::channel(2);
        sender
            .send(Arc::new(FrontendEvent::DirectChannelCreate(direct(
                "10", "one",
            ))))
            .unwrap();
        sender
            .send(Arc::new(FrontendEvent::MessageDelete(
                FrontendMessageDelete {
                    id: Box::<str>::from("100"),
                    channel_id: Box::<str>::from("10"),
                },
            )))
            .unwrap();
        sender
            .send(Arc::new(FrontendEvent::DirectChannelCreate(direct(
                "11", "two",
            ))))
            .unwrap();

        let mut state = DirectTopologyState::new(8);
        let report = state.drain(&mut receiver, 8);

        assert!(report.lagged >= 1);
        assert_eq!(state.dropped_gateway_events(), report.lagged);
        assert!(report.processed >= 1);
    }
}
