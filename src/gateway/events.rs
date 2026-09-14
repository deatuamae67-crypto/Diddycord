use std::sync::Arc;

use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct FrontendMessage {
    pub id: Box<str>,
    pub channel_id: Box<str>,
    pub author_username: Box<str>,
    pub content: Box<str>,
}

#[derive(Clone, Debug)]
pub struct FrontendMessageUpdate {
    pub id: Box<str>,
    pub channel_id: Box<str>,
    pub author_username: Option<Box<str>>,
    pub content: Option<Box<str>>,
}

#[derive(Clone, Debug)]
pub struct FrontendMessageDelete {
    pub id: Box<str>,
    pub channel_id: Box<str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendChannel {
    pub id: Box<str>,
    pub name: Option<Box<str>>,
    pub kind: u8,
    pub position: i32,
    pub parent_id: Option<Box<str>>,
}

#[derive(Clone, Debug)]
pub struct FrontendGuildSnapshot {
    pub id: Box<str>,
    pub name: Box<str>,
    pub unavailable: bool,
    pub channels: Vec<FrontendChannel>,
}

#[derive(Clone, Debug)]
pub struct FrontendGuildUpdate {
    pub id: Box<str>,
    pub name: Option<Box<str>>,
    pub unavailable: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct FrontendGuildDelete {
    pub id: Box<str>,
    pub unavailable: bool,
}

#[derive(Clone, Debug)]
pub struct FrontendChannelChange {
    pub guild_id: Box<str>,
    pub channel: FrontendChannel,
}

#[derive(Clone, Debug)]
pub struct FrontendChannelDelete {
    pub guild_id: Box<str>,
    pub channel_id: Box<str>,
}

#[derive(Debug)]
pub enum FrontendEvent {
    GatewayReady,
    GatewayResumed,
    Message(FrontendMessage),
    MessageUpdate(FrontendMessageUpdate),
    MessageDelete(FrontendMessageDelete),
    GuildCreate(FrontendGuildSnapshot),
    GuildUpdate(FrontendGuildUpdate),
    GuildDelete(FrontendGuildDelete),
    ChannelCreate(FrontendChannelChange),
    ChannelUpdate(FrontendChannelChange),
    ChannelDelete(FrontendChannelDelete),
}

#[derive(Deserialize)]
struct MessageCreate<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    channel_id: &'a str,
    #[serde(default, borrow)]
    content: Option<&'a str>,
    #[serde(default, borrow)]
    author: Option<MessageCreateAuthor<'a>>,
}

#[derive(Deserialize)]
struct MessageCreateAuthor<'a> {
    #[serde(borrow)]
    username: &'a str,
}

#[derive(Deserialize)]
struct MessageUpdate<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    channel_id: &'a str,
    #[serde(default, borrow)]
    content: Option<&'a str>,
    #[serde(default, borrow)]
    author: Option<MessageUpdateAuthor<'a>>,
}

#[derive(Deserialize)]
struct MessageUpdateAuthor<'a> {
    #[serde(default, borrow)]
    username: Option<&'a str>,
}

#[derive(Deserialize)]
struct MessageDelete<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    channel_id: &'a str,
}

#[derive(Deserialize)]
struct GuildCreate<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    name: &'a str,
    #[serde(default)]
    unavailable: bool,
    #[serde(default, borrow)]
    channels: Vec<GuildChannel<'a>>,
}

#[derive(Deserialize)]
struct GuildUpdate<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(default, borrow)]
    name: Option<&'a str>,
    #[serde(default)]
    unavailable: Option<bool>,
}

#[derive(Deserialize)]
struct GuildDelete<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(default)]
    unavailable: bool,
}

#[derive(Deserialize)]
struct GuildChannel<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(default, borrow)]
    name: Option<&'a str>,
    #[serde(rename = "type")]
    kind: u8,
    #[serde(default)]
    position: i32,
    #[serde(default, borrow)]
    parent_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct ChannelChange<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    guild_id: &'a str,
    #[serde(default, borrow)]
    name: Option<&'a str>,
    #[serde(rename = "type")]
    kind: u8,
    #[serde(default)]
    position: i32,
    #[serde(default, borrow)]
    parent_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct ChannelDelete<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    guild_id: &'a str,
}

pub(super) fn emit(frontend: &broadcast::Sender<Arc<FrontendEvent>>, event: FrontendEvent) {
    if frontend.receiver_count() != 0 {
        let _ = frontend.send(Arc::new(event));
    }
}

pub(super) fn emit_message_create(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(message) = serde_json::from_str::<MessageCreate<'_>>(raw.get()) else {
        return;
    };
    let Some(author) = message.author else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::Message(FrontendMessage {
            id: Box::<str>::from(message.id),
            channel_id: Box::<str>::from(message.channel_id),
            author_username: Box::<str>::from(author.username),
            content: Box::<str>::from(message.content.unwrap_or("")),
        }),
    );
}

pub(super) fn emit_message_update(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(message) = serde_json::from_str::<MessageUpdate<'_>>(raw.get()) else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::MessageUpdate(FrontendMessageUpdate {
            id: Box::<str>::from(message.id),
            channel_id: Box::<str>::from(message.channel_id),
            author_username: message
                .author
                .and_then(|author| author.username)
                .map(Box::<str>::from),
            content: message.content.map(Box::<str>::from),
        }),
    );
}

pub(super) fn emit_message_delete(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(message) = serde_json::from_str::<MessageDelete<'_>>(raw.get()) else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::MessageDelete(FrontendMessageDelete {
            id: Box::<str>::from(message.id),
            channel_id: Box::<str>::from(message.channel_id),
        }),
    );
}

pub(super) fn emit_guild_create(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(guild) = serde_json::from_str::<GuildCreate<'_>>(raw.get()) else {
        return;
    };

    let channels = guild
        .channels
        .into_iter()
        .map(frontend_channel)
        .collect::<Vec<_>>();

    emit(
        frontend,
        FrontendEvent::GuildCreate(FrontendGuildSnapshot {
            id: Box::<str>::from(guild.id),
            name: Box::<str>::from(guild.name),
            unavailable: guild.unavailable,
            channels,
        }),
    );
}

pub(super) fn emit_guild_update(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(guild) = serde_json::from_str::<GuildUpdate<'_>>(raw.get()) else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::GuildUpdate(FrontendGuildUpdate {
            id: Box::<str>::from(guild.id),
            name: guild.name.map(Box::<str>::from),
            unavailable: guild.unavailable,
        }),
    );
}

pub(super) fn emit_guild_delete(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(guild) = serde_json::from_str::<GuildDelete<'_>>(raw.get()) else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::GuildDelete(FrontendGuildDelete {
            id: Box::<str>::from(guild.id),
            unavailable: guild.unavailable,
        }),
    );
}

pub(super) fn emit_channel_create(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    emit_channel_change(frontend, raw, false);
}

pub(super) fn emit_channel_update(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    emit_channel_change(frontend, raw, true);
}

fn emit_channel_change(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
    update: bool,
) {
    let Ok(channel) = serde_json::from_str::<ChannelChange<'_>>(raw.get()) else {
        return;
    };

    let event = FrontendChannelChange {
        guild_id: Box::<str>::from(channel.guild_id),
        channel: FrontendChannel {
            id: Box::<str>::from(channel.id),
            name: channel.name.map(Box::<str>::from),
            kind: channel.kind,
            position: channel.position,
            parent_id: channel.parent_id.map(Box::<str>::from),
        },
    };

    emit(
        frontend,
        if update {
            FrontendEvent::ChannelUpdate(event)
        } else {
            FrontendEvent::ChannelCreate(event)
        },
    );
}

pub(super) fn emit_channel_delete(
    frontend: &broadcast::Sender<Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(channel) = serde_json::from_str::<ChannelDelete<'_>>(raw.get()) else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::ChannelDelete(FrontendChannelDelete {
            guild_id: Box::<str>::from(channel.guild_id),
            channel_id: Box::<str>::from(channel.id),
        }),
    );
}

fn frontend_channel(channel: GuildChannel<'_>) -> FrontendChannel {
    FrontendChannel {
        id: Box::<str>::from(channel.id),
        name: channel.name.map(Box::<str>::from),
        kind: channel.kind,
        position: channel.position,
        parent_id: channel.parent_id.map(Box::<str>::from),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selective_message_parser_skips_unneeded_metadata() {
        let json = r#"{
            "id":"555",
            "channel_id":"123",
            "content":"hello",
            "author":{"username":"tester","id":"999","avatar":"x"},
            "attachments":[{"id":"1","filename":"large.bin","metadata":{"nested":[1,2,3]}}],
            "embeds":[{"title":"ignored","fields":[{"name":"x","value":"y"}]}],
            "flags":4096,
            "reactions":[{"count":100,"emoji":{"name":"ignored"}}]
        }"#;
        let raw: &RawValue = serde_json::from_str(json).unwrap();
        let (tx, mut rx) = broadcast::channel(8);
        emit_message_create(&tx, raw);

        let event = rx.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::Message(message) => {
                assert_eq!(&*message.id, "555");
                assert_eq!(&*message.channel_id, "123");
                assert_eq!(&*message.author_username, "tester");
                assert_eq!(&*message.content, "hello");
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn partial_message_update_keeps_only_present_fields() {
        let json = r#"{
            "id":"555",
            "channel_id":"123",
            "content":"edited",
            "embeds":[{"title":"ignored"}],
            "flags":0
        }"#;
        let raw: &RawValue = serde_json::from_str(json).unwrap();
        let (tx, mut rx) = broadcast::channel(8);
        emit_message_update(&tx, raw);

        let event = rx.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::MessageUpdate(update) => {
                assert_eq!(&*update.id, "555");
                assert_eq!(&*update.channel_id, "123");
                assert!(update.author_username.is_none());
                assert_eq!(update.content.as_deref(), Some("edited"));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn guild_create_parser_ignores_members_roles_and_emojis() {
        let json = r#"{
            "id":"999",
            "name":"tiny guild",
            "unavailable":false,
            "channels":[
                {"id":"10","name":"general","type":0,"position":1,"parent_id":null,"permission_overwrites":[{"id":"1"}]},
                {"id":"11","name":"voice","type":2,"position":2,"parent_id":"12","bitrate":96000}
            ],
            "members":[{"user":{"id":"500","username":"ignored"},"roles":["1","2"]}],
            "roles":[{"id":"1","name":"ignored"}],
            "emojis":[{"id":"2","name":"ignored"}],
            "presences":[{"user":{"id":"500"},"activities":[{"name":"ignored"}]}]
        }"#;
        let raw: &RawValue = serde_json::from_str(json).unwrap();
        let (tx, mut rx) = broadcast::channel(8);
        emit_guild_create(&tx, raw);

        let event = rx.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::GuildCreate(guild) => {
                assert_eq!(guild.id.as_ref(), "999");
                assert_eq!(guild.name.as_ref(), "tiny guild");
                assert!(!guild.unavailable);
                assert_eq!(guild.channels.len(), 2);
                assert_eq!(guild.channels[0].id.as_ref(), "10");
                assert_eq!(guild.channels[0].name.as_deref(), Some("general"));
                assert_eq!(guild.channels[0].kind, 0);
                assert_eq!(guild.channels[1].parent_id.as_deref(), Some("12"));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn channel_update_parser_keeps_only_topology_fields() {
        let json = r#"{
            "id":"10",
            "guild_id":"999",
            "name":"renamed",
            "type":0,
            "position":4,
            "parent_id":"12",
            "permission_overwrites":[{"id":"ignored","allow":"123"}],
            "rate_limit_per_user":30
        }"#;
        let raw: &RawValue = serde_json::from_str(json).unwrap();
        let (tx, mut rx) = broadcast::channel(8);
        emit_channel_update(&tx, raw);

        let event = rx.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::ChannelUpdate(change) => {
                assert_eq!(change.guild_id.as_ref(), "999");
                assert_eq!(change.channel.id.as_ref(), "10");
                assert_eq!(change.channel.name.as_deref(), Some("renamed"));
                assert_eq!(change.channel.position, 4);
                assert_eq!(change.channel.parent_id.as_deref(), Some("12"));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn delete_parser_extracts_only_message_identity() {
        let json = r#"{"id":"555","channel_id":"123","guild_id":"999"}"#;
        let raw: &RawValue = serde_json::from_str(json).unwrap();
        let (tx, mut rx) = broadcast::channel(8);
        emit_message_delete(&tx, raw);

        let event = rx.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::MessageDelete(delete) => {
                assert_eq!(&*delete.id, "555");
                assert_eq!(&*delete.channel_id, "123");
            }
            _ => panic!("unexpected event"),
        }
    }
}
