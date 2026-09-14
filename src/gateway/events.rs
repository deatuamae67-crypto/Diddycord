use std::sync::Arc;

use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::broadcast;

#[derive(Debug)]
pub struct FrontendMessage {
    pub id: Box<str>,
    pub channel_id: Box<str>,
    pub author_username: Box<str>,
    pub content: Box<str>,
}

#[derive(Debug)]
pub struct FrontendMessageUpdate {
    pub id: Box<str>,
    pub channel_id: Box<str>,
    pub author_username: Option<Box<str>>,
    pub content: Option<Box<str>>,
}

#[derive(Debug)]
pub struct FrontendMessageDelete {
    pub id: Box<str>,
    pub channel_id: Box<str>,
}

#[derive(Debug)]
pub enum FrontendEvent {
    GatewayReady,
    GatewayResumed,
    Message(FrontendMessage),
    MessageUpdate(FrontendMessageUpdate),
    MessageDelete(FrontendMessageDelete),
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
