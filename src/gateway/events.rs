use std::sync::Arc;

use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::broadcast;

#[derive(Debug)]
pub struct FrontendMessage {
    pub channel_id: Box<str>,
    pub author_username: Box<str>,
    pub content: Box<str>,
}

#[derive(Debug)]
pub enum FrontendEvent {
    GatewayReady,
    GatewayResumed,
    Message(FrontendMessage),
}

#[derive(Deserialize)]
struct MessageCreate<'a> {
    #[serde(borrow)]
    channel_id: &'a str,
    #[serde(default, borrow)]
    content: Option<&'a str>,
    #[serde(default, borrow)]
    author: Option<MessageAuthor<'a>>,
}

#[derive(Deserialize)]
struct MessageAuthor<'a> {
    #[serde(borrow)]
    username: &'a str,
}

pub(super) fn emit(frontend: &broadcast::Sender<Arc<FrontendEvent>>, event: FrontendEvent) {
    if frontend.receiver_count() != 0 {
        let _ = frontend.send(Arc::new(event));
    }
}

pub(super) fn emit_message(frontend: &broadcast::Sender<Arc<FrontendEvent>>, raw: &RawValue) {
    let Ok(message) = serde_json::from_str::<MessageCreate<'_>>(raw.get()) else {
        return;
    };
    let Some(author) = message.author else {
        return;
    };

    emit(
        frontend,
        FrontendEvent::Message(FrontendMessage {
            channel_id: Box::<str>::from(message.channel_id),
            author_username: Box::<str>::from(author.username),
            content: Box::<str>::from(message.content.unwrap_or("")),
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selective_message_parser_skips_unneeded_metadata() {
        let json = r#"{
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
        emit_message(&tx, raw);

        let event = rx.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::Message(message) => {
                assert_eq!(&*message.channel_id, "123");
                assert_eq!(&*message.author_username, "tester");
                assert_eq!(&*message.content, "hello");
            }
            _ => panic!("unexpected event"),
        }
    }
}
