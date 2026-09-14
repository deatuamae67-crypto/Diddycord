use std::fmt;

use serde::{
    de::{IgnoredAny, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::value::RawValue;
use tokio::sync::broadcast;

use super::events::{emit, FrontendEvent};

const DM_CHANNEL_KIND: u8 = 1;

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
    frontend: &broadcast::Sender<std::sync::Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    emit_direct_channel_change(frontend, raw, false);
}

pub(super) fn emit_direct_channel_update(
    frontend: &broadcast::Sender<std::sync::Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    emit_direct_channel_change(frontend, raw, true);
}

fn emit_direct_channel_change(
    frontend: &broadcast::Sender<std::sync::Arc<FrontendEvent>>,
    raw: &RawValue,
    update: bool,
) {
    let Ok(channel) = serde_json::from_str::<DirectChannel<'_>>(raw.get()) else {
        return;
    };
    if channel.guild_id.is_some() || channel.kind != DM_CHANNEL_KIND || !is_snowflake(channel.id) {
        return;
    }

    let recipient = channel.recipient.filter(|recipient| {
        is_snowflake(recipient.id) && !recipient.username.is_empty()
    });
    let event = FrontendDirectChannel {
        id: Box::<str>::from(channel.id),
        recipient_id: recipient.as_ref().map(|recipient| Box::<str>::from(recipient.id)),
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
    frontend: &broadcast::Sender<std::sync::Arc<FrontendEvent>>,
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
        let raw: &RawValue = serde_json::from_str(
            r#"{"id":"777","guild_id":"9","type":0,"name":"general"}"#,
        )
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
}
