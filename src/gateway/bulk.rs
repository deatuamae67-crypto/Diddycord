use std::fmt;

use serde::{
    de::{self, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::value::RawValue;
use tokio::sync::broadcast;

use super::events::{emit, FrontendEvent, FrontendMessageDelete};

const MAX_BULK_DELETE_IDS: usize = 100;

#[derive(Deserialize)]
struct MessageBulkDelete<'a> {
    #[serde(borrow, deserialize_with = "deserialize_bulk_ids")]
    ids: Vec<&'a str>,
    #[serde(borrow)]
    channel_id: &'a str,
}

pub(super) fn emit_message_delete_bulk(
    frontend: &broadcast::Sender<std::sync::Arc<FrontendEvent>>,
    raw: &RawValue,
) {
    let Ok(delete) = serde_json::from_str::<MessageBulkDelete<'_>>(raw.get()) else {
        return;
    };

    if !is_snowflake(delete.channel_id) || delete.ids.iter().any(|id| !is_snowflake(id)) {
        return;
    }

    for id in delete.ids {
        emit(
            frontend,
            FrontendEvent::MessageDelete(FrontendMessageDelete {
                id: Box::<str>::from(id),
                channel_id: Box::<str>::from(delete.channel_id),
            }),
        );
    }
}

fn deserialize_bulk_ids<'de, D>(deserializer: D) -> Result<Vec<&'de str>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BulkIdsVisitor;

    impl<'de> Visitor<'de> for BulkIdsVisitor {
        type Value = Vec<&'de str>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_BULK_DELETE_IDS} Discord message snowflakes"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut ids = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(MAX_BULK_DELETE_IDS),
            );

            while let Some(id) = sequence.next_element::<&'de str>()? {
                if ids.len() >= MAX_BULK_DELETE_IDS {
                    return Err(de::Error::custom(
                        "MESSAGE_DELETE_BULK exceeded the defensive ID limit",
                    ));
                }
                ids.push(id);
            }

            Ok(ids)
        }
    }

    deserializer.deserialize_seq(BulkIdsVisitor)
}

fn is_snowflake(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_delete_emits_existing_single_delete_events() {
        let raw: &RawValue = serde_json::from_str(
            r#"{"ids":["101","102","103"],"channel_id":"22","guild_id":"9"}"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_message_delete_bulk(&sender, raw);

        for expected in ["101", "102", "103"] {
            let event = receiver.try_recv().unwrap();
            match event.as_ref() {
                FrontendEvent::MessageDelete(delete) => {
                    assert_eq!(delete.id.as_ref(), expected);
                    assert_eq!(delete.channel_id.as_ref(), "22");
                }
                _ => panic!("unexpected event"),
            }
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn bulk_delete_rejects_non_snowflake_ids() {
        let raw: &RawValue = serde_json::from_str(
            r#"{"ids":["101","../../bad"],"channel_id":"22"}"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_message_delete_bulk(&sender, raw);

        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn bulk_delete_rejects_payloads_above_defensive_limit() {
        let ids = (0..=MAX_BULK_DELETE_IDS)
            .map(|id| format!("\"{}\"", id + 1))
            .collect::<Vec<_>>()
            .join(",");
        let json = format!(r#"{{"ids":[{ids}],"channel_id":"22"}}"#);
        let raw: &RawValue = serde_json::from_str(&json).unwrap();
        let (sender, mut receiver) = broadcast::channel(128);

        emit_message_delete_bulk(&sender, raw);

        assert!(receiver.try_recv().is_err());
    }
}
