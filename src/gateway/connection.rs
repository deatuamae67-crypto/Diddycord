use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::time::{sleep, timeout, timeout_at, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

use super::{
    bulk::emit_message_delete_bulk,
    direct::{
        emit_direct_channel_create, emit_direct_channel_delete, emit_direct_channel_update,
        FrontendDirectChannel,
    },
    events::{
        emit, emit_channel_create, emit_channel_delete, emit_channel_update, emit_guild_create,
        emit_guild_delete, emit_guild_update, emit_message_create, emit_message_delete,
        emit_message_update, emit_thread_create, emit_thread_delete, emit_thread_list_sync,
        emit_thread_update,
    },
    identity::CurrentUser,
    protocol::{
        receive_hello, send_heartbeat, send_identify, send_resume, GatewayEnvelope, ReadyData,
    },
    recovery::{
        classify_close, gateway_url, heartbeat_jitter, invalid_session_delay, resume_or_reidentify,
        AuthMode, ConnectionExit, ConnectionNext, SessionState,
    },
    BoxError, NetworkBackbone, NetworkStatus, CONNECT_TIMEOUT, COOPERATIVE_YIELD_EVERY,
    INITIAL_GATEWAY_URL, MAX_GATEWAY_MESSAGE,
};
use crate::gateway::events::FrontendEvent;
use crate::gateway::io_error;

#[derive(Deserialize)]
struct DirectMessageDiscovery<'a> {
    #[serde(borrow)]
    channel_id: &'a str,
    #[serde(default, borrow)]
    guild_id: Option<&'a str>,
    #[serde(default, borrow)]
    author: Option<DirectMessageAuthor<'a>>,
}

#[derive(Deserialize)]
struct DirectMessageAuthor<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    username: &'a str,
    #[serde(default, borrow)]
    global_name: Option<&'a str>,
    #[serde(default, borrow)]
    avatar: Option<&'a str>,
}

impl NetworkBackbone {
    pub(super) async fn run_connection(
        &self,
        session: &mut SessionState,
        requested_auth: AuthMode,
    ) -> Result<ConnectionExit, BoxError> {
        let auth = if requested_auth == AuthMode::Resume && session.can_resume() {
            AuthMode::Resume
        } else {
            AuthMode::Identify
        };

        let url = match auth {
            AuthMode::Identify => INITIAL_GATEWAY_URL.to_owned(),
            AuthMode::Resume => gateway_url(
                session
                    .resume_gateway_url
                    .as_deref()
                    .ok_or_else(|| io_error("resume URL missing"))?,
            ),
        };

        let ws_config = WebSocketConfig::default()
            .read_buffer_size(16 * 1024)
            .write_buffer_size(2 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(MAX_GATEWAY_MESSAGE))
            .max_frame_size(Some(MAX_GATEWAY_MESSAGE));

        let (mut socket, _) = timeout(
            CONNECT_TIMEOUT,
            connect_async_with_config(url.as_str(), Some(ws_config), false),
        )
        .await
        .map_err(|_| io_error("Discord Gateway connection timed out"))??;

        let heartbeat_interval = receive_hello(&mut socket).await?;
        self.set_status(match auth {
            AuthMode::Identify => NetworkStatus::Identifying,
            AuthMode::Resume => NetworkStatus::Resuming,
        });

        match auth {
            AuthMode::Identify => {
                send_identify(&mut socket, self.config.token.as_ref(), self.config.intents).await?;
            }
            AuthMode::Resume => {
                let session_id = session
                    .session_id
                    .as_deref()
                    .ok_or_else(|| io_error("resume URL missing for resume"))?;
                let seq = session
                    .seq
                    .ok_or_else(|| io_error("sequence number missing for resume"))?;
                send_resume(&mut socket, self.config.token.as_ref(), session_id, seq).await?;
            }
        }

        let mut next_heartbeat = Instant::now() + heartbeat_jitter(heartbeat_interval);
        let mut awaiting_heartbeat_ack = false;
        let mut stable = false;
        let mut frames_since_yield = 0u32;

        loop {
            let frame = match timeout_at(next_heartbeat, socket.next()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(error))) => return Err(error.into()),
                Ok(None) => {
                    return Ok(ConnectionExit {
                        next: resume_or_reidentify(session),
                        stable,
                    })
                }
                Err(_) => {
                    if awaiting_heartbeat_ack {
                        return Ok(ConnectionExit {
                            next: resume_or_reidentify(session),
                            stable,
                        });
                    }

                    send_heartbeat(&mut socket, session.seq).await?;
                    awaiting_heartbeat_ack = true;
                    next_heartbeat = Instant::now() + heartbeat_interval;
                    continue;
                }
            };

            frames_since_yield = frames_since_yield.saturating_add(1);

            match frame {
                Message::Text(text) => {
                    let Ok(envelope) = serde_json::from_str::<GatewayEnvelope<'_>>(text.as_str())
                    else {
                        continue;
                    };

                    if let Some(seq) = envelope.s {
                        session.seq = Some(seq);
                    }

                    match envelope.op {
                        0 => {
                            stable = true;
                            match envelope.t {
                                Some("READY") => {
                                    if let Some(raw) = envelope.d {
                                        if let Ok(ready) =
                                            serde_json::from_str::<ReadyData<'_>>(raw.get())
                                        {
                                            session.session_id =
                                                Some(Box::<str>::from(ready.session_id));
                                            session.resume_gateway_url =
                                                Some(Box::<str>::from(ready.resume_gateway_url));
                                            if let Some(user) = CurrentUser::from_ready(raw) {
                                                self.set_self_user(user);
                                            }
                                            self.set_status(NetworkStatus::Ready);
                                            emit(&self.frontend, FrontendEvent::GatewayReady);
                                        }
                                    }
                                }
                                Some("RESUMED") => {
                                    self.set_status(NetworkStatus::Ready);
                                    emit(&self.frontend, FrontendEvent::GatewayResumed);
                                }
                                Some("USER_UPDATE") => {
                                    if let Some(raw) = envelope.d {
                                        if let Some(user) = CurrentUser::from_user_update(raw) {
                                            self.set_self_user(user);
                                        }
                                    }
                                }
                                Some("GUILD_CREATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_guild_create(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("GUILD_UPDATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_guild_update(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("GUILD_DELETE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_guild_delete(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("CHANNEL_CREATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_channel_create(&self.frontend, raw);
                                            emit_direct_channel_create(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("CHANNEL_UPDATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_channel_update(&self.frontend, raw);
                                            emit_direct_channel_update(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("CHANNEL_DELETE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_channel_delete(&self.frontend, raw);
                                            emit_direct_channel_delete(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("THREAD_CREATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_thread_create(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("THREAD_UPDATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_thread_update(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("THREAD_DELETE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_thread_delete(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("THREAD_LIST_SYNC") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_thread_list_sync(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("MESSAGE_CREATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            let self_user = self.self_user.borrow();
                                            emit_direct_message_discovery(
                                                &self.frontend,
                                                raw,
                                                self_user
                                                    .as_ref()
                                                    .map(|user| user.id.as_ref()),
                                            );
                                            drop(self_user);
                                            emit_message_create(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("MESSAGE_UPDATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_message_update(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("MESSAGE_DELETE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_message_delete(&self.frontend, raw);
                                        }
                                    }
                                }
                                Some("MESSAGE_DELETE_BULK") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_message_delete_bulk(&self.frontend, raw);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        1 => {
                            send_heartbeat(&mut socket, session.seq).await?;
                            awaiting_heartbeat_ack = true;
                        }
                        7 => {
                            return Ok(ConnectionExit {
                                next: resume_or_reidentify(session),
                                stable,
                            });
                        }
                        9 => {
                            let resumable = envelope
                                .d
                                .and_then(|raw| serde_json::from_str::<bool>(raw.get()).ok())
                                .unwrap_or(false);

                            sleep(invalid_session_delay()).await;
                            return Ok(ConnectionExit {
                                next: if resumable && session.can_resume() {
                                    ConnectionNext::Resume
                                } else {
                                    ConnectionNext::Reidentify
                                },
                                stable,
                            });
                        }
                        11 => {
                            awaiting_heartbeat_ack = false;
                        }
                        _ => {}
                    }
                }
                Message::Close(frame) => {
                    let next = frame
                        .map(|frame| classify_close(u16::from(frame.code), session))
                        .unwrap_or_else(|| resume_or_reidentify(session));
                    return Ok(ConnectionExit { next, stable });
                }
                Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            }

            if frames_since_yield >= COOPERATIVE_YIELD_EVERY {
                frames_since_yield = 0;
                tokio::task::yield_now().await;
            }
        }
    }
}

fn emit_direct_message_discovery(
    frontend: &tokio::sync::broadcast::Sender<std::sync::Arc<FrontendEvent>>,
    raw: &RawValue,
    self_user_id: Option<&str>,
) {
    let Ok(message) = serde_json::from_str::<DirectMessageDiscovery<'_>>(raw.get()) else {
        return;
    };
    if message.guild_id.is_some() || !is_snowflake(message.channel_id) {
        return;
    }

    let recipient = match (message.author, self_user_id) {
        (Some(author), Some(self_id))
            if author.id != self_id
                && is_snowflake(author.id)
                && !author.username.is_empty() =>
        {
            Some(author)
        }
        _ => None,
    };

    emit(
        frontend,
        FrontendEvent::DirectChannelUpdate(FrontendDirectChannel {
            id: Box::<str>::from(message.channel_id),
            recipient_id: recipient
                .as_ref()
                .map(|author| Box::<str>::from(author.id)),
            recipient_username: recipient
                .as_ref()
                .map(|author| Box::<str>::from(author.username)),
            recipient_global_name: recipient
                .as_ref()
                .and_then(|author| author.global_name)
                .map(Box::<str>::from),
            recipient_avatar_hash: recipient
                .as_ref()
                .and_then(|author| author.avatar)
                .map(Box::<str>::from),
        }),
    );
}

fn is_snowflake(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[test]
    fn inbound_dm_message_discovers_recipient_before_timeline_event() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "id":"9999",
                "channel_id":"777",
                "content":"hello",
                "author":{"id":"42","username":"alice","global_name":"Alice","avatar":"hash"}
            }"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_message_discovery(&sender, raw, Some("100"));
        emit_message_create(&sender, raw);

        let discovered = receiver.try_recv().unwrap();
        match discovered.as_ref() {
            FrontendEvent::DirectChannelUpdate(channel) => {
                assert_eq!(channel.id.as_ref(), "777");
                assert_eq!(channel.recipient_id.as_deref(), Some("42"));
                assert_eq!(channel.display_name(), Some("Alice"));
                assert_eq!(channel.recipient_avatar_hash.as_deref(), Some("hash"));
            }
            _ => panic!("expected direct-channel discovery first"),
        }
        assert!(matches!(
            receiver.try_recv().unwrap().as_ref(),
            FrontendEvent::Message(_)
        ));
    }

    #[test]
    fn outbound_dm_echo_never_uses_current_bot_as_recipient() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "id":"9999",
                "channel_id":"777",
                "content":"hello",
                "author":{"id":"100","username":"diddy","global_name":"Diddy","avatar":"self"}
            }"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_message_discovery(&sender, raw, Some("100"));

        let event = receiver.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::DirectChannelUpdate(channel) => {
                assert_eq!(channel.id.as_ref(), "777");
                assert_eq!(channel.recipient_id, None);
                assert_eq!(channel.recipient_username, None);
                assert_eq!(channel.recipient_global_name, None);
                assert_eq!(channel.recipient_avatar_hash, None);
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn unknown_self_identity_keeps_dm_navigable_without_guessing_recipient() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "id":"9999",
                "channel_id":"777",
                "author":{"id":"42","username":"alice"}
            }"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_message_discovery(&sender, raw, None);

        let event = receiver.try_recv().unwrap();
        match event.as_ref() {
            FrontendEvent::DirectChannelUpdate(channel) => {
                assert_eq!(channel.id.as_ref(), "777");
                assert_eq!(channel.display_name(), None);
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn guild_message_is_not_direct_message_discovery() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "id":"9999",
                "channel_id":"777",
                "guild_id":"555",
                "author":{"id":"42","username":"alice"}
            }"#,
        )
        .unwrap();
        let (sender, mut receiver) = broadcast::channel(8);

        emit_direct_message_discovery(&sender, raw, Some("100"));

        assert!(receiver.try_recv().is_err());
    }
}
