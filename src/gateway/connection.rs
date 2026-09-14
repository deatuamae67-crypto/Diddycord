use futures_util::StreamExt;
use tokio::time::{sleep, timeout, timeout_at, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

use super::{
    events::{emit, emit_message},
    protocol::{receive_hello, send_heartbeat, send_identify, send_resume, GatewayEnvelope, ReadyData},
    recovery::{
        classify_close, gateway_url, heartbeat_jitter, invalid_session_delay, resume_or_reidentify,
        AuthMode, ConnectionExit, ConnectionNext, SessionState,
    },
    BoxError, NetworkBackbone, CONNECT_TIMEOUT, COOPERATIVE_YIELD_EVERY, INITIAL_GATEWAY_URL,
    MAX_GATEWAY_MESSAGE,
};
use crate::gateway::events::FrontendEvent;
use crate::gateway::io_error;

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

        match auth {
            AuthMode::Identify => {
                send_identify(
                    &mut socket,
                    self.config.token.as_ref(),
                    self.config.intents,
                )
                .await?;
            }
            AuthMode::Resume => {
                let session_id = session
                    .session_id
                    .as_deref()
                    .ok_or_else(|| io_error("session ID missing for resume"))?;
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
                                            session.resume_gateway_url = Some(Box::<str>::from(
                                                ready.resume_gateway_url,
                                            ));
                                            emit(&self.frontend, FrontendEvent::GatewayReady);
                                        }
                                    }
                                }
                                Some("RESUMED") => {
                                    emit(&self.frontend, FrontendEvent::GatewayResumed);
                                }
                                Some("MESSAGE_CREATE") => {
                                    if self.frontend.receiver_count() != 0 {
                                        if let Some(raw) = envelope.d {
                                            emit_message(&self.frontend, raw);
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
