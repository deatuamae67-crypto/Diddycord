use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::{
    net::TcpStream,
    time::{timeout, timeout_at, Instant},
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

use super::{io_error, BoxError, HELLO_TIMEOUT, WRITE_TIMEOUT};

pub(super) type GatewaySocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize)]
pub(super) struct GatewayEnvelope<'a> {
    pub(super) op: u8,
    #[serde(default)]
    pub(super) s: Option<u64>,
    #[serde(default, borrow)]
    pub(super) t: Option<&'a str>,
    #[serde(default, borrow)]
    pub(super) d: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct HelloData {
    heartbeat_interval: u64,
}

#[derive(Deserialize)]
pub(super) struct ReadyData<'a> {
    #[serde(borrow)]
    pub(super) session_id: &'a str,
    #[serde(borrow)]
    pub(super) resume_gateway_url: &'a str,
}

#[derive(Serialize)]
struct IdentifyPayload<'a> {
    op: u8,
    d: IdentifyData<'a>,
}

#[derive(Serialize)]
struct IdentifyData<'a> {
    token: &'a str,
    intents: u64,
    properties: IdentifyProperties<'a>,
}

#[derive(Serialize)]
struct IdentifyProperties<'a> {
    os: &'a str,
    browser: &'a str,
    device: &'a str,
}

#[derive(Serialize)]
struct ResumePayload<'a> {
    op: u8,
    d: ResumeData<'a>,
}

#[derive(Serialize)]
struct ResumeData<'a> {
    token: &'a str,
    session_id: &'a str,
    seq: u64,
}

#[derive(Serialize)]
struct HeartbeatPayload {
    op: u8,
    d: Option<u64>,
}

pub(super) async fn receive_hello(socket: &mut GatewaySocket) -> Result<Duration, BoxError> {
    let deadline = Instant::now() + HELLO_TIMEOUT;

    loop {
        let frame = timeout_at(deadline, socket.next())
            .await
            .map_err(|_| io_error("timed out waiting for Discord HELLO"))?
            .ok_or_else(|| io_error("Discord Gateway closed before HELLO"))??;

        match frame {
            Message::Text(text) => {
                let envelope: GatewayEnvelope<'_> = serde_json::from_str(text.as_str())?;
                if envelope.op != 10 {
                    continue;
                }

                let raw = envelope
                    .d
                    .ok_or_else(|| io_error("HELLO payload did not contain d"))?;
                let hello: HelloData = serde_json::from_str(raw.get())?;

                if !(1_000..=300_000).contains(&hello.heartbeat_interval) {
                    return Err(io_error("Discord returned an invalid heartbeat interval").into());
                }

                return Ok(Duration::from_millis(hello.heartbeat_interval));
            }
            Message::Close(_) => {
                return Err(io_error("Discord Gateway closed before HELLO").into())
            }
            Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
        }
    }
}

pub(super) async fn send_identify(
    socket: &mut GatewaySocket,
    token: &str,
    intents: u64,
) -> Result<(), BoxError> {
    let payload = IdentifyPayload {
        op: 2,
        d: IdentifyData {
            token,
            intents,
            properties: IdentifyProperties {
                os: std::env::consts::OS,
                browser: "diddycord",
                device: "diddycord",
            },
        },
    };
    send_json(socket, &payload).await
}

pub(super) async fn send_resume(
    socket: &mut GatewaySocket,
    token: &str,
    session_id: &str,
    seq: u64,
) -> Result<(), BoxError> {
    let payload = ResumePayload {
        op: 6,
        d: ResumeData {
            token,
            session_id,
            seq,
        },
    };
    send_json(socket, &payload).await
}

pub(super) async fn send_heartbeat(
    socket: &mut GatewaySocket,
    seq: Option<u64>,
) -> Result<(), BoxError> {
    send_json(socket, &HeartbeatPayload { op: 1, d: seq }).await
}

async fn send_json<T: Serialize>(socket: &mut GatewaySocket, payload: &T) -> Result<(), BoxError> {
    let json = serde_json::to_string(payload)?;
    timeout(WRITE_TIMEOUT, socket.send(Message::text(json)))
        .await
        .map_err(|_| io_error("Discord Gateway write timed out"))??;
    Ok(())
}
