use std::{error::Error, io, sync::Arc, time::Duration};

use tokio::{sync::broadcast, time::sleep};

mod connection;
mod events;
mod protocol;
mod recovery;

pub use events::{FrontendEvent, FrontendMessage, FrontendMessageDelete, FrontendMessageUpdate};
use recovery::{reconnect_delay, AuthMode, ConnectionNext, SessionState};

pub(super) const GATEWAY_VERSION: u8 = 9;
pub(super) const INITIAL_GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=9&encoding=json";
pub(super) const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const MAX_GATEWAY_MESSAGE: usize = 16 * 1024 * 1024;
pub(super) const COOPERATIVE_YIELD_EVERY: u32 = 64;
pub(super) const IDENTIFY_MIN_DELAY: Duration = Duration::from_secs(5);

pub const DEFAULT_INTENTS: u64 = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);

pub(super) type BoxError = Box<dyn Error + Send + Sync + 'static>;

pub struct GatewayConfig {
    pub(super) token: Box<str>,
    pub(super) intents: u64,
}

impl GatewayConfig {
    pub fn new(token: impl Into<Box<str>>, intents: u64) -> Self {
        Self {
            token: token.into(),
            intents,
        }
    }

    pub fn intents(&self) -> u64 {
        self.intents
    }
}

pub struct NetworkBackbone {
    pub(super) config: GatewayConfig,
    pub(super) frontend: broadcast::Sender<Arc<FrontendEvent>>,
}

impl NetworkBackbone {
    pub fn new(config: GatewayConfig, frontend_capacity: usize) -> Self {
        let (frontend, _) = broadcast::channel(frontend_capacity.max(8));
        Self { config, frontend }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<FrontendEvent>> {
        self.frontend.subscribe()
    }

    pub async fn run(self) -> Result<(), BoxError> {
        let mut session = SessionState::default();
        let mut next_auth = AuthMode::Identify;
        let mut failed_attempts = 0u32;

        loop {
            let result = self.run_connection(&mut session, next_auth).await;
            let (next, stable) = match result {
                Ok(exit) => (exit.next, exit.stable),
                Err(_) => {
                    let next = if session.can_resume() {
                        ConnectionNext::Resume
                    } else {
                        ConnectionNext::Reidentify
                    };
                    (next, false)
                }
            };

            if stable {
                failed_attempts = 0;
            } else {
                failed_attempts = failed_attempts.saturating_add(1);
            }

            match next {
                ConnectionNext::Resume if session.can_resume() => {
                    next_auth = AuthMode::Resume;
                }
                ConnectionNext::Resume | ConnectionNext::Reidentify => {
                    session.clear();
                    next_auth = AuthMode::Identify;
                }
                ConnectionNext::Fatal(code) => {
                    return Err(io_error(format!(
                        "Discord Gateway closed with non-recoverable code {code}"
                    ))
                    .into());
                }
            }

            let delay = reconnect_delay(failed_attempts.saturating_sub(1), next_auth);
            sleep(delay).await;
        }
    }
}

pub(super) fn io_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message.into())
}
