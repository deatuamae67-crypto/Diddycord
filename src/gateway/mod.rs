use std::{error::Error, io, sync::Arc, time::Duration};

use tokio::{
    sync::{broadcast, watch},
    time::sleep,
};

mod bulk;
mod connection;
mod events;
mod protocol;
mod recovery;

pub use events::{
    FrontendChannel, FrontendChannelChange, FrontendChannelDelete, FrontendEvent,
    FrontendGuildDelete, FrontendGuildSnapshot, FrontendGuildUpdate, FrontendMessage,
    FrontendMessageDelete, FrontendMessageUpdate, FrontendThread, FrontendThreadDelete,
    FrontendThreadListSync,
};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkStatus {
    Idle,
    Connecting { resume: bool },
    Identifying,
    Resuming,
    Ready,
    Reconnecting { attempt: u32, resume: bool },
    Stopped,
    Fatal { code: u16 },
}

#[derive(Clone)]
pub struct NetworkControl {
    shutdown: watch::Sender<bool>,
    status: watch::Receiver<NetworkStatus>,
}

impl NetworkControl {
    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub fn status(&self) -> NetworkStatus {
        *self.status.borrow()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<NetworkStatus> {
        self.status.clone()
    }
}

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
    shutdown: watch::Sender<bool>,
    status: watch::Sender<NetworkStatus>,
}

impl NetworkBackbone {
    pub fn new(config: GatewayConfig, frontend_capacity: usize) -> Self {
        let (frontend, _) = broadcast::channel(frontend_capacity.max(8));
        let (shutdown, _) = watch::channel(false);
        let (status, _) = watch::channel(NetworkStatus::Idle);
        Self {
            config,
            frontend,
            shutdown,
            status,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<FrontendEvent>> {
        self.frontend.subscribe()
    }

    pub fn control(&self) -> NetworkControl {
        NetworkControl {
            shutdown: self.shutdown.clone(),
            status: self.status.subscribe(),
        }
    }

    pub fn subscribe_status(&self) -> watch::Receiver<NetworkStatus> {
        self.status.subscribe()
    }

    pub async fn run(self) -> Result<(), BoxError> {
        let mut shutdown = self.shutdown.subscribe();
        if *shutdown.borrow() {
            self.set_status(NetworkStatus::Stopped);
            return Ok(());
        }

        let mut session = SessionState::default();
        let mut next_auth = AuthMode::Identify;
        let mut failed_attempts = 0u32;

        loop {
            let resume = next_auth == AuthMode::Resume && session.can_resume();
            self.set_status(NetworkStatus::Connecting { resume });

            let result = tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    self.set_status(NetworkStatus::Stopped);
                    return Ok(());
                }
                result = self.run_connection(&mut session, next_auth) => result,
            };

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
                    self.set_status(NetworkStatus::Fatal { code });
                    return Err(io_error(format!(
                        "Discord Gateway closed with non-recoverable code {code}"
                    ))
                    .into());
                }
            }

            let resume = next_auth == AuthMode::Resume && session.can_resume();
            self.set_status(NetworkStatus::Reconnecting {
                attempt: failed_attempts.max(1),
                resume,
            });

            let delay = reconnect_delay(failed_attempts.saturating_sub(1), next_auth);
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    self.set_status(NetworkStatus::Stopped);
                    return Ok(());
                }
                _ = sleep(delay) => {}
            }
        }
    }

    pub(super) fn set_status(&self, status: NetworkStatus) {
        self.status.send_replace(status);
    }
}

pub(super) fn io_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_starts_idle_and_can_be_cloned() {
        let backbone = NetworkBackbone::new(GatewayConfig::new("test-token", DEFAULT_INTENTS), 8);
        let control = backbone.control();
        let cloned = control.clone();

        assert_eq!(control.status(), NetworkStatus::Idle);
        assert_eq!(cloned.status(), NetworkStatus::Idle);
    }

    #[test]
    fn shutdown_before_run_avoids_network_io() {
        let backbone = NetworkBackbone::new(GatewayConfig::new("test-token", DEFAULT_INTENTS), 8);
        let control = backbone.control();
        control.shutdown();

        crate::runtime::build_runtime()
            .unwrap()
            .block_on(backbone.run())
            .unwrap();

        assert_eq!(control.status(), NetworkStatus::Stopped);
    }
}
