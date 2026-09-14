use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{GATEWAY_VERSION, IDENTIFY_MIN_DELAY};

#[derive(Default)]
pub(super) struct SessionState {
    pub(super) session_id: Option<Box<str>>,
    pub(super) resume_gateway_url: Option<Box<str>>,
    pub(super) seq: Option<u64>,
}

impl SessionState {
    pub(super) fn can_resume(&self) -> bool {
        self.session_id.is_some() && self.resume_gateway_url.is_some() && self.seq.is_some()
    }

    pub(super) fn clear(&mut self) {
        self.session_id = None;
        self.resume_gateway_url = None;
        self.seq = None;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuthMode {
    Identify,
    Resume,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConnectionNext {
    Resume,
    Reidentify,
    Fatal(u16),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ConnectionExit {
    pub(super) next: ConnectionNext,
    pub(super) stable: bool,
}

pub(super) fn classify_close(code: u16, session: &SessionState) -> ConnectionNext {
    match code {
        4001 | 4002 | 4003 | 4004 | 4005 | 4010 | 4011 | 4012 | 4013 | 4014 => {
            ConnectionNext::Fatal(code)
        }
        4007 | 4009 => ConnectionNext::Reidentify,
        _ => resume_or_reidentify(session),
    }
}

pub(super) fn resume_or_reidentify(session: &SessionState) -> ConnectionNext {
    if session.can_resume() {
        ConnectionNext::Resume
    } else {
        ConnectionNext::Reidentify
    }
}

pub(super) fn gateway_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}v={GATEWAY_VERSION}&encoding=json")
}

pub(super) fn heartbeat_jitter(interval: Duration) -> Duration {
    let millionths = time_entropy() % 1_000_000;
    let interval_micros = interval.as_micros().min(u64::MAX as u128) as u64;
    Duration::from_micros(interval_micros.saturating_mul(millionths) / 1_000_000)
}

pub(super) fn invalid_session_delay() -> Duration {
    Duration::from_millis(1_000 + (time_entropy() % 4_001))
}

pub(super) fn reconnect_delay(attempt: u32, auth: AuthMode) -> Duration {
    let (base_secs, cap_secs) = match auth {
        AuthMode::Resume => (1u64, 30u64),
        AuthMode::Identify => (IDENTIFY_MIN_DELAY.as_secs(), 120u64),
    };
    let exponent = attempt.min(7);
    let delay_secs = base_secs.saturating_mul(1u64 << exponent).min(cap_secs);
    let jitter_ms = time_entropy() % 500;
    Duration::from_millis(delay_secs * 1_000 + jitter_ms)
}

fn time_entropy() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64 ^ duration.as_secs().rotate_left(17))
        .unwrap_or(0x9e37_79b9_7f4a_7c15)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resumable_session() -> SessionState {
        SessionState {
            session_id: Some(Box::from("session")),
            resume_gateway_url: Some(Box::from("wss://resume.discord.gg")),
            seq: Some(42),
        }
    }

    #[test]
    fn resume_url_gets_gateway_parameters() {
        assert_eq!(
            gateway_url("wss://resume.discord.gg/"),
            "wss://resume.discord.gg?v=9&encoding=json"
        );
        assert_eq!(
            gateway_url("wss://resume.discord.gg/?foo=bar"),
            "wss://resume.discord.gg/?foo=bar&v=9&encoding=json"
        );
    }

    #[test]
    fn close_codes_choose_safe_recovery_mode() {
        let resumable = resumable_session();
        assert_eq!(classify_close(1006, &resumable), ConnectionNext::Resume);
        assert_eq!(classify_close(4007, &resumable), ConnectionNext::Reidentify);
        assert_eq!(classify_close(4014, &resumable), ConnectionNext::Fatal(4014));

        let empty = SessionState::default();
        assert_eq!(classify_close(1006, &empty), ConnectionNext::Reidentify);
    }

    #[test]
    fn identify_backoff_never_drops_below_five_seconds() {
        let delay = reconnect_delay(0, AuthMode::Identify);
        assert!(delay >= IDENTIFY_MIN_DELAY);
    }
}
