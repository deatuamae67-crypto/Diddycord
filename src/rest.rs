use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    redirect::Policy,
    Client, StatusCode,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc},
    time::{sleep_until, Instant},
};

const API_BASE: &str = "https://discord.com/api/v9";
const USER_AGENT_VALUE: &str = "Diddycord/0.1 (+https://github.com/deatuamae67-crypto/Diddycord)";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY: usize = 256 * 1024;
const MAX_HISTORY_RESPONSE_BODY: usize = 2 * 1024 * 1024;
const MAX_MESSAGE_CHARS: usize = 2_000;
const MAX_HISTORY_MESSAGES: u8 = 100;
const MAX_RATE_LIMIT_DELAY: Duration = Duration::from_secs(300);
const MAX_RATE_LIMIT_RETRIES: u8 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestOperation {
    SendMessage,
    EditMessage,
    DeleteMessage,
    FetchMessages,
}

#[derive(Debug)]
pub struct RestMessage {
    pub id: Box<str>,
    pub channel_id: Box<str>,
    pub author_username: Option<Box<str>>,
    pub content: Box<str>,
}

#[derive(Debug)]
pub enum RestEvent {
    MessageSent {
        request_id: u32,
        message: RestMessage,
    },
    MessageEdited {
        request_id: u32,
        message: RestMessage,
    },
    MessageDeleted {
        request_id: u32,
        channel_id: Box<str>,
        message_id: Box<str>,
    },
    MessagesFetched {
        request_id: u32,
        channel_id: Box<str>,
        messages: Vec<RestMessage>,
    },
    Failed {
        request_id: u32,
        operation: RestOperation,
        status: Option<u16>,
        retryable: bool,
        message: Box<str>,
    },
}

#[derive(Debug)]
pub enum RestBuildError {
    InvalidToken,
    Client(reqwest::Error),
}

impl fmt::Display for RestBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToken => f.write_str("Discord bot token cannot be used as an HTTP header"),
            Self::Client(_) => f.write_str("failed to build Discord REST HTTP client"),
        }
    }
}

impl Error for RestBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidToken => None,
            Self::Client(error) => Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestSubmitError {
    InvalidChannelId,
    InvalidMessageId,
    InvalidHistoryLimit,
    EmptyContent,
    ContentTooLong,
    QueueFull,
    Closed,
}

impl fmt::Display for RestSubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidChannelId => "channel ID must be a decimal Discord snowflake",
            Self::InvalidMessageId => "message ID must be a decimal Discord snowflake",
            Self::InvalidHistoryLimit => "history limit must be between 1 and 100 messages",
            Self::EmptyContent => "message content cannot be empty",
            Self::ContentTooLong => "message content exceeds the 2000-character limit",
            Self::QueueFull => "Discord REST command queue is full",
            Self::Closed => "Discord REST dispatcher is closed",
        };
        f.write_str(message)
    }
}

impl Error for RestSubmitError {}

#[derive(Clone)]
pub struct RestHandle {
    commands: mpsc::Sender<RestCommand>,
    events: broadcast::Sender<Arc<RestEvent>>,
    next_request_id: Arc<AtomicU32>,
}

pub struct RestDispatcher {
    client: Client,
    commands: mpsc::Receiver<RestCommand>,
    events: broadcast::Sender<Arc<RestEvent>>,
    rate_limit_until: Option<Instant>,
}

impl RestDispatcher {
    pub fn new(
        token: &str,
        command_capacity: usize,
        event_capacity: usize,
    ) -> Result<(RestHandle, Self), RestBuildError> {
        crate::install_crypto_provider();

        if token.is_empty() {
            return Err(RestBuildError::InvalidToken);
        }

        let mut authorization = HeaderValue::from_str(&format!("Bot {token}"))
            .map_err(|_| RestBuildError::InvalidToken)?;
        authorization.set_sensitive(true);

        let mut headers = HeaderMap::with_capacity(2);
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .default_headers(headers)
            .user_agent(USER_AGENT_VALUE)
            .redirect(Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(1)
            .tcp_nodelay(true)
            .build()
            .map_err(RestBuildError::Client)?;

        let (command_tx, command_rx) = mpsc::channel(command_capacity.max(4));
        let (event_tx, _) = broadcast::channel(event_capacity.max(8));

        let handle = RestHandle {
            commands: command_tx,
            events: event_tx.clone(),
            next_request_id: Arc::new(AtomicU32::new(1)),
        };

        Ok((
            handle,
            Self {
                client,
                commands: command_rx,
                events: event_tx,
                rate_limit_until: None,
            },
        ))
    }

    pub async fn run(mut self) {
        while let Some(command) = self.commands.recv().await {
            self.wait_for_rate_limit().await;
            let event = self.execute(&command).await;
            if self.events.receiver_count() != 0 {
                let _ = self.events.send(Arc::new(event));
            }
        }
    }

    async fn wait_for_rate_limit(&mut self) {
        let Some(until) = self.rate_limit_until.take() else {
            return;
        };
        if until > Instant::now() {
            sleep_until(until).await;
        }
    }

    async fn execute(&mut self, command: &RestCommand) -> RestEvent {
        let mut rate_limit_retries = 0u8;

        loop {
            let response = match self.send_once(command).await {
                Ok(response) => response,
                Err(RequestError::Network(error)) => {
                    return command.failed(
                        None,
                        error.is_timeout() || error.is_connect(),
                        "Discord REST network request failed",
                    )
                }
                Err(RequestError::Serialization) => {
                    return command.failed(None, false, "failed to serialize Discord REST request")
                }
                Err(RequestError::BodyTooLarge) => {
                    return command.failed(
                        None,
                        false,
                        "Discord REST response exceeded safety limit",
                    )
                }
            };

            if let Some(delay) = response.exhausted_reset {
                self.rate_limit_until = Some(Instant::now() + delay);
            }

            if response.status == StatusCode::TOO_MANY_REQUESTS {
                if rate_limit_retries >= MAX_RATE_LIMIT_RETRIES {
                    return command.failed(
                        Some(response.status.as_u16()),
                        true,
                        "Discord REST rate limit retry budget exhausted",
                    );
                }

                let delay = response
                    .retry_after
                    .or_else(|| retry_after_from_body(&response.body))
                    .unwrap_or(Duration::from_secs(1));
                self.rate_limit_until = Some(Instant::now() + delay);
                rate_limit_retries = rate_limit_retries.saturating_add(1);
                self.wait_for_rate_limit().await;
                continue;
            }

            if !response.status.is_success() {
                let retryable = response.status.is_server_error();
                let message = discord_error_message(&response.body)
                    .unwrap_or("Discord REST request was rejected");
                return command.failed(Some(response.status.as_u16()), retryable, message);
            }

            return command.success(response.body);
        }
    }

    async fn send_once(&self, command: &RestCommand) -> Result<HttpResponse, RequestError> {
        let builder = match &command.kind {
            RestCommandKind::SendMessage {
                channel_id,
                content,
            } => {
                let body = serde_json::to_vec(&MessageBody {
                    content: content.as_ref(),
                })
                .map_err(|_| RequestError::Serialization)?;
                self.client
                    .post(format!("{API_BASE}/channels/{channel_id}/messages"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(body)
            }
            RestCommandKind::EditMessage {
                channel_id,
                message_id,
                content,
            } => {
                let body = serde_json::to_vec(&MessageBody {
                    content: content.as_ref(),
                })
                .map_err(|_| RequestError::Serialization)?;
                self.client
                    .patch(format!(
                        "{API_BASE}/channels/{channel_id}/messages/{message_id}"
                    ))
                    .header(CONTENT_TYPE, "application/json")
                    .body(body)
            }
            RestCommandKind::DeleteMessage {
                channel_id,
                message_id,
            } => self.client.delete(format!(
                "{API_BASE}/channels/{channel_id}/messages/{message_id}"
            )),
            RestCommandKind::FetchMessages {
                channel_id,
                before,
                limit,
            } => self
                .client
                .get(history_url(channel_id.as_ref(), before.as_deref(), *limit)),
        };

        let response = builder.send().await.map_err(RequestError::Network)?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(parse_seconds_header);
        let exhausted_reset = if response
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|value| value.to_str().ok())
            == Some("0")
        {
            response
                .headers()
                .get("x-ratelimit-reset-after")
                .and_then(parse_seconds_header)
        } else {
            None
        };

        let body = read_limited_body(response, command.response_body_limit()).await?;

        Ok(HttpResponse {
            status,
            retry_after,
            exhausted_reset,
            body,
        })
    }
}

impl RestHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<RestEvent>> {
        self.events.subscribe()
    }

    pub fn try_send_message(
        &self,
        channel_id: &str,
        content: &str,
    ) -> Result<u32, RestSubmitError> {
        validate_channel_id(channel_id)?;
        validate_content(content)?;

        self.submit(RestCommandKind::SendMessage {
            channel_id: Box::<str>::from(channel_id),
            content: Box::<str>::from(content),
        })
    }

    pub fn try_edit_message(
        &self,
        channel_id: &str,
        message_id: &str,
        content: &str,
    ) -> Result<u32, RestSubmitError> {
        validate_channel_id(channel_id)?;
        validate_message_id(message_id)?;
        validate_content(content)?;

        self.submit(RestCommandKind::EditMessage {
            channel_id: Box::<str>::from(channel_id),
            message_id: Box::<str>::from(message_id),
            content: Box::<str>::from(content),
        })
    }

    pub fn try_delete_message(
        &self,
        channel_id: &str,
        message_id: &str,
    ) -> Result<u32, RestSubmitError> {
        validate_channel_id(channel_id)?;
        validate_message_id(message_id)?;

        self.submit(RestCommandKind::DeleteMessage {
            channel_id: Box::<str>::from(channel_id),
            message_id: Box::<str>::from(message_id),
        })
    }

    pub fn try_fetch_messages(
        &self,
        channel_id: &str,
        limit: u8,
    ) -> Result<u32, RestSubmitError> {
        self.submit_history(channel_id, None, limit)
    }

    pub fn try_fetch_messages_before(
        &self,
        channel_id: &str,
        before_message_id: &str,
        limit: u8,
    ) -> Result<u32, RestSubmitError> {
        validate_message_id(before_message_id)?;
        self.submit_history(
            channel_id,
            Some(Box::<str>::from(before_message_id)),
            limit,
        )
    }

    fn submit_history(
        &self,
        channel_id: &str,
        before: Option<Box<str>>,
        limit: u8,
    ) -> Result<u32, RestSubmitError> {
        validate_channel_id(channel_id)?;
        validate_history_limit(limit)?;

        self.submit(RestCommandKind::FetchMessages {
            channel_id: Box::<str>::from(channel_id),
            before,
            limit,
        })
    }

    fn submit(&self, kind: RestCommandKind) -> Result<u32, RestSubmitError> {
        let request_id = self.next_request_id();
        let command = RestCommand { request_id, kind };

        match self.commands.try_send(command) {
            Ok(()) => Ok(request_id),
            Err(mpsc::error::TrySendError::Full(_)) => Err(RestSubmitError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(RestSubmitError::Closed),
        }
    }

    fn next_request_id(&self) -> u32 {
        loop {
            let value = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            if value != 0 {
                return value;
            }
        }
    }
}

struct RestCommand {
    request_id: u32,
    kind: RestCommandKind,
}

impl RestCommand {
    fn operation(&self) -> RestOperation {
        match &self.kind {
            RestCommandKind::SendMessage { .. } => RestOperation::SendMessage,
            RestCommandKind::EditMessage { .. } => RestOperation::EditMessage,
            RestCommandKind::DeleteMessage { .. } => RestOperation::DeleteMessage,
            RestCommandKind::FetchMessages { .. } => RestOperation::FetchMessages,
        }
    }

    fn response_body_limit(&self) -> usize {
        match &self.kind {
            RestCommandKind::FetchMessages { .. } => MAX_HISTORY_RESPONSE_BODY,
            _ => MAX_RESPONSE_BODY,
        }
    }

    fn failed(&self, status: Option<u16>, retryable: bool, message: &str) -> RestEvent {
        RestEvent::Failed {
            request_id: self.request_id,
            operation: self.operation(),
            status,
            retryable,
            message: Box::<str>::from(message),
        }
    }

    fn success(&self, body: Vec<u8>) -> RestEvent {
        match &self.kind {
            RestCommandKind::SendMessage { .. } => match parse_rest_message(&body) {
                Some(message) => RestEvent::MessageSent {
                    request_id: self.request_id,
                    message,
                },
                None => self.failed(
                    Some(StatusCode::OK.as_u16()),
                    false,
                    "Discord returned an invalid message payload",
                ),
            },
            RestCommandKind::EditMessage { .. } => match parse_rest_message(&body) {
                Some(message) => RestEvent::MessageEdited {
                    request_id: self.request_id,
                    message,
                },
                None => self.failed(
                    Some(StatusCode::OK.as_u16()),
                    false,
                    "Discord returned an invalid message payload",
                ),
            },
            RestCommandKind::DeleteMessage {
                channel_id,
                message_id,
            } => RestEvent::MessageDeleted {
                request_id: self.request_id,
                channel_id: channel_id.clone(),
                message_id: message_id.clone(),
            },
            RestCommandKind::FetchMessages {
                channel_id, limit, ..
            } => match parse_rest_messages(&body, channel_id.as_ref(), usize::from(*limit)) {
                Some(messages) => RestEvent::MessagesFetched {
                    request_id: self.request_id,
                    channel_id: channel_id.clone(),
                    messages,
                },
                None => self.failed(
                    Some(StatusCode::OK.as_u16()),
                    false,
                    "Discord returned an invalid message history payload",
                ),
            },
        }
    }
}

enum RestCommandKind {
    SendMessage {
        channel_id: Box<str>,
        content: Box<str>,
    },
    EditMessage {
        channel_id: Box<str>,
        message_id: Box<str>,
        content: Box<str>,
    },
    DeleteMessage {
        channel_id: Box<str>,
        message_id: Box<str>,
    },
    FetchMessages {
        channel_id: Box<str>,
        before: Option<Box<str>>,
        limit: u8,
    },
}

#[derive(Serialize)]
struct MessageBody<'a> {
    content: &'a str,
}

#[derive(Deserialize)]
struct ApiMessage<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    channel_id: &'a str,
    #[serde(default, borrow)]
    content: Option<&'a str>,
    #[serde(default, borrow)]
    author: Option<ApiAuthor<'a>>,
}

#[derive(Deserialize)]
struct ApiAuthor<'a> {
    #[serde(default, borrow)]
    username: Option<&'a str>,
}

#[derive(Deserialize)]
struct ApiError<'a> {
    #[serde(default, borrow)]
    message: Option<&'a str>,
    #[serde(default)]
    retry_after: Option<f64>,
}

struct HttpResponse {
    status: StatusCode,
    retry_after: Option<Duration>,
    exhausted_reset: Option<Duration>,
    body: Vec<u8>,
}

enum RequestError {
    Network(reqwest::Error),
    Serialization,
    BodyTooLarge,
}

async fn read_limited_body(
    mut response: reqwest::Response,
    max_body: usize,
) -> Result<Vec<u8>, RequestError> {
    if response
        .content_length()
        .map(|length| length > max_body as u64)
        .unwrap_or(false)
    {
        return Err(RequestError::BodyTooLarge);
    }

    let initial_capacity = response.content_length().unwrap_or(0).min(max_body as u64) as usize;
    let mut body = Vec::with_capacity(initial_capacity);

    while let Some(chunk) = response.chunk().await.map_err(RequestError::Network)? {
        if body.len().saturating_add(chunk.len()) > max_body {
            return Err(RequestError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

fn history_url(channel_id: &str, before: Option<&str>, limit: u8) -> String {
    let mut url = format!("{API_BASE}/channels/{channel_id}/messages?limit={limit}");
    if let Some(before) = before {
        url.push_str("&before=");
        url.push_str(before);
    }
    url
}

fn parse_rest_message(body: &[u8]) -> Option<RestMessage> {
    let message = serde_json::from_slice::<ApiMessage<'_>>(body).ok()?;
    api_message_to_rest(message)
}

fn parse_rest_messages(
    body: &[u8],
    expected_channel_id: &str,
    limit: usize,
) -> Option<Vec<RestMessage>> {
    let messages = serde_json::from_slice::<Vec<ApiMessage<'_>>>(body).ok()?;
    if messages.len() > limit {
        return None;
    }

    let mut parsed = Vec::with_capacity(messages.len());
    for message in messages {
        if message.channel_id != expected_channel_id {
            return None;
        }
        parsed.push(api_message_to_rest(message)?);
    }
    Some(parsed)
}

fn api_message_to_rest(message: ApiMessage<'_>) -> Option<RestMessage> {
    if !is_snowflake(message.id) || !is_snowflake(message.channel_id) {
        return None;
    }

    Some(RestMessage {
        id: Box::<str>::from(message.id),
        channel_id: Box::<str>::from(message.channel_id),
        author_username: message
            .author
            .and_then(|author| author.username)
            .map(Box::<str>::from),
        content: Box::<str>::from(message.content.unwrap_or("")),
    })
}

fn discord_error_message(body: &[u8]) -> Option<&str> {
    serde_json::from_slice::<ApiError<'_>>(body)
        .ok()
        .and_then(|error| error.message)
}

fn retry_after_from_body(body: &[u8]) -> Option<Duration> {
    serde_json::from_slice::<ApiError<'_>>(body)
        .ok()
        .and_then(|error| error.retry_after)
        .and_then(seconds_to_duration)
}

fn parse_seconds_header(value: &HeaderValue) -> Option<Duration> {
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .and_then(seconds_to_duration)
}

fn seconds_to_duration(seconds: f64) -> Option<Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }

    let max_millis = MAX_RATE_LIMIT_DELAY.as_millis() as f64;
    let millis = (seconds * 1_000.0).ceil().max(1.0).min(max_millis) as u64;
    Some(Duration::from_millis(millis))
}

fn validate_channel_id(value: &str) -> Result<(), RestSubmitError> {
    if is_snowflake(value) {
        Ok(())
    } else {
        Err(RestSubmitError::InvalidChannelId)
    }
}

fn validate_message_id(value: &str) -> Result<(), RestSubmitError> {
    if is_snowflake(value) {
        Ok(())
    } else {
        Err(RestSubmitError::InvalidMessageId)
    }
}

fn validate_history_limit(limit: u8) -> Result<(), RestSubmitError> {
    if (1..=MAX_HISTORY_MESSAGES).contains(&limit) {
        Ok(())
    } else {
        Err(RestSubmitError::InvalidHistoryLimit)
    }
}

fn is_snowflake(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn validate_content(content: &str) -> Result<(), RestSubmitError> {
    if content.is_empty() {
        return Err(RestSubmitError::EmptyContent);
    }
    if content.chars().take(MAX_MESSAGE_CHARS + 1).count() > MAX_MESSAGE_CHARS {
        return Err(RestSubmitError::ContentTooLong);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_validation_rejects_path_injection() {
        assert!(is_snowflake("123456789012345678"));
        assert!(!is_snowflake(""));
        assert!(!is_snowflake("123/../../channels"));
        assert!(!is_snowflake("12a3"));
    }

    #[test]
    fn content_limit_counts_unicode_characters_not_utf8_bytes() {
        let content = "é".repeat(MAX_MESSAGE_CHARS);
        assert_eq!(validate_content(&content), Ok(()));

        let too_long = "é".repeat(MAX_MESSAGE_CHARS + 1);
        assert_eq!(
            validate_content(&too_long),
            Err(RestSubmitError::ContentTooLong)
        );
    }

    #[test]
    fn history_limit_is_strictly_bounded() {
        assert_eq!(validate_history_limit(1), Ok(()));
        assert_eq!(validate_history_limit(100), Ok(()));
        assert_eq!(
            validate_history_limit(0),
            Err(RestSubmitError::InvalidHistoryLimit)
        );
    }

    #[test]
    fn history_url_uses_only_validated_numeric_anchors() {
        assert_eq!(
            history_url("22", None, 25),
            "https://discord.com/api/v9/channels/22/messages?limit=25"
        );
        assert_eq!(
            history_url("22", Some("55"), 50),
            "https://discord.com/api/v9/channels/22/messages?limit=50&before=55"
        );
    }

    #[test]
    fn rate_limit_seconds_are_bounded_and_rounded_up() {
        assert_eq!(seconds_to_duration(0.001), Some(Duration::from_millis(1)));
        assert_eq!(
            seconds_to_duration(1.234),
            Some(Duration::from_millis(1234))
        );
        assert_eq!(seconds_to_duration(-1.0), None);
        assert_eq!(seconds_to_duration(f64::NAN), None);
        assert_eq!(seconds_to_duration(9999.0), Some(MAX_RATE_LIMIT_DELAY));
    }

    #[test]
    fn message_response_parser_ignores_unneeded_fields() {
        let body = br#"{
            "id":"55",
            "channel_id":"22",
            "content":"hello",
            "author":{"username":"diddy","id":"11","avatar":"x"},
            "attachments":[{"id":"1"}],
            "embeds":[{"title":"ignored"}],
            "flags":0
        }"#;

        let message = parse_rest_message(body).unwrap();
        assert_eq!(message.id.as_ref(), "55");
        assert_eq!(message.channel_id.as_ref(), "22");
        assert_eq!(message.author_username.as_deref(), Some("diddy"));
        assert_eq!(message.content.as_ref(), "hello");
    }

    #[test]
    fn history_response_parser_is_selective_and_bounded() {
        let body = br#"[
            {
                "id":"55",
                "channel_id":"22",
                "content":"newer",
                "author":{"username":"diddy","id":"11","avatar":"x"},
                "attachments":[{"id":"1","filename":"ignored.bin"}],
                "embeds":[{"title":"ignored"}]
            },
            {
                "id":"54",
                "channel_id":"22",
                "content":"older",
                "author":{"username":"other","id":"12"},
                "reactions":[{"count":99}]
            }
        ]"#;

        let messages = parse_rest_messages(body, "22", 2).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id.as_ref(), "55");
        assert_eq!(messages[0].content.as_ref(), "newer");
        assert_eq!(messages[1].author_username.as_deref(), Some("other"));
        assert!(parse_rest_messages(body, "22", 1).is_none());
        assert!(parse_rest_messages(body, "23", 2).is_none());
    }

    #[test]
    fn retry_after_body_is_parsed_without_generic_json_dom() {
        let body = br#"{"message":"rate limited","retry_after":0.125,"global":false}"#;
        assert_eq!(
            retry_after_from_body(body),
            Some(Duration::from_millis(125))
        );
    }
}
