use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentUser {
    pub id: Box<str>,
    pub username: Box<str>,
    pub global_name: Option<Box<str>>,
    pub discriminator: Option<Box<str>>,
    pub avatar_hash: Option<Box<str>>,
    pub bot: bool,
}

impl CurrentUser {
    pub fn display_name(&self) -> &str {
        self.global_name.as_deref().unwrap_or(self.username.as_ref())
    }

    pub(super) fn from_ready(raw: &RawValue) -> Option<Self> {
        let ready = serde_json::from_str::<ReadyIdentity<'_>>(raw.get()).ok()?;
        Self::from_borrowed(ready.user)
    }

    pub(super) fn from_user_update(raw: &RawValue) -> Option<Self> {
        let user = serde_json::from_str::<BorrowedUser<'_>>(raw.get()).ok()?;
        Self::from_borrowed(user)
    }

    fn from_borrowed(user: BorrowedUser<'_>) -> Option<Self> {
        if !is_snowflake(user.id) || user.username.is_empty() {
            return None;
        }

        Some(Self {
            id: Box::<str>::from(user.id),
            username: Box::<str>::from(user.username),
            global_name: user.global_name.map(Box::<str>::from),
            discriminator: user.discriminator.map(Box::<str>::from),
            avatar_hash: user.avatar.map(Box::<str>::from),
            bot: user.bot,
        })
    }
}

#[derive(Deserialize)]
struct ReadyIdentity<'a> {
    #[serde(borrow)]
    user: BorrowedUser<'a>,
}

#[derive(Deserialize)]
struct BorrowedUser<'a> {
    #[serde(borrow)]
    id: &'a str,
    #[serde(borrow)]
    username: &'a str,
    #[serde(default, borrow)]
    global_name: Option<&'a str>,
    #[serde(default, borrow)]
    discriminator: Option<&'a str>,
    #[serde(default, borrow)]
    avatar: Option<&'a str>,
    #[serde(default)]
    bot: bool,
}

fn is_snowflake(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_identity_parser_ignores_unrelated_payload_fields() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "v":9,
                "session_id":"session",
                "resume_gateway_url":"wss://gateway.discord.gg",
                "user":{
                    "id":"123456789",
                    "username":"diddy",
                    "global_name":"Diddy Bot",
                    "discriminator":"0",
                    "avatar":"abc123",
                    "bot":true,
                    "public_flags":65536,
                    "flags":65536
                },
                "guilds":[{"id":"1","unavailable":true}],
                "application":{"id":"999","flags":0}
            }"#,
        )
        .unwrap();

        let user = CurrentUser::from_ready(raw).unwrap();
        assert_eq!(user.id.as_ref(), "123456789");
        assert_eq!(user.username.as_ref(), "diddy");
        assert_eq!(user.display_name(), "Diddy Bot");
        assert_eq!(user.discriminator.as_deref(), Some("0"));
        assert_eq!(user.avatar_hash.as_deref(), Some("abc123"));
        assert!(user.bot);
    }

    #[test]
    fn user_update_supports_nullable_profile_fields() {
        let raw: &RawValue = serde_json::from_str(
            r#"{
                "id":"123456789",
                "username":"renamed",
                "global_name":null,
                "discriminator":"0",
                "avatar":null,
                "bot":true,
                "email":null,
                "verified":true
            }"#,
        )
        .unwrap();

        let user = CurrentUser::from_user_update(raw).unwrap();
        assert_eq!(user.display_name(), "renamed");
        assert!(user.global_name.is_none());
        assert!(user.avatar_hash.is_none());
    }

    #[test]
    fn malformed_identity_is_rejected_without_allocation_state() {
        let raw: &RawValue = serde_json::from_str(
            r#"{"id":"not-a-snowflake","username":"diddy","bot":true}"#,
        )
        .unwrap();

        assert!(CurrentUser::from_user_update(raw).is_none());
    }
}
