pub mod gateway;
pub mod rest;
pub mod runtime;
pub mod state;
pub mod topology;

pub use gateway::{
    FrontendChannel, FrontendChannelChange, FrontendChannelDelete, FrontendEvent,
    FrontendGuildDelete, FrontendGuildSnapshot, FrontendGuildUpdate, FrontendMessage,
    FrontendMessageDelete, FrontendMessageUpdate, GatewayConfig, NetworkBackbone, DEFAULT_INTENTS,
};
pub use rest::{
    RestBuildError, RestDispatcher, RestEvent, RestHandle, RestMessage, RestOperation,
    RestSubmitError,
};
pub use runtime::{build_runtime, recommended_worker_threads};
pub use state::{DrainReport, FrontendState};
pub use topology::{GuildRef, TopologyState, DEFAULT_MAX_CHANNELS_PER_GUILD, DEFAULT_MAX_GUILDS};

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
