pub mod gateway;
pub mod history;
pub mod rest;
pub mod runtime;
pub mod state;
pub mod topology;

pub use gateway::{
    CurrentUser, FrontendChannel, FrontendChannelChange, FrontendChannelDelete, FrontendEvent,
    FrontendGuildDelete, FrontendGuildSnapshot, FrontendGuildUpdate, FrontendMessage,
    FrontendMessageDelete, FrontendMessageUpdate, FrontendThread, FrontendThreadDelete,
    FrontendThreadListSync, GatewayConfig, NetworkBackbone, NetworkControl, NetworkStatus,
    DEFAULT_INTENTS,
};
pub use history::{HistoryCompletion, HistoryLoadStatus, HistoryPager};
pub use rest::{
    RestBuildError, RestDispatcher, RestEvent, RestHandle, RestMessage, RestOperation,
    RestSubmitError,
};
pub use runtime::{build_runtime, recommended_worker_threads};
pub use state::{DrainReport, FrontendState};
pub use topology::{
    GuildRef, TopologyState, DEFAULT_MAX_CHANNELS_PER_GUILD, DEFAULT_MAX_GUILDS,
    DEFAULT_MAX_THREADS_PER_GUILD,
};

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
