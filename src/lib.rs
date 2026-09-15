pub mod gateway;
pub mod history;
pub mod rest;
pub mod runtime;
pub mod state;
pub mod topology;

#[cfg(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "macos",
    target_os = "android"
))]
mod gui_app;

pub use gateway::{
    CurrentUser, DirectDrainReport, DirectTopologyState, FrontendChannel, FrontendChannelChange,
    FrontendChannelDelete, FrontendDirectChannel, FrontendDirectChannelDelete, FrontendEvent,
    FrontendGuildDelete, FrontendGuildSnapshot, FrontendGuildUpdate, FrontendMessage,
    FrontendMessageDelete, FrontendMessageUpdate, FrontendThread, FrontendThreadDelete,
    FrontendThreadListSync, GatewayConfig, NetworkBackbone, NetworkControl, NetworkStatus,
    DEFAULT_INTENTS, DEFAULT_MAX_DIRECT_CHANNELS,
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

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
pub fn run_desktop_gui() -> Result<(), eframe::Error> {
    gui_app::run_desktop()
}

#[cfg(target_os = "android")]
#[no_mangle]
pub fn android_main(app: winit::platform::android::activity::AndroidApp) {
    if let Err(error) = gui_app::run_android(app) {
        panic!("Diddycord Android GUI failed: {error}");
    }
}
