pub mod gateway;
pub mod runtime;

pub use gateway::{
    FrontendEvent, FrontendMessage, GatewayConfig, NetworkBackbone, DEFAULT_INTENTS,
};
pub use runtime::{build_runtime, recommended_worker_threads};

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
