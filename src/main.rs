#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
fn main() -> Result<(), eframe::Error> {
    diddycord::run_desktop_gui()
}

#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    use std::{env, io};

    use diddycord::{
        build_runtime, install_crypto_provider, GatewayConfig, NetworkBackbone, DEFAULT_INTENTS,
    };

    install_crypto_provider();
    let token = env::var("DISCORD_BOT_TOKEN")
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "DISCORD_BOT_TOKEN is not set"))?;
    let intents = env::var("DISCORD_INTENTS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTENTS);

    let runtime = build_runtime()?;
    let backbone = NetworkBackbone::new(GatewayConfig::new(token, intents), 256);
    runtime.block_on(backbone.run())
}
