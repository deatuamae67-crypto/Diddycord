use std::{env, error::Error, io};

use diddycord::{build_runtime, install_crypto_provider, GatewayConfig, NetworkBackbone, DEFAULT_INTENTS};

type BoxError = Box<dyn Error + Send + Sync + 'static>;

fn io_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message.into())
}

fn main() -> Result<(), BoxError> {
    install_crypto_provider();

    let token = env::var("DISCORD_BOT_TOKEN")
        .map_err(|_| io_error("DISCORD_BOT_TOKEN is not set"))?;
    let intents = env::var("DISCORD_INTENTS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTENTS);

    let runtime = build_runtime()?;
    let backbone = NetworkBackbone::new(GatewayConfig::new(token, intents), 256);
    runtime.block_on(backbone.run())
}
