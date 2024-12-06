use clap::Parser;
use color_eyre::Result;
use common_x::{log::LogConfig, signal::waiting_for_shutdown};
use eldegoss::{config::Config as EldegossConfig, eldegoss::Eldegoss, util::Args};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub eldegoss_config: EldegossConfig,

    pub log_config: LogConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;

    common_x::log::init_log(config.log_config);

    info!("Loaded config {:?}", config.eldegoss_config);

    let eldegoss = Eldegoss::serve(config.eldegoss_config)
        .await
        .map_err(|err| {
            error!("{err:?}");
            err
        })?;

    loop {
        tokio::select! {
            Ok(msg) = eldegoss.recv() => {
                debug!("Received {msg:?}");
            }
            _ = waiting_for_shutdown() => {
                break;
            }
        }
    }
    debug!("Shutdown");
    Ok(())
}
