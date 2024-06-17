use clap::Parser;
use color_eyre::Result;
use eldegoss::{config::Config, eldegoss::Eldegoss, util::Args};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info,quinn=info");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;
    info!("Loaded config {config:?}");

    let eldegoss = Eldegoss::serve(config).await?;

    let mut stats = eldegoss::util::Stats::new(100000);
    while let Ok(_msg) = eldegoss.recv().await {
        stats.increment();
    }
    Ok(())
}
