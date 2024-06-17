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

    loop {
        eldegoss.send(vec![0; 1024]).await.ok();
    }
}
