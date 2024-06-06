use clap::Parser;
use color_eyre::Result;
use common_x::signal::shutdown_signal;
use eldegoss::{
    config::Config,
    session::{Session, Subscriber},
    util::Args,
};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("debug");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;

    info!("config: {:#?}", config);

    let mut stats = eldegoss::util::Stats::new(100000);
    let callback = vec![Subscriber::new("topic", move |_msg| {
        stats.increment();
    })];

    let _session = Session::serve(config, callback).await;
    shutdown_signal().await;
    info!("shutdown");
    Ok(())
}
