use clap::Parser;
use color_eyre::Result;
use eldegoss::{
    config::Config,
    session::{Session, Subscriber},
    util::Args,
};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;

    info!("config: {:#?}", config);

    let (_, rv) = flume::bounded(10240);

    let mut stats = eldegoss::util::Stats::new(100000);
    let callback = vec![Subscriber::new("topic", move |_msg| {
        stats.increment();
    })];

    Session::serve(config, rv, callback).await;
    Ok(())
}
