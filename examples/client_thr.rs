use clap::Parser;
use color_eyre::Result;
use eldegoss::{session::Session, util::Args, Config};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;

    info!("id: {:#?}", config);

    let session = Session::serve(config).await;

    let mut stats = eldegoss::util::Stats::new(10000);
    loop {
        let _ = session.recv_msg().await;
        stats.increment();
    }
}
