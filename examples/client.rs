use clap::Parser;
use color_eyre::Result;
use common_x::signal::shutdown_signal;
use eldegoss::{session::Session, util::Args, Config};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;
    info!("id: {}", config.id);

    let session = Session::serve(config).await;

    loop {
        select! {
            Ok(msg) = session.recv_msg() => {
                info!("recv msg: {} - {}", msg.origin(), msg.topic());
            }
            _ = shutdown_signal() => {
                info!("shutdown");
                break;
            }
        }
    }
    Ok(())
}
