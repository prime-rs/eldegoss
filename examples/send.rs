use clap::Parser;
use color_eyre::Result;
use common_x::signal::shutdown_signal;
use eldegoss::{config::Config, protocol::Message, session::Session, util::Args};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;
    info!("id: {}", config.id);

    let session = Session::serve(config).await;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let mut count = 0;
    loop {
        select! {
            _ = interval.tick() => {
                let msg = Message::pub_msg("topic", vec![count]);
                session.send_msg(msg).await;
                count += 1;
                if count == 100 {
                    count = 0;
                }
            }
            _ = shutdown_signal() => {
                info!("shutdown");
                break;
            }
        }
    }
    Ok(())
}
