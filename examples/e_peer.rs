use clap::Parser;
use color_eyre::Result;
use common_x::signal::shutdown_signal;
use eldegoss::{
    config::Config,
    protocol::Message,
    session::{Session, Subscriber},
    util::Args,
};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;
    info!("id: {}", config.id);

    let (tx, rv) = flume::bounded(1024 * 1024);
    let callback = vec![Subscriber::new("topic", move |net_msg| {
        info!("net_msg: {:?}", net_msg);
    })];

    Session::serve(config, rv, callback).await;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let mut count = 0;
    loop {
        select! {
            _ = interval.tick() => {
                let msg = Message::put("topic", vec![count]);
                tx.send_async(msg).await.ok();
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
