use clap::Parser;
use color_eyre::Result;
use common_x::signal::shutdown_signal;
use eldegoss::{
    config::Config,
    protocol::Message,
    session::{Session, Subscriber},
    util::Args,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("debug");
    let args = Args::parse();
    let config: Config = common_x::configure::file_config(&args.config)?;

    info!("config: {:#?}", config);

    let mut stats = eldegoss::util::Stats::new(100000);
    let callback = vec![Subscriber::new("topic", move |_net_msg| {
        stats.increment();
    })];

    // for graceful shutdown
    let session = Session::serve(config, callback).await;
    let sender = session.sender();

    let sender_handle = tokio::spawn(async move {
        loop {
            let msg = Message::push("topic", vec![0; 1024]);
            sender.send_async(msg).await.ok();
        }
    });
    shutdown_signal().await;
    sender_handle.abort();
    info!("shutdown");
    Ok(())
}
