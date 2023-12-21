use clap::Parser;
use color_eyre::Result;
use eldegoss::{
    config::Config,
    protocol::Message,
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

    let (tx, rv) = flume::bounded(1024 * 1024);
    let callback = vec![Subscriber::new("topic", move |net_msg| {
        info!("net_msg: {:?}", net_msg);
    })];

    tokio::spawn(Session::serve(config, rv, callback));

    let mut stats = eldegoss::util::Stats::new(10000);
    loop {
        let msg = Message::put("topic", vec![0; 1024]);
        tx.send_async(msg).await.ok();
        stats.increment();
    }
}
