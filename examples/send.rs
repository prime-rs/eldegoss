use common_x::signal::shutdown_signal;
use eldegoss::{protocol::Message, session::Session, Config};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 15)]
async fn main() {
    common_x::log::init_log_filter("debug,quinn_udp=info");

    let config = Config {
        id: 2,
        listen: "[::]:4722".to_string(),
        cert_path: "./config/cert/client_cert.pem".into(),
        private_key_path: "./config/cert/client_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        subscription_list: vec!["topic".to_owned()],
        ..Default::default()
    };
    info!("id: {}", config.id);

    let session = Session::serve(config).await;

    let mut stats = eldegoss::util::Stats::new(10000);
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let mut count = 0;
    loop {
        select! {
            _ = interval.tick() => {
                let msg = Message::pub_msg("topic", vec![count]);
                session.send_msg(msg).await;
                stats.increment();
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
}
