use eldegoss::{quic::Server, Config};
use tracing::info;

#[tokio::main]
async fn main() {
    common_x::log::init_log_filter("info");

    let config = Config {
        id: 1,
        listen: "[::]:4721".to_string(),
        cert_path: "./config/cert/server_cert.pem".into(),
        private_key_path: "./config/cert/server_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        ..Default::default()
    };
    info!("id: {}", config.id);

    let server = Server::init(config);
    server.subscription_list.write().insert("topic".to_owned());

    server.serve().await;
    // let mut stats = eldegoss::util::Stats::new(10000);
    while (server.recv_msg().await).is_ok() {
        // stats.increment();
    }
}
