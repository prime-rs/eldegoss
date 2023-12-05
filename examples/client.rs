use eldegoss::{quic::Server, Config};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() {
    common_x::log::init_log_filter("info");

    let config = Config {
        connect: ["127.0.0.1:4721".to_string()].to_vec(),
        listen: "[::]:0".to_string(),
        cert_path: "./config/cert/client_cert.pem".into(),
        private_key_path: "./config/cert/client_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        ..Default::default()
    };
    info!("id: {}", config.id);
    let server = Server::init(config);
    server.subscription_list.write().insert("topic".to_owned());

    server.serve().await;

    let mut stats = eldegoss::util::Stats::new(1000);
    while (server.recv_msg().await).is_ok() {
        stats.increment();
    }
}