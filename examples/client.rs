use eldegoss::{quic::Server, Config};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() {
    common_x::log::init_log_filter("debug,quinn_udp=info");

    let config = Config {
        connect: ["127.0.0.1:4722".to_string()].to_vec(),
        listen: "[::]:0".to_string(),
        cert_path: "./config/cert/client_cert.pem".into(),
        private_key_path: "./config/cert/client_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        subscription_list: vec!["topic".to_owned()],
        ..Default::default()
    };
    info!("id: {}", config.id);

    let server = Server::serve(config).await;

    let mut stats = eldegoss::util::Stats::new(1000);
    while let Ok(msg) = server.recv_msg().await {
        info!("recv msg: {msg:?}");
        stats.increment();
    }
}
