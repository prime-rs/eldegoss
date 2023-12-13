use common_x::signal::shutdown_signal;
use eldegoss::{server::Server, Config};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 31)]
async fn main() {
    common_x::log::init_log_filter("debug,quinn_udp=info");

    let config = Config {
        id: 1,
        connect: ["127.0.0.1:4723".to_string()].to_vec(),
        listen: "[::]:4721".to_string(),
        cert_path: "./config/cert/server_cert.pem".into(),
        private_key_path: "./config/cert/server_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        subscription_list: vec!["topic".to_owned()],
        ..Default::default()
    };
    info!("id: {}", config.id);

    let server = Server::serve(config).await;

    let mut stats = eldegoss::util::Stats::new(10000);
    loop {
        select! {
            Ok(msg) = server.recv_msg() => {
                info!("recv msg: {} - {}", msg.origin(), msg.topic());
                stats.increment();
            }
            _ = shutdown_signal() => {
                info!("shutdown");
                break;
            }
        }
    }
}
