use eldegoss::{protocol::Message, server::Server, Config};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 15)]
async fn main() {
    common_x::log::init_log_filter("info");

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

    let server = Server::serve(config).await;

    // let mut stats = eldegoss::util::Stats::new(10000);
    loop {
        let msg = Message::pub_msg("topic".to_string(), vec![0; 1024]);
        server.send_msg(msg).await;
        // stats.increment();
    }
}
