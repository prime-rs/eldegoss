use eldegoss::{protocol::Message, quic::Server, Config};
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() {
    common_x::log::init_log_filter("debug");

    let config = Config {
        id: 2,
        connect: ["127.0.0.1:4721".to_string()].to_vec(),
        listen: "[::]:4722".to_string(),
        cert_path: "./config/cert/client_cert.pem".into(),
        private_key_path: "./config/cert/client_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        subscription_list: vec!["topic".to_owned()],
        ..Default::default()
    };
    info!("id: {}", config.id);
    let server = Server::init(config);

    let mut send_test_msg_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    server.serve().await;

    // let mut stats = eldegoss::util::Stats::new(10000);
    loop {
        send_test_msg_interval.tick().await;
        let msg = Message::msg(0, "topic".to_owned(), vec![]);
        server.send_msg(msg).await;
        // stats.increment();
    }
}
