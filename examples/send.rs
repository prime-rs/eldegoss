use std::time::Duration;

use eldegoss::{
    protocol::{Message, Msg},
    quic::Server,
    Config,
};
use tracing::info;

#[tokio::main]
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

    let mut send_test_msg_interval = tokio::time::interval(Duration::from_secs(1));
    server.serve().await;

    send_test_msg_interval.tick().await;
    // let mut stats = Stats::new(10000);
    loop {
        let msg = Message::Msg(Msg {
            origin: 0,
            from: 0,
            to: 0,
            topic: "topic".to_owned(),
            body: vec![0; 1024],
        });
        server.send_msg(msg);
        // stats.increment();
    }
}
