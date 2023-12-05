use std::time::{Duration, Instant};

use eldegoss::{
    protocol::{Message, Msg},
    quic::Server,
    Config,
};
use tracing::info;

struct Stats {
    round_count: usize,
    round_size: usize,
    finished_rounds: usize,
    round_start: Instant,
    global_start: Option<Instant>,
}
impl Stats {
    fn new(round_size: usize) -> Self {
        Stats {
            round_count: 0,
            round_size,
            finished_rounds: 0,
            round_start: Instant::now(),
            global_start: None,
        }
    }
    fn increment(&mut self) {
        if self.round_count == 0 {
            self.round_start = Instant::now();
            if self.global_start.is_none() {
                self.global_start = Some(self.round_start)
            }
            self.round_count += 1;
        } else if self.round_count < self.round_size {
            self.round_count += 1;
        } else {
            self.print_round();
            self.finished_rounds += 1;
            self.round_count = 0;
        }
    }
    fn print_round(&self) {
        let elapsed = self.round_start.elapsed().as_secs_f64();
        let throughtput = (self.round_size as f64) / elapsed;
        info!("{throughtput} msg/s");
    }
}
impl Drop for Stats {
    fn drop(&mut self) {
        let elapsed = self.global_start.unwrap().elapsed().as_secs_f64();
        let total = self.round_size * self.finished_rounds + self.round_count;
        let throughtput = total as f64 / elapsed;
        info!("Received {total} messages over {elapsed:.2}s: {throughtput}msg/s");
    }
}

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
    // let mut send_test_msg_interval1 = tokio::time::interval(Duration::from_micros(20));
    server.serve().await;

    send_test_msg_interval.tick().await;
    let mut stats = Stats::new(10000);
    loop {
        // send_test_msg_interval1.tick().await;
        let msg = Message::Msg(Msg {
            origin: 0,
            from: 0,
            to: 0,
            topic: "topic".to_owned(),
            body: vec![0; 1024],
        });
        server.send_msg(msg);
        stats.increment();
    }
}
