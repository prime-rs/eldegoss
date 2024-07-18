use std::time::Instant;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = "config/client.toml")]
    pub config: String,
}

#[derive(Debug)]
pub struct Stats {
    round_count: usize,
    round_size: usize,
    finished_rounds: usize,
    round_start: Instant,
    global_start: Option<Instant>,
}
impl Stats {
    pub fn new(round_size: usize) -> Self {
        Stats {
            round_count: 0,
            round_size,
            finished_rounds: 0,
            round_start: Instant::now(),
            global_start: None,
        }
    }
    pub fn increment(&mut self) {
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
        if let Some(global_start) = self.global_start {
            let elapsed = global_start.elapsed().as_secs_f64();
            let total = self.round_size * self.finished_rounds + self.round_count;
            let throughtput = total as f64 / elapsed;
            info!("Received {total} messages over {elapsed:.2}s: {throughtput}msg/s");
        }
    }
}

#[tokio::test]
async fn cert() {
    use common_x::{
        file::create_file,
        tls::{new_ca, new_end_entity},
    };
    // ca
    let (ca_cert, ca_key_pair) = new_ca();
    create_file("./config/cert/ca_cert.pem", ca_cert.pem().as_bytes())
        .await
        .unwrap();
    create_file(
        "./config/cert/ca_key.pem",
        ca_key_pair.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();

    // server cert
    let (server_cert, server_key) = new_end_entity("test-host", &ca_cert, &ca_key_pair);
    create_file(
        "./config/cert/server_cert.pem",
        server_cert.pem().as_bytes(),
    )
    .await
    .unwrap();
    create_file(
        "./config/cert/server_key.pem",
        server_key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();

    // client cert
    let (client_cert, client_key) = new_end_entity("client.test-host", &ca_cert, &ca_key_pair);
    create_file(
        "./config/cert/client_cert.pem",
        client_cert.pem().as_bytes(),
    )
    .await
    .unwrap();
    create_file(
        "./config/cert/client_key.pem",
        client_key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
}
