use std::time::Instant;

use clap::Parser;
use color_eyre::{eyre::eyre, Result};
use quinn::{RecvStream, SendStream};

use crate::protocol::Sample;

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

#[inline]
pub(crate) async fn read_msg(recv: &mut RecvStream) -> Result<Sample> {
    let mut length = [0_u8, 0_u8, 0_u8, 0_u8];
    recv.read_exact(&mut length).await?;
    let n = u32::from_le_bytes(length) as usize;
    if n == 0 {
        warn!("read 0 bytes");
        return Err(eyre!("read 0 bytes"));
    }
    let bytes = &mut vec![0_u8; n];
    recv.read_exact(bytes).await?;
    Sample::decode(bytes)
}

#[inline]
pub(crate) async fn write_msg(send: &mut SendStream, msg: Sample) -> Result<()> {
    let msg_bytes = msg.encode();
    let len_bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
    let bytes = [len_bytes, msg_bytes].concat();
    send.write_all(&bytes).await?;
    Ok(())
}
