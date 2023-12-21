use std::future::Future;
use std::time::Instant;

use clap::Parser;
use color_eyre::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use quinn::{RecvStream, SendStream};

use crate::{protocol::Message, session::id_u128};

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
pub async fn read_msg(recv: &mut RecvStream) -> Result<Message> {
    let mut length = [0_u8, 0_u8, 0_u8, 0_u8];
    recv.read_exact(&mut length).await?;
    let n = u32::from_le_bytes(length) as usize;
    if n == 0 {
        warn!("read 0 bytes");
        return Ok(Message::None);
    }
    let bytes = &mut vec![0_u8; n];
    recv.read_exact(bytes).await?;
    Message::decode(bytes)
}

#[inline]
pub async fn write_msg(send: &mut SendStream, mut msg: Message) -> Result<()> {
    msg.set_origin(id_u128());
    let msg_bytes = msg.encode();
    let len_bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
    let bytes = [len_bytes, msg_bytes].concat();
    send.write_all(&bytes).await?;
    Ok(())
}

pub async fn select_ok<F, A, B>(futs: impl IntoIterator<Item = F>) -> Result<A, B>
where
    F: Future<Output = Result<A, B>>,
{
    let mut futs: FuturesUnordered<F> = futs.into_iter().collect();

    let mut last_error: Option<B> = None;
    while let Some(next) = futs.next().await {
        match next {
            Ok(ok) => return Ok(ok),
            Err(err) => {
                last_error = Some(err);
            }
        }
    }
    Err(last_error.expect("Empty iterator."))
}
