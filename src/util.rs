use std::future::Future;
use std::time::Instant;

use color_eyre::{eyre::eyre, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use quinn::{RecvStream, SendStream};

use crate::{
    protocol::{decode_msg, encode_msg, Message},
    server::config,
};

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
        return Err(eyre!("read 0 bytes"));
    }
    let bytes = &mut vec![0_u8; n];
    recv.read_exact(bytes).await?;
    decode_msg(bytes)
}

#[inline]
pub async fn write_msg(send: &mut SendStream, mut msg: Message) -> Result<()> {
    msg.set_origin(config().id);
    let mut msg_bytes = encode_msg(&msg);
    let mut bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
    bytes.append(&mut msg_bytes);
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
