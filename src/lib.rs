#[macro_use]
extern crate tracing as logger;

pub mod quic;

pub type QuicSendStream = quinn::SendStream;
pub type QuicRecvStream = quinn::RecvStream;
