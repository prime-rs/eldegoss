use eldegoss::{
    protocol::{decode_msg, encode_msg, Message},
    util::Stats,
};
use tracing::info;

fn main() {
    common_x::log::init_log_filter("info");
    let msg = Message::msg(0, "topic".to_owned(), vec![0; 1024]);
    let bytes = encode_msg(&msg);
    info!("bytes len: {:?}", bytes.len());
    let mut stats = Stats::new(1000);
    for _ in 0..10000 {
        decode_msg(&bytes).ok();
        stats.increment();
    }
}
