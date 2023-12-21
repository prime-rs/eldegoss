use eldegoss::{protocol::Message, util::Stats};
use tracing::info;

fn main() {
    common_x::log::init_log_filter("info");
    let mut stats = Stats::new(1000);
    let msg = Message::msg(0, "topic".to_owned(), vec![0; 1024]);
    let bytes = msg.encode();
    info!("bytes len: {:?}", bytes.len());
    for _ in 0..10000 {
        msg.encode();
        stats.increment();
    }
}
