use eldegoss::{
    protocol::{decode_msg, encode_msg, Message},
    util::Stats,
};

fn main() {
    common_x::log::init_log_filter("info");
    let msg = Message::msg(0, "topic".to_owned(), vec![0; 1024]);
    let bytes = encode_msg(&msg);
    let mut stats = Stats::new(1000);
    for _ in 0..10000 {
        let _ = decode_msg(&bytes);
        stats.increment();
    }
}
