use eldegoss::{
    protocol::{encode_msg, Message},
    util::Stats,
};

fn main() {
    common_x::log::init_log_filter("info");
    let mut stats = Stats::new(1000);
    let msg = Message::msg(0, "topic".to_owned(), vec![0; 1024]);
    for _ in 0..10000 {
        let _ = encode_msg(&msg);
        stats.increment();
    }
}
