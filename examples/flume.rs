use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() {
    common_x::log::init_log_filter("info");

    let (tx, rv) = flume::unbounded::<u8>();

    let rv_ = rv.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            let msg = rv_.recv_async().await.unwrap();
            info!("1: {}", msg);
        }
    });
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            let msg = rv.recv_async().await.unwrap();
            info!("2: {}", msg);
        }
    });

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut index = 0;
    loop {
        interval.tick().await;
        tx.send_async(index).await.unwrap();
        index += 1;
    }
}
