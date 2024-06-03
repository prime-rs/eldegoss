use std::{net::SocketAddr, sync::Arc, time::Duration};

use color_eyre::Result;

use common_x::{
    signal::shutdown_signal,
    tls::{read_certs, read_key},
};
use eldegoss::protocol::Message;
use quinn::{Endpoint, ServerConfig, TransportConfig};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");

    let mut server_config = ServerConfig::with_single_cert(
        read_certs("./config/cert/client_cert.pem")?,
        read_key("./config/cert/client_key.pem")?,
    )?;
    let mut transport_config = TransportConfig::default();
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    transport_config.max_concurrent_uni_streams(0_u8.into());
    transport_config.max_concurrent_bidi_streams(1_u8.into());
    server_config.transport_config(Arc::new(transport_config));
    let addr = "[::]:4722".parse::<SocketAddr>()?;
    let endpoint = Endpoint::server(server_config, addr)?;

    let connecting = endpoint.accept().await.unwrap();
    let conn = connecting.await?;

    let (mut tx, _) = conn.open_bi().await?;

    let mut stats = eldegoss::util::Stats::new(10000);

    let msg = Message::push("topic", vec![0; 1024]);
    let msg_bytes = msg.sample().encode();
    info!("bytes len: {:?}", msg_bytes.len());
    let bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
    let bytes = [bytes, msg_bytes].concat();

    loop {
        select! {
            _ = tx.write_all(&bytes) => {
                stats.increment();
            }
            _ = conn.closed() => {
                if let Some(reason) = conn.close_reason() {
                    info!("connection closed, reason: {:?}", reason);
                }
                break;
            }
            _ = shutdown_signal() => {
                conn.close(0_u32.into(), b"shutdown");
                info!("shutdown");
                break;
            }
        }
    }
    Ok(())
}
