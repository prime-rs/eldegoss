use std::{net::SocketAddr, sync::Arc, time::Duration};

use color_eyre::Result;

use common_x::cert::{read_certs, read_key};
use quinn::{Endpoint, ServerConfig, TransportConfig};

#[tokio::main(flavor = "multi_thread", worker_threads = 15)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");

    let mut server_config = ServerConfig::with_single_cert(
        read_certs(&"./config/cert/client_cert.pem".into())?,
        read_key(&"./config/cert/client_key.pem".into())?,
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

    loop {
        let msg_bytes = vec![0; 1024];
        let bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
        let bytes = [bytes, msg_bytes].concat();
        tx.write_all(&bytes).await?;
        stats.increment();
    }
}
