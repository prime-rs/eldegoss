use std::{sync::Arc, time::Duration};

use color_eyre::Result;

use common_x::{signal::shutdown_signal, tls::create_any_server_name_config};
use quinn::{ClientConfig, Endpoint, TransportConfig};
use tokio::select;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 30)]
async fn main() -> Result<()> {
    common_x::log::init_log_filter("info");

    let client_crypto = create_any_server_name_config("./config/cert/ca_cert.pem")?;

    let mut client_config = ClientConfig::new(Arc::new(client_crypto));
    let mut transport_config = TransportConfig::default();
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = Endpoint::client("[::]:0".parse::<std::net::SocketAddr>()?)?;
    endpoint.set_default_client_config(client_config.clone());

    let conn = endpoint
        .connect(
            "127.0.0.1:4722".parse::<std::net::SocketAddr>().unwrap(),
            "localhost",
        )?
        .await?;

    let (_, mut rv) = conn.accept_bi().await?;

    let mut stats = eldegoss::util::Stats::new(10000);

    let bytes = &mut vec![0_u8; 1028];
    loop {
        select! {
            _ = rv.read_exact(bytes) => {
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
