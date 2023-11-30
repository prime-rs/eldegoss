use serde::{Deserialize, Serialize};

#[macro_use]
extern crate tracing as logger;

pub mod quic;

pub type QuicSendStream = quinn::SendStream;
pub type QuicRecvStream = quinn::RecvStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    listen: String,
    connect: Vec<String>,

    ca_path: String,
    cert_path: String,
    private_key_path: String,

    keep_alive_interval: u64,
    check_peer_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ca_path: Default::default(),
            connect: Default::default(),
            listen: Default::default(),
            cert_path: Default::default(),
            private_key_path: Default::default(),
            keep_alive_interval: 5,
            check_peer_interval: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub origin: u64,
    pub to: u64,
    pub msg: Option<u64>,
}

#[tokio::test]
async fn cert() {
    use common_x::{
        cert::{ca_cert, create_csr, restore_ca_cert, sign_csr},
        file::{create_file, read_file_to_string},
    };
    // ca
    let (_, ca_cert_pem, ca_key_pem) = ca_cert();
    create_file("./config/cert/ca_cert.pem", ca_cert_pem.as_bytes())
        .await
        .unwrap();
    create_file("./config/cert/ca_key.pem", ca_key_pem.as_bytes())
        .await
        .unwrap();

    // server csr
    let (csr_pem, key_pem) = create_csr("test-host");
    create_file("./config/cert/server_csr.pem", csr_pem.as_bytes())
        .await
        .unwrap();
    create_file("./config/cert/server_key.pem", key_pem.as_bytes())
        .await
        .unwrap();
    // server sign
    let ca_cert_pem = read_file_to_string("./config/cert/ca_cert.pem")
        .await
        .unwrap();
    let ca_key_pem = read_file_to_string("./config/cert/ca_key.pem")
        .await
        .unwrap();
    let ca = restore_ca_cert(&ca_cert_pem, &ca_key_pem);
    let csr_pem = read_file_to_string("./config/cert/server_csr.pem")
        .await
        .unwrap();
    let cert_pem = sign_csr(&csr_pem, &ca);
    create_file("./config/cert/server_cert.pem", cert_pem.as_bytes())
        .await
        .unwrap();

    // client csr
    let (csr_pem, key_pem) = create_csr("client.test-host");
    create_file("./config/cert/client_csr.pem", csr_pem.as_bytes())
        .await
        .unwrap();
    create_file("./config/cert/client_key.pem", key_pem.as_bytes())
        .await
        .unwrap();
    // client sign
    let ca_cert_pem = read_file_to_string("./config/cert/ca_cert.pem")
        .await
        .unwrap();
    let ca_key_pem = read_file_to_string("./config/cert/ca_key.pem")
        .await
        .unwrap();
    let ca = restore_ca_cert(&ca_cert_pem, &ca_key_pem);
    let csr_pem = read_file_to_string("./config/cert/client_csr.pem")
        .await
        .unwrap();
    let cert_pem = sign_csr(&csr_pem, &ca);
    create_file("./config/cert/client_cert.pem", cert_pem.as_bytes())
        .await
        .unwrap();
}
