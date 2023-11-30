use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use color_eyre::Result;
use parking_lot::RwLock;
use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
};
use serde::{Deserialize, Serialize};

use common_x::cert::{read_ca, read_certs, read_key, WebPkiVerifierAnyServerName};
use tokio::{select, task::JoinHandle};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkMsg {
    pub origin: u64,
    pub to: u64,
    pub msg: Option<u64>,
}

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

type Membership = HashMap<u64, (Peer, JoinHandle<()>)>;

#[derive(Debug, Clone)]
pub struct Server {
    pub config: Config,
    pub tx: flume::Sender<NetworkMsg>,
    pub rx: flume::Receiver<NetworkMsg>,
    pub membership: Arc<RwLock<Membership>>,
}

impl Server {
    pub async fn serve(self) -> Result<()> {
        self.connect().await?;
        self.run_server().await
    }

    async fn connect(&self) -> Result<()> {
        let Config {
            ca_path,
            connect,
            keep_alive_interval,
            ..
        } = &self.config;

        let client_crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(WebPkiVerifierAnyServerName::new(read_ca(
                &ca_path.into(),
            )?)))
            .with_no_client_auth();

        let mut client_config = ClientConfig::new(Arc::new(client_crypto));
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        client_config.transport_config(Arc::new(transport_config));

        for connect in connect {
            let mut endpoint = Endpoint::client("[::]:0".parse::<std::net::SocketAddr>()?)?;
            endpoint.set_default_client_config(client_config.clone());
            let connection = endpoint
                .connect(connect.parse::<std::net::SocketAddr>()?, "localhost")?
                .await?;
            self.clone().handle_connection(connection).await;
        }
        Ok(())
    }

    async fn run_server(&self) -> Result<()> {
        let Config {
            keep_alive_interval,
            cert_path,
            listen,
            private_key_path,
            check_peer_interval,
            ..
        } = &self.config;
        let mut server_config = ServerConfig::with_single_cert(
            read_certs(&cert_path.into())?,
            read_key(&private_key_path.into())?,
        )?;
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        server_config.transport_config(Arc::new(transport_config));
        let addr = listen.parse::<SocketAddr>()?;
        let endpoint = Endpoint::server(server_config, addr)?;
        info!("listening on {}", endpoint.local_addr()?);
        let mut check_peer_interval =
            tokio::time::interval(Duration::from_secs(*check_peer_interval));
        loop {
            select! {
                Ok(msg) = self.rx.recv_async() => {
                    self.handle_out_msg(msg).await?;
                }
                Some(connecting) = endpoint.accept() => {
                    debug!("connection incoming");
                    let peer_network = self.clone();
                    tokio::spawn(async {
                        match connecting.await {
                            Ok(connection) => peer_network.handle_connection(connection).await,
                            Err(e) => error!("connecting failed: {:?}", e),
                        }
                    });
                }
                _ = check_peer_interval.tick() => self.check_membership().await
            }
        }
    }

    async fn handle_connection(self, connection: Connection) {
        let remote_address = connection.remote_address();

        let peer = Peer {
            id: connection.stable_id() as u64,
            connection,
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        };

        let peer_ = peer.clone();
        let handle = tokio::spawn(async move {
            select! {
                _ = peer_.read_datagrams() => {},
                _ = peer_.read_uni() => {},
                _ = peer_.read_bi() => {},
            }
        });

        self.membership
            .write()
            .insert(peer.id, (peer.clone(), handle));

        info!("new peer({}): {remote_address}", peer.id);
    }

    async fn handle_out_msg(&self, msg: NetworkMsg) -> Result<()> {
        debug!("send msg: {:?}", msg);
        if msg.to == 0 {
            let peers: Vec<Peer> = {
                let membership_reader = self.membership.read();
                membership_reader
                    .values()
                    .map(|(peer, _)| peer)
                    .cloned()
                    .collect()
            };
            for peer in peers {
                send_msg(&peer.connection, msg.clone()).await?;
            }
        } else {
            let connection = if let Some((peer, _)) = self.membership.read().get(&msg.origin) {
                peer.connection.clone()
            } else {
                return Ok(());
            };
            send_msg(&connection, msg).await?;
        }
        Ok(())
    }

    async fn check_membership(&self) {
        let mut remove_ids = vec![];
        self.membership.read().values().for_each(|(peer, _)| {
            if let Some(reason) = peer.connection.close_reason() {
                info!("peer({}) closed: {}", peer.id, reason);
                remove_ids.push(peer.id);
            }
        });
        for remove_id in remove_ids {
            if let Some((_, handle)) = self.membership.write().remove(&remove_id) {
                handle.abort();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: u64,
    pub connection: Connection,
    pub tx: flume::Sender<NetworkMsg>,
    pub rx: flume::Receiver<NetworkMsg>,
}

impl Peer {
    async fn read_bi(&self) {
        while let Ok(stream) = self.connection.accept_bi().await {
            tokio::spawn(write_msg(
                stream.0,
                NetworkMsg {
                    origin: self.id,
                    to: 0,
                    msg: Some(1),
                },
            ));
            tokio::spawn(handle_stream(self.id, stream.1, self.tx.clone()));
        }
    }

    async fn read_uni(&self) {
        while let Ok(stream) = self.connection.accept_uni().await {
            tokio::spawn(handle_stream(self.id, stream, self.tx.clone()));
        }
    }

    async fn read_datagrams(&self) {
        while let Ok(datagram) = self.connection.read_datagram().await {
            info!("recv dg: {}", String::from_utf8_lossy(&datagram));
        }
    }
}

pub async fn write_msg(mut send: SendStream, msg: NetworkMsg) -> Result<()> {
    send.write_all(&bincode::serialize(&msg)?).await?;
    send.finish().await?;
    Ok(())
}

pub async fn send_msg(connection: &Connection, msg: NetworkMsg) -> Result<()> {
    let mut send = connection.open_uni().await?;
    send.write_all(&bincode::serialize(&msg)?).await?;
    send.finish().await?;
    Ok(())
}

async fn handle_stream(
    peer_id: u64,
    mut recv: RecvStream,
    tx: flume::Sender<NetworkMsg>,
) -> Result<()> {
    let req = recv.read_to_end(1024 * 1024 * 16).await?;
    let mut msg = bincode::deserialize::<NetworkMsg>(&req)?;
    debug!("recv msg: {:?}", msg);
    msg.origin = peer_id;
    tx.send_async(msg).await?;
    Ok(())
}

#[tokio::test]
async fn test_server() {
    common_x::log::init_log_filter("debug");

    let config: Config = Config {
        listen: "[::]:4721".to_string(),
        cert_path: "./config/cert/server_cert.pem".into(),
        private_key_path: "./config/cert/server_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        ..Default::default()
    };
    let server = Server {
        config,
        tx: flume::unbounded().0,
        rx: flume::unbounded().1,
        membership: Arc::new(RwLock::new(HashMap::new())),
    };
    if let Err(e) = server.serve().await {
        error!("server failed: {e}");
    }
}

#[tokio::test]
async fn test_client() {
    common_x::log::init_log_filter("debug");

    let config: Config = Config {
        connect: ["127.0.0.1:4721".to_string()].to_vec(),
        listen: "[::]:4722".to_string(),
        cert_path: "./config/cert/client_cert.pem".into(),
        private_key_path: "./config/cert/client_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        ..Default::default()
    };
    let (tx, rx) = flume::unbounded();
    let network = Server {
        config,
        tx: flume::unbounded().0,
        rx,
        membership: Arc::new(RwLock::new(HashMap::new())),
    };

    let network_ = network.clone();
    let handle = Arc::new(RwLock::new(tokio::spawn(async move {
        if let Err(e) = network_.serve().await {
            error!("{e}");
        }
    })));
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    let mut check_first = true;
    loop {
        select! {
            _ = interval.tick() => {
                // send msg
                let msg = NetworkMsg {
                    origin: 0,
                    to: 0,
                    msg: Some(1),
                };
                tx.send(msg).unwrap();
                // reconnect
                {
                    if check_first {
                        check_first = false;
                        continue;
                    }
                    if network.membership.read().is_empty() {
                        info!("reconnect...");
                        handle.read().abort();
                        let network_ = network.clone();
                        *handle.write() = tokio::spawn(async move {
                            if let Err(e) = network_.serve().await {
                                error!("{e}");
                            }
                        });
                    }
                }
            }
        }
    }
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
