use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use color_eyre::Result;
use futures::{select, FutureExt, StreamExt};
use parking_lot::RwLock;
use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
};
use serde::{Deserialize, Serialize};

use common_x::cert::{read_ca, read_certs, read_key};

pub type MsgSendStream = SendStream;
pub type MsgRecvStream = RecvStream;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkMsg {
    pub origin: u64,
    pub to: String,
    pub msg: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Config {
    Client {
        ca_path: PathBuf,
        connect: String,
        server_name: String,
        keep_alive_interval: u64,
        check_peer_interval: u64,
    },
    Server {
        port: u16,
        cert_path: PathBuf,
        key_path: PathBuf,
        keep_alive_interval: u64,
        check_peer_interval: u64,
    },
}

impl Default for Config {
    fn default() -> Self {
        Self::Client {
            ca_path: Default::default(),
            connect: Default::default(),
            server_name: Default::default(),
            keep_alive_interval: 5,
            check_peer_interval: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Server {
    pub config: Config,
    pub tx: flume::Sender<NetworkMsg>,
    pub rx: flume::Receiver<NetworkMsg>,
    pub peers: Arc<RwLock<HashMap<u64, Peer>>>,
}

impl Server {
    pub async fn serve(self) -> Result<()> {
        let config = &self.config;
        match config {
            Config::Client {
                ca_path,
                connect,
                server_name,
                keep_alive_interval,
                check_peer_interval,
            } => {
                self.run_client(
                    ca_path,
                    keep_alive_interval,
                    connect,
                    server_name,
                    check_peer_interval,
                )
                .await
            }
            Config::Server {
                cert_path,
                key_path,
                port,
                keep_alive_interval,
                check_peer_interval,
            } => {
                self.run_server(
                    cert_path,
                    key_path,
                    port,
                    keep_alive_interval,
                    check_peer_interval,
                )
                .await
            }
        }
    }

    async fn run_client(
        &self,
        ca_path: &PathBuf,
        keep_alive_interval: &u64,
        connect: &str,
        server_name: &str,
        check_peer_interval: &u64,
    ) -> Result<()> {
        let mut endpoint = Endpoint::client("[::]:0".parse::<std::net::SocketAddr>()?)?;
        let mut client_config = ClientConfig::with_root_certificates(read_ca(ca_path)?);
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        client_config.transport_config(Arc::new(transport_config));
        endpoint.set_default_client_config(client_config);
        let connection = endpoint
            .connect(connect.parse::<std::net::SocketAddr>()?, server_name)?
            .await?;
        async_std::task::spawn(self.clone().handle_connection(connection));
        let mut check_peer_interval =
            async_std::stream::interval(Duration::from_secs(*check_peer_interval));
        loop {
            select! {
                _ = self.handle_out_msg().fuse() => {},
                _ = check_peer_interval.next().fuse() => {
                    self.check_peer();
                }
            }
        }
    }

    async fn run_server(
        &self,
        cert_path: &PathBuf,
        key_path: &PathBuf,
        port: &u16,
        keep_alive_interval: &u64,
        check_peer_interval: &u64,
    ) -> Result<()> {
        let mut server_config =
            ServerConfig::with_single_cert(read_certs(cert_path)?, read_key(key_path)?)?;
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        server_config.transport_config(Arc::new(transport_config));
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), *port);
        let endpoint = Endpoint::server(server_config, addr)?;
        info!("listening on {}", endpoint.local_addr()?);
        let mut check_peer_interval =
            async_std::stream::interval(Duration::from_secs(*check_peer_interval));
        loop {
            select! {
                _ = self.handle_out_msg().fuse() => {},
                _ = check_peer_interval.next().fuse() => {
                    self.check_peer();
                }
                connecting = endpoint.accept().fuse() => if let Some(connecting) = connecting {
                    debug!("connection incoming");
                    let peer_network = self.clone();
                    async_std::task::spawn(async move {
                        match connecting.await {
                            Ok(connection) => peer_network.handle_connection(connection).await,
                            Err(e) => error!("connecting failed: {:?}", e),
                        }
                    });
                }
            }
        }
    }

    async fn handle_connection(self, connection: Connection) {
        debug!("remote: {}", &connection.remote_address());

        let peer = Peer {
            id: connection.stable_id() as u64,
            connection,
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        };

        self.peers.write().insert(peer.id, peer.clone());

        info!("new peer id: {}", peer.id);

        select! {
            _ = peer.read_datagrams().fuse() => {},
            _ = peer.read_uni().fuse() => {},
            _ = peer.read_bi().fuse() => {},
        }
    }

    async fn handle_out_msg(&self) -> Result<()> {
        if let Ok(msg) = self.rx.recv_async().await {
            debug!("send msg: {:?}", msg);
            if msg.origin == 0 {
                let peers: Vec<Peer> = {
                    let peers_lock = self.peers.read();
                    peers_lock.values().cloned().collect()
                };
                for peer in peers {
                    send_msg(&peer.connection, msg.clone()).await?;
                }
            } else {
                let connection = if let Some(peer) = self.peers.read().get(&msg.origin) {
                    peer.connection.clone()
                } else {
                    return Ok(());
                };
                send_msg(&connection, msg).await?;
            }
        }
        Ok(())
    }

    fn check_peer(&self) {
        let mut remove_ids = vec![];
        self.peers.read().values().for_each(|peer| {
            if let Some(reason) = peer.connection.close_reason() {
                info!("peer({}) closed: {}", peer.id, reason);
                remove_ids.push(peer.id);
            }
        });
        remove_ids.iter().for_each(|id| {
            self.peers.write().remove(id);
        });
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
            // TODO
            // async_std::task::spawn(write_msg(
            //     stream.0,
            //     NetworkMsg::put(Some(self.id), Message::Ok),
            // ));
            async_std::task::spawn(handle_stream(self.id, stream.1, self.tx.clone()));
        }
    }

    async fn read_uni(&self) {
        while let Ok(stream) = self.connection.accept_uni().await {
            async_std::task::spawn(handle_stream(self.id, stream, self.tx.clone()));
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
    msg.origin = peer_id;
    tx.send_async(msg).await?;
    Ok(())
}

#[async_std::test]
async fn test_server() {
    common_x::log::init_log_filter("debug");

    let config: Config = Config::Server {
        port: 4721,
        cert_path: "../config/server_cert.pem".into(),
        key_path: "../config/server_key.pem".into(),
        keep_alive_interval: 5,
        check_peer_interval: 2,
    };
    let network = Server {
        config,
        tx: flume::unbounded().0,
        rx: flume::unbounded().1,
        peers: Arc::new(RwLock::new(HashMap::new())),
    };
    if let Err(e) = network.serve().await {
        error!("{e}");
    }
}

#[async_std::test]
async fn test_client() {
    common_x::log::init_log_filter("debug");

    let config: Config = Config::Client {
        ca_path: "../config/ca_cert.pem".into(),
        connect: "127.0.0.1:4721".to_string(),
        server_name: "test-host".to_string(),
        keep_alive_interval: 5,
        check_peer_interval: 2,
    };
    let network = Server {
        config,
        tx: flume::unbounded().0,
        rx: flume::unbounded().1,
        peers: Arc::new(RwLock::new(HashMap::new())),
    };
    if let Err(e) = network.serve().await {
        error!("{e}");
    }
}
