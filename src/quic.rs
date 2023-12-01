use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use color_eyre::{eyre::eyre, Result};
use flume::{Receiver, Sender};
use parking_lot::RwLock;
use quinn::{
    ClientConfig, Connecting, Connection, Endpoint, RecvStream, SendStream, ServerConfig,
    TransportConfig,
};

use common_x::cert::{read_ca, read_certs, read_key, WebPkiVerifierAnyServerName};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tokio::select;

use crate::{Config, EldegossId, Member, Membership, Message, MessageBody};

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn init_config(config: Config) {
    CONFIG.set(config).unwrap();
}

pub fn config() -> &'static Config {
    CONFIG.get().unwrap()
}

#[derive(Debug, Clone)]
pub struct Neighbor {
    pub id: EldegossId,
    pub connection: Connection,
    pub tx: Sender<Message>,
}

impl Neighbor {
    async fn handle(self) {
        select! {
            _ = self.read_bi() => {}
            _ = self.read_uni() => {}
            _ = self.read_datagrams() => {}
        }
    }

    async fn read_bi(&self) {
        while let Ok((tx, rv)) = self.connection.accept_bi().await {
            tokio::spawn(write_msg(
                tx,
                Message::to(self.id.to_u128(), MessageBody::Ok),
            ));
            tokio::spawn(handle_stream(self.id, rv, self.tx.clone()));
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

#[derive(Debug, Clone)]
pub struct Server {
    pub tx_to_app: Sender<Message>,
    pub rx_from_app: Receiver<Message>,
    pub tx_to_neighbor: Sender<Message>,
    pub rx_from_neighbor: Receiver<Message>,
    pub neighbors: Arc<RwLock<HashMap<EldegossId, Neighbor>>>,
    pub membership: Arc<RwLock<Membership>>,
    pub subscription_list: Arc<RwLock<HashSet<String>>>,
}

impl Server {
    pub fn new(tx: Sender<Message>, rx: Receiver<Message>) -> Self {
        let member = Member::new(config().id.into());
        let mut membership = Membership::default();
        membership.add_member(member);
        let (tx_to_neighbor, rx_from_neighbor) = flume::unbounded();
        Self {
            tx_to_app: tx,
            rx_from_app: rx,
            neighbors: Arc::new(RwLock::new(HashMap::new())),
            membership: Arc::new(RwLock::new(membership)),
            subscription_list: Arc::new(RwLock::new(HashSet::new())),
            tx_to_neighbor,
            rx_from_neighbor,
        }
    }

    pub async fn serve(self) -> Result<()> {
        self.connect().await?;
        self.run_server().await?;
        self.swim().await?;
        while let Ok(msg) = self.rx_from_neighbor.recv_async().await {
            self.dispatch(msg, true).await?;
        }
        Ok(())
    }

    async fn swim(&self) -> Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config().check_neighbor_interval));
        loop {
            interval.tick().await;
            let msg = Message::to(0, MessageBody::Membership(self.membership.read().clone()));
            self.tx_to_app.send_async(msg).await?;
        }
    }

    async fn connect(&self) -> Result<()> {
        let Config {
            ca_path,
            connect,
            keep_alive_interval,
            ..
        } = &config();

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
            if let Err(e) = self.clone().join(connection).await {
                error!("join failed: {:?}", e);
            }
        }
        Ok(())
    }

    async fn run_server(&self) -> Result<()> {
        let Config {
            keep_alive_interval,
            cert_path,
            listen,
            private_key_path,
            check_neighbor_interval,
            ..
        } = &config();
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
        let mut check_neighbor_interval =
            tokio::time::interval(Duration::from_secs(*check_neighbor_interval));
        loop {
            select! {
                Ok(msg) = self.rx_from_app.recv_async() => {
                    self.dispatch(msg, config().self_recv).await?;
                }
                Some(connecting) = endpoint.accept() => {
                    debug!("connection incoming");
                    if let Err(e) = self.clone().handle_join_request(connecting).await {
                        error!("handle join request failed: {:?}", e);
                    }
                }
                _ = check_neighbor_interval.tick() => self.check_neighbors().await
            }
        }
    }

    async fn join(self, connection: Connection) -> Result<()> {
        let Config {
            msg_timeout,
            msg_max_size,
            ..
        } = config();
        let remote_address = connection.remote_address();
        let mut msg_timeout = tokio::time::interval(Duration::from_secs(*msg_timeout));
        select! {
            _ = msg_timeout.tick() => {
                Err(eyre!("join request timeout: {remote_address}"))
            }
            Ok((tx, mut rv)) = connection.open_bi() => {
                tokio::spawn(write_msg(tx, Message::to(0, MessageBody::JoinReq(
                    self.subscription_list.read().clone().into_iter().collect(),
                ))));
                let req = rv.read_to_end(*msg_max_size).await?;
                let msg = bincode::deserialize::<Message>(&req)?;
                if let MessageBody::JoinRsp(membership) = msg.body {
                    self.membership.write().merge(&membership);

                    debug!("membership: {:#?}", self.membership.read());

                    let neighbor = Neighbor {
                        id: msg.origin.into(),
                        connection,
                        tx: self.tx_to_neighbor.clone(),
                    };

                    tokio::spawn(neighbor.clone().handle());

                    self.neighbors
                        .write()
                        .insert(neighbor.id, neighbor.clone());

                    info!("new neighbor({}): {remote_address}", neighbor.id);
                    Ok(())
                } else {
                    Err(eyre!("invalid join response: {remote_address}"))
                }
            }
        }
    }

    async fn handle_join_request(self, connecting: Connecting) -> Result<()> {
        let Config {
            msg_timeout,
            msg_max_size,
            id,
            ..
        } = config();
        match connecting.await {
            Ok(connection) => {
                let remote_address = connection.remote_address();
                let mut msg_timeout = tokio::time::interval(Duration::from_secs(*msg_timeout));
                select! {
                    _ = msg_timeout.tick() => {
                        Err(eyre!("join request timeout: {remote_address}"))
                    }
                    Ok((tx, mut rv)) = connection.accept_bi() => {
                        let req = rv.read_to_end(*msg_max_size).await?;
                        let msg = bincode::deserialize::<Message>(&req)?;
                        if let MessageBody::JoinReq(subscription_list) = msg.body {
                            let mut neighbor_list = HashSet::new();
                            neighbor_list.insert((*id).into());
                            let member = Member {
                                id: msg.origin.into(),
                                subscription_list: HashSet::from_iter(subscription_list),
                                neighbor_list,
                            };
                            self.membership.write().add_member(member);
                            let memberlist = self.membership.read().clone();
                            debug!("memberlist: {:#?}", memberlist);

                            tokio::spawn(write_msg(tx, Message::to(
                                msg.origin,
                                MessageBody::JoinRsp(
                                    memberlist,
                                ),
                            )));

                            let neighbor = Neighbor {
                                id: msg.origin.into(),
                                connection,
                                tx: self.tx_to_neighbor.clone(),
                            };

                            tokio::spawn(neighbor.clone().handle());

                            self.neighbors
                                .write()
                                .insert(neighbor.id, neighbor.clone());

                            info!("new neighbor({}): {remote_address}", neighbor.id);
                            Ok(())
                        } else {
                            Err(eyre!("invalid join response: {remote_address}"))
                        }
                    }
                }
            }
            Err(e) => Err(eyre!("connecting failed: {:?}", e)),
        }
    }

    async fn dispatch(&self, msg: Message, self_recv: bool) -> Result<()> {
        debug!("dispatch msg: {:?}", msg);
        match (msg.to, msg.topic.as_str()) {
            (0, "") => {
                // TODO
            }
            (0, topic) => {
                if self_recv && self.subscription_list.read().contains(topic) {
                    let _ = self.tx_to_app.send(msg.clone());
                }
                if let Some(subscribers) = self.membership.read().subscription_map.get(topic) {
                    subscribers.par_iter().for_each(|member_id| {
                        if let Some(neighbor) = self.neighbors.read().get(member_id) {
                            let connection = neighbor.connection.clone();
                            tokio::spawn(send_uni_msg(connection, msg.clone()));
                        }
                    })
                }
            }
            (to, "") => {
                if to == config().id {
                    if self_recv {
                        let _ = self.tx_to_app.send(msg.clone());
                    }
                } else if let Some(neighbor) = self.neighbors.read().get(&to.into()) {
                    let connection = neighbor.connection.clone();
                    tokio::spawn(send_uni_msg(connection, msg.clone()));
                } else {
                    self.membership
                        .read()
                        .member_map
                        .par_iter()
                        .filter(|(_, member)| member.neighbor_list.contains(&to.into()))
                        .for_each(|(id, _)| {
                            if let Some(neighbor) = self.neighbors.read().get(id) {
                                let connection = neighbor.connection.clone();
                                tokio::spawn(send_uni_msg(connection, msg.clone()));
                            }
                        })
                }
            }
            (to, topic) => {
                if to == config().id {
                    if self_recv && self.subscription_list.read().contains(topic) {
                        let _ = self.tx_to_app.send(msg.clone());
                    }
                } else if let Some(subscribers) = self.membership.read().subscription_map.get(topic)
                {
                    if let Some(subscriber_id) = subscribers.get(&to.into()) {
                        if let Some(neighbor) = self.neighbors.read().get(subscriber_id) {
                            let connection = neighbor.connection.clone();
                            tokio::spawn(send_uni_msg(connection, msg.clone()));
                        } else {
                            let mut empty = true;
                            self.membership
                                .read()
                                .member_map
                                .iter()
                                .filter(|(_, member)| member.neighbor_list.contains(subscriber_id))
                                .for_each(|(id, _)| {
                                    if let Some(neighbor) = self.neighbors.read().get(id) {
                                        empty = false;
                                        let connection = neighbor.connection.clone();
                                        tokio::spawn(send_uni_msg(connection, msg.clone()));
                                    }
                                });
                            if empty {
                                self.neighbors.read().par_iter().for_each(|(_, neighbor)| {
                                    let connection = neighbor.connection.clone();
                                    tokio::spawn(send_uni_msg(connection, msg.clone()));
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn check_neighbors(&self) {
        let mut remove_ids = vec![];
        self.neighbors.read().values().for_each(|neighbor| {
            if let Some(reason) = neighbor.connection.close_reason() {
                info!("neighbor({}) closed: {}", neighbor.id, reason);
                remove_ids.push(neighbor.id);
            }
        });
        for remove_id in remove_ids {
            self.neighbors.write().remove(&remove_id);
            self.membership.write().add_check_member(remove_id);
            // TODO: Notify other nodes to check this member
        }
    }
}

pub async fn write_msg(mut send: SendStream, mut msg: Message) -> Result<()> {
    msg.origin = config().id;
    send.write_all(&bincode::serialize(&msg)?).await?;
    send.finish().await?;
    Ok(())
}

pub async fn read_msg(mut recv: RecvStream) -> Result<Message> {
    let req = recv.read_to_end(config().msg_max_size).await?;
    Ok(bincode::deserialize::<Message>(&req)?)
}

async fn handle_stream(
    neighbor_id: EldegossId,
    mut recv: RecvStream,
    tx: Sender<Message>,
) -> Result<()> {
    let req = recv.read_to_end(config().msg_max_size).await?;
    let mut msg = bincode::deserialize::<Message>(&req)?;
    debug!("recv msg: {:?}", msg);
    msg.origin = neighbor_id.to_u128();

    tx.send_async(msg).await?;
    Ok(())
}

pub async fn send_uni_msg(connection: Connection, mut msg: Message) -> Result<()> {
    msg.origin = config().id;
    let mut send = connection.open_uni().await?;
    send.write_all(&bincode::serialize(&msg)?).await?;
    send.finish().await?;
    Ok(())
}

#[tokio::test]
async fn neighbor0() {
    common_x::log::init_log_filter("debug");

    init_config(Config {
        id: 1,
        listen: "[::]:4721".to_string(),
        cert_path: "./config/cert/server_cert.pem".into(),
        private_key_path: "./config/cert/server_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        ..Default::default()
    });
    info!("id: {}", config().id);
    let server = Server::new(flume::unbounded().0, flume::unbounded().1);
    if let Err(e) = server.serve().await {
        error!("server failed: {e}");
    }
}

#[tokio::test]
async fn neighbor1() {
    common_x::log::init_log_filter("debug");

    init_config(Config {
        id: 2,
        connect: ["127.0.0.1:4721".to_string()].to_vec(),
        listen: "[::]:4723".to_string(),
        cert_path: "./config/cert/client_cert.pem".into(),
        private_key_path: "./config/cert/client_key.pem".into(),
        ca_path: "./config/cert/ca_cert.pem".into(),
        ..Default::default()
    });
    info!("id: {}", config().id);
    let (tx, rx) = flume::unbounded();
    let server = Server::new(flume::unbounded().0, rx);

    let mut send_test_msg_interval = tokio::time::interval(Duration::from_secs(1));

    tokio::spawn(async move {
        loop {
            send_test_msg_interval.tick().await;
            let msg = Message::to(1, MessageBody::Ok);
            let _ = tx.send_async(msg).await;
        }
    });

    if let Err(e) = server.serve().await {
        error!("server failed: {e}");
    }
}
