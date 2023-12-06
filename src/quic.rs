use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_lock::{Mutex, RwLock};
use color_eyre::{eyre::eyre, Result};
use flume::{Receiver, Sender};
use quinn::{
    ClientConfig, Connecting, Connection, Endpoint, RecvStream, SendStream, ServerConfig,
    TransportConfig,
};

use common_x::cert::{read_ca, read_certs, read_key, WebPkiVerifierAnyServerName};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tokio::select;

use crate::{
    protocol::{decode_msg, encode_msg, EldegossMsg, EldegossMsgBody, Message},
    Config, EldegossId, Member, Membership,
};

static CONFIG: OnceLock<Config> = OnceLock::new();

fn init_config(config: Config) {
    CONFIG.set(config).unwrap()
}

pub fn config() -> &'static Config {
    CONFIG.get_or_init(Config::default)
}

fn rng() -> &'static Mutex<ChaCha8Rng> {
    static RNG: OnceLock<Mutex<ChaCha8Rng>> = OnceLock::new();
    RNG.get_or_init(|| Mutex::new(ChaCha8Rng::seed_from_u64(rand::random())))
}

async fn read_msg(recv: &mut RecvStream) -> Result<Message> {
    let mut length = [0_u8, 0_u8, 0_u8, 0_u8];
    recv.read_exact(&mut length).await?;
    let n = u32::from_le_bytes(length) as usize;
    let bytes = &mut vec![0_u8; n];
    recv.read_exact(bytes).await?;
    decode_msg(bytes)
}

pub async fn write_msg(send: &mut SendStream, mut msg: Message) -> Result<()> {
    msg.set_origin(config().id);
    let mut msg_bytes = encode_msg(&msg);
    let mut bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
    bytes.append(&mut msg_bytes);
    send.write_all(&bytes).await?;
    Ok(())
}

#[derive(Debug)]
pub struct Neighbor {
    pub id: EldegossId,
    pub locator: Arc<Mutex<String>>,
    pub connection: Connection,
    pub send: Arc<Mutex<quinn::SendStream>>,
    pub recv: Arc<Mutex<quinn::RecvStream>>,
    pub server: Server,
}

impl Neighbor {
    async fn handle(&self) {
        let mut recv = self.recv.lock().await;
        loop {
            match read_msg(&mut recv).await {
                Ok(msg) => {
                    self.server.dispatch(msg, self.id.to_u128()).await;
                }
                Err(e) => {
                    debug!("neighbor handle recv msg failed: {e}");
                    break;
                }
            }
        }
    }

    pub async fn send_msg(&self, msg: &Message) {
        let mut msg_bytes = encode_msg(msg);
        let mut bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
        bytes.append(&mut msg_bytes);
        if let Err(e) = self.send.lock().await.write_all(&bytes).await {
            debug!("neighbor({}) send msg failed: {e:?}", self.id);
        }
    }

    pub async fn set_locator(&self, locator: String) {
        *self.locator.lock().await = locator;
    }
}

type MsgForRecv = (Sender<Message>, Receiver<Message>);

#[derive(Debug, Clone)]
pub struct Server {
    pub msg_for_recv: MsgForRecv,
    pub neighbors: Arc<RwLock<HashMap<EldegossId, Arc<Neighbor>>>>,
    pub membership: Arc<RwLock<Membership>>,
    pub connect_neighbors: Arc<RwLock<HashMap<String, u128>>>,
}

impl Server {
    fn init(config_: Config) -> Self {
        init_config(config_);
        let member = Member::new(
            config().id.into(),
            HashSet::from_iter(config().subscription_list.clone()),
        );
        let mut membership = Membership::default();
        membership.add_member(member);
        Self {
            msg_for_recv: flume::unbounded(),
            neighbors: Default::default(),
            membership: Arc::new(RwLock::new(membership)),
            connect_neighbors: Default::default(),
        }
    }

    pub async fn serve(config: Config) -> Self {
        let server = Self::init(config);
        tokio::spawn(server.clone().run_server());
        let _ = server.connect().await;
        server
    }

    pub async fn recv_msg(&self) -> Result<Message> {
        Ok(self.msg_for_recv.1.recv_async().await?)
    }

    // TODO: 异步加速
    pub async fn send_msg(&self, mut msg: Message) {
        msg.set_origin(config().id);
        self.dispatch(msg, 0).await
    }

    async fn to_recv_msg(&self, msg: Message) {
        if let Err(e) = self.msg_for_recv.0.send_async(msg).await {
            debug!("to_recv_msg failed: {:?}", e);
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

        for conn in connect {
            if self.connect_neighbors.read().await.contains_key(conn) {
                continue;
            }
            let _ = self.connect_to(&client_config, conn.to_string());
        }

        let server = self.clone();
        tokio::spawn(async move {
            let Config { connect, .. } = &config();
            let mut interval =
                tokio::time::interval(Duration::from_secs(config().keep_alive_interval));
            interval.tick().await;
            loop {
                interval.tick().await;
                for conn in connect {
                    if server.connect_neighbors.read().await.contains_key(conn) {
                        continue;
                    }
                    info!("reconnect to: {conn}");
                    let _ = server.connect_to(&client_config, conn.to_string());
                }
            }
        });
        Ok(())
    }

    fn connect_to(&self, client_config: &ClientConfig, connect: String) -> Result<()> {
        let mut endpoint = Endpoint::client("[::]:0".parse::<std::net::SocketAddr>()?)?;
        endpoint.set_default_client_config(client_config.clone());
        let server = self.clone();
        tokio::spawn(async move {
            if let Ok(connection) = endpoint
                .connect(
                    connect.parse::<std::net::SocketAddr>().unwrap(),
                    "localhost",
                )
                .unwrap()
                .await
            {
                let _ = server.join(connection, connect).await;
            }
        });
        Ok(())
    }

    async fn run_server(self) -> Result<()> {
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
        check_neighbor_interval.tick().await;
        loop {
            select! {
                Some(connecting) = endpoint.accept() => {
                    debug!("connection incoming");
                    tokio::spawn(self.clone().handle_join_request(connecting));
                }
                _ = check_neighbor_interval.tick() => self.maintain_membership().await
            }
        }
    }

    async fn join(self, connection: Connection, locator: String) -> Result<()> {
        let Config {
            msg_timeout,
            subscription_list,
            ..
        } = config();
        let remote_address = connection.remote_address();
        let mut msg_timeout = tokio::time::interval(Duration::from_secs(*msg_timeout));
        msg_timeout.tick().await;
        select! {
            _ = msg_timeout.tick() => {
                debug!("join request timeout: {remote_address}");
                Err(eyre!("join request timeout: {remote_address}"))
            }
            Ok((mut tx, mut rv)) = connection.open_bi() => {
                let _ = write_msg(
                    &mut tx,
                    Message::eldegoss(
                        0,
                        EldegossMsgBody::JoinReq(subscription_list.clone()),
                    ),
                )
                .await;
                let msg = read_msg(&mut rv).await?;
                if let Message::EldegossMsg(EldegossMsg {
                    origin,
                    body: EldegossMsgBody::JoinRsp(membership),
                    ..
                }) = msg
                {
                    self.membership.write().await.merge(&membership);

                    let neighbor = Arc::new(Neighbor {
                        id: origin.into(),
                        locator: Arc::new(Mutex::new(locator.clone())),
                        connection,
                        server: self.clone(),
                        send: Arc::new(Mutex::new(tx)),
                        recv: Arc::new(Mutex::new(rv)),
                    });
                    let old_neighbor = self.neighbors.read().await.get(&neighbor.id).cloned();
                    if let Some(old_neighbor) = old_neighbor {
                        old_neighbor.set_locator(locator.clone()).await;
                        info!("update neighbor({}): {remote_address}", origin);
                    } else {
                        let neighbor_ = neighbor.clone();
                        tokio::spawn(async move {
                            neighbor_.handle().await;
                        });
                        info!("new neighbor({}): {remote_address}", origin);
                        self.neighbors.write().await.insert(neighbor.id, neighbor);
                    }
                    self.connect_neighbors.write().await.insert(locator, origin);

                    Ok(())
                } else {
                    debug!("invalid join response: {remote_address}");
                    Err(eyre!("invalid join response: {remote_address}"))
                }
            }
        }
    }

    async fn handle_join_request(self, connecting: Connecting) -> Result<()> {
        let Config { id, .. } = config();
        match connecting.await {
            Ok(connection) => {
                let remote_address = connection.remote_address();
                if let Ok((mut tx, mut rv)) = connection.accept_bi().await {
                    let msg = read_msg(&mut rv).await?;

                    if let Message::EldegossMsg(EldegossMsg {
                        origin,
                        body: EldegossMsgBody::JoinReq(subscription_list),
                        ..
                    }) = msg
                    {
                        if self.neighbors.read().await.contains_key(&origin.into()) {
                            info!("neighbor({origin}) already exists: {remote_address}");
                            let membership = self.membership.read().await.clone();
                            let _ = write_msg(
                                &mut tx,
                                Message::eldegoss(0, EldegossMsgBody::JoinRsp(membership)),
                            )
                            .await;
                            let neighbor = Arc::new(Neighbor {
                                id: EldegossId::rand(),
                                locator: Arc::new(Mutex::new("".to_string())),
                                connection,
                                server: self.clone(),
                                send: Arc::new(Mutex::new(tx)),
                                recv: Arc::new(Mutex::new(rv)),
                            });

                            self.neighbors.write().await.insert(neighbor.id, neighbor);
                            return Ok(());
                        }

                        let mut neighbor_list = HashSet::new();
                        neighbor_list.insert((*id).into());
                        let member = Member {
                            id: origin.into(),
                            subscription_list: HashSet::from_iter(subscription_list),
                            neighbor_list,
                        };
                        self.membership.write().await.add_member(member.clone());

                        self.send_msg(Message::eldegoss(0, EldegossMsgBody::AddMember(member)))
                            .await;

                        let membership = self.membership.read().await.clone();
                        debug!("memberlist: {membership:#?}");

                        let _ = write_msg(
                            &mut tx,
                            Message::eldegoss(0, EldegossMsgBody::JoinRsp(membership)),
                        )
                        .await;

                        let neighbor = Arc::new(Neighbor {
                            id: origin.into(),
                            locator: Arc::new(Mutex::new(remote_address.to_string())),
                            connection,
                            server: self.clone(),
                            send: Arc::new(Mutex::new(tx)),
                            recv: Arc::new(Mutex::new(rv)),
                        });

                        let neighbor_ = neighbor.clone();
                        tokio::spawn(async move {
                            neighbor_.handle().await;
                        });
                        info!("new neighbor({origin}): {remote_address}");

                        self.neighbors.write().await.insert(neighbor.id, neighbor);

                        Ok(())
                    } else {
                        debug!("invalid join response: {remote_address}");
                        Err(eyre!("invalid join response: {remote_address}"))
                    }
                } else {
                    debug!("accept_bi failed: {remote_address}");
                    Err(eyre!("accept_bi failed: {remote_address}"))
                }
            }
            Err(e) => {
                debug!("connecting failed: {e:?}");
                Err(eyre!("connecting failed: {e:?}"))
            }
        }
    }

    // TODO: 按接收与发送拆分函数, 区分参数格式
    async fn dispatch(&self, msg: Message, received_from: u128) {
        // debug!("dispatch({received_from}) msg: {:?}", msg);
        let membership = self.membership.read().await.clone();
        let origin = msg.origin();
        match (msg.to(), msg.topic().as_str()) {
            (0, "") => {
                self.gossip_msg(&msg, received_from).await;
                if received_from != 0 {
                    self.handle_recv_msg(msg).await;
                }
            }
            (0, topic) => {
                self.gossip_msg(&msg, received_from).await;

                if received_from != 0 && config().subscription_list.contains(&topic.to_owned()) {
                    self.to_recv_msg(msg).await;
                }
            }
            (to, "") => {
                if to == config().id {
                    if received_from != 0 {
                        self.handle_recv_msg(msg).await;
                    }
                } else if let Some(neighbor) = self.neighbors.read().await.get(&to.into()) {
                    neighbor.send_msg(&msg).await;
                } else {
                    let member_ids = membership
                        .member_map
                        .par_iter()
                        .filter_map(|(id, member)| {
                            if member.neighbor_list.contains(&to.into()) {
                                Some(id)
                            } else {
                                None
                            }
                        })
                        .cloned()
                        .collect::<Vec<_>>();

                    for id in member_ids {
                        if let Some(neighbor) = self.neighbors.read().await.get(&id) {
                            neighbor.send_msg(&msg).await;
                        }
                    }
                }
            }
            (to, topic) => {
                if to == config().id {
                    if received_from != 0 && config().subscription_list.contains(&topic.to_owned())
                    {
                        self.to_recv_msg(msg).await;
                    }
                } else if let Some(subscribers) = membership.subscription_map.get(topic) {
                    if let Some(subscriber_id) = subscribers.get(&to.into()) {
                        if let Some(neighbor) = self.neighbors.read().await.get(subscriber_id) {
                            neighbor.send_msg(&msg).await;
                        } else {
                            let mut empty = true;
                            let member_ids = membership
                                .member_map
                                .par_iter()
                                .filter_map(|(id, member)| {
                                    if member.neighbor_list.contains(subscriber_id) {
                                        Some(id)
                                    } else {
                                        None
                                    }
                                })
                                .cloned()
                                .collect::<Vec<_>>();

                            for id in member_ids {
                                if let Some(neighbor) = self.neighbors.read().await.get(&id) {
                                    empty = false;
                                    neighbor.send_msg(&msg).await;
                                }
                            }
                            if empty {
                                for (_, neighbor) in self.neighbors.read().await.iter() {
                                    let neighbor_id = neighbor.id.to_u128();
                                    if neighbor_id != origin && neighbor_id != received_from {
                                        neighbor.send_msg(&msg).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_recv_msg(&self, msg: Message) {
        match &msg {
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::AddMember(member),
                ..
            }) => {
                self.membership.write().await.add_member(member.clone());
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::RemoveMember(id),
                ..
            }) => {
                self.membership.write().await.remove_member((*id).into());
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::CheckReq(check_id),
                ..
            }) => {
                let result = self
                    .neighbors
                    .read()
                    .await
                    .contains_key(&(*check_id).into());
                let mut msg = Message::eldegoss(0, EldegossMsgBody::CheckRsp(*check_id, result));
                msg.set_origin(config().id);
                self.gossip_msg(&msg, 0).await;
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::CheckRsp(id, result),
                ..
            }) => {
                if *result {
                    self.membership
                        .write()
                        .await
                        .wait_for_remove_member_list
                        .retain(|id_| &id_.to_u128() != id);
                }
            }
            _ => {
                self.to_recv_msg(msg).await;
            }
        }
    }

    async fn gossip_msg(&self, msg: &Message, received_from: u128) {
        if received_from != 0 && msg.origin() == config().id {
            return;
        }
        if self.neighbors.read().await.len() <= config().gossip_fanout {
            for (_, neighbor) in self.neighbors.read().await.iter() {
                let neighbor_id = neighbor.id.to_u128();
                if neighbor_id != msg.origin() && neighbor_id != received_from {
                    neighbor.send_msg(msg).await;
                }
            }
        } else {
            let neighbors = self
                .neighbors
                .read()
                .await
                .iter()
                .filter_map(|(id, neighbor)| {
                    let id = id.to_u128();
                    if id != msg.origin() && id != received_from {
                        Some(neighbor)
                    } else {
                        None
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            for _ in 0..config().gossip_fanout {
                let index = rng().lock().await.gen_range(0..neighbors.len());
                neighbors[index].send_msg(msg).await;
            }
        }
    }

    async fn check_member(&self, check_id: EldegossId) -> Result<bool> {
        let membership = self.membership.read().await.clone();
        if let Some(check_member) = membership.member_map.get(&check_id) {
            for neighbor_id in &check_member.neighbor_list {
                self.send_msg(Message::eldegoss(
                    neighbor_id.to_u128(),
                    EldegossMsgBody::CheckReq(check_id.to_u128()),
                ))
                .await;
            }
        }
        Ok(false)
    }

    async fn maintain_membership(&self) {
        // remove member
        {
            let mut remove_ids = vec![];
            {
                let mut membership = self.membership.write().await;
                while let Some(remove_id) = membership.wait_for_remove_member_list.pop() {
                    info!("remove member: {}", remove_id);
                    membership.remove_member(remove_id);
                    remove_ids.push(remove_id);
                }
            }

            for remove_id in remove_ids {
                self.send_msg(Message::eldegoss(
                    0,
                    EldegossMsgBody::RemoveMember(remove_id.to_u128()),
                ))
                .await;
            }
        }

        // check member
        {
            loop {
                let check_id = self.membership.write().await.get_check_member();
                if let Some(check_id) = check_id {
                    info!("check member: {}", check_id);
                    let _ = self.check_member(check_id).await;
                } else {
                    break;
                }
            }
        }

        let mut remove_ids = vec![];
        {
            self.neighbors.read().await.values().for_each(|neighbor| {
                if let Some(reason) = neighbor.connection.close_reason() {
                    remove_ids.push((neighbor.id, neighbor.locator.clone(), reason));
                }
            });
        }

        {
            let mut neighbors = self.neighbors.write().await;
            let mut membership = self.membership.write().await;
            let mut connect_neighbors = self.connect_neighbors.write().await;
            for (remove_id, locator, reason) in remove_ids {
                let locator = locator.lock().await;
                connect_neighbors.remove(locator.as_str());
                neighbors.remove(&remove_id);
                membership.add_check_member(remove_id, !locator.is_empty());

                info!(
                    "neighbor({}) [{}] |{}| closed: {}",
                    remove_id,
                    locator,
                    !locator.is_empty(),
                    reason
                );
            }
        }
    }
}
