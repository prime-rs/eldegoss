use std::{
    collections::HashMap,
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

use common_x::cert::{create_any_server_name_config, read_certs, read_key};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
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

#[inline]
async fn read_msg(recv: &mut RecvStream) -> Result<Message> {
    let mut length = [0_u8, 0_u8, 0_u8, 0_u8];
    recv.read_exact(&mut length).await?;
    let n = u32::from_le_bytes(length) as usize;
    let bytes = &mut vec![0_u8; n];
    recv.read_exact(bytes).await?;
    decode_msg(bytes)
}

#[inline]
pub async fn write_msg(send: &mut SendStream, mut msg: Message) -> Result<()> {
    msg.set_origin(config().id);
    let mut msg_bytes = encode_msg(&msg);
    let mut bytes = (msg_bytes.len() as u32).to_le_bytes().to_vec();
    bytes.append(&mut msg_bytes);
    send.write_all(&bytes).await?;
    Ok(())
}

// TODO: optim shutdown, reconnect, check
#[derive(Debug)]
struct Neighbor {
    id: EldegossId,
    locator: String,
    connection: Connection,
    send: Arc<Mutex<quinn::SendStream>>,
    recv: Arc<Mutex<quinn::RecvStream>>,
    server: Server,
}

impl Neighbor {
    fn handle(&self) {
        let id = self.id.to_u128();
        let recv = self.recv.clone();
        let connection = self.connection.clone();
        let locator = self.locator.clone();
        let server = self.server.clone();
        tokio::spawn(async move {
            let mut recv = recv.lock().await;
            loop {
                match read_msg(&mut recv).await {
                    Ok(msg) => {
                        let server = server.clone();
                        tokio::spawn(async move {
                            server.dispatch(msg, id).await;
                        });
                    }
                    Err(e) => {
                        debug!("neighbor handle recv msg failed: {e}");
                        if let Some(close_reason) = connection.close_reason() {
                            server.connect_neighbors.lock().await.remove(&locator);
                            server.neighbors.write().await.remove(&id.into());
                            server.check_member_list.lock().await.push(id.into());
                            info!("neighbor({id}) closed: {close_reason}");
                        }
                        break;
                    }
                }
            }
        });
    }

    #[inline]
    pub async fn send_msg(&self, msg: &Message) -> Result<()> {
        let msg_bytes = encode_msg(msg);
        let len = msg_bytes.len() as u32;
        let len_bytes = len.to_le_bytes().to_vec();
        let mut send = self.send.lock().await;
        send.write_all(&len_bytes).await?;
        send.write_all(&msg_bytes).await?;
        Ok(())
    }

    pub fn set_locator(&mut self, locator: String) {
        self.locator = locator;
    }
}

type MsgForRecv = (Sender<Message>, Receiver<Message>);

#[derive(Debug, Clone)]
pub struct Server {
    msg_for_recv: MsgForRecv,
    neighbors: Arc<RwLock<HashMap<EldegossId, Neighbor>>>,
    membership: Arc<Mutex<Membership>>,
    connect_neighbors: Arc<Mutex<HashMap<String, u128>>>,
    check_member_list: Arc<Mutex<Vec<EldegossId>>>,
    wait_for_remove_member_list: Arc<Mutex<Vec<EldegossId>>>,
}

impl Server {
    fn init(config_: Config) -> Self {
        init_config(config_);
        let member = Member::new(config().id.into(), vec![]);
        let mut membership = Membership::default();
        membership.add_member(member);
        Self {
            msg_for_recv: flume::unbounded(),
            neighbors: Default::default(),
            membership: Arc::new(Mutex::new(membership)),
            connect_neighbors: Default::default(),
            check_member_list: Default::default(),
            wait_for_remove_member_list: Default::default(),
        }
    }

    pub async fn serve(config: Config) -> Self {
        let server = Self::init(config);
        tokio::spawn(server.clone().run_server());
        let _ = server.connect().await;
        server
    }

    #[inline]
    pub async fn recv_msg(&self) -> Result<Message> {
        Ok(self.msg_for_recv.1.recv_async().await?)
    }

    #[inline]
    pub async fn send_msg(&self, mut msg: Message) {
        msg.set_origin(config().id);
        if msg.to() != 0 {
            if let Some(neighbor) = self.neighbors.read().await.get(&msg.to().into()) {
                let _ = neighbor.send_msg(&msg).await;
                return;
            }
        }
        self.gossip_msg(&msg, 0).await;
    }

    #[inline]
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
            check_neighbor_interval,
            ..
        } = &config();

        let client_crypto = create_any_server_name_config(ca_path)?;

        let mut client_config = ClientConfig::new(Arc::new(client_crypto));
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        client_config.transport_config(Arc::new(transport_config));

        for conn in connect {
            if self.connect_neighbors.lock().await.contains_key(conn) {
                continue;
            }
            self.connect_to(&client_config, conn.to_string()).await.ok();
        }

        let server = self.clone();
        tokio::spawn(async move {
            let Config { connect, .. } = &config();
            let mut interval = tokio::time::interval(Duration::from_secs(*check_neighbor_interval));
            interval.tick().await;
            loop {
                interval.tick().await;
                for conn in connect {
                    if server.connect_neighbors.lock().await.contains_key(conn) {
                        continue;
                    }
                    info!("reconnect to: {conn}");
                    server
                        .connect_to(&client_config, conn.to_string())
                        .await
                        .ok();
                }
            }
        });
        Ok(())
    }

    async fn connect_to(&self, client_config: &ClientConfig, connect: String) -> Result<()> {
        let mut endpoint = Endpoint::client("[::]:0".parse::<std::net::SocketAddr>()?)?;
        endpoint.set_default_client_config(client_config.clone());
        if let Ok(connection) = endpoint
            .connect(
                connect.parse::<std::net::SocketAddr>().unwrap(),
                "localhost",
            )
            .unwrap()
            .await
        {
            let _ = self.join(connection, connect).await;
        }
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
                    debug!("connected");
                }
                _ = check_neighbor_interval.tick() => self.maintain_membership().await
            }
        }
    }

    async fn join(&self, connection: Connection, locator: String) -> Result<()> {
        let Config { msg_timeout, .. } = config();
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
                        EldegossMsgBody::JoinReq(vec![]),
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
                    {
                        self.membership.lock().await.merge(&membership);
                    }

                    let neighbor = Neighbor {
                        id: origin.into(),
                        locator: locator.clone(),
                        connection,
                        server: self.clone(),
                        send: Arc::new(Mutex::new(tx)),
                        recv: Arc::new(Mutex::new(rv)),
                    };
                    let mut is_old = false;
                    if let Some(old_neighbor) = self.neighbors.write().await.get_mut(&neighbor.id) {
                        old_neighbor.set_locator(locator.clone());
                        info!("update neighbor({}): {remote_address}", origin);
                        self.connect_neighbors.lock().await.insert(locator.clone(), origin);
                        is_old = true;
                    }
                    if !is_old {
                        neighbor.handle();
                        info!("new neighbor({}): {remote_address}", origin);
                        self.insert_neighbor(locator, neighbor).await;
                    }

                    Ok(())
                } else {
                    debug!("invalid join response: {remote_address}");
                    Err(eyre!("invalid join response: {remote_address}"))
                }
            }
        }
    }

    async fn handle_join_request(self, connecting: Connecting) -> Result<()> {
        match connecting.await {
            Ok(connection) => {
                let remote_address = connection.remote_address();
                if let Ok((mut tx, mut rv)) = connection.accept_bi().await {
                    let msg = read_msg(&mut rv).await?;

                    if let Message::EldegossMsg(EldegossMsg {
                        origin,
                        body: EldegossMsgBody::JoinReq(meta_data),
                        ..
                    }) = msg
                    {
                        let mut is_reconnect = false;
                        let is_new = if let Some(neighbor) =
                            self.neighbors.read().await.get(&origin.into())
                        {
                            if neighbor.connection.close_reason().is_some() {
                                is_reconnect = true;
                            }
                            false
                        } else {
                            true
                        };

                        if !is_new && !is_reconnect {
                            info!("neighbor({origin}) already exists: {remote_address}");
                            return Err(eyre!(
                                "neighbor({origin}) already exists: {remote_address}"
                            ));
                        }

                        let membership = self.membership.lock().await.clone();
                        debug!("memberlist: {membership:#?}");

                        let _ = write_msg(
                            &mut tx,
                            Message::eldegoss(0, EldegossMsgBody::JoinRsp(membership)),
                        )
                        .await;

                        let member = Member::new(origin.into(), meta_data);
                        {
                            self.membership.lock().await.add_member(member.clone());
                        }

                        self.send_msg(Message::eldegoss(0, EldegossMsgBody::AddMember(member)))
                            .await;

                        let neighbor = Neighbor {
                            id: origin.into(),
                            locator: remote_address.to_string(),
                            connection,
                            server: self.clone(),
                            send: Arc::new(Mutex::new(tx)),
                            recv: Arc::new(Mutex::new(rv)),
                        };
                        neighbor.handle();
                        info!("new neighbor({origin}): {remote_address}");

                        self.insert_neighbor(remote_address.to_string(), neighbor)
                            .await;

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

    async fn insert_neighbor(&self, locator: String, neighbor: Neighbor) {
        let id = neighbor.id.to_u128();
        self.wait_for_remove_member_list
            .lock()
            .await
            .retain(|x| x.to_u128() != id);
        self.check_member_list
            .lock()
            .await
            .retain(|x| x.to_u128() != id);

        self.connect_neighbors.lock().await.insert(locator, id);
        self.neighbors.write().await.insert(neighbor.id, neighbor);
    }

    #[inline]
    async fn dispatch(&self, msg: Message, received_from: u128) {
        // debug!("dispatch({received_from}) msg: {:?}", msg);
        match (msg.to(), msg.topic().as_str()) {
            (0, "") => {
                self.gossip_msg(&msg, received_from).await;
                self.handle_recv_msg(msg).await;
            }
            (0, topic) => {
                self.gossip_msg(&msg, received_from).await;

                if config().subscription_list.contains(&topic.to_owned()) {
                    self.to_recv_msg(msg).await;
                }
            }
            (to, "") => {
                if to == config().id {
                    self.handle_recv_msg(msg).await;
                } else if let Some(neighbor) = self.neighbors.read().await.get(&to.into()) {
                    let _ = neighbor.send_msg(&msg).await;
                } else {
                    self.gossip_msg(&msg, received_from).await;
                }
            }
            (to, topic) => {
                if to == config().id {
                    if config().subscription_list.contains(&topic.to_owned()) {
                        self.to_recv_msg(msg).await;
                    }
                } else {
                    self.gossip_msg(&msg, received_from).await;
                }
            }
        }
    }

    #[inline]
    async fn handle_recv_msg(&self, msg: Message) {
        match &msg {
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::AddMember(member),
                ..
            }) => {
                self.membership.lock().await.add_member(member.clone());
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::RemoveMember(id),
                ..
            }) => {
                self.membership.lock().await.remove_member((*id).into());
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::CheckReq(check_id),
                ..
            }) => {
                let result = config().id == *check_id
                    || self
                        .neighbors
                        .read()
                        .await
                        .contains_key(&(*check_id).into());
                debug!("check member: {} result: {}", check_id, result);
                let msg = Message::eldegoss(0, EldegossMsgBody::CheckRsp(*check_id, result));
                self.send_msg(msg).await;
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::CheckRsp(id, result),
                ..
            }) => {
                debug!("recv check member: {} result: {}", id, result);
                if *result {
                    self.wait_for_remove_member_list
                        .lock()
                        .await
                        .retain(|id_| &id_.to_u128() != id);
                }
            }
            _ => {
                self.to_recv_msg(msg).await;
            }
        }
    }

    #[inline]
    async fn gossip_msg(&self, msg: &Message, received_from: u128) {
        if received_from != 0 && msg.origin() == config().id {
            return;
        }
        if self.neighbors.read().await.len() <= config().gossip_fanout {
            for (_, neighbor) in self.neighbors.read().await.iter() {
                let neighbor_id = neighbor.id.to_u128();
                if neighbor_id != msg.origin() && neighbor_id != received_from {
                    let _ = neighbor.send_msg(msg).await;
                }
            }
        } else {
            let neighbor_ids = self
                .neighbors
                .read()
                .await
                .keys()
                .filter(|eid| {
                    let id = eid.to_u128();
                    id != msg.origin() && id != received_from
                })
                .cloned()
                .collect::<Vec<_>>();
            let len = neighbor_ids.len();
            for _ in 0..config().gossip_fanout {
                let index = rng().lock().await.gen_range(0..len);
                if let Some(nerghbor) = self.neighbors.read().await.get(&neighbor_ids[index]) {
                    let _ = nerghbor.send_msg(msg).await;
                }
            }
        }
    }

    async fn maintain_membership(&self) {
        // remove member
        {
            let mut remove_ids = vec![];
            {
                while let Some(remove_id) = self.wait_for_remove_member_list.lock().await.pop() {
                    info!("remove member: {}", remove_id);
                    self.membership.lock().await.remove_member(remove_id);
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
                let check_id = if let Some(id) = self.check_member_list.lock().await.pop() {
                    self.wait_for_remove_member_list.lock().await.push(id);
                    Some(id)
                } else {
                    None
                };
                if let Some(check_id) = check_id {
                    info!("check member: {}", check_id);
                    self.send_msg(Message::eldegoss(
                        0,
                        EldegossMsgBody::CheckReq(check_id.to_u128()),
                    ))
                    .await;
                } else {
                    break;
                }
            }
        }
    }
}
