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
use rand::Rng;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tokio::select;

use crate::{
    protocol::{decode_msg, encode_msg, EldegossMsg, EldegossMsgBody, Message},
    Config, EldegossId, Member, Membership,
};

static CONFIG: OnceLock<Config> = OnceLock::new();

fn init_config(config: Config) {
    CONFIG.set(config).unwrap();
}

pub fn config() -> &'static Config {
    CONFIG.get().unwrap()
}

#[derive(Debug, Clone)]
pub struct Neighbor {
    pub id: EldegossId,
    pub connection: Connection,
    pub server: Server,
}

impl Neighbor {
    async fn handle(self) {
        self.read_uni().await;
    }

    // use for app msg
    async fn read_uni(&self) {
        let mut stats = crate::util::Stats::new(10000);
        while let Ok(stream) = self.connection.accept_uni().await {
            stats.increment();
            tokio::spawn(handle_stream(self.id, stream, self.server.clone()));
        }
    }
}

type MsgForSend = (
    Sender<(Connection, Message)>,
    Receiver<(Connection, Message)>,
);

type MsgForRecv = (Sender<Message>, Receiver<Message>);

#[derive(Debug, Clone)]
pub struct Server {
    pub msg_for_recv: MsgForRecv,
    pub msg_for_send: MsgForSend,
    pub neighbors: Arc<RwLock<HashMap<EldegossId, Neighbor>>>,
    pub membership: Arc<RwLock<Membership>>,
    pub subscription_list: Arc<RwLock<HashSet<String>>>,
}

impl Server {
    pub fn init(config_: Config) -> Self {
        init_config(config_);
        let member = Member::new(config().id.into());
        let mut membership = Membership::default();
        membership.add_member(member);
        Self {
            msg_for_recv: flume::unbounded(),
            msg_for_send: flume::unbounded(),
            neighbors: Arc::new(RwLock::new(HashMap::new())),
            membership: Arc::new(RwLock::new(membership)),
            subscription_list: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn serve(&self) {
        tokio::spawn(self.clone().run_server());
        let _ = self.connect().await;
    }

    pub async fn recv_msg(&self) -> Result<Message> {
        Ok(self.msg_for_recv.1.recv_async().await?)
    }

    pub fn send_msg(&self, msg: Message) {
        self.dispatch(msg, false)
    }

    fn to_send_msg(&self, connection: &Connection, mut msg: Message) {
        msg.set_from(config().id);
        let connection = connection.clone();
        if let Err(e) = self.msg_for_send.0.send((connection, msg)) {
            debug!("to_send_msg failed: {:?}", e);
        }
    }

    fn to_recv_msg(&self, msg: Message) {
        let msg_for_recv = self.msg_for_recv.0.clone();
        let msg = msg.clone();
        if let Err(e) = msg_for_recv.send_timeout(msg, Duration::from_millis(100)) {
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

        loop {
            select! {
                Ok((connection, msg)) = self.msg_for_send.1.recv_async() => {
                    if let Err(e) = send_uni_msg(connection, msg).await {
                        debug!("send_uni_msg failed: {:?}", e);
                    }
                }
                Some(connecting) = endpoint.accept() => {
                    debug!("connection incoming");
                    tokio::spawn(self.clone().handle_join_request(connecting));
                }
                _ = check_neighbor_interval.tick() => self.maintain_membership().await
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
            Ok((mut tx, mut rv)) = connection.open_bi() => {
                let subscription_list = self.subscription_list.read().clone();
                let _ = write_msg(
                    &mut tx,
                    Message::eldegoss(
                        0,
                        EldegossMsgBody::JoinReq(subscription_list.into_iter().collect()),
                    ),
                )
                .await;
                let req = rv.read_to_end(*msg_max_size).await?;
                let msg = decode_msg(&req)?;
                if let Message::EldegossMsg(EldegossMsg {
                    origin,
                    body: EldegossMsgBody::JoinRsp(membership),
                    ..
                }) = msg
                {
                    self.membership.write().merge(&membership);

                    debug!("membership: {:#?}", self.membership.read());

                    let neighbor = Neighbor {
                        id: origin.into(),
                        connection,
                        server: self.clone(),
                    };

                    tokio::spawn(neighbor.clone().handle());

                    self.neighbors.write().insert(neighbor.id, neighbor.clone());

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
                    Ok((mut tx, mut rv)) = connection.accept_bi() => {
                        let req = rv.read_to_end(*msg_max_size).await?;
                        let msg = decode_msg(&req)?;

                        if let Message::EldegossMsg(EldegossMsg {
                            origin,
                            body: EldegossMsgBody::JoinReq(subscription_list),
                            ..
                        }) = msg
                        {
                            if self.membership.read().contains(&origin.into()) {
                                return Err(eyre!("already joined: {}", origin));
                            }

                            let mut neighbor_list = HashSet::new();
                            neighbor_list.insert((*id).into());
                            let member = Member {
                                id: origin.into(),
                                subscription_list: HashSet::from_iter(subscription_list),
                                neighbor_list,
                            };
                            self.membership.write().add_member(member.clone());

                            self.dispatch(
                                Message::eldegoss(
                                    0,
                                    EldegossMsgBody::AddMember(member)
                                ),
                                false,
                            );

                            let memberlist = self.membership.read().clone();
                            debug!("memberlist: {:#?}", memberlist);

                            let _ = write_msg(
                                &mut tx,
                                Message::eldegoss(
                                    0,
                                    EldegossMsgBody::JoinRsp(memberlist),
                                ),
                            )
                            .await;

                            let neighbor = Neighbor {
                                id: origin.into(),
                                connection,
                                server: self.clone(),
                            };

                            tokio::spawn(neighbor.clone().handle());

                            self.neighbors.write().insert(neighbor.id, neighbor.clone());

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

    fn dispatch(&self, msg: Message, is_received: bool) {
        debug!("dispatch msg: {:?}", msg);
        match (msg.to(), msg.topic().as_str()) {
            (0, "") => {
                if is_received {
                    self.handle_recv_msg(&msg);
                }

                self.gossip_msg(&msg);
            }
            (0, topic) => {
                if is_received && self.subscription_list.read().contains(topic) {
                    self.to_recv_msg(msg.clone());
                }

                self.gossip_msg(&msg);
            }
            (to, "") => {
                if to == config().id {
                    if is_received {
                        self.handle_recv_msg(&msg);
                    }
                } else if let Some(neighbor) = self.neighbors.read().get(&to.into()) {
                    self.to_send_msg(&neighbor.connection, msg.clone());
                } else {
                    self.membership
                        .read()
                        .member_map
                        .par_iter()
                        .filter(|(_, member)| member.neighbor_list.contains(&to.into()))
                        .for_each(|(id, _)| {
                            if let Some(neighbor) = self.neighbors.read().get(id) {
                                self.to_send_msg(&neighbor.connection, msg.clone());
                            }
                        })
                }
            }
            (to, topic) => {
                if to == config().id {
                    if is_received && self.subscription_list.read().contains(topic) {
                        self.to_recv_msg(msg.clone());
                    }
                } else if let Some(subscribers) = self.membership.read().subscription_map.get(topic)
                {
                    if let Some(subscriber_id) = subscribers.get(&to.into()) {
                        if let Some(neighbor) = self.neighbors.read().get(subscriber_id) {
                            self.to_send_msg(&neighbor.connection, msg.clone());
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
                                        self.to_send_msg(&neighbor.connection, msg.clone());
                                    }
                                });
                            if empty {
                                self.neighbors.read().par_iter().for_each(|(_, neighbor)| {
                                    if neighbor.id.to_u128() != msg.origin() {
                                        self.to_send_msg(&neighbor.connection, msg.clone());
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    fn handle_recv_msg(&self, msg: &Message) {
        match &msg {
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::AddMember(member),
                ..
            }) => {
                self.membership.write().add_member(member.clone());
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::RemoveMember(id),
                ..
            }) => {
                self.membership.write().remove_member((*id).into());
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::CheckReq(check_id),
                ..
            }) => {
                let result = self.neighbors.read().contains_key(&(*check_id).into());
                self.gossip_msg(&Message::eldegoss(
                    0,
                    EldegossMsgBody::CheckRsp(*check_id, result),
                ));
            }
            Message::EldegossMsg(EldegossMsg {
                body: EldegossMsgBody::CheckRsp(id, result),
                ..
            }) => {
                if *result {
                    self.membership
                        .write()
                        .wait_for_remove_member_list
                        .retain(|id_| &id_.to_u128() != id);
                }
            }
            _ => {
                self.to_recv_msg(msg.clone());
            }
        }
    }

    fn gossip_msg(&self, msg: &Message) {
        let neighbors = self.neighbors.read();
        if neighbors.len() <= config().gossip_fanout {
            neighbors.iter().for_each(|(_, neighbor)| {
                if neighbor.id.to_u128() != msg.origin() {
                    self.to_send_msg(&neighbor.connection, msg.clone());
                }
            });
        } else {
            let neighbor_ids = neighbors
                .iter()
                .filter_map(|(id, neighbor)| {
                    if id.to_u128() != msg.origin() {
                        Some(neighbor)
                    } else {
                        None
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut rng = rand::thread_rng();
            for _ in 0..config().gossip_fanout {
                let index = rng.gen_range(0..neighbor_ids.len());
                self.to_send_msg(&neighbor_ids[index].connection, msg.clone());
            }
        }
    }

    fn check_member(&self, check_id: EldegossId) -> Result<bool> {
        if let Some(check_member) = self.membership.read().member_map.get(&check_id) {
            check_member
                .neighbor_list
                .par_iter()
                .for_each(|neighbor_id| {
                    self.dispatch(
                        Message::eldegoss(
                            neighbor_id.to_u128(),
                            EldegossMsgBody::CheckReq(check_id.to_u128()),
                        ),
                        false,
                    );
                });
        }
        Ok(false)
    }

    async fn maintain_membership(&self) {
        {
            let mut membership = self.membership.write();
            while let Some(remove_id) = membership.wait_for_remove_member_list.pop() {
                info!("remove member: {}", remove_id);
                membership.remove_member(remove_id);
                self.dispatch(
                    Message::eldegoss(0, EldegossMsgBody::RemoveMember(remove_id.to_u128())),
                    false,
                );
            }
        }

        {
            loop {
                let check_id = self.membership.write().get_check_member();
                if let Some(check_id) = check_id {
                    info!("check member: {}", check_id);
                    let _ = self.check_member(check_id);
                } else {
                    break;
                }
            }
        }

        let mut remove_ids = vec![];
        {
            self.neighbors.read().values().for_each(|neighbor| {
                if let Some(reason) = neighbor.connection.close_reason() {
                    info!("neighbor({}) closed: {}", neighbor.id, reason);
                    remove_ids.push(neighbor.id);
                }
            });
        }

        {
            let mut neighbors = self.neighbors.write();
            let mut membership = self.membership.write();
            for remove_id in remove_ids {
                neighbors.remove(&remove_id);
                membership.add_check_member(remove_id);
            }
        }
    }
}

pub async fn write_msg(send: &mut SendStream, mut msg: Message) -> Result<()> {
    msg.set_origin(config().id);
    send.write_all(&encode_msg(&msg)).await?;
    send.finish().await?;
    Ok(())
}

pub async fn read_msg(mut recv: RecvStream) -> Result<Message> {
    let req = recv.read_to_end(config().msg_max_size).await?;
    decode_msg(&req)
}

async fn handle_stream(
    neighbor_id: EldegossId,
    mut recv: RecvStream,
    server: Server,
) -> Result<()> {
    let req = recv.read_to_end(config().msg_max_size).await?;
    let mut msg = decode_msg(&req)?;
    debug!("recv msg: {:?}", msg);
    msg.set_origin(neighbor_id.to_u128());

    server.dispatch(msg, true);
    Ok(())
}

pub async fn send_uni_msg(connection: Connection, msg: Message) -> Result<()> {
    let mut send = connection
        .open_uni()
        .await
        .map_err(|e| eyre!("send_uni_msg{msg:?} open uni failed: {e}"))?;
    send.write_all(&encode_msg(&msg))
        .await
        .map_err(|e| eyre!("send_uni_msg{msg:?} write_all failed: {:?}", e))?;
    send.finish()
        .await
        .map_err(|e| eyre!("send_uni_msg{msg:?} finish failed: {:?}", e))?;
    Ok(())
}
