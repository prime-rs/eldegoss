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
    pub server: Server,
}

impl Neighbor {
    async fn handle(self) {
        select! {
            _ = self.read_uni() => {}
            _ = self.read_datagrams() => {}
        }
    }

    // use for app msg
    async fn read_uni(&self) {
        while let Ok(stream) = self.connection.accept_uni().await {
            tokio::spawn(handle_stream(self.id, stream, self.server.clone()));
        }
    }

    // TODO: use for app msg now, maybe use for other purpose
    async fn read_datagrams(&self) {
        while let Ok(datagram) = self.connection.read_datagram().await {
            if let Ok(mut msg) = bincode::deserialize::<Message>(&datagram) {
                debug!("recv msg: {:?}", msg);
                msg.origin = self.id.to_u128();

                self.server.dispatch(msg, true);
            }
        }
    }
}

type MsgForSend = (
    Sender<(Connection, Message)>,
    Receiver<(Connection, Message)>,
);

#[derive(Debug, Clone)]
pub struct Server {
    pub msg_to_app: Sender<Message>,
    pub msg_for_send: MsgForSend,
    pub neighbors: Arc<RwLock<HashMap<EldegossId, Neighbor>>>,
    pub membership: Arc<RwLock<Membership>>,
    pub subscription_list: Arc<RwLock<HashSet<String>>>,
}

impl Server {
    pub fn new(tx: Sender<Message>) -> Self {
        let member = Member::new(config().id.into());
        let mut membership = Membership::default();
        membership.add_member(member);
        Self {
            msg_to_app: tx,
            neighbors: Arc::new(RwLock::new(HashMap::new())),
            membership: Arc::new(RwLock::new(membership)),
            subscription_list: Arc::new(RwLock::new(HashSet::new())),
            msg_for_send: flume::unbounded(),
        }
    }

    pub async fn serve(&self) {
        tokio::spawn(self.clone().run_server());
        let _ = self.connect().await;
    }

    pub fn send_msg(&self, msg: Message) {
        self.dispatch(msg, false)
    }

    fn to_send_msg(&self, connection: &Connection, mut msg: Message) {
        msg.origin = config().id;
        if let Err(e) = self.msg_for_send.0.send((connection.clone(), msg)) {
            error!("to_send_msg failed: {:?}", e);
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
                    send_uni_msg(connection, msg).await;
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
                let _ = write_msg(&mut tx, Message::to(0, MessageBody::JoinReq(
                    subscription_list.into_iter().collect(),
                ))).await;
                let req = rv.read_to_end(*msg_max_size).await?;
                let msg = bincode::deserialize::<Message>(&req)?;
                if let MessageBody::JoinRsp(membership) = msg.body {
                    self.membership.write().merge(&membership);

                    debug!("membership: {:#?}", self.membership.read());

                    let neighbor = Neighbor {
                        id: msg.origin.into(),
                        connection,
                        server: self.clone(),
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
                    Ok((mut tx, mut rv)) = connection.accept_bi() => {
                        let req = rv.read_to_end(*msg_max_size).await?;
                        let msg = bincode::deserialize::<Message>(&req)?;

                        if self.membership.read().contains(&msg.origin.into()) {
                            return Err(eyre!("already joined: {}", msg.origin));
                        }

                        if let MessageBody::JoinReq(subscription_list) = msg.body {
                            let mut neighbor_list = HashSet::new();
                            neighbor_list.insert((*id).into());
                            let member = Member {
                                id: msg.origin.into(),
                                subscription_list: HashSet::from_iter(subscription_list),
                                neighbor_list,
                            };
                            self.membership.write().add_member(member.clone());

                            self.dispatch(
                                Message::publish(
                                    "".to_string(),
                                    MessageBody::AddMember(member),
                                ),
                                false,
                            );

                            let memberlist = self.membership.read().clone();
                            debug!("memberlist: {:#?}", memberlist);

                            let _ = write_msg(&mut tx, Message::to(
                                msg.origin,
                                MessageBody::JoinRsp(
                                    memberlist,
                                ),
                            )).await;

                            let neighbor = Neighbor {
                                id: msg.origin.into(),
                                connection,
                                server: self.clone(),
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

    fn dispatch(&self, msg: Message, is_received: bool) {
        debug!("dispatch msg: {:?}", msg);
        match (msg.to, msg.topic.as_str()) {
            (0, "") => {
                if is_received {
                    self.handle_recv_msg(&msg);
                }

                self.gossip_msg(&msg);
            }
            (0, topic) => {
                if is_received && self.subscription_list.read().contains(topic) {
                    let _ = self.msg_to_app.send(msg.clone());
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
                        let _ = self.msg_to_app.send(msg.clone());
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
                                    if neighbor.id.to_u128() != msg.origin {
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
        match &msg.body {
            MessageBody::AddMember(member) => {
                self.membership.write().add_member(member.clone());
            }
            MessageBody::RemoveMember(id) => {
                self.membership.write().remove_member((*id).into());
            }
            MessageBody::CheckReq(check_id) => {
                let result = self.neighbors.read().contains_key(&(*check_id).into());
                self.gossip_msg(&Message::publish(
                    "".to_string(),
                    MessageBody::CheckRsp(*check_id, result),
                ));
            }
            MessageBody::CheckRsp(id, result) => {
                if *result {
                    self.membership
                        .write()
                        .wait_for_remove_member_list
                        .retain(|id_| &id_.to_u128() != id);
                }
            }
            _ => {
                let _ = self.msg_to_app.send(msg.clone());
            }
        }
    }

    fn gossip_msg(&self, msg: &Message) {
        if self.neighbors.read().len() <= config().gossip_fanout {
            self.neighbors.read().par_iter().for_each(|(_, neighbor)| {
                if neighbor.id.to_u128() != msg.origin {
                    self.to_send_msg(&neighbor.connection, msg.clone());
                }
            });
        } else {
            let neighbor_ids = self
                .neighbors
                .read()
                .iter()
                .filter_map(|(id, neighbor)| {
                    if id.to_u128() != msg.origin {
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
                        Message::to(
                            neighbor_id.to_u128(),
                            MessageBody::CheckReq(check_id.to_u128()),
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
                debug!("remove member: {}", remove_id);
                membership.remove_member(remove_id);
                self.dispatch(
                    Message::publish(
                        "".to_string(),
                        MessageBody::RemoveMember(remove_id.to_u128()),
                    ),
                    false,
                );
            }
        }

        {
            loop {
                let check_id = self.membership.write().get_check_member();
                if let Some(check_id) = check_id {
                    debug!("check member: {}", check_id);
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
    server: Server,
) -> Result<()> {
    let req = recv.read_to_end(config().msg_max_size).await?;
    let mut msg = bincode::deserialize::<Message>(&req)?;
    debug!("recv msg: {:?}", msg);
    msg.origin = neighbor_id.to_u128();

    server.dispatch(msg, true);
    Ok(())
}

pub async fn send_uni_msg(connection: Connection, msg: Message) {
    let connection = connection.clone();
    let mut send = connection
        .open_uni()
        .await
        .map_err(|e| error!("send_uni_msg{msg:?} open uni failed: {e}"))
        .unwrap();
    send.write_all(
        &bincode::serialize(&msg)
            .map_err(|e| error!("send_uni_msg{msg:?} serialize failed: {:?}", e))
            .unwrap(),
    )
    .await
    .map_err(|e| error!("send_uni_msg{msg:?} write_all failed: {:?}", e))
    .unwrap();
    send.finish()
        .await
        .map_err(|e| error!("send_uni_msg{msg:?} finish failed: {:?}", e))
        .unwrap();
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use tokio::time::Instant;

    use crate::{
        quic::{config, init_config, Server},
        Config, Message, MessageBody,
    };

    struct Stats {
        round_count: usize,
        round_size: usize,
        finished_rounds: usize,
        round_start: Instant,
        global_start: Option<Instant>,
    }
    impl Stats {
        fn new(round_size: usize) -> Self {
            Stats {
                round_count: 0,
                round_size,
                finished_rounds: 0,
                round_start: Instant::now(),
                global_start: None,
            }
        }
        fn increment(&mut self) {
            if self.round_count == 0 {
                self.round_start = Instant::now();
                if self.global_start.is_none() {
                    self.global_start = Some(self.round_start)
                }
                self.round_count += 1;
            } else if self.round_count < self.round_size {
                self.round_count += 1;
            } else {
                self.print_round();
                self.finished_rounds += 1;
                self.round_count = 0;
            }
        }
        fn print_round(&self) {
            let elapsed = self.round_start.elapsed().as_secs_f64();
            let throughtput = (self.round_size as f64) / elapsed;
            info!("{throughtput} msg/s");
        }
    }
    impl Drop for Stats {
        fn drop(&mut self) {
            let elapsed = self.global_start.unwrap().elapsed().as_secs_f64();
            let total = self.round_size * self.finished_rounds + self.round_count;
            let throughtput = total as f64 / elapsed;
            info!("Received {total} messages over {elapsed:.2}s: {throughtput}msg/s");
        }
    }

    #[tokio::test]
    async fn neighbor0() {
        common_x::log::init_log_filter("info");

        init_config(Config {
            id: 1,
            listen: "[::]:4721".to_string(),
            cert_path: "./config/cert/server_cert.pem".into(),
            private_key_path: "./config/cert/server_key.pem".into(),
            ca_path: "./config/cert/ca_cert.pem".into(),
            ..Default::default()
        });
        info!("id: {}", config().id);

        let (tx, rx) = flume::bounded(80000);
        let server = Server::new(tx);

        server.serve().await;
        let mut stats = Stats::new(1000);
        while (rx.recv_async().await).is_ok() {
            stats.increment();
        }
    }

    #[tokio::test]
    async fn neighbor1() {
        common_x::log::init_log_filter("info");

        init_config(Config {
            connect: ["127.0.0.1:4721".to_string()].to_vec(),
            listen: "[::]:0".to_string(),
            cert_path: "./config/cert/client_cert.pem".into(),
            private_key_path: "./config/cert/client_key.pem".into(),
            ca_path: "./config/cert/ca_cert.pem".into(),
            ..Default::default()
        });
        info!("id: {}", config().id);
        let server = Server::new(flume::unbounded().0);

        let mut send_test_msg_interval = tokio::time::interval(Duration::from_secs(1));
        let mut send_test_msg_interval1 = tokio::time::interval(Duration::from_micros(30));
        server.serve().await;

        send_test_msg_interval.tick().await;
        let mut stats = Stats::new(1000);
        loop {
            send_test_msg_interval1.tick().await;
            let msg = Message::to(1, MessageBody::Data(vec![0; 1024]));
            server.send_msg(msg);
            stats.increment();
        }
    }
}
