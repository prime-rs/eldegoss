use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use color_eyre::{eyre::eyre, Result};
use flume::{Receiver, Sender};
use mini_moka::sync::Cache;
use quinn::{
    crypto::rustls::QuicClientConfig, ClientConfig, Connection, Endpoint, Incoming, ServerConfig,
    TransportConfig,
};
use tokio::sync::{Mutex, RwLock};

use common_x::{
    graceful_shutdown::{close_chain, CloseHandler},
    tls::{create_any_server_name_config, read_certs, read_key},
};
use tokio::select;
use uhlc::{HLCBuilder, Timestamp, HLC};

use crate::{
    config::Config,
    link::Link,
    protocol::{Command, Message, Payload, Sample},
    util::{is_match, read_msg, write_msg},
    EldegossId, Member, Membership,
};

static CONFIG: OnceLock<Config> = OnceLock::new();
static ID: OnceLock<u128> = OnceLock::new();
static HLC: OnceLock<HLC> = OnceLock::new();

fn init_config(config: Config) {
    let id = EldegossId::from(&config.id);
    ID.set(id.to_u128()).unwrap();
    CONFIG.set(config).unwrap();
}

pub fn config() -> &'static Config {
    CONFIG.get_or_init(Config::default)
}

pub fn hlc() -> &'static HLC {
    HLC.get_or_init(|| {
        HLCBuilder::new()
            .with_id(uhlc::ID::try_from(id_u128()).unwrap())
            .build()
    })
}

pub fn id_u128() -> u128 {
    *ID.get_or_init(|| 1u128)
}

#[test]
fn test_hlc() {
    let hlc = HLCBuilder::new()
        .with_id(uhlc::ID::try_from(u128::MAX).unwrap())
        .build();
    let hlc1 = HLCBuilder::new()
        .with_id(uhlc::ID::try_from(1u128).unwrap())
        .build();
    let t = hlc.new_timestamp();
    let t1 = hlc1.new_timestamp();
    println!("{t}");
    println!("{t1}");
    assert!(t1 > t);
}

type MsgForRecv = (Sender<Message>, Receiver<Message>);

pub struct Subscriber {
    pub key_expr: String,
    pub callback: Box<dyn FnMut(Message) + Send + Sync + 'static>,
}

impl Subscriber {
    pub fn new<C>(key_expr: &str, callback: C) -> Self
    where
        C: FnMut(Message) + Send + Sync + 'static,
    {
        Self {
            key_expr: key_expr.to_owned(),
            callback: Box::new(callback),
        }
    }
}

pub struct Session {
    pub(crate) sender: Sender<Message>,
    pub(crate) membership: Arc<Mutex<Membership>>,
    pub(crate) close_handler: CloseHandler,
}

impl Drop for Session {
    fn drop(&mut self) {
        close_chain().lock().close();
        self.close_handler.handle();
        info!("Session: Active shutdown");
    }
}

impl Session {
    pub async fn serve(config: Config, subscribers: Vec<Subscriber>) -> Self {
        let (msg_for_send_tx, msg_for_send_rv) = flume::bounded(10240);
        let close_handler = close_chain().lock().handler(0);
        let close_handler_rt = close_chain().lock().handler(1);
        let session_runtime = SessionRuntime::init(config, msg_for_send_rv);

        tokio::spawn(session_runtime.clone().run_server());

        tokio::spawn(session_runtime.clone().connector());

        let session_runtime_ = session_runtime.clone();
        tokio::spawn(async move {
            select! {
                _ = session_runtime_.handle_send() => {}
                _ = session_runtime_.handle_recv(subscribers) => {}
                _ = close_handler_rt.handle_async() => {
                    info!("handle msg: Active shutdown");
                }
            }
        });

        Self {
            membership: session_runtime.membership.clone(),
            sender: msg_for_send_tx,
            close_handler,
        }
    }

    pub fn sender(&self) -> Sender<Message> {
        self.sender.clone()
    }

    pub async fn membership(&self) -> tokio::sync::MutexGuard<'_, Membership> {
        self.membership.lock().await
    }
}

#[derive(Clone)]
pub(crate) struct SessionRuntime {
    pub(crate) msg_for_recv: MsgForRecv,
    pub(crate) msg_for_send: Receiver<Message>,
    pub(crate) links: Arc<RwLock<HashMap<EldegossId, Sender<Arc<Sample>>>>>,
    pub(crate) membership: Arc<Mutex<Membership>>,
    pub(crate) connected_locators: Arc<Mutex<HashSet<String>>>,
    pub(crate) check_member_list: Arc<Mutex<Vec<EldegossId>>>,
    pub(crate) wait_for_remove_member_list: Arc<Mutex<Vec<EldegossId>>>,
    pub(crate) cache: Cache<Timestamp, ()>,
}

impl SessionRuntime {
    fn init(config_: Config, msg_for_send: Receiver<Message>) -> Self {
        init_config(config_);
        let member = Member::new(id_u128().into(), vec![]);
        let mut membership = Membership::default();
        membership.add_member(member);

        // cache
        let cache: Cache<Timestamp, ()> = Cache::builder()
            .weigher(|_, _| 128u32 + 64u32)
            .max_capacity(1024 * 8)
            .time_to_live(Duration::from_secs(1))
            .build();

        Self {
            msg_for_recv: flume::unbounded(),
            links: Default::default(),
            membership: Arc::new(Mutex::new(membership)),
            connected_locators: Default::default(),
            check_member_list: Default::default(),
            wait_for_remove_member_list: Default::default(),
            msg_for_send,
            cache,
        }
    }

    async fn handle_send(&self) {
        while let Ok(msg) = self.msg_for_send.recv_async().await {
            self.send_msg(msg.sample()).await;
        }
    }

    async fn handle_recv(&self, subscribers: Vec<Subscriber>) {
        let mut subscribers = subscribers
            .into_iter()
            .map(|subscriber| (subscriber.key_expr, subscriber.callback))
            .collect::<HashMap<_, _>>();

        while let Ok(msg) = self.msg_for_recv.1.recv_async().await {
            if let Some(callback) = subscribers.get_mut(&msg.key_expr) {
                callback(msg);
            }
        }
    }

    #[inline]
    async fn send_msg(&self, msg: Sample) {
        let msg = Arc::new(msg);
        self.gossip_msg(msg, 0).await;
    }

    #[inline]
    async fn to_recv_msg(&self, msg: &Sample) {
        if let Err(e) = self.msg_for_recv.0.send_async(msg.message()).await {
            debug!("to_recv_msg failed: {:?}", e);
        }
    }

    async fn connector(self) -> Result<()> {
        let Config {
            ca_path,
            connect,
            keep_alive_interval,
            check_link_interval,
            ..
        } = &config();

        let client_crypto = create_any_server_name_config(ca_path)?;
        let quic_config: QuicClientConfig = client_crypto.try_into().unwrap();
        let mut client_config = ClientConfig::new(Arc::new(quic_config));
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        client_config.transport_config(Arc::new(transport_config));

        for conn in connect {
            if self.connected_locators.lock().await.contains(conn) {
                continue;
            }
            self.connect_to(&client_config, conn.to_string()).await.ok();
        }

        tokio::spawn(async move {
            let Config { connect, .. } = &config();
            let close_handler = close_chain().lock().handler(1);
            let mut interval = tokio::time::interval(Duration::from_secs(*check_link_interval));
            interval.tick().await;
            loop {
                select! {
                    _ = close_handler.handle_async() => {
                        info!("connector: Active shutdown");
                        break;
                    }
                    _ = interval.tick() => {
                        for conn in connect {
                            if self.connected_locators.lock().await.contains(conn) {
                                continue;
                            }
                            info!("reconnect to: {conn}");
                            self
                                .connect_to(&client_config, conn.to_string())
                                .await
                                .ok();
                        }
                    }
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
            self.join(connection, connect).await.ok();
        }
        Ok(())
    }

    async fn run_server(self) -> Result<()> {
        let Config {
            keep_alive_interval,
            cert_path,
            listen,
            private_key_path,
            check_link_interval,
            ..
        } = config();
        let mut server_config = ServerConfig::with_single_cert(
            read_certs(cert_path.to_string())?,
            read_key(private_key_path.to_string())?,
        )?;
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(*keep_alive_interval)));
        server_config.transport_config(Arc::new(transport_config));
        let addr = listen.parse::<SocketAddr>()?;
        let endpoint = Endpoint::server(server_config, addr)?;
        info!("listening on {}", endpoint.local_addr()?);
        let mut check_link_interval =
            tokio::time::interval(Duration::from_secs(*check_link_interval));
        check_link_interval.tick().await;
        let close_handler = close_chain().lock().handler(1);
        loop {
            select! {
                Some(connecting) = endpoint.accept() => {
                    debug!("connection incoming");
                    tokio::spawn(self.clone().handle_join_request(connecting));
                    debug!("connected");
                }
                _ = close_handler.handle_async() => {
                    info!("serve: Active shutdown");
                    return Ok(());
                }
                _ = check_link_interval.tick() => self.maintain_membership().await
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
                write_msg(
                    &mut tx,
                    Sample::control(
                        Command::JoinReq(vec![]),
                    ),
                )
                .await.ok();
                let msg = read_msg(&mut rv).await?;
                if let Payload::Control(Command::JoinRsp((is_existed, membership))) = &msg.payload
                {
                    let origin = msg.origin();
                    {
                        self.membership.lock().await.merge(membership);
                    }

                    if *is_existed {
                        if self.links.read().await.contains_key(&origin.into()) {
                            info!("update link({origin}): {remote_address}");
                            self.connected_locators.lock().await.insert(locator.clone());
                        }
                        return Err(eyre!("link({origin}) already exists: {remote_address}"));
                    }

                    let (send, recv) = flume::bounded(1024);
                    let link = Link {
                        id: origin.into(),
                        locator: locator.clone(),
                        connection,
                        session: self.clone(),
                        send: tx,
                        recv: rv,
                        msg_to_send: recv,
                        close_handler: close_chain().lock().handler(2),
                    };
                    let mut is_old = false;
                    if self.links.read().await.contains_key(&link.id()) {
                        info!("update link({origin}): {remote_address}");
                        self.connected_locators.lock().await.insert(locator.clone());
                        is_old = true;
                    }
                    if !is_old {
                        let id = link.id();
                        tokio::spawn(link.handle());
                        info!("new link({origin}): {remote_address}");
                        self.insert_link(id, locator, send).await;
                    }

                    Ok(())
                } else {
                    debug!("invalid join response: {remote_address}");
                    Err(eyre!("invalid join response: {remote_address}"))
                }
            }
        }
    }

    async fn handle_join_request(self, connecting: Incoming) -> Result<()> {
        match connecting.await {
            Ok(connection) => {
                let remote_address = connection.remote_address();
                if let Ok((mut tx, mut rv)) = connection.accept_bi().await {
                    let msg = read_msg(&mut rv).await?;

                    if let Payload::Control(Command::JoinReq(meta_data)) = &msg.payload {
                        let origin = msg.origin();
                        let mut is_reconnect = false;
                        let is_new = if let Some(link) = self.links.read().await.get(&origin.into())
                        {
                            if link.is_disconnected() {
                                is_reconnect = true;
                            }
                            false
                        } else {
                            true
                        };

                        let membership = self.membership.clone();
                        debug!("memberlist: {membership:#?}");

                        if !is_new && !is_reconnect {
                            write_msg(
                                &mut tx,
                                Sample::control(Command::JoinRsp((
                                    true,
                                    membership.lock().await.clone(),
                                ))),
                            )
                            .await
                            .ok();
                            info!("link({origin}) already exists: {remote_address}");
                            tx.stopped().await.ok();
                            return Err(eyre!("link({origin}) already exists: {remote_address}"));
                        }

                        write_msg(
                            &mut tx,
                            Sample::control(Command::JoinRsp((
                                false,
                                membership.lock().await.clone(),
                            ))),
                        )
                        .await
                        .ok();

                        let member = Member::new(origin.into(), meta_data.to_vec());
                        {
                            self.membership.lock().await.add_member(member.clone());
                        }

                        self.send_msg(Sample::control(Command::AddMember(member)))
                            .await;

                        let (send, recv) = flume::bounded(1024);
                        let link = Link {
                            id: origin.into(),
                            locator: remote_address.to_string(),
                            connection,
                            session: self.clone(),
                            send: tx,
                            recv: rv,
                            msg_to_send: recv,
                            close_handler: close_chain().lock().handler(2),
                        };
                        let id = link.id();
                        tokio::spawn(link.handle());
                        info!("new link({origin}): {remote_address}");

                        self.insert_link(id, remote_address.to_string(), send).await;

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

    async fn insert_link(&self, id: EldegossId, locator: String, link: Sender<Arc<Sample>>) {
        let id_u128 = id.to_u128();
        self.wait_for_remove_member_list
            .lock()
            .await
            .retain(|x| x.to_u128() != id_u128);
        self.check_member_list
            .lock()
            .await
            .retain(|x| x.to_u128() != id_u128);

        self.connected_locators.lock().await.insert(locator);
        self.links.write().await.insert(id, link);
    }

    #[inline]
    pub(crate) async fn dispatch(&self, msg: Arc<Sample>, received_from: u128) {
        if self.cache.contains_key(&msg.timestamp) {
            debug!("duplicate msg: {:?}", msg.timestamp);
            return;
        }
        self.cache.insert(msg.timestamp, ());

        if config()
            .subscription_list
            .iter()
            .filter(|s| is_match(s, msg.key_expr.as_str()))
            .count()
            > 0
        {
            self.handle_recv_msg(&msg).await;
        }
        self.gossip_msg(msg, received_from).await;
    }

    #[inline]
    async fn handle_recv_msg(&self, msg: &Sample) {
        match &msg.payload {
            Payload::Control(Command::AddMember(member)) => {
                self.membership.lock().await.add_member(member.clone());
            }
            Payload::Control(Command::RemoveMember(id)) => {
                self.membership.lock().await.remove_member((*id).into());
            }
            Payload::Control(Command::CheckReq(check_id)) => {
                let result = id_u128() == *check_id
                    || self.links.read().await.contains_key(&(*check_id).into());
                debug!("check member: {} result: {}", check_id, result);
                let msg = Sample::control(Command::CheckRsp(*check_id, result));
                self.send_msg(msg).await;
            }
            Payload::Control(Command::CheckRsp(id, result)) => {
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
    async fn gossip_msg(&self, msg: Arc<Sample>, received_from: u128) {
        if self.links.read().await.is_empty() || (received_from != 0 && msg.origin() == id_u128()) {
            return;
        }
        let links = self
            .links
            .read()
            .await
            .iter()
            .filter_map(|(eid, tx)| {
                let id = eid.to_u128();
                if id != msg.origin() && id != received_from {
                    Some((*eid, tx.clone()))
                } else {
                    None
                }
            })
            .clone()
            .collect::<Vec<_>>();

        for link in links {
            if link
                .1
                .send_timeout(msg.clone(), Duration::from_millis(500))
                .is_err()
            {
                self.links.write().await.remove(&link.0);
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
                self.send_msg(Sample::control(Command::RemoveMember(remove_id.to_u128())))
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
                    self.send_msg(Sample::control(Command::CheckReq(check_id.to_u128())))
                        .await;
                } else {
                    break;
                }
            }
        }
    }
}
