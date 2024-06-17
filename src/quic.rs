use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use color_eyre::{eyre::eyre, Result};
use common_x::{
    graceful_shutdown::close_chain,
    tls::{create_any_server_name_config, read_certs, read_key},
};
use flume::{Receiver, Sender};
use foca::Identity;
use mini_moka::sync::Cache;
use quinn::{
    crypto::rustls::QuicClientConfig, ClientConfig, Connection, Endpoint, RecvStream, SendStream,
    ServerConfig, TransportConfig,
};
use tokio::sync::{Mutex, RwLock};
use uhlc::{Timestamp, ID};

use crate::{
    config::Config,
    membership::FocaEvent,
    protocol::{EldegossId, Message, Payload, Sample},
};

pub(crate) struct Link {
    eid: EldegossId,
    recv: Arc<Mutex<RecvStream>>,
    send: Mutex<SendStream>,
}

impl Link {
    #[inline]
    pub(crate) async fn send(&self, sample: &Sample) -> Result<()> {
        let row_bytes = sample.encode()?;
        let msg_len = row_bytes.len() as u32;
        let len_bytes = msg_len.to_le_bytes().to_vec();
        let bytes = [len_bytes, row_bytes.to_vec()].concat();
        self.send
            .lock()
            .await
            .write_all(&bytes)
            .await
            .map_err(|e| e.into())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_listener(
    mine_eid: EldegossId,
    config: Config,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
    inbound_foca_tx: Sender<FocaEvent>,
    inbound_msg_tx: Sender<Message>,
    outbound_msg_rvc: Receiver<Message>,
    connected_locators: Arc<Mutex<HashSet<String>>>,
    inbound_msg_cache: Cache<Timestamp, ()>,
) -> Result<()> {
    let Config {
        keep_alive_interval,
        cert_path,
        listen,
        private_key_path,
        ..
    } = config;

    let mut server_config =
        ServerConfig::with_single_cert(read_certs(cert_path)?, read_key(private_key_path)?)?;
    let mut transport_config = TransportConfig::default();
    transport_config.keep_alive_interval(Some(Duration::from_secs(keep_alive_interval)));
    server_config.transport_config(Arc::new(transport_config));
    let addr = listen.parse::<SocketAddr>()?;
    let endpoint = Endpoint::server(server_config, addr)?;
    info!("listening on {}", endpoint.local_addr()?);

    tokio::spawn(accept_task(
        mine_eid.clone(),
        endpoint,
        link_pool.clone(),
        inbound_foca_tx,
        inbound_msg_tx,
        connected_locators,
        inbound_msg_cache,
    ));

    tokio::spawn(start_outbound_sender(outbound_msg_rvc, mine_eid, link_pool));

    Ok(())
}

pub(crate) async fn start_outbound_sender(
    outbound_msg_rvc: Receiver<Message>,
    mine_eid: EldegossId,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
) {
    let close_handler = close_chain().lock().handler(1);
    loop {
        tokio::select! {
            Ok(msg) = outbound_msg_rvc.recv_async() => {
                gossip_msg(Sample::new_msg(msg), mine_eid.id(), mine_eid.id(), link_pool.clone()).await;
            }
            _ = close_handler.handle_async() => {
                info!("connector: Active shutdown");
                break;
            }
        }
    }
}

pub(crate) async fn start_connector(
    mine_eid: EldegossId,
    config: Config,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
    inbound_foca_tx: Sender<FocaEvent>,
    inbound_msg_tx: Sender<Message>,
    connected_locators: Arc<Mutex<HashSet<String>>>,
    inbound_msg_cache: Cache<Timestamp, ()>,
) -> Result<()> {
    let Config {
        ca_path,
        connect,
        keep_alive_interval,
        check_link_interval,
        ..
    } = config;

    let client_crypto = create_any_server_name_config(&ca_path)?;
    let quic_config: QuicClientConfig = client_crypto.try_into().unwrap();
    let mut client_config = ClientConfig::new(Arc::new(quic_config));
    let mut transport_config = TransportConfig::default();
    transport_config.keep_alive_interval(Some(Duration::from_secs(keep_alive_interval)));
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = Endpoint::client("[::]:0".parse::<std::net::SocketAddr>()?)?;
    endpoint.set_default_client_config(client_config.clone());

    for locator in &connect {
        connect_to(
            &endpoint,
            locator.to_owned(),
            &mine_eid,
            &link_pool,
            &inbound_foca_tx,
            &inbound_msg_tx,
            connected_locators.clone(),
            inbound_msg_cache.clone(),
        )
        .await
        .ok();
    }

    // reconnect
    tokio::spawn(async move {
        let close_handler = close_chain().lock().handler(1);
        let mut interval = tokio::time::interval(Duration::from_secs(check_link_interval));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = close_handler.handle_async() => {
                    info!("connector: Active shutdown");
                    break;
                }
                _ = interval.tick() => {
                    for locator in &connect {
                        if connected_locators.lock().await.contains(locator) {
                            continue;
                        }
                        info!("reconnect to: {locator}");
                        connect_to(
                            &endpoint,
                            locator.to_owned(),
                            &mine_eid,
                            &link_pool,
                            &inbound_foca_tx,
                            &inbound_msg_tx,
                            connected_locators.clone(),
                            inbound_msg_cache.clone(),
                        )
                        .await
                        .ok();
                    }
                }
            }
        }
    });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn connect_to(
    endpoint: &Endpoint,
    locator: String,
    mine_eid: &EldegossId,
    link_pool: &Arc<RwLock<HashMap<ID, Arc<Link>>>>,
    inbound_foca_tx: &Sender<FocaEvent>,
    inbound_msg_tx: &Sender<Message>,
    connected_locators: Arc<Mutex<HashSet<String>>>,
    inbound_msg_cache: Cache<Timestamp, ()>,
) -> Result<(), color_eyre::eyre::Error> {
    let connection = endpoint
        .connect(
            locator.parse::<std::net::SocketAddr>().unwrap(),
            "localhost",
        )?
        .await?;
    let (send, recv) = connection.open_bi().await?;
    if init_handshake(
        mine_eid.clone(),
        locator.to_string(),
        connection,
        send,
        recv,
        link_pool.clone(),
        inbound_foca_tx.clone(),
        inbound_msg_tx.clone(),
        connected_locators.clone(),
        inbound_msg_cache,
    )
    .await
    .map_err(|err| error!("{err:?}"))
    .is_ok()
    {
        connected_locators.lock().await.insert(locator);
    }
    Ok(())
}

async fn accept_task(
    mine_eid: EldegossId,
    endpoint: Endpoint,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
    inbound_foca_tx: Sender<FocaEvent>,
    inbound_msg_tx: Sender<Message>,
    connected_locators: Arc<Mutex<HashSet<String>>>,
    inbound_msg_cache: Cache<Timestamp, ()>,
) -> Result<()> {
    let close_handler = close_chain().lock().handler(1);

    let (new_link_sender, new_link_receiver) = flume::bounded(16);

    loop {
        tokio::select! {
            Some(connecting) = endpoint.accept() => {
                if let Ok(conn) = connecting.await {
                    if let Ok((send, recv)) = conn.accept_bi().await {
                        new_link_sender.send_async((conn, send, recv)).await.ok();
                    }
                }
            }
            Ok((conn, send, recv)) = new_link_receiver.recv_async() => {
                init_handshake(
                    mine_eid.clone(),
                    conn.remote_address().to_string(),
                    conn,
                    send,
                    recv,
                    link_pool.clone(),
                    inbound_foca_tx.clone(),
                    inbound_msg_tx.clone(),
                    connected_locators.clone(),
                    inbound_msg_cache.clone(),
                )
                .await
                .map_err(|err| error!("{err:?}"))
                .ok();
            }
            _ = close_handler.handle_async() => {
                info!("serve: Active shutdown");
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn init_handshake(
    mine_eid: EldegossId,
    locator: String,
    conn: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
    inbound_foca_tx: Sender<FocaEvent>,
    inbound_msg_tx: Sender<Message>,
    connected_locators: Arc<Mutex<HashSet<String>>>,
    inbound_msg_cache: Cache<Timestamp, ()>,
) -> Result<()> {
    let bytes = bincode::serialize(&mine_eid)?;
    send.write_all(&bytes).await?;
    let mut bytes = [0; 24];

    tokio::time::timeout(Duration::from_secs(2), recv.read_exact(&mut bytes)).await??;
    let other_eid: EldegossId = bincode::deserialize(&bytes)?;
    info!("handshake with {other_eid:?}");

    let link = Arc::new(Link {
        eid: other_eid.clone(),
        recv: Arc::new(Mutex::new(recv)),
        send: Mutex::new(send),
    });
    let recv = link.recv.clone();

    tokio::spawn(link_task(
        other_eid.clone(),
        locator,
        conn,
        recv,
        inbound_foca_tx.clone(),
        inbound_msg_tx,
        connected_locators,
        inbound_msg_cache,
        link_pool.clone(),
    ));

    // insert into link pool
    link_pool.write().await.insert(link.eid.addr(), link);

    // announce to this link
    inbound_foca_tx
        .send_async(FocaEvent::Announce(other_eid.clone()))
        .await
        .ok();

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn link_task(
    link_eid: EldegossId,
    locator: String,
    conn: Connection,
    recv: Arc<Mutex<RecvStream>>,
    inbound_foca_tx: Sender<FocaEvent>,
    inbound_msg_tx: Sender<Message>,
    connected_locators: Arc<Mutex<HashSet<String>>>,
    inbound_msg_cache: Cache<Timestamp, ()>,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
) -> Result<()> {
    let close_handler = close_chain().lock().handler(2);
    let mut recv = recv.lock().await;
    loop {
        tokio::select! {
            Ok(sample) = read_sample(&mut recv) => {
                dispatch(
                    inbound_msg_cache.clone(),
                    sample,
                    link_eid.id(),
                    inbound_msg_tx.clone(),
                    inbound_foca_tx.clone(),
                    link_pool.clone(),
                )
                .await;
            }
            _ = conn.closed() => {
                info!(
                    "Link({link_eid:?}) connection closed, reason: {:?}",
                    conn.close_reason()
                );
                break;
            }
            _ = close_handler.handle_async() => {
                info!("serve: Active shutdown");
                break;
            }
        }
    }

    connected_locators.lock().await.remove(&locator);

    Ok(())
}

#[inline]
async fn read_sample(recv: &mut RecvStream) -> Result<Sample> {
    let mut length = [0_u8, 0_u8, 0_u8, 0_u8];
    recv.read_exact(&mut length).await?;
    let n = u32::from_le_bytes(length) as usize;
    if n == 0 {
        warn!("read 0 bytes");
        return Err(eyre!("read 0 bytes"));
    }
    let mut bytes = vec![0_u8; n];
    recv.read_exact(&mut bytes).await?;

    Sample::decode(&bytes)
}

#[inline]
pub(crate) async fn dispatch(
    inbound_msg_cache: Cache<Timestamp, ()>,
    sample: Sample,
    received_from: ID,
    inbound_msg_tx: Sender<Message>,
    inbound_foca_tx: Sender<FocaEvent>,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
) {
    let timestamp = sample.header.timestamp();
    debug!("dispatch: {timestamp:?}");
    if inbound_msg_cache.contains_key(&timestamp) {
        debug!("duplicate msg: {:?}", timestamp);
        return;
    }

    inbound_msg_cache.insert(timestamp, ());

    match &sample.payload {
        Payload::FocaData(msg) => {
            inbound_foca_tx
                .send_async(FocaEvent::Data(msg.clone()))
                .await
                .ok();
        }
        Payload::Message(msg) => {
            inbound_msg_tx
                .send_async(Message::new(timestamp, msg.clone()))
                .await
                .ok();
        }
    };

    gossip_msg(sample, *timestamp.get_id(), received_from, link_pool).await;
}

#[inline]
async fn gossip_msg(
    sample: Sample,
    origin: ID,
    received_from: ID,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
) {
    for (eid, link) in link_pool.read().await.iter() {
        // not send to origin and not send to received_from
        if eid != &received_from && eid != &origin {
            let link = link.clone();
            link.send(&sample).await.ok();
        }
    }
}
