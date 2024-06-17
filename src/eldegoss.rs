use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use color_eyre::{eyre::eyre, Result};
use flume::{Receiver, Sender};
use mini_moka::sync::Cache;
use tokio::sync::{Mutex, RwLock};
use uhlc::{Timestamp, ID};

use crate::{
    config::Config,
    membership::{start_foca, FocaEvent, Membership},
    protocol::{EldegossId, Message},
    quic::{start_connector, start_listener},
};

type MessageChannel = (Sender<Message>, Receiver<Message>);

pub struct Eldegoss {
    eid: EldegossId,
    hlc: uhlc::HLC,
    config: Config,
    membership: Membership,
    inbound_msg_channel: MessageChannel,
    outbound_msg_channel: MessageChannel,
    inbound_notification_channel: MessageChannel,
    outbound_notification_channel: MessageChannel,
}

impl Eldegoss {
    pub const fn eid(&self) -> &EldegossId {
        &self.eid
    }

    pub const fn hlc(&self) -> &uhlc::HLC {
        &self.hlc
    }

    pub const fn config(&self) -> &Config {
        &self.config
    }

    pub async fn num_members(&self) -> usize {
        self.membership.read().await.num_members()
    }

    pub async fn members(&self) -> Vec<EldegossId> {
        self.membership
            .read()
            .await
            .iter_members()
            .map(|member| member.id().to_owned())
            .collect::<Vec<_>>()
    }

    pub async fn recv(&self) -> Result<Message> {
        self.inbound_msg_channel
            .1
            .recv_async()
            .await
            .map_err(|err| eyre!("{err:?}"))
    }

    pub async fn send(&self, payload: Vec<u8>) -> Result<()> {
        self.outbound_msg_channel
            .0
            .send_async(Message::new(self.hlc.new_timestamp(), payload.into()))
            .await
            .map_err(|err| eyre!("{err:?}"))
    }

    pub async fn recv_notification(&self) -> Result<Message> {
        self.inbound_notification_channel
            .1
            .recv_async()
            .await
            .map_err(|err| eyre!("{err:?}"))
    }

    pub async fn send_notification(&self, payload: Vec<u8>) -> Result<()> {
        self.outbound_notification_channel
            .0
            .send_async(Message::new(self.hlc.new_timestamp(), payload.into()))
            .await
            .map_err(|err| eyre!("{err:?}"))
    }
}

// serve
impl Eldegoss {
    pub async fn serve(config: Config) -> Result<Self> {
        let hlc = uhlc::HLCBuilder::new()
            .with_id(ID::from_str(&config.id).map_err(|err| eyre!("{err:?}"))?)
            .build();
        let eid = EldegossId::new(hlc.new_timestamp());

        let link_pool = Arc::new(RwLock::new(HashMap::new()));
        let connected_locators = Arc::new(Mutex::new(HashSet::new()));

        let inbound_notification_channel = flume::bounded(10240);
        let outbound_notification_channel = flume::bounded(10240);
        let inbound_msg_channel = flume::bounded(10240);
        let outbound_msg_channel = flume::bounded(10240);

        // cache
        let inbound_msg_cache: Cache<Timestamp, ()> = Cache::builder()
            .weigher(|_, _| 128u32 + 64u32)
            .max_capacity(1024 * 8)
            .time_to_live(Duration::from_secs(1))
            .build();

        let (membership, foca_event_tx) = start_foca(
            eid.clone(),
            inbound_notification_channel.0.clone(),
            outbound_notification_channel.1.clone(),
            link_pool.clone(),
        )
        .await?;

        start_listener(
            eid.clone(),
            config.clone(),
            link_pool.clone(),
            foca_event_tx.clone(),
            inbound_msg_channel.0.clone(),
            outbound_msg_channel.1.clone(),
            connected_locators.clone(),
            inbound_msg_cache.clone(),
        )
        .await?;

        start_connector(
            eid.clone(),
            config.clone(),
            link_pool,
            foca_event_tx.clone(),
            inbound_msg_channel.0.clone(),
            connected_locators,
            inbound_msg_cache,
        )
        .await?;

        // Foca and Network is running, we can tell it to announce to our target
        for announce in &config.announce {
            let dst = EldegossId::from_str(announce).map_err(|err| eyre!("{err:?}"))?;
            foca_event_tx
                .send_async(FocaEvent::Announce(dst))
                .await
                .ok();
        }

        Ok(Self {
            eid,
            hlc,
            config,
            membership,
            inbound_msg_channel,
            outbound_msg_channel,
            inbound_notification_channel,
            outbound_notification_channel,
        })
    }
}
