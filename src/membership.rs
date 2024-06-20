use std::collections::HashMap;
use std::num::{NonZeroU8, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use bincode::DefaultOptions;
use bytes::Bytes;
use color_eyre::eyre::eyre;
use color_eyre::Result;
use flume::Sender;
use foca::{BincodeCodec, Config as FocaConfig, Foca, Identity, Notification, Timer};
use foca::{NoCustomBroadcast, PeriodicParams};
use rand::{rngs::StdRng, SeedableRng};
use tokio::sync::RwLock;
use uhlc::ID;

use crate::protocol::{EldegossId, Sample};
use crate::quic::{gossip_msg, Link};

pub(crate) enum FocaEvent {
    Data(Bytes),
    Announce(EldegossId),
    Timer(Timer<EldegossId>),
}

impl Identity for EldegossId {
    type Addr = ID;

    fn renew(&self) -> Option<Self> {
        Some(Self::new(
            uhlc::HLCBuilder::new()
                .with_id(self.addr())
                .build()
                .new_timestamp(),
        ))
    }

    fn addr(&self) -> ID {
        self.id()
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.clock() > adversary.clock()
    }
}

pub(crate) type Membership =
    Arc<RwLock<Foca<EldegossId, BincodeCodec<DefaultOptions>, StdRng, NoCustomBroadcast>>>;

pub(crate) async fn start_foca(
    identity: EldegossId,
    link_pool: Arc<RwLock<HashMap<EldegossId, Arc<Link>>>>,
) -> Result<(Membership, Sender<FocaEvent>)> {
    let (foca_event_tx, foca_event_rv) = flume::bounded(1024);
    let (outbound_foca_data_tx, outbound_foca_data_rv) = flume::bounded(1024);

    let foca_config = {
        let period = Duration::from_secs(15);
        FocaConfig {
            probe_period: period,
            probe_rtt: Duration::from_secs(5),
            num_indirect_probes: NonZeroUsize::new(5).unwrap(),
            max_transmissions: NonZeroU8::new(10).unwrap(),
            suspect_to_down_after: Duration::from_secs(30),
            remove_down_after: Duration::from_secs(60 * 60 * 24),
            max_packet_size: NonZeroUsize::new(1024 * 1024).unwrap(),
            notify_down_members: true,
            periodic_announce: Some(PeriodicParams {
                frequency: Duration::from_secs(30),
                num_members: NonZeroUsize::new(1).unwrap(),
            }),
            periodic_announce_to_down_members: Some(PeriodicParams {
                frequency: Duration::from_secs(65),
                num_members: NonZeroUsize::new(2).unwrap(),
            }),
            periodic_gossip: Some(PeriodicParams {
                frequency: Duration::from_millis(200),
                num_members: NonZeroUsize::new(3).unwrap(),
            }),
        }
    };
    let foca = Arc::new(RwLock::new(Foca::new(
        identity.clone(),
        foca_config,
        StdRng::from_entropy(),
        BincodeCodec(bincode::DefaultOptions::new()),
    )));
    let membership = foca.clone();

    let mut runtime = foca::AccumulatingRuntime::new();

    let foca_event_tx_ = foca_event_tx.clone();
    tokio::spawn(async move {
        while let Ok(input) = foca_event_rv.recv_async().await {
            let result = match input {
                FocaEvent::Timer(timer) => foca
                    .write()
                    .await
                    .handle_timer(timer, &mut runtime)
                    .map_err(|err| eyre!("Error handling timer: {err:?}")),
                FocaEvent::Data(data) => foca
                    .write()
                    .await
                    .handle_data(&data, &mut runtime)
                    .map_err(|err| eyre!("Error handling data: {err:?}")),
                FocaEvent::Announce(dst) => foca
                    .write()
                    .await
                    .announce(dst, &mut runtime)
                    .map_err(|err| eyre!("Error announce: {err:?}")),
            };

            if let Err(error) = result {
                error!("foca handle event error: {error}");
            }

            while let Some((dst, data)) = runtime.to_send() {
                outbound_foca_data_tx.send_async((dst, data)).await.ok();
            }

            while let Some((delay, event)) = runtime.to_schedule() {
                let foca_event_tx_ = foca_event_tx_.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    foca_event_tx_
                        .send_async(FocaEvent::Timer(event))
                        .await
                        .ok();
                });
            }

            while let Some(notification) = runtime.to_notify() {
                match notification {
                    Notification::MemberUp(eid) => info!("member up: {eid:?}"),
                    Notification::MemberDown(eid) => info!("member down: {eid:?}"),
                    Notification::Idle => info!("cluster empty"),
                    Notification::Rename(old, new) => {
                        info!("member {old:?} is now known as {new:?}")
                    }
                    Notification::Active => info!("current identity is active"),
                    Notification::Defunct => {
                        info!("current identity is defunct, need change identity")
                    }
                    Notification::Rejoin(eid) => info!("member rejoin: {eid:?}"),
                }
                let members = foca
                    .read()
                    .await
                    .iter_members()
                    .map(|member| member.id().to_owned())
                    .collect::<Vec<_>>();
                info!("membership: {members:#?}");
            }
        }
    });

    tokio::spawn(async move {
        while let Ok((_eid, data)) = outbound_foca_data_rv.recv_async().await {
            // info!("outbound foca msg: {eid:?}");
            gossip_msg(
                Sample::new_foca(identity.hlc().new_timestamp(), data.clone()),
                identity.id(),
                link_pool.clone(),
            )
            .await;
        }
    });

    Ok((membership, foca_event_tx))
}
