use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

use bincode::DefaultOptions;
use bytes::Bytes;
use color_eyre::eyre::eyre;
use color_eyre::Result;
use common_x::graceful_shutdown::close_chain;
use flume::Sender;
use foca::NoCustomBroadcast;
use foca::{BincodeCodec, Config as FocaConfig, Foca, Identity, Notification, Timer};
use rand::{rngs::StdRng, SeedableRng};
use tokio::sync::RwLock;
use tokio::time::{sleep_until, Instant};
use uhlc::ID;

use crate::protocol::{EldegossId, Sample};
use crate::quic::Link;

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

async fn launch_scheduler(timer_tx: Sender<FocaEvent>) -> Sender<(Instant, Timer<EldegossId>)> {
    let (tx, rx) = flume::bounded(1024);

    let mut queue = TimerQueue::new();
    tokio::spawn(async move {
        'handler: loop {
            let now = Instant::now();

            macro_rules! submit_event {
                ($event:expr) => {
                    if let Err(err) = timer_tx.send_async(FocaEvent::Timer($event)).await {
                        error!(
                            "Error submitting timer event. Shutting down timer task: {}",
                            err
                        );
                        break 'handler;
                    }
                };
                ($when:expr, $event:expr) => {
                    if $when < now {
                        submit_event!($event);
                    } else {
                        queue.enqueue($when, $event);
                    }
                };
            }

            // XXX Maybe watch for large `now - _ins` deltas to detect runtime lag
            while let Some((_ins, event)) = queue.pop_next(&now) {
                submit_event!(event);
            }

            // If the queue is not empty, we have a deadline: can only
            // wait until we reach `wake_at`
            if let Some(wake_at) = queue.next_deadline() {
                // wait for input OR sleep
                let sleep_fut = sleep_until(*wake_at);
                let recv_fut = rx.recv_async();

                tokio::select! {
                    _ = sleep_fut => {
                        // woke up after deadline, time to handle events
                        continue 'handler;
                    },
                    maybe = recv_fut => {
                        if maybe.is_err() {
                            // channel closed
                            break 'handler;
                        }
                        let (when, event) = maybe.expect("checked for None already");
                        submit_event!(when, event);
                    }
                };
            } else {
                // Otherwise we'll wait until someone submits a new deadline
                if let Ok((when, event)) = rx.recv_async().await {
                    submit_event!(when, event);
                } else {
                    // channel closed
                    break 'handler;
                }
            }
        }
    });

    tx
}

// Just a (Instant, Timer) min-heap
struct TimerQueue(BinaryHeap<Reverse<(Instant, Timer<EldegossId>)>>);

impl TimerQueue {
    fn new() -> Self {
        Self(Default::default())
    }

    fn next_deadline(&self) -> Option<&Instant> {
        self.0.peek().map(|Reverse((deadline, _))| deadline)
    }

    fn enqueue(&mut self, deadline: Instant, event: Timer<EldegossId>) {
        self.0.push(Reverse((deadline, event)));
    }

    fn pop_next(&mut self, deadline: &Instant) -> Option<(Instant, Timer<EldegossId>)> {
        if self
            .0
            .peek()
            .map(|Reverse((when, _))| when < deadline)
            .unwrap_or(false)
        {
            self.0.pop().map(|Reverse(inner)| inner)
        } else {
            None
        }
    }
}

pub(crate) type Membership =
    Arc<RwLock<Foca<EldegossId, BincodeCodec<DefaultOptions>, StdRng, NoCustomBroadcast>>>;

pub(crate) async fn start_foca(
    identity: EldegossId,
    link_pool: Arc<RwLock<HashMap<ID, Arc<Link>>>>,
) -> Result<(Membership, Sender<FocaEvent>)> {
    let (foca_event_tx, foca_event_rv) = flume::bounded(1024);
    let (outbound_foca_data_tx, outbound_foca_data_rv) = flume::bounded(1024);

    let foca = Arc::new(RwLock::new(Foca::new(
        identity.clone(),
        FocaConfig::simple(),
        StdRng::from_entropy(),
        BincodeCodec(bincode::DefaultOptions::new()),
    )));
    let membership = foca.clone();

    let scheduler = launch_scheduler(foca_event_tx.clone()).await;

    let mut runtime = foca::AccumulatingRuntime::new();
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
                error!("Ignored error: {error}");
            }

            while let Some((dst, data)) = runtime.to_send() {
                outbound_foca_data_tx.send_async((dst, data)).await.ok();
            }

            let now = Instant::now();
            while let Some((delay, event)) = runtime.to_schedule() {
                scheduler
                    .send((now + delay, event))
                    .expect("error handling");
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
            }
        }
    });

    tokio::spawn(async move {
        let close_handler = close_chain().lock().handler(1);
        loop {
            tokio::select! {
                Ok((eid, data)) = outbound_foca_data_rv.recv_async() => {
                    debug!("outbound_msg send: {eid:?} {data:?}");
                    if let Some(link) = link_pool.read().await.get(&eid.addr()) {
                        link.send(&Sample::new_foca(identity.hlc().new_timestamp(), data))
                            .await
                            .map_err(|e| error!("outbound_msg send error: {e:?}"))
                            .ok();
                    }
                }
                _ = close_handler.handle_async() => {
                    info!("serve: Active shutdown");
                    break;
                }
            }
        }
    });

    Ok((membership, foca_event_tx))
}
