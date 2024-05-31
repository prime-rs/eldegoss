use std::sync::Arc;

use common_x::graceful_shutdown::CloseHandler;
use flume::Receiver;
use quinn::{Connection, RecvStream, SendStream};
use tokio::select;

use crate::{protocol::Sample, session::SessionRuntime, util::read_msg, EldegossId};

pub(crate) struct Link {
    pub(crate) id: EldegossId,
    pub(crate) locator: String,
    pub(crate) connection: Connection,
    pub(crate) msg_to_send: Receiver<Arc<Sample>>,
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
    pub(crate) session: SessionRuntime,
    pub(crate) close_handler: CloseHandler,
}

impl Link {
    pub(crate) const fn id(&self) -> EldegossId {
        self.id
    }

    pub(crate) async fn handle(self) {
        let Link {
            id,
            locator,
            connection,
            recv,
            send,
            session,
            msg_to_send,
            close_handler,
        } = self;

        select! {
            _ = reader(session.clone(), id, recv) => {
                connection.close(0_u32.into(), b"close by read msg error");
            }
            _ = writer(msg_to_send, send) => {
                connection.close(0_u32.into(), b"close by write msg error");
            }
            _ = close_handler.handle_async() => {
                connection.close(0_u32.into(), b"Active shutdown");
                info!("Link({id}): Active shutdown");
            }
            _ = connection.closed() => {
                info!("Link({}) connection closed, reason: {:?}", id, connection.close_reason());
            }
        }

        session.links.write().await.remove(&id);
        session.connected_locators.lock().await.remove(&locator);
        session.check_member_list.lock().await.push(id);
        info!("Link({}) closed", id);
    }
}

async fn reader(session: SessionRuntime, id: EldegossId, mut recv: RecvStream) {
    loop {
        match read_msg(&mut recv).await {
            Ok(msg) => {
                let id = id.to_u128();
                session.dispatch(msg.into(), id).await;
            }
            Err(e) => {
                warn!("link handle recv msg failed: {e}");
                break;
            }
        }
    }
}

async fn writer(msg_to_send: Receiver<Arc<Sample>>, mut send: SendStream) {
    while let Ok(sample) = msg_to_send.recv_async().await {
        let msg_bytes = sample.encode();
        let len = msg_bytes.len() as u32;
        let len_bytes = len.to_le_bytes().to_vec();
        let bytes = [len_bytes, msg_bytes].concat();
        if let Err(e) = send.write_all(&bytes).await {
            warn!("link handle send msg failed: {e}");
            break;
        };
    }
}
