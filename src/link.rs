use flume::Receiver;
use quinn::Connection;
use tokio::select;

use crate::{server::Server, util::read_msg, EldegossId};

#[derive(Debug)]
pub(crate) struct Link {
    pub(crate) id: EldegossId,
    pub(crate) locator: String,
    pub(crate) connection: Connection,
    pub(crate) msg_to_send: Receiver<Vec<u8>>,
    pub(crate) send: quinn::SendStream,
    pub(crate) recv: quinn::RecvStream,
    pub(crate) server: Server,
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
            mut recv,
            mut send,
            server,
            msg_to_send,
            ..
        } = self;

        loop {
            select! {
                result = read_msg(&mut recv) => match result {
                    Ok(msg) => {
                        let server = server.clone();
                        let id = id.to_u128();
                        tokio::spawn(async move {
                            server.dispatch(msg, id).await;
                        });
                    }
                    Err(e) => {
                        error!("link handle recv msg failed: {e}");
                        if let Some(close_reason) = connection.close_reason() {
                            server.connected_locators.lock().await.remove(&locator);
                            server.links.write().await.remove(&id);
                            server.check_member_list.lock().await.push(id);
                            info!("link({id}) closed: {close_reason}");
                        }
                        break;
                    }
                },
                Ok(msg_bytes) = msg_to_send.recv_async() => {
                    let len = msg_bytes.len() as u32;
                    let len_bytes = len.to_le_bytes().to_vec();
                    send.write_all(&len_bytes).await.ok();
                    send.write_all(&msg_bytes).await.ok();
                },
            }
        }
        info!("link({id}) handle end");
    }
}
