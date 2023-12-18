use color_eyre::Result;
use flume::Receiver;
use quinn::{Connection, RecvStream, SendStream};

use crate::{session::Session, util::read_msg, EldegossId};

#[derive(Debug)]
pub(crate) struct Link {
    pub(crate) id: EldegossId,
    pub(crate) locator: String,
    pub(crate) connection: Connection,
    pub(crate) msg_to_send: Receiver<Vec<u8>>,
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
    pub(crate) session: Session,
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
            ..
        } = self;

        tokio::spawn(writer(msg_to_send, send));
        tokio::spawn(reader(session, id, locator, connection, recv));
    }
}

async fn reader(
    session: Session,
    id: EldegossId,
    locator: String,
    connection: Connection,
    mut recv: RecvStream,
) {
    loop {
        match read_msg(&mut recv).await {
            Ok(msg) => {
                let id = id.to_u128();
                session.dispatch(msg, id).await;
            }
            Err(e) => {
                error!("link handle recv msg failed: {e}");
                if let Some(close_reason) = connection.close_reason() {
                    session.connected_locators.lock().await.remove(&locator);
                    session.links.write().await.remove(&id);
                    session.check_member_list.lock().await.push(id);
                    info!("link({id}) closed: {close_reason}");
                }
                break;
            }
        }
    }
}

async fn writer(msg_to_send: Receiver<Vec<u8>>, mut send: SendStream) -> Result<()> {
    while let Ok(msg_bytes) = msg_to_send.recv_async().await {
        let len = msg_bytes.len() as u32;
        let len_bytes = len.to_le_bytes().to_vec();
        send.write_all(&len_bytes).await?;
        send.write_all(&msg_bytes).await?;
    }
    Ok(())
}
