use color_eyre::Result;
use flume::Receiver;
use quinn::{Connection, RecvStream, SendStream};

use crate::{
    protocol::Message,
    session::Session,
    util::{read_msg, write_msg},
    EldegossId,
};

#[derive(Debug)]
pub(crate) struct Link {
    pub(crate) id: EldegossId,
    pub(crate) locator: String,
    pub(crate) connection: Connection,
    pub(crate) msg_to_send: Receiver<Vec<u8>>,
    pub(crate) send: Vec<SendStream>,
    pub(crate) recv: Vec<RecvStream>,
    pub(crate) session: Session,
    pub(crate) is_server: bool,
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
            session,
            msg_to_send,
            ..
        } = self;

        if self.is_server {
            while send.len() < 16 {
                if let Ok((mut tx, rv)) = connection.open_bi().await {
                    write_msg(&mut tx, Message::to_msg(id.to_u128(), vec![]))
                        .await
                        .ok();
                    send.push(tx);
                    recv.push(rv);
                }
            }
        } else {
            while send.len() < 16 {
                if let Ok((tx, rv)) = connection.accept_bi().await {
                    send.push(tx);
                    recv.push(rv);
                }
            }
        }

        for tx in send {
            tokio::spawn(writer(msg_to_send.clone(), tx));
        }
        for rv in recv {
            tokio::spawn(reader(
                session.clone(),
                id,
                locator.clone(),
                connection.clone(),
                rv,
            ));
        }
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
