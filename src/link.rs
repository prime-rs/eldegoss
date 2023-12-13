use std::sync::Arc;

use async_lock::Mutex;
use color_eyre::Result;
use quinn::Connection;

use crate::{
    protocol::{encode_msg, Message},
    server::Server,
    util::read_msg,
    EldegossId,
};

#[derive(Debug)]
pub(crate) struct Link {
    id: EldegossId,
    locator: String,
    connection: Connection,
    send: Arc<Mutex<quinn::SendStream>>,
    recv: Arc<Mutex<quinn::RecvStream>>,
    server: Server,
}

impl Link {
    pub(crate) fn new(
        id: EldegossId,
        locator: String,
        connection: Connection,
        server: Server,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    ) -> Self {
        Self {
            id,
            locator,
            connection,
            send: Arc::new(Mutex::new(send)),
            recv: Arc::new(Mutex::new(recv)),
            server,
        }
    }

    pub(crate) const fn id(&self) -> EldegossId {
        self.id
    }

    pub(crate) fn id_u128(&self) -> u128 {
        self.id.to_u128()
    }

    pub(crate) fn close_reason(&self) -> Option<String> {
        self.connection.close_reason().map(|r| r.to_string())
    }

    pub(crate) fn handle(&self) {
        let id = self.id.to_u128();
        let recv = self.recv.clone();
        let connection = self.connection.clone();
        let locator = self.locator.clone();
        let server = self.server.clone();
        tokio::spawn(async move {
            let mut recv = recv.lock().await;
            loop {
                match read_msg(&mut recv).await {
                    Ok(msg) => {
                        let server = server.clone();
                        tokio::spawn(async move {
                            server.dispatch(msg, id).await;
                        });
                    }
                    Err(e) => {
                        debug!("link handle recv msg failed: {e}");
                        if let Some(close_reason) = connection.close_reason() {
                            server.connect_links.lock().await.remove(&locator);
                            server.links.write().await.remove(&id.into());
                            server.check_member_list.lock().await.push(id.into());
                            info!("link({id}) closed: {close_reason}");
                        }
                        break;
                    }
                }
            }
        });
    }

    #[inline]
    pub(crate) async fn send_msg(&self, msg: &Message) -> Result<()> {
        let msg_bytes = encode_msg(msg);
        let len = msg_bytes.len() as u32;
        let len_bytes = len.to_le_bytes().to_vec();
        let mut send = self.send.lock().await;
        send.write_all(&len_bytes).await?;
        send.write_all(&msg_bytes).await?;
        Ok(())
    }

    pub(crate) fn set_locator(&mut self, locator: String) {
        self.locator = locator;
    }
}
