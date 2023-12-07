use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

use quic::config;
use serde::{Deserialize, Serialize};

#[macro_use]
extern crate tracing as logger;

pub mod protocol;
pub mod quic;
pub mod util;

pub type QuicSendStream = quinn::SendStream;
pub type QuicRecvStream = quinn::RecvStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct EldegossId(uhlc::ID);

impl EldegossId {
    pub fn rand() -> Self {
        Self(uhlc::ID::rand())
    }
    pub fn to_u128(&self) -> u128 {
        u128::from_le_bytes(self.0.to_le_bytes())
    }
}

impl Default for EldegossId {
    fn default() -> Self {
        Self::rand()
    }
}

impl Display for EldegossId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EldegossId({:x})", self.to_u128())
    }
}

impl From<uhlc::ID> for EldegossId {
    fn from(id: uhlc::ID) -> Self {
        Self(id)
    }
}

impl From<u128> for EldegossId {
    fn from(id: u128) -> Self {
        uhlc::ID::try_from(id).unwrap().into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub id: u128,

    pub listen: String,
    pub connect: Vec<String>,

    pub ca_path: String,
    pub cert_path: String,
    pub private_key_path: String,

    pub subscription_list: Vec<String>,

    pub keep_alive_interval: u64,
    pub check_neighbor_interval: u64,
    pub msg_timeout: u64,
    pub msg_max_size: usize,

    pub gossip_fanout: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: rand::random(),
            ca_path: Default::default(),
            connect: Default::default(),
            listen: Default::default(),
            cert_path: Default::default(),
            private_key_path: Default::default(),
            subscription_list: Default::default(),
            keep_alive_interval: 5,
            check_neighbor_interval: 3,
            msg_timeout: 2,
            msg_max_size: 1024 * 1024 * 16,
            gossip_fanout: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    id: EldegossId,
    meta_data: Vec<u8>,
}

impl Hash for Member {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Member {
    pub fn new(id: EldegossId, meta_data: Vec<u8>) -> Self {
        Self { id, meta_data }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Membership {
    member_map: HashMap<EldegossId, Vec<u8>>,
}

impl Membership {
    pub fn contains(&self, id: &EldegossId) -> bool {
        self.member_map.contains_key(id)
    }

    pub fn merge(&mut self, other: &Self) {
        self.member_map.extend(other.member_map.clone());
        debug!("after merge: {:#?}", self.member_map);
    }

    pub fn add_member(&mut self, member: Member) {
        debug!("add member: {:?}", member);
        self.member_map.insert(member.id, member.meta_data);
        debug!("after add: {:#?}", self.member_map);
    }

    pub fn remove_member(&mut self, id: EldegossId) {
        if config().id == id.to_u128() {
            info!("cant remove self");
            return;
        }
        info!("remove member: {id}");
        self.member_map.remove(&id);
        debug!("after remove: {:#?}", self.member_map);
    }
}

#[tokio::test]
async fn cert() {
    use common_x::{
        cert::{ca_cert, create_csr, restore_ca_cert, sign_csr},
        file::{create_file, read_file_to_string},
    };
    // ca
    let (_, ca_cert_pem, ca_key_pem) = ca_cert();
    create_file("./config/cert/ca_cert.pem", ca_cert_pem.as_bytes())
        .await
        .unwrap();
    create_file("./config/cert/ca_key.pem", ca_key_pem.as_bytes())
        .await
        .unwrap();

    // server csr
    let (csr_pem, key_pem) = create_csr("test-host");
    create_file("./config/cert/server_csr.pem", csr_pem.as_bytes())
        .await
        .unwrap();
    create_file("./config/cert/server_key.pem", key_pem.as_bytes())
        .await
        .unwrap();
    // server sign
    let ca_cert_pem = read_file_to_string("./config/cert/ca_cert.pem")
        .await
        .unwrap();
    let ca_key_pem = read_file_to_string("./config/cert/ca_key.pem")
        .await
        .unwrap();
    let ca = restore_ca_cert(&ca_cert_pem, &ca_key_pem);
    let csr_pem = read_file_to_string("./config/cert/server_csr.pem")
        .await
        .unwrap();
    let cert_pem = sign_csr(&csr_pem, &ca);
    create_file("./config/cert/server_cert.pem", cert_pem.as_bytes())
        .await
        .unwrap();

    // client csr
    let (csr_pem, key_pem) = create_csr("client.test-host");
    create_file("./config/cert/client_csr.pem", csr_pem.as_bytes())
        .await
        .unwrap();
    create_file("./config/cert/client_key.pem", key_pem.as_bytes())
        .await
        .unwrap();
    // client sign
    let ca_cert_pem = read_file_to_string("./config/cert/ca_cert.pem")
        .await
        .unwrap();
    let ca_key_pem = read_file_to_string("./config/cert/ca_key.pem")
        .await
        .unwrap();
    let ca = restore_ca_cert(&ca_cert_pem, &ca_key_pem);
    let csr_pem = read_file_to_string("./config/cert/client_csr.pem")
        .await
        .unwrap();
    let cert_pem = sign_csr(&csr_pem, &ca);
    create_file("./config/cert/client_cert.pem", cert_pem.as_bytes())
        .await
        .unwrap();
}
