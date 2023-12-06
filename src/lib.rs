use std::collections::{HashMap, HashSet};
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
    pub id: EldegossId,
    pub subscription_list: HashSet<String>,
    pub neighbor_list: HashSet<EldegossId>,
}

impl Hash for Member {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Member {
    pub fn new(id: EldegossId, subscription_list: HashSet<String>) -> Self {
        Self {
            id,
            subscription_list,
            neighbor_list: Default::default(),
        }
    }
    pub fn add_neighbor(&mut self, id: EldegossId) {
        self.neighbor_list.insert(id);
    }
    pub fn remove_neighbor(&mut self, id: EldegossId) {
        self.neighbor_list.remove(&id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Membership {
    subscription_map: HashMap<String, HashSet<EldegossId>>,
    member_map: HashMap<EldegossId, Member>,
    check_member_list: Vec<EldegossId>,
    wait_for_remove_member_list: Vec<EldegossId>,
}

impl Membership {
    pub fn contains(&self, id: &EldegossId) -> bool {
        self.member_map.contains_key(id)
    }

    pub fn merge(&mut self, other: &Self) {
        for (subscription, ids) in &other.subscription_map {
            if let Some(self_ids) = self.subscription_map.get_mut(subscription) {
                self_ids.extend(ids);
            } else {
                self.subscription_map
                    .insert(subscription.to_string(), ids.clone());
            }
        }
        for (id, member) in &other.member_map {
            if let Some(self_member) = self.member_map.get_mut(id) {
                self_member
                    .subscription_list
                    .extend(member.subscription_list.clone());
                self_member.neighbor_list.extend(&member.neighbor_list);
            } else {
                self.member_map.insert(*id, member.clone());
            }
        }
        debug!("after merge: {:#?}", self.member_map);
    }

    pub fn add_member(&mut self, member: Member) {
        for subscription in &member.subscription_list {
            if let Some(ids) = self.subscription_map.get_mut(subscription) {
                ids.insert(member.id);
            } else {
                let mut ids = HashSet::new();
                ids.insert(member.id);
                self.subscription_map.insert(subscription.to_string(), ids);
            }
        }
        for neighbor_id in &member.neighbor_list {
            if let Some(neighbor) = self.member_map.get_mut(neighbor_id) {
                neighbor.add_neighbor(member.id);
            }
        }
        debug!("add member: {}", member.id);
        self.member_map.insert(member.id, member);
        debug!("after add: {:#?}", self.member_map);
    }

    pub fn remove_member(&mut self, id: EldegossId) {
        debug!("remove member: {id}");
        if let Some(member) = self.member_map.remove(&id) {
            for subscription in &member.subscription_list {
                let mut empty = false;
                if let Some(ids) = self.subscription_map.get_mut(subscription) {
                    ids.remove(&member.id);
                    if ids.is_empty() {
                        empty = true;
                    }
                }
                if empty {
                    self.subscription_map.remove(subscription);
                }
            }
            for neighbor_id in &member.neighbor_list {
                if let Some(neighbor) = self.member_map.get_mut(neighbor_id) {
                    neighbor.remove_neighbor(member.id);
                }
            }
        }
        debug!("after remove: {:#?}", self.member_map);
    }

    pub fn add_check_member(&mut self, id: EldegossId) {
        if let Some(self_member) = self.member_map.get_mut(&config().id.into()) {
            self_member.remove_neighbor(id);
        }
        self.check_member_list.push(id);
    }

    pub fn get_check_member(&mut self) -> Option<EldegossId> {
        if let Some(id) = self.check_member_list.pop() {
            self.wait_for_remove_member_list.push(id);
            Some(id)
        } else {
            None
        }
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
