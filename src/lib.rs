use std::fmt::{Display, Formatter};
use std::hash::Hash;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::session::id_u128;

#[macro_use]
extern crate tracing as logger;

pub mod config;
pub(crate) mod link;
pub mod protocol;
pub mod session;
pub mod util;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct EldegossId(u128);

impl EldegossId {
    pub fn rand() -> Self {
        Self(u128::from_le_bytes(uhlc::ID::rand().to_le_bytes()))
    }

    #[inline]
    pub const fn to_u128(&self) -> u128 {
        self.0
    }

    #[inline]
    pub fn hex(&self) -> String {
        let bytes = self.0.to_be_bytes();
        hex::encode(bytes)
    }
}

impl Default for EldegossId {
    fn default() -> Self {
        Self::rand()
    }
}

impl Display for EldegossId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EldegossId({})", self.hex())
    }
}

impl From<uhlc::ID> for EldegossId {
    fn from(id: uhlc::ID) -> Self {
        Self(u128::from_le_bytes(id.to_le_bytes()))
    }
}

impl From<u128> for EldegossId {
    fn from(id: u128) -> Self {
        if id == 0 {
            panic!("id can't be 0")
        }
        Self(id)
    }
}

impl From<&str> for EldegossId {
    fn from(id: &str) -> Self {
        let id = hex::decode(id).unwrap();
        let id = u128::from_be_bytes(id[..16].try_into().unwrap());
        id.into()
    }
}

impl From<String> for EldegossId {
    fn from(id: String) -> Self {
        id.as_str().into()
    }
}

impl From<&String> for EldegossId {
    fn from(id: &String) -> Self {
        id.as_str().into()
    }
}

#[test]
fn test_id() {
    let id1 = EldegossId::from("00000000000000000000000000000001");
    let id2 = EldegossId::from(1);
    assert_eq!(id1, id2);
    let id1 = EldegossId::from("10000000000000000000000000000000");
    let id2 = EldegossId::from(21267647932558653966460912964485513216);
    assert_eq!(id1, id2);
    let id = EldegossId::rand();
    println!("{}", id);
    println!("hex: {}", id.hex());
    println!("u128: {}", id.to_u128());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    id: EldegossId,
    meta_data: Vec<u8>,
}

impl Member {
    pub fn new(id: EldegossId, meta_data: Vec<u8>) -> Self {
        Self { id, meta_data }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Membership {
    member_map: DashMap<EldegossId, Vec<u8>>,
}

impl Membership {
    pub fn contains(&self, id: &EldegossId) -> bool {
        self.member_map.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.member_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.member_map.is_empty()
    }

    pub(crate) fn merge(&self, other: &Self) {
        for o in other.member_map.iter() {
            self.member_map.insert(*o.key(), o.value().clone());
        }
    }

    pub(crate) fn add_member(&self, member: Member) {
        debug!("add member: {:?}", member);
        self.member_map.insert(member.id, member.meta_data);
        debug!("after add: {:#?}", self.member_map);
    }

    pub(crate) fn remove_member(&self, id: EldegossId) {
        if id_u128() == id.to_u128() {
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
        file::create_file,
        tls::{new_ca, new_end_entity},
    };
    // ca
    let (ca_cert, ca_key_pair) = new_ca();
    create_file("./config/cert/ca_cert.pem", ca_cert.pem().as_bytes())
        .await
        .unwrap();
    create_file(
        "./config/cert/ca_key.pem",
        ca_key_pair.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();

    // server cert
    let (server_cert, server_key) = new_end_entity("test-host", &ca_cert, &ca_key_pair);
    create_file(
        "./config/cert/server_cert.pem",
        server_cert.pem().as_bytes(),
    )
    .await
    .unwrap();
    create_file(
        "./config/cert/server_key.pem",
        server_key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();

    // client cert
    let (client_cert, client_key) = new_end_entity("client.test-host", &ca_cert, &ca_key_pair);
    create_file(
        "./config/cert/client_cert.pem",
        client_cert.pem().as_bytes(),
    )
    .await
    .unwrap();
    create_file(
        "./config/cert/client_key.pem",
        client_key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
}
