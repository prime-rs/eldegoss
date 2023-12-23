use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

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
    member_map: HashMap<EldegossId, Vec<u8>>,
}

impl Membership {
    pub fn contains(&self, id: &EldegossId) -> bool {
        self.member_map.contains_key(id)
    }

    pub fn get(&self, id: &EldegossId) -> Option<&Vec<u8>> {
        self.member_map.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&EldegossId, &Vec<u8>)> {
        self.member_map.iter()
    }

    pub fn len(&self) -> usize {
        self.member_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.member_map.is_empty()
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.member_map.extend(other.member_map.clone());
        debug!("after merge: {:#?}", self.member_map);
    }

    pub(crate) fn add_member(&mut self, member: Member) {
        debug!("add member: {:?}", member);
        self.member_map.insert(member.id, member.meta_data);
        debug!("after add: {:#?}", self.member_map);
    }

    pub(crate) fn remove_member(&mut self, id: EldegossId) {
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
