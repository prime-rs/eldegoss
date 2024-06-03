use color_eyre::{eyre::ContextCompat, Result};
use serde::{Deserialize, Serialize};
use strum::{Display, FromRepr};
use uhlc::Timestamp;

use crate::{session::hlc, Member, Membership};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, FromRepr, Display)]
#[repr(u8)]
pub(crate) enum Flags {
    Ack = 0,
    Control = 1,
    Push,
    Send,
    Query,
    Reply,

    Err = 0b11111111,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum Command {
    AddMember(Member),
    RemoveMember(u128),
    JoinReq(Vec<u8>),
    JoinRsp((bool, Membership)),
    CheckReq(u128),
    CheckRsp(u128, bool),
}

#[derive(Debug, Default)]
pub struct Message {
    pub timestamp: Option<Timestamp>,
    pub key_expr: String,
    pub payload: Payload,
}

impl Message {
    pub fn push(key_expr: &str, payload: Vec<u8>) -> Self {
        Self {
            key_expr: key_expr.to_owned(),
            payload: Payload::Push(payload),
            timestamp: None,
        }
    }

    pub fn send(key_expr: &str, payload: Vec<u8>) -> Self {
        Self {
            key_expr: key_expr.to_owned(),
            payload: Payload::Send(payload),
            timestamp: None,
        }
    }

    pub fn query(key_expr: &str, payload: Vec<u8>) -> Self {
        Self {
            key_expr: key_expr.to_owned(),
            payload: Payload::Query(payload),
            timestamp: None,
        }
    }

    pub fn reply(&self, payload: Vec<u8>) -> Self {
        Self {
            key_expr: self.key_expr.to_owned(),
            payload: Payload::Reply(payload),
            timestamp: None,
        }
    }

    pub fn ack(&self) -> Self {
        Self {
            key_expr: self.key_expr.to_owned(),
            payload: Payload::Ack,
            timestamp: self.timestamp,
        }
    }

    pub fn err(&self, err: &str) -> Self {
        Self {
            key_expr: self.key_expr.to_owned(),
            payload: Payload::Err(err.to_owned()),
            timestamp: self.timestamp,
        }
    }

    pub fn control(command: Command) -> Self {
        Self {
            key_expr: String::new(),
            payload: Payload::Control(command),
            timestamp: None,
        }
    }

    pub fn origin(&self) -> u128 {
        self.timestamp
            .map(|timestamp| u128::from_le_bytes(timestamp.get_id().to_le_bytes()))
            .unwrap_or_default()
    }

    pub fn time(&self) -> u64 {
        self.timestamp
            .map(|timestamp| u64::from_le_bytes(timestamp.get_time().0.to_le_bytes()))
            .unwrap_or_default()
    }

    #[inline]
    pub fn sample(self) -> Sample {
        Sample {
            key_expr: self.key_expr,
            payload: self.payload,
            timestamp: self.timestamp.unwrap_or(hlc().new_timestamp()),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub enum Payload {
    Control(Command),
    Push(Vec<u8>),
    Send(Vec<u8>),
    Query(Vec<u8>),
    Reply(Vec<u8>),
    #[default]
    Ack,
    Err(String),
}

#[derive(Debug)]
pub struct Sample {
    pub timestamp: Timestamp,
    pub key_expr: String,
    pub payload: Payload,
}

impl Sample {
    pub(crate) fn control(command: Command) -> Self {
        Self {
            payload: Payload::Control(command),
            timestamp: hlc().new_timestamp(),
            key_expr: Default::default(),
        }
    }

    pub(crate) fn message(&self) -> Message {
        Message {
            key_expr: self.key_expr.clone(),
            payload: self.payload.clone(),
            timestamp: Some(self.timestamp),
        }
    }

    pub(crate) fn origin(&self) -> u128 {
        u128::from_le_bytes(self.timestamp.get_id().to_le_bytes())
    }
}

impl Sample {
    #[inline]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let timestamp = self.timestamp;
        match &self.payload {
            Payload::Control(command) => {
                buf.push(Flags::Control as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                buf.extend_from_slice(&bincode::serialize(&command).unwrap());
            }
            Payload::Ack => {
                buf.push(Flags::Ack as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
            }
            Payload::Err(err) => {
                buf.push(Flags::Err as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                buf.extend_from_slice(err.as_bytes());
            }
            Payload::Push(bytes) => {
                buf.push(Flags::Push as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                buf.extend_from_slice(&(self.key_expr.len() as u32).to_be_bytes());
                buf.extend_from_slice(self.key_expr.as_bytes());
                buf.extend_from_slice(bytes);
            }
            Payload::Send(bytes) => {
                buf.push(Flags::Send as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                buf.extend_from_slice(&(self.key_expr.len() as u32).to_be_bytes());
                buf.extend_from_slice(self.key_expr.as_bytes());
                buf.extend_from_slice(bytes);
            }
            Payload::Query(bytes) => {
                buf.push(Flags::Query as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                buf.extend_from_slice(&(self.key_expr.len() as u32).to_be_bytes());
                buf.extend_from_slice(self.key_expr.as_bytes());
                buf.extend_from_slice(bytes);
            }
            Payload::Reply(bytes) => {
                buf.push(Flags::Reply as u8);
                buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                buf.extend_from_slice(&(self.key_expr.len() as u32).to_be_bytes());
                buf.extend_from_slice(self.key_expr.as_bytes());
                buf.extend_from_slice(bytes);
            }
        }
        buf
    }

    #[inline]
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let flags = Flags::from_repr(bytes[0]).context("invalid flag")?;
        let id = u128::from_le_bytes(bytes[1..17].try_into()?);
        let time = u64::from_be_bytes(bytes[17..25].try_into()?);
        let timestamp = Timestamp::new(uhlc::NTP64(time), uhlc::ID::try_from(id).unwrap());
        match flags {
            Flags::Control => {
                let commond = bincode::deserialize::<Command>(&bytes[25..])?;
                Ok(Sample {
                    timestamp,
                    key_expr: Default::default(),
                    payload: Payload::Control(commond),
                })
            }
            Flags::Ack => Ok(Sample {
                timestamp,
                key_expr: Default::default(),
                payload: Payload::Ack,
            }),
            Flags::Err => {
                let err = String::from_utf8(bytes[25..].to_vec())?;
                Ok(Sample {
                    timestamp,
                    key_expr: Default::default(),
                    payload: Payload::Err(err),
                })
            }
            Flags::Push => {
                let key_expr_len = u32::from_be_bytes(bytes[25..29].try_into()?);
                let key_expr = String::from_utf8(bytes[29..29 + key_expr_len as usize].to_vec())?;
                let body = bytes[29 + key_expr_len as usize..].to_vec();
                Ok(Sample {
                    timestamp,
                    key_expr,
                    payload: Payload::Push(body),
                })
            }
            Flags::Send => {
                let key_expr_len = u32::from_be_bytes(bytes[25..29].try_into()?);
                let key_expr = String::from_utf8(bytes[29..29 + key_expr_len as usize].to_vec())?;
                let body = bytes[29 + key_expr_len as usize..].to_vec();
                Ok(Sample {
                    timestamp,
                    key_expr,
                    payload: Payload::Send(body),
                })
            }
            Flags::Query => {
                let key_expr_len = u32::from_be_bytes(bytes[25..29].try_into()?);
                let key_expr = String::from_utf8(bytes[29..29 + key_expr_len as usize].to_vec())?;
                let body = bytes[29 + key_expr_len as usize..].to_vec();
                Ok(Sample {
                    timestamp,
                    key_expr,
                    payload: Payload::Query(body),
                })
            }
            Flags::Reply => {
                let key_expr_len = u32::from_be_bytes(bytes[25..29].try_into()?);
                let key_expr = String::from_utf8(bytes[29..29 + key_expr_len as usize].to_vec())?;
                let body = bytes[29 + key_expr_len as usize..].to_vec();
                Ok(Sample {
                    timestamp,
                    key_expr,
                    payload: Payload::Reply(body),
                })
            }
        }
    }
}

#[test]
fn test_sample_encode() {
    use crate::util::Stats;

    common_x::log::init_log_filter("info");
    let mut stats = Stats::new(10000);
    let msg = Message::push("key_expr", vec![0; 1024]).sample();
    let bytes = msg.encode();
    info!("bytes len: {:?}", bytes.len());
    for _ in 0..100000 {
        msg.encode();
        stats.increment();
    }
}

#[test]
fn test_sample_decode() {
    use crate::util::Stats;

    common_x::log::init_log_filter("info");
    let msg = Message::push("key_expr", vec![0; 1024]).sample();
    let bytes = msg.encode();
    info!("bytes len: {:?}", bytes.len());
    let mut stats = Stats::new(10000);
    for _ in 0..100000 {
        Sample::decode(&bytes).ok();
        stats.increment();
    }
}

#[test]
fn test_encode_decode_msg() {
    let msg = Sample::control(Command::AddMember(Member::new(1.into(), vec![])));
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Sample::decode(&buf);
    println!("msg is {:?}", msg);

    let msg = Message::push("key_expr", vec![0; 1024]).sample();
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Sample::decode(&buf);
    println!("msg is {:?}", msg);
}

#[test]
fn test_flags() {
    let flags = Flags::Ack;
    println!("flags is {flags}");
}
