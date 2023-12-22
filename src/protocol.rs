use std::{
    fmt::{self, Display},
    str::FromStr,
};

use bitflags::bitflags;
use color_eyre::{eyre::eyre, Result};
use serde::{Deserialize, Serialize};
use uhlc::Timestamp;

use crate::{session::hlc, Member, Membership};

bitflags! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct Flags: u8 {
        const Control = 0b10000000;
        const Broadcast = 0b01000000;
        const PubSub = 0b00100000;

        const To = !(Self::Control.bits() | Self::Broadcast.bits() | Self::PubSub.bits());
        const ControlBroadcast = Self::Control.bits() | Self::Broadcast.bits();
        const PubSubBroadcast = Self::PubSub.bits() | Self::Broadcast.bits();
    }
}

impl Display for Flags {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

impl FromStr for Flags {
    type Err = bitflags::parser::ParseError;

    fn from_str(flags: &str) -> Result<Self, Self::Err> {
        bitflags::parser::from_str(flags)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Command {
    AddMember(Member),
    RemoveMember(u128),
    JoinReq(Vec<u8>),
    JoinRsp(Membership),
    CheckReq(u128),
    CheckRsp(u128, bool),
}

#[derive(Debug, Default)]
pub struct Message {
    pub timestamp: Option<Timestamp>,
    pub to: u128,
    pub topic: String,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(to: u128, topic: &str, payload: Vec<u8>) -> Self {
        Self {
            to,
            topic: topic.to_owned(),
            payload,
            ..Default::default()
        }
    }

    pub fn to(to: u128, payload: Vec<u8>) -> Self {
        Self {
            to,
            payload,
            ..Default::default()
        }
    }

    pub fn put(topic: &str, payload: Vec<u8>) -> Self {
        Self {
            topic: topic.to_owned(),
            payload,
            ..Default::default()
        }
    }

    #[inline]
    pub(crate) fn sample(self) -> Sample {
        Sample {
            to: self.to,
            topic: self.topic,
            payload: Payload::Message(self.payload),
            timestamp: Default::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Payload {
    Control(Command),
    Message(Vec<u8>),
}

#[derive(Debug)]
pub(crate) struct Sample {
    pub timestamp: Option<Timestamp>,
    pub to: u128,
    pub topic: String,
    pub payload: Payload,
}

impl Sample {
    pub(crate) fn control(to: u128, command: Command) -> Self {
        Self {
            to,
            payload: Payload::Control(command),
            timestamp: Default::default(),
            topic: Default::default(),
        }
    }

    pub(crate) fn message(&self) -> Message {
        Message {
            to: self.to,
            topic: self.topic.clone(),
            payload: match &self.payload {
                Payload::Control(_) => vec![],
                Payload::Message(bytes) => bytes.clone(),
            },
            timestamp: self.timestamp,
        }
    }

    pub(crate) fn origin(&self) -> u128 {
        self.timestamp
            .map(|timestamp| u128::from_le_bytes(timestamp.get_id().to_le_bytes()))
            .unwrap_or_default()
    }
}

impl Sample {
    #[inline]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let timestamp = self.timestamp.unwrap_or_else(|| hlc().new_timestamp());
        match &self.payload {
            Payload::Control(command) => {
                if self.to == 0 {
                    buf.push(Flags::ControlBroadcast.bits());
                    buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                    buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                } else {
                    buf.push(Flags::Control.bits());
                    buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                    buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                    buf.extend_from_slice(&self.to.to_be_bytes());
                }
                buf.extend_from_slice(&bincode::serialize(&command).unwrap());
                buf
            }
            Payload::Message(bytes) => {
                if !self.topic.is_empty() {
                    if self.to == 0 {
                        buf.push(Flags::PubSubBroadcast.bits());
                        buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                        buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                    } else {
                        buf.push(Flags::PubSub.bits());
                        buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                        buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                        buf.extend_from_slice(&self.to.to_be_bytes());
                    }
                    buf.extend_from_slice(&(self.topic.len() as u32).to_be_bytes());
                    buf.extend_from_slice(self.topic.as_bytes());
                } else if self.to != 0 {
                    buf.push(Flags::To.bits());
                    buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                    buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                    buf.extend_from_slice(&self.to.to_be_bytes());
                } else {
                    buf.push(Flags::Broadcast.bits());
                    buf.extend_from_slice(&timestamp.get_id().to_le_bytes());
                    buf.extend_from_slice(&timestamp.get_time().0.to_be_bytes());
                }
                buf.extend_from_slice(bytes);
                buf
            }
        }
    }

    #[inline]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let flags = Flags::from_bits_truncate(bytes[0]);
        let id = u128::from_le_bytes(bytes[1..17].try_into()?);
        let time = u64::from_be_bytes(bytes[17..25].try_into()?);
        let timestamp = Some(Timestamp::new(
            uhlc::NTP64(time),
            uhlc::ID::try_from(id).unwrap(),
        ));
        match flags {
            Flags::Control => {
                let to = u128::from_be_bytes(bytes[25..41].try_into()?);
                let commond = bincode::deserialize::<Command>(&bytes[41..])?;
                Ok(Sample {
                    to,
                    timestamp,
                    topic: Default::default(),
                    payload: Payload::Control(commond),
                })
            }
            Flags::ControlBroadcast => {
                let commond = bincode::deserialize::<Command>(&bytes[25..])?;
                Ok(Sample {
                    to: 0,
                    timestamp,
                    topic: Default::default(),
                    payload: Payload::Control(commond),
                })
            }
            Flags::PubSub => {
                let to = u128::from_be_bytes(bytes[25..41].try_into()?);
                let topic_len = u32::from_be_bytes(bytes[41..45].try_into()?);
                let topic = String::from_utf8(bytes[45..45 + topic_len as usize].to_vec())?;
                let body = bytes[45 + topic_len as usize..].to_vec();
                Ok(Sample {
                    timestamp,
                    to,
                    topic,
                    payload: Payload::Message(body),
                })
            }
            Flags::PubSubBroadcast => {
                let topic_len = u32::from_be_bytes(bytes[25..29].try_into()?);
                let topic = String::from_utf8(bytes[29..29 + topic_len as usize].to_vec())?;
                let body = bytes[29 + topic_len as usize..].to_vec();
                Ok(Sample {
                    timestamp,
                    to: Default::default(),
                    topic,
                    payload: Payload::Message(body),
                })
            }
            Flags::Broadcast => {
                let body = bytes[25..].to_vec();
                Ok(Sample {
                    timestamp,
                    to: Default::default(),
                    topic: Default::default(),
                    payload: Payload::Message(body),
                })
            }
            Flags::To => {
                let to = u128::from_be_bytes(bytes[25..41].try_into()?);
                let body = bytes[41..].to_vec();
                Ok(Sample {
                    timestamp,
                    to,
                    topic: Default::default(),
                    payload: Payload::Message(body),
                })
            }
            _ => Err(eyre!("unknown flags")),
        }
    }
}

#[test]
fn test_sample_encode() {
    use crate::util::Stats;

    common_x::log::init_log_filter("info");
    let mut stats = Stats::new(10000);
    let msg = Message::new(0, "topic", vec![0; 1024]).sample();
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
    let msg = Message::new(0, "topic", vec![0; 1024]).sample();
    let bytes = msg.encode();
    info!("bytes len: {:?}", bytes.len());
    let mut stats = Stats::new(10000);
    for _ in 0..100000 {
        Sample::decode(&bytes).ok();
        stats.increment();
    }
}

#[test]
fn test_flags() {
    let mut flags = Flags::empty();
    assert_eq!(flags, Flags::empty());
    flags = Flags::To;
    println!("flags is {flags}");
    println!("flags is {}", flags.bits());
}

#[test]
fn test_encode_decode_msg() {
    let msg = Sample::control(3, Command::AddMember(Member::new(1.into(), vec![])));
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Sample::decode(&buf);
    println!("msg is {:?}", msg);

    let msg = Sample {
        to: 0,
        topic: "topic".to_owned(),
        timestamp: Default::default(),
        payload: Payload::Message(vec![1, 2, 3, 4, 5]),
    };
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Sample::decode(&buf);
    println!("msg is {:?}", msg);
}
