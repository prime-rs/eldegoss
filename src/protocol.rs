use std::{
    fmt::{self, Display},
    str::FromStr,
};

use bitflags::bitflags;
use color_eyre::{eyre::eyre, Result};
use serde::{Deserialize, Serialize};

use crate::{Member, Membership};

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

#[derive(Debug)]
pub(crate) struct Control {
    pub origin: u128,
    pub to: u128,
    pub command: Command,
}

#[derive(Debug, Default)]
pub struct Message {
    pub origin: u128,
    pub to: u128,
    pub topic: String,
    pub body: Vec<u8>,
}

impl Message {
    pub fn new(to: u128, topic: &str, body: Vec<u8>) -> Self {
        Self {
            to,
            topic: topic.to_owned(),
            body,
            ..Default::default()
        }
    }

    pub fn to(to: u128, body: Vec<u8>) -> Self {
        Self {
            to,
            body,
            ..Default::default()
        }
    }

    pub fn put(topic: &str, body: Vec<u8>) -> Self {
        Self {
            topic: topic.to_owned(),
            body,
            ..Default::default()
        }
    }

    #[inline]
    pub(crate) const fn sample(self) -> Sample {
        Sample::Message(self)
    }
}

#[derive(Debug)]
pub(crate) enum Sample {
    Control(Control),
    Message(Message),
}

impl Sample {
    pub(crate) const fn control(to: u128, body: Command) -> Self {
        Self::Control(Control {
            origin: 0,
            to,
            command: body,
        })
    }

    #[inline]
    pub(crate) const fn origin(&self) -> u128 {
        match self {
            Sample::Control(msg) => msg.origin,
            Sample::Message(msg) => msg.origin,
        }
    }

    #[inline]
    pub(crate) const fn to(&self) -> u128 {
        match self {
            Sample::Control(msg) => msg.to,
            Sample::Message(msg) => msg.to,
        }
    }

    #[inline]
    pub(crate) fn topic(&self) -> String {
        match self {
            Sample::Control(_) => "".to_owned(),
            Sample::Message(msg) => msg.topic.clone(),
        }
    }

    #[inline]
    pub(crate) fn set_origin(&mut self, origin: u128) {
        match self {
            Sample::Control(msg) => msg.origin = origin,
            Sample::Message(msg) => msg.origin = origin,
        }
    }
}

impl Sample {
    #[inline]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Sample::Control(msg) => {
                if msg.to == 0 {
                    buf.push(Flags::ControlBroadcast.bits());
                    buf.extend_from_slice(&msg.origin.to_be_bytes());
                } else {
                    buf.push(Flags::Control.bits());
                    buf.extend_from_slice(&msg.origin.to_be_bytes());
                    buf.extend_from_slice(&msg.to.to_be_bytes());
                }
                buf.extend_from_slice(&bincode::serialize(&msg.command).unwrap());
                buf
            }
            Sample::Message(msg) => {
                if !msg.topic.is_empty() {
                    if msg.to == 0 {
                        buf.push(Flags::PubSubBroadcast.bits());
                        buf.extend_from_slice(&msg.origin.to_be_bytes());
                    } else {
                        buf.push(Flags::PubSub.bits());
                        buf.extend_from_slice(&msg.origin.to_be_bytes());
                        buf.extend_from_slice(&msg.to.to_be_bytes());
                    }
                    buf.extend_from_slice(&(msg.topic.len() as u32).to_be_bytes());
                    buf.extend_from_slice(msg.topic.as_bytes());
                } else if msg.to != 0 {
                    buf.push(Flags::To.bits());
                    buf.extend_from_slice(&msg.origin.to_be_bytes());
                    buf.extend_from_slice(&msg.to.to_be_bytes());
                } else {
                    buf.push(Flags::Broadcast.bits());
                    buf.extend_from_slice(&msg.origin.to_be_bytes());
                }
                buf.extend_from_slice(&msg.body);
                buf
            }
        }
    }

    #[inline]
    pub(crate) fn decode(msg: &[u8]) -> Result<Self> {
        let flags = Flags::from_bits_truncate(msg[0]);
        let origin = u128::from_be_bytes(msg[1..17].try_into()?);
        match flags {
            Flags::Control => {
                let to = u128::from_be_bytes(msg[17..33].try_into()?);
                let body = bincode::deserialize::<Command>(&msg[33..])?;
                Ok(Sample::Control(Control {
                    origin,
                    to,
                    command: body,
                }))
            }
            Flags::ControlBroadcast => {
                let body = bincode::deserialize::<Command>(&msg[17..])?;
                Ok(Sample::Control(Control {
                    origin,
                    to: 0,
                    command: body,
                }))
            }
            Flags::PubSub => {
                let to = u128::from_be_bytes(msg[17..33].try_into()?);
                let topic_len = u32::from_be_bytes(msg[33..37].try_into()?);
                let topic = String::from_utf8(msg[37..37 + topic_len as usize].to_vec())?;
                let body = msg[37 + topic_len as usize..].to_vec();
                Ok(Sample::Message(Message {
                    origin,
                    to,
                    topic,
                    body,
                }))
            }
            Flags::PubSubBroadcast => {
                let topic_len = u32::from_be_bytes(msg[17..21].try_into()?);
                let topic = String::from_utf8(msg[21..21 + topic_len as usize].to_vec())?;
                let body = msg[21 + topic_len as usize..].to_vec();
                Ok(Sample::Message(Message {
                    origin,
                    to: 0,
                    topic,
                    body,
                }))
            }
            Flags::Broadcast => {
                let body = msg[17..].to_vec();
                Ok(Sample::Message(Message {
                    origin,
                    to: 0,
                    topic: "".to_owned(),
                    body,
                }))
            }
            Flags::To => {
                let to = u128::from_be_bytes(msg[17..33].try_into()?);
                let body = msg[33..].to_vec();
                Ok(Sample::Message(Message {
                    origin,
                    to,
                    topic: "".to_owned(),
                    body,
                }))
            }
            _ => Err(eyre!("unknown flags")),
        }
    }
}

#[test]
fn test_sample_encode() {
    use crate::util::Stats;

    common_x::log::init_log_filter("info");
    let mut stats = Stats::new(1000);
    let msg = Message::new(0, "topic", vec![0; 1024]).sample();
    let bytes = msg.encode();
    info!("bytes len: {:?}", bytes.len());
    for _ in 0..10000 {
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
    let mut stats = Stats::new(1000);
    for _ in 0..10000 {
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
    let msg = Sample::Control(Control {
        origin: 1,
        to: 3,
        command: Command::AddMember(Member::new(1.into(), vec![])),
    });
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Sample::decode(&buf);
    println!("msg is {:?}", msg);

    let msg = Sample::Message(Message {
        origin: 1,
        to: 0,
        topic: "topic".to_owned(),
        body: vec![1, 2, 3],
    });
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Sample::decode(&buf);
    println!("msg is {:?}", msg);
}
