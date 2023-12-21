use std::{
    fmt::{self, Display},
    str::FromStr,
};

use bitflags::bitflags;
use color_eyre::{eyre::Ok, Result};
use serde::{Deserialize, Serialize};

use crate::{Member, Membership};

bitflags! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Flags: u8 {
        const Eldegoss = 0b10000000;
        const Broadcast = 0b01000000;
        const PubSub = 0b00100000;

        const To = !(Self::Eldegoss.bits() | Self::Broadcast.bits() | Self::PubSub.bits());
        const EldegossBroadcast = Self::Eldegoss.bits() | Self::Broadcast.bits();
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
pub enum EldegossMsgBody {
    AddMember(Member),
    RemoveMember(u128),
    JoinReq(Vec<u8>),
    JoinRsp(Membership),
    CheckReq(u128),
    CheckRsp(u128, bool),
}

#[derive(Debug)]
pub struct EldegossMsg {
    pub origin: u128,
    pub to: u128,
    pub body: EldegossMsgBody,
}

#[derive(Debug, Default)]
pub struct Msg {
    pub origin: u128,
    pub to: u128,
    pub topic: String,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub enum Message {
    EldegossMsg(EldegossMsg),
    Msg(Msg),
    None,
}

impl Message {
    pub const fn eldegoss(to: u128, body: EldegossMsgBody) -> Self {
        Self::EldegossMsg(EldegossMsg {
            origin: 0,
            to,
            body,
        })
    }

    pub fn msg(to: u128, topic: String, body: Vec<u8>) -> Self {
        Self::Msg(Msg {
            origin: 0,
            to,
            topic,
            body,
        })
    }

    pub fn to_msg(to: u128, body: Vec<u8>) -> Self {
        Self::Msg(Msg {
            origin: 0,
            to,
            topic: Default::default(),
            body,
        })
    }

    pub fn pub_msg(topic: &str, body: Vec<u8>) -> Self {
        Self::Msg(Msg {
            origin: 0,
            to: 0,
            topic: topic.to_owned(),
            body,
        })
    }

    #[inline]
    pub const fn origin(&self) -> u128 {
        match self {
            Message::EldegossMsg(msg) => msg.origin,
            Message::Msg(msg) => msg.origin,
            Message::None => 0,
        }
    }

    #[inline]
    pub const fn to(&self) -> u128 {
        match self {
            Message::EldegossMsg(msg) => msg.to,
            Message::Msg(msg) => msg.to,
            Message::None => 0,
        }
    }

    #[inline]
    pub fn topic(&self) -> String {
        match self {
            Message::EldegossMsg(_) => "".to_owned(),
            Message::Msg(msg) => msg.topic.clone(),
            Message::None => "".to_owned(),
        }
    }

    #[inline]
    pub fn set_origin(&mut self, origin: u128) {
        match self {
            Message::EldegossMsg(msg) => msg.origin = origin,
            Message::Msg(msg) => msg.origin = origin,
            Message::None => {}
        }
    }

    #[inline]
    pub fn set_to(&mut self, to: u128) {
        match self {
            Message::EldegossMsg(msg) => msg.to = to,
            Message::Msg(msg) => msg.to = to,
            Message::None => {}
        }
    }
}

impl Message {
    #[inline]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Message::EldegossMsg(msg) => {
                if msg.to == 0 {
                    buf.push(Flags::EldegossBroadcast.bits());
                    buf.extend_from_slice(&msg.origin.to_be_bytes());
                } else {
                    buf.push(Flags::Eldegoss.bits());
                    buf.extend_from_slice(&msg.origin.to_be_bytes());
                    buf.extend_from_slice(&msg.to.to_be_bytes());
                }
                buf.extend_from_slice(&bincode::serialize(&msg.body).unwrap());
                buf
            }
            Message::Msg(msg) => {
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
            Message::None => vec![],
        }
    }

    #[inline]
    pub fn decode(msg: &[u8]) -> Result<Self> {
        if msg.len() < 17 {
            return Ok(Message::None);
        }
        let flags = Flags::from_bits_truncate(msg[0]);
        let origin = u128::from_be_bytes(msg[1..17].try_into()?);
        match flags {
            Flags::Eldegoss => {
                let to = u128::from_be_bytes(msg[17..33].try_into()?);
                let body = bincode::deserialize::<EldegossMsgBody>(&msg[33..])?;
                Ok(Message::EldegossMsg(EldegossMsg { origin, to, body }))
            }
            Flags::EldegossBroadcast => {
                let body = bincode::deserialize::<EldegossMsgBody>(&msg[17..])?;
                Ok(Message::EldegossMsg(EldegossMsg {
                    origin,
                    to: 0,
                    body,
                }))
            }
            Flags::PubSub => {
                let to = u128::from_be_bytes(msg[17..33].try_into()?);
                let topic_len = u32::from_be_bytes(msg[33..37].try_into()?);
                let topic = String::from_utf8(msg[37..37 + topic_len as usize].to_vec())?;
                let body = msg[37 + topic_len as usize..].to_vec();
                Ok(Message::Msg(Msg {
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
                Ok(Message::Msg(Msg {
                    origin,
                    to: 0,
                    topic,
                    body,
                }))
            }
            Flags::Broadcast => {
                let body = msg[17..].to_vec();
                Ok(Message::Msg(Msg {
                    origin,
                    to: 0,
                    topic: "".to_owned(),
                    body,
                }))
            }
            Flags::To => {
                let to = u128::from_be_bytes(msg[17..33].try_into()?);
                let body = msg[33..].to_vec();
                Ok(Message::Msg(Msg {
                    origin,
                    to,
                    topic: "".to_owned(),
                    body,
                }))
            }
            _ => Ok(Message::None),
        }
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
    let msg = Message::EldegossMsg(EldegossMsg {
        origin: 1,
        to: 3,
        body: EldegossMsgBody::AddMember(Member::new(1.into(), vec![])),
    });
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Message::decode(&buf);
    println!("msg is {:?}", msg);

    let msg = Message::Msg(Msg {
        origin: 1,
        to: 0,
        topic: "topic".to_owned(),
        body: vec![1, 2, 3],
    });
    let buf = msg.encode();
    println!("buf is {:?}", buf);
    let msg = Message::decode(&buf);
    println!("msg is {:?}", msg);
}
