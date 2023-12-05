use std::{
    fmt::{self, Display},
    str::FromStr,
};

use bitflags::bitflags;
use color_eyre::{eyre::eyre, Result};
use serde::{Deserialize, Serialize};

use crate::{quic::config, Member, Membership};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum EldegossMsgBody {
    AddMember(Member),
    RemoveMember(u128),
    JoinReq(Vec<String>),
    JoinRsp(Membership),
    CheckReq(u128),
    CheckRsp(u128, bool),
    Data(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct EldegossMsg {
    pub origin: u128,
    pub from: u128,
    pub to: u128,
    pub body: EldegossMsgBody,
}

#[derive(Debug, Clone)]
pub enum Message {
    EldegossMsg(EldegossMsg),
    Msg(Msg),
}

impl Message {
    pub fn eldegoss(to: u128, body: EldegossMsgBody) -> Self {
        Self::EldegossMsg(EldegossMsg {
            origin: config().id,
            from: config().id,
            to,
            body,
        })
    }

    pub fn msg(to: u128, topic: String, body: Vec<u8>) -> Self {
        Self::Msg(Msg {
            origin: config().id,
            from: config().id,
            to,
            topic,
            body,
        })
    }

    pub const fn origin(&self) -> u128 {
        match self {
            Message::EldegossMsg(msg) => msg.origin,
            Message::Msg(msg) => msg.origin,
        }
    }

    pub const fn from(&self) -> u128 {
        match self {
            Message::EldegossMsg(msg) => msg.from,
            Message::Msg(msg) => msg.from,
        }
    }

    pub const fn to(&self) -> u128 {
        match self {
            Message::EldegossMsg(msg) => msg.to,
            Message::Msg(msg) => msg.to,
        }
    }

    pub fn topic(&self) -> String {
        match self {
            Message::EldegossMsg(_) => "".to_owned(),
            Message::Msg(msg) => msg.topic.clone(),
        }
    }

    pub fn set_origin(&mut self, origin: u128) {
        match self {
            Message::EldegossMsg(msg) => msg.origin = origin,
            Message::Msg(msg) => msg.origin = origin,
        }
    }

    pub fn set_from(&mut self, from: u128) {
        match self {
            Message::EldegossMsg(msg) => msg.from = from,
            Message::Msg(msg) => msg.from = from,
        }
    }

    pub fn set_to(&mut self, to: u128) {
        match self {
            Message::EldegossMsg(msg) => msg.to = to,
            Message::Msg(msg) => msg.to = to,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Msg {
    origin: u128,
    from: u128,
    to: u128,
    topic: String,
    body: Vec<u8>,
}

pub fn encode_msg(msg: &Message) -> Vec<u8> {
    match msg {
        Message::EldegossMsg(msg) => {
            let mut buf = Vec::new();
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
            let mut buf = Vec::new();
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

/// decode msg from bytes.
/// Notes: origin is not set in here, it should be set by receiver.
pub fn decode_msg(msg: &[u8]) -> Result<Message> {
    let flags = Flags::from_bits_truncate(msg[0]);
    let origin = u128::from_be_bytes(msg[1..17].try_into()?);
    match flags {
        Flags::Eldegoss => {
            let to = u128::from_be_bytes(msg[17..33].try_into()?);
            let body = bincode::deserialize::<EldegossMsgBody>(&msg[33..])?;
            Ok(Message::EldegossMsg(EldegossMsg {
                origin,
                to,
                body,
                from: 0,
            }))
        }
        Flags::EldegossBroadcast => {
            let body = bincode::deserialize::<EldegossMsgBody>(&msg[17..])?;
            Ok(Message::EldegossMsg(EldegossMsg {
                origin,
                from: 0,
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
                from: 0,
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
                from: 0,
                to: 0,
                topic,
                body,
            }))
        }
        Flags::Broadcast => {
            let body = msg[17..].to_vec();
            Ok(Message::Msg(Msg {
                origin,
                from: 0,
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
                from: 0,
                to,
                topic: "".to_owned(),
                body,
            }))
        }
        _ => Err(eyre!("unknown flags")),
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
        from: 2,
        to: 3,
        body: EldegossMsgBody::AddMember(Member {
            id: 1.into(),
            neighbor_list: std::collections::HashSet::new(),
            subscription_list: std::collections::HashSet::new(),
        }),
    });
    let buf = encode_msg(&msg);
    println!("buf is {:?}", buf);
    let msg = decode_msg(&buf);
    println!("msg is {:?}", msg);

    let msg = Message::Msg(Msg {
        origin: 1,
        from: 0,
        to: 0,
        topic: "topic".to_owned(),
        body: vec![1, 2, 3],
    });
    let buf = encode_msg(&msg);
    println!("buf is {:?}", buf);
    let msg = decode_msg(&buf);
    println!("msg is {:?}", msg);
}
