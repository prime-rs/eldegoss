use std::str::FromStr;

use bytes::Bytes;
use color_eyre::{
    eyre::{eyre, Error},
    Result,
};
use serde::{Deserialize, Serialize};
use uhlc::{Timestamp, ID};

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct EldegossId(Timestamp);

impl EldegossId {
    #[inline]
    pub const fn new(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    #[inline]
    pub fn id(&self) -> ID {
        *self.0.get_id()
    }

    #[inline]
    pub fn clock(&self) -> u64 {
        self.0.get_time().as_u64()
    }

    #[inline]
    pub const fn timestamp(&self) -> Timestamp {
        self.0
    }

    #[inline]
    pub fn hlc(&self) -> uhlc::HLC {
        uhlc::HLCBuilder::new().with_id(self.id()).build()
    }
}

impl Default for EldegossId {
    fn default() -> Self {
        Self::new(uhlc::HLCBuilder::new().build().new_timestamp())
    }
}

impl FromStr for EldegossId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = ID::from_str(s).map_err(|err| eyre!("{err:?}"))?;
        Ok(Self(
            uhlc::HLCBuilder::new().with_id(id).build().new_timestamp(),
        ))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageHeader(Timestamp);

impl MessageHeader {
    #[inline]
    pub const fn new(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    #[inline]
    pub fn id(&self) -> ID {
        *self.0.get_id()
    }

    #[inline]
    pub fn clock(&self) -> u64 {
        self.0.get_time().as_u64()
    }

    #[inline]
    pub const fn timestamp(&self) -> Timestamp {
        self.0
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub header: MessageHeader,
    pub payload: Bytes,
}

impl Message {
    #[inline]
    pub fn new(timestamp: Timestamp, payload: Bytes) -> Self {
        Self {
            header: MessageHeader::new(timestamp),
            payload,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Payload {
    FocaData(Bytes),
    Message(Bytes),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Sample {
    pub header: MessageHeader,
    pub payload: Payload,
}

impl Sample {
    #[inline]
    pub fn new_msg(msg: Message) -> Self {
        Self {
            header: msg.header,
            payload: Payload::Message(msg.payload),
        }
    }

    #[inline]
    pub fn new_foca(timestamp: Timestamp, payload: Bytes) -> Self {
        Self {
            header: MessageHeader::new(timestamp),
            payload: Payload::FocaData(payload),
        }
    }

    #[inline]
    pub fn encode(&self) -> Result<Bytes> {
        bincode::serialize(self)
            .map_err(|err| eyre!("sample encode failed: {err:?}"))
            .map(Bytes::from)
    }

    #[inline]
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|err| eyre!("sample decode failed: {err:?}"))
    }
}
