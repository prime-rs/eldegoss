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
    pub const fn new(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    pub fn id(&self) -> ID {
        *self.0.get_id()
    }

    pub fn clock(&self) -> u64 {
        self.0.get_time().as_u64()
    }

    pub const fn timestamp(&self) -> Timestamp {
        self.0
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
    pub const fn new(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    pub fn id(&self) -> ID {
        *self.0.get_id()
    }

    pub fn clock(&self) -> u64 {
        self.0.get_time().as_u64()
    }

    pub const fn timestamp(&self) -> Timestamp {
        self.0
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub header: MessageHeader,
    pub key_expr: String,
    pub payload: Bytes,
}

impl Message {
    pub fn new(timestamp: Timestamp, key_expr: String, payload: Bytes) -> Self {
        Self {
            header: MessageHeader::new(timestamp),
            key_expr,
            payload,
        }
    }

    pub fn key_expr(&self) -> &str {
        &self.key_expr
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Sample {
    FocaData(Bytes),
    Message(Message),
}

impl Sample {
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
