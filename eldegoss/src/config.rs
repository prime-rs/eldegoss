use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub id: String,

    pub listen: String,
    pub connect: Vec<String>,

    pub ca_path: String,
    pub cert_path: String,
    pub private_key_path: String,

    pub keep_alive_interval: u64,
    pub check_link_interval: u64,
    pub timeout: u64,

    pub gossip_fanout: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: uhlc::ID::rand().to_string(),
            ca_path: Default::default(),
            connect: Default::default(),
            listen: Default::default(),
            cert_path: Default::default(),
            private_key_path: Default::default(),
            keep_alive_interval: 5,
            check_link_interval: 1,
            timeout: 10,
            gossip_fanout: 3,
        }
    }
}
