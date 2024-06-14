use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub id: String,

    pub listen: String,
    pub connect: Vec<String>,
    pub announce: Vec<String>,

    pub ca_path: String,
    pub cert_path: String,
    pub private_key_path: String,

    pub subscription_list: Vec<String>,

    pub keep_alive_interval: u64,
    pub check_link_interval: u64,
    pub msg_timeout: u64,

    pub gossip_fanout: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: uhlc::ID::rand().to_string(),
            announce: Default::default(),
            ca_path: Default::default(),
            connect: Default::default(),
            listen: Default::default(),
            cert_path: Default::default(),
            private_key_path: Default::default(),
            subscription_list: Default::default(),
            keep_alive_interval: 5,
            check_link_interval: 1,
            msg_timeout: 2,
            gossip_fanout: 3,
        }
    }
}
