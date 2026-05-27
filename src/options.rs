use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionConsumerOptions {
    pub start_from: StartFrom,
    pub batch_size: u8,
    #[serde(with = "duration_millis")]
    pub realtime_poll_interval: Duration,
}

impl Default for TransactionConsumerOptions {
    fn default() -> Self {
        Self {
            start_from: StartFrom::Stored,
            batch_size: 100,
            realtime_poll_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StartFrom {
    Beginning,
    #[default]
    Stored,
}

mod duration_millis {
    use super::*;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(
            duration
                .as_millis()
                .try_into()
                .map_err(serde::ser::Error::custom)?,
        )
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}
