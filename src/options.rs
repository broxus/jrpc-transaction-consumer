use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionConsumerOptions {
    pub start_from: StartFrom,
    pub batch_size: u8,
}

impl Default for TransactionConsumerOptions {
    fn default() -> Self {
        Self {
            start_from: StartFrom::Stored,
            batch_size: 100,
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
