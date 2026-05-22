use anyhow::Result;
use tycho_types::models::StdAddr;

#[async_trait::async_trait]
pub trait TransactionSource: Send + Sync {
    async fn get_transactions(
        &self,
        account: &StdAddr,
        last_transaction_lt: Option<u64>,
        limit: u8,
    ) -> Result<Vec<String>>;
}
