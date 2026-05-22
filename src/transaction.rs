use anyhow::{Context, Result};
use futures::channel::oneshot;
use tycho_types::models::{StdAddr, Transaction};

use crate::AccountCursor;
use crate::storage::PostgresCursorStorage;

pub struct ConsumedTransaction {
    pub account: StdAddr,
    pub transaction: Transaction,
    pub boc: String,
    cursor: AccountCursor,
    storage: PostgresCursorStorage,
    commit_channel: Option<oneshot::Sender<()>>,
}

impl ConsumedTransaction {
    pub(crate) fn new(
        account: StdAddr,
        transaction: Transaction,
        boc: String,
        cursor: AccountCursor,
        storage: PostgresCursorStorage,
    ) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                account,
                transaction,
                boc,
                cursor,
                storage,
                commit_channel: Some(tx),
            },
            rx,
        )
    }

    pub fn next_last_transaction_lt(&self) -> Option<u64> {
        self.cursor.last_transaction_lt
    }

    pub async fn commit(mut self) -> Result<()> {
        self.storage
            .store_cursor(&self.account, self.cursor)
            .await
            .context("Failed to store transaction cursor")?;

        let committer = self.commit_channel.take().context("Already committed")?;
        committer
            .send(())
            .map_err(|_| anyhow::anyhow!("Failed committing"))?;

        Ok(())
    }

    pub fn into_inner(self) -> (StdAddr, Transaction, String) {
        (self.account, self.transaction, self.boc)
    }
}
