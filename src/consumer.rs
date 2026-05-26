use std::sync::Arc;

use anyhow::{Context, Result};
use futures::channel::mpsc;
use futures::{SinkExt, Stream};
use sqlx::{Pool, Postgres};
use tycho_types::boc::BocRepr;
use tycho_types::models::{StdAddr, Transaction};

use crate::storage::PostgresCursorStorage;
use crate::{
    AccountCursor, ConsumedTransaction, IntoSubscriptionAccount, JrpcClient, StartFrom,
    TransactionConsumerOptions, TransactionSource,
};

#[derive(Clone)]
pub struct TransactionConsumer {
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    options: TransactionConsumerOptions,
}

impl TransactionConsumer {
    async fn new(
        source: Arc<dyn TransactionSource>,
        pool: Pool<Postgres>,
        options: TransactionConsumerOptions,
    ) -> Result<Self> {
        let storage = PostgresCursorStorage::from_pool(pool).await?;
        Ok(Self {
            source,
            storage,
            options,
        })
    }

    pub async fn from_jrpc(
        endpoint: impl Into<String>,
        pool: Pool<Postgres>,
        options: TransactionConsumerOptions,
    ) -> Result<Self> {
        Self::new(Arc::new(JrpcClient::new(endpoint)), pool, options).await
    }

    pub async fn stream_transactions<I, A>(
        &self,
        accounts: I,
    ) -> Result<impl Stream<Item = Result<ConsumedTransaction>>>
    where
        I: IntoIterator<Item = A>,
        A: IntoSubscriptionAccount,
    {
        let accounts = accounts
            .into_iter()
            .map(IntoSubscriptionAccount::into_std_addr)
            .collect::<Result<Vec<_>>>()?;

        let (tx, rx) = mpsc::channel(1);
        let source = self.source.clone();
        let storage = self.storage.clone();
        let options = self.options;

        tokio::spawn(async move {
            run_worker(source, storage, options, accounts, tx).await;
        });

        Ok(rx)
    }

    pub async fn cursor<A>(&self, account: A) -> Result<Option<AccountCursor>>
    where
        A: IntoSubscriptionAccount,
    {
        let account = account.into_std_addr()?;
        self.storage.load_cursor(&account).await
    }

    pub async fn is_account_synced<A>(&self, account: A) -> Result<bool>
    where
        A: IntoSubscriptionAccount,
    {
        Ok(self
            .cursor(account)
            .await?
            .map(|cursor| cursor.synced)
            .unwrap_or(false))
    }

    pub async fn all_accounts_synced<I, A>(&self, accounts: I) -> Result<bool>
    where
        I: IntoIterator<Item = A>,
        A: IntoSubscriptionAccount,
    {
        for account in accounts {
            if !self.is_account_synced(account).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

async fn run_worker(
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    options: TransactionConsumerOptions,
    accounts: Vec<StdAddr>,
    mut tx: mpsc::Sender<Result<ConsumedTransaction>>,
) {
    let limit = options.batch_size.clamp(1, 100);

    for account in accounts {
        let result = stream_account(
            source.clone(),
            storage.clone(),
            options,
            limit,
            account,
            &mut tx,
        )
        .await;

        if let Err(e) = result {
            let _ = tx.send(Err(e)).await;
            return;
        }
    }
}

async fn stream_account(
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    options: TransactionConsumerOptions,
    limit: u8,
    account: StdAddr,
    tx: &mut mpsc::Sender<Result<ConsumedTransaction>>,
) -> Result<()> {
    let mut cursor = match options.start_from {
        StartFrom::Beginning => AccountCursor::default(),
        StartFrom::Stored => storage.load_cursor(&account).await?.unwrap_or_default(),
    };

    if cursor.synced {
        return Ok(());
    }

    loop {
        let transactions = source
            .get_transactions(&account, cursor.last_transaction_lt, limit)
            .await
            .with_context(|| format!("Failed to fetch transactions for {account}"))?;

        if transactions.is_empty() {
            cursor.synced = true;
            storage
                .store_cursor(&account, cursor)
                .await
                .with_context(|| format!("Failed to mark {account} as synced"))?;
            tracing::info!(account = %account, "account synced");
            return Ok(());
        }

        for boc in transactions {
            let transaction = BocRepr::decode_base64::<Transaction, _>(&boc)
                .with_context(|| format!("Failed to decode transaction for {account}"))?;

            anyhow::ensure!(
                transaction.account == account.address,
                "JRPC returned transaction for a different account"
            );

            cursor = AccountCursor {
                last_transaction_lt: Some(transaction.prev_trans_lt),
                synced: false,
                updated_at: 0,
            };
            let (item, rx) = ConsumedTransaction::new(
                account.clone(),
                transaction,
                boc,
                cursor,
                storage.clone(),
            );

            if tx.send(Ok(item)).await.is_err() {
                return Ok(());
            }

            if rx.await.is_err() {
                return Ok(());
            }
        }
    }
}
