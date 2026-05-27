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

        for account in accounts {
            let source = self.source.clone();
            let storage = self.storage.clone();
            let options = self.options;
            let tx = tx.clone();

            tokio::spawn(async move {
                run_account_worker(source, storage, options, account, tx).await;
            });
        }

        drop(tx);

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

async fn run_account_worker(
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    options: TransactionConsumerOptions,
    account: StdAddr,
    mut tx: mpsc::Sender<Result<ConsumedTransaction>>,
) {
    let limit = options.batch_size.clamp(1, 100);
    let cursor = sync_account(
        source.clone(),
        storage.clone(),
        options,
        limit,
        account.clone(),
        &mut tx,
    )
    .await;

    match cursor {
        Ok(Some(cursor)) => {
            listen_realtime_account(
                source,
                storage,
                limit,
                options.realtime_poll_interval,
                AccountSubscription { account, cursor },
                &mut tx,
            )
            .await;
        }
        Ok(None) => {}
        Err(e) => {
            let _ = tx.send(Err(e)).await;
        }
    }
}

async fn sync_account(
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    options: TransactionConsumerOptions,
    limit: u8,
    account: StdAddr,
    tx: &mut mpsc::Sender<Result<ConsumedTransaction>>,
) -> Result<Option<AccountCursor>> {
    let mut cursor = match options.start_from {
        StartFrom::Beginning => AccountCursor::default(),
        StartFrom::Stored => storage.load_cursor(&account).await?.unwrap_or_default(),
    };

    if cursor.synced {
        return Ok(Some(cursor));
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
            return Ok(Some(cursor));
        }

        for boc in transactions {
            let fetched = decode_transaction(&account, boc)?;
            let latest_transaction_lt = cursor
                .latest_transaction_lt
                .or(Some(fetched.transaction.lt));
            let next_cursor = AccountCursor {
                last_transaction_lt: Some(fetched.transaction.prev_trans_lt),
                latest_transaction_lt,
                synced: false,
                updated_at: cursor.updated_at,
            };
            let (item, rx) = ConsumedTransaction::new(
                account.clone(),
                fetched.transaction,
                fetched.boc,
                next_cursor,
                storage.clone(),
            );

            if tx.send(Ok(item)).await.is_err() {
                return Ok(None);
            }

            if rx.await.is_err() {
                return Ok(None);
            }

            cursor = next_cursor;
        }
    }
}

async fn listen_realtime_account(
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    limit: u8,
    realtime_poll_interval: std::time::Duration,
    mut subscription: AccountSubscription,
    tx: &mut mpsc::Sender<Result<ConsumedTransaction>>,
) {
    loop {
        let result = poll_realtime_account(
            source.clone(),
            storage.clone(),
            limit,
            &mut subscription,
            tx,
        )
        .await;

        match result {
            Ok(true) => {}
            Ok(false) => return,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }

        tokio::time::sleep(realtime_poll_interval).await;
    }
}

async fn poll_realtime_account(
    source: Arc<dyn TransactionSource>,
    storage: PostgresCursorStorage,
    limit: u8,
    subscription: &mut AccountSubscription,
    tx: &mut mpsc::Sender<Result<ConsumedTransaction>>,
) -> Result<bool> {
    let transactions = collect_realtime_transactions(
        source,
        &subscription.account,
        subscription.cursor.latest_transaction_lt,
        limit,
    )
    .await?;

    for fetched in transactions {
        let next_cursor = AccountCursor {
            last_transaction_lt: subscription.cursor.last_transaction_lt,
            latest_transaction_lt: Some(fetched.transaction.lt),
            synced: true,
            updated_at: subscription.cursor.updated_at,
        };
        let (item, rx) = ConsumedTransaction::new(
            subscription.account.clone(),
            fetched.transaction,
            fetched.boc,
            next_cursor,
            storage.clone(),
        );

        if tx.send(Ok(item)).await.is_err() {
            return Ok(false);
        }

        if rx.await.is_err() {
            return Ok(false);
        }

        subscription.cursor = next_cursor;
    }

    Ok(true)
}

async fn collect_realtime_transactions(
    source: Arc<dyn TransactionSource>,
    account: &StdAddr,
    latest_transaction_lt: Option<u64>,
    limit: u8,
) -> Result<Vec<FetchedTransaction>> {
    let mut last_transaction_lt = None;
    let mut transactions = Vec::new();

    loop {
        let page = source
            .get_transactions(account, last_transaction_lt, limit)
            .await
            .with_context(|| format!("Failed to fetch realtime transactions for {account}"))?;

        if page.is_empty() {
            break;
        }

        let mut reached_known_transaction = false;
        let mut page_last_transaction_lt = None;

        for boc in page {
            let fetched = decode_transaction(account, boc)?;
            page_last_transaction_lt = Some(fetched.transaction.prev_trans_lt);

            if Some(fetched.transaction.lt) == latest_transaction_lt {
                reached_known_transaction = true;
                break;
            }

            transactions.push(fetched);
        }

        if reached_known_transaction {
            break;
        }

        last_transaction_lt = page_last_transaction_lt;

        if last_transaction_lt.is_none() {
            break;
        }
    }

    transactions.reverse();
    Ok(transactions)
}

fn decode_transaction(account: &StdAddr, boc: String) -> Result<FetchedTransaction> {
    let transaction = BocRepr::decode_base64::<Transaction, _>(&boc)
        .with_context(|| format!("Failed to decode transaction for {account}"))?;

    anyhow::ensure!(
        transaction.account == account.address,
        "JRPC returned transaction for a different account"
    );

    Ok(FetchedTransaction { boc, transaction })
}

struct AccountSubscription {
    account: StdAddr,
    cursor: AccountCursor,
}

struct FetchedTransaction {
    boc: String,
    transaction: Transaction,
}
