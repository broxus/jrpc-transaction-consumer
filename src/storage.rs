use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres, Row};
use tycho_types::models::StdAddr;

const CREATE_CURSOR_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS transaction_consumer_cursors (
    account TEXT PRIMARY KEY,
    last_transaction_lt NUMERIC(20, 0),
    synced BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at BIGINT NOT NULL DEFAULT (
        floor(extract(epoch FROM (CURRENT_TIMESTAMP(3) AT TIME ZONE 'utc')) * 1000)::bigint
    ),
    CONSTRAINT transaction_consumer_cursors_last_transaction_lt_check
        CHECK (
            last_transaction_lt IS NULL
            OR (
                last_transaction_lt >= 0
                AND last_transaction_lt <= 18446744073709551615
            )
        )
)
";

const SELECT_CURSOR: &str = r"
SELECT
    last_transaction_lt::text AS last_transaction_lt,
    synced,
    updated_at
FROM transaction_consumer_cursors
WHERE account = $1
";

const UPSERT_CURSOR: &str = r"
INSERT INTO transaction_consumer_cursors (account, last_transaction_lt, synced)
VALUES ($1, ($2::text)::numeric(20, 0), $3)
ON CONFLICT (account) DO UPDATE
SET
    last_transaction_lt = EXCLUDED.last_transaction_lt,
    synced = EXCLUDED.synced,
    updated_at = floor(extract(epoch FROM (CURRENT_TIMESTAMP(3) AT TIME ZONE 'utc')) * 1000)::bigint
";

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountCursor {
    pub last_transaction_lt: Option<u64>,
    pub synced: bool,
    pub updated_at: i64,
}

#[derive(Clone)]
pub(crate) struct PostgresCursorStorage {
    pool: Pool<Postgres>,
}

impl PostgresCursorStorage {
    pub(crate) async fn from_pool(pool: Pool<Postgres>) -> Result<Self> {
        let storage = Self { pool };
        storage.ensure_schema().await?;
        Ok(storage)
    }

    async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(CREATE_CURSOR_TABLE)
            .execute(&self.pool)
            .await
            .context("Failed to ensure cursor table")?;

        Ok(())
    }

    pub(crate) async fn load_cursor(&self, account: &StdAddr) -> Result<Option<AccountCursor>> {
        let account = account.to_string();
        let row = sqlx::query(SELECT_CURSOR)
            .bind(&account)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("Failed to load cursor for {account}"))?;

        let Some(row) = row else {
            return Ok(None);
        };

        let last_transaction_lt: Option<String> = row
            .try_get("last_transaction_lt")
            .context("Failed to decode cursor lt")?;
        let synced = row
            .try_get("synced")
            .context("Failed to decode cursor sync state")?;
        let updated_at = row
            .try_get("updated_at")
            .context("Failed to decode cursor update time")?;

        Ok(Some(AccountCursor {
            last_transaction_lt: last_transaction_lt
                .map(|lt| {
                    lt.parse::<u64>()
                        .with_context(|| format!("Failed to parse cursor lt {lt}"))
                })
                .transpose()?,
            synced,
            updated_at,
        }))
    }

    pub(crate) async fn store_cursor(
        &self,
        account: &StdAddr,
        cursor: AccountCursor,
    ) -> Result<()> {
        let account = account.to_string();
        let last_transaction_lt = cursor
            .last_transaction_lt
            .map(|last_transaction_lt| format!("{last_transaction_lt}"));

        sqlx::query(UPSERT_CURSOR)
            .bind(&account)
            .bind(last_transaction_lt)
            .bind(cursor.synced)
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to store cursor for {account}"))?;

        Ok(())
    }
}
