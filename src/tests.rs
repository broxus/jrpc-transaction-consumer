use anyhow::Result;
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;

use crate::{TransactionConsumer, TransactionConsumerOptions};

#[tokio::test]
pub async fn transaction_test() -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5u32)
        .connect("postgresql://postgres:postgres@localhost:5432/test")
        .await
        .expect("fail pg pool");

    let consumer = TransactionConsumer::from_jrpc(
        "https://jrpc.venom.foundation",
        pool,
        TransactionConsumerOptions::default(),
    )
    .await?;
    let stream = consumer
        .stream_transactions(vec![
            "0:c786613a020cc55913022edeeedd96ebd7de91174cdb94ca92f7af88c910c686",
            "0:91bb611575285d48eed3385e3184fc502cd3bb4a54938b0759d9d8c804e96e28",
        ])
        .await?;
    futures::pin_mut!(stream);
    while let Some(consumed) = stream.next().await {
        let consumed = consumed?;
        println!(
            "{} {}",
            consumed.account, consumed.transaction.prev_trans_lt
        );
        consumed.commit().await?;
    }
    Ok(())
}
