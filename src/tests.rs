use crate::{StartFrom, TransactionConsumer, TransactionConsumerOptions};
use anyhow::Result;
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

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
        TransactionConsumerOptions {
            start_from: StartFrom::Beginning,
            batch_size: 100,
            realtime_poll_interval: Duration::from_secs(1),
        },
    )
    .await?;
    let stream = consumer
        .stream_transactions(vec![
            "0:7094fc3cb69fa1b7bde8e830e2cd74bc9455d93561ce2c562182215686eb45e2",
            "0:8268a0771fb1fc016d475446ba4b87ca7fbb0db76480d4991216ea7586ddb889",
        ])
        .await?;
    futures::pin_mut!(stream);
    let mut count = 0;

    while let Some(consumed) = stream.next().await {
        let consumed = consumed?;

        println!(
            "{} {}",
            consumed.account, consumed.transaction.prev_trans_lt,
        );

        println!("{:?}", consumed.transaction);

        consumed.commit().await?;
        count += 1;

        if count >= 4 {
            break;
        }
    }

    Ok(())
}
