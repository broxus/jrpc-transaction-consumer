use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tycho_types::models::StdAddr;

use crate::TransactionSource;

#[derive(Clone)]
pub struct JrpcClient {
    endpoint: Arc<str>,
    client: reqwest::Client,
}

impl JrpcClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_client(endpoint, reqwest::Client::new())
    }

    pub fn with_client(endpoint: impl Into<String>, client: reqwest::Client) -> Self {
        let endpoint = endpoint.into();
        Self {
            endpoint: Arc::from(endpoint),
            client,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait::async_trait]
impl TransactionSource for JrpcClient {
    async fn get_transactions(
        &self,
        account: &StdAddr,
        last_transaction_lt: Option<u64>,
        limit: u8,
    ) -> Result<Vec<String>> {
        let params = GetTransactionsListParams {
            account: account.to_string(),
            limit,
            last_transaction_lt: last_transaction_lt
                .map(|last_transaction_lt| format!("{last_transaction_lt}")),
        };
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 0,
            method: "getTransactionsList",
            params,
        };

        let endpoint = self.endpoint();
        let response = self
            .client
            .post(endpoint)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("Failed to call {endpoint}"))?
            .error_for_status()
            .with_context(|| format!("JRPC HTTP error from {endpoint}"))?
            .json::<JsonRpcResponse<Vec<String>>>()
            .await
            .context("Failed to decode JRPC response")?;

        match (response.result, response.error) {
            (Some(result), None) => Ok(result),
            (_, Some(error)) => {
                let code = error.code;
                let message = error.message;
                anyhow::bail!("JRPC error {code}: {message}")
            }
            (None, None) => anyhow::bail!("JRPC response has neither result nor error"),
        }
    }
}

#[derive(Serialize)]
struct JsonRpcRequest<T> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: T,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetTransactionsListParams {
    account: String,
    limit: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_transaction_lt: Option<String>,
}

#[derive(Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}
