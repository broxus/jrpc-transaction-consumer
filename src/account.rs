use anyhow::{Context, Result};
use tycho_types::models::{IntAddr, StdAddr};

pub trait IntoSubscriptionAccount {
    fn into_std_addr(self) -> Result<StdAddr>;
}

impl IntoSubscriptionAccount for StdAddr {
    fn into_std_addr(self) -> Result<StdAddr> {
        Ok(self)
    }
}

impl IntoSubscriptionAccount for &StdAddr {
    fn into_std_addr(self) -> Result<StdAddr> {
        Ok(self.clone())
    }
}

impl IntoSubscriptionAccount for IntAddr {
    fn into_std_addr(self) -> Result<StdAddr> {
        match self {
            IntAddr::Std(account) => Ok(account),
            IntAddr::Var(_) => anyhow::bail!("variable-length addresses are not supported by JRPC"),
        }
    }
}

impl IntoSubscriptionAccount for &IntAddr {
    fn into_std_addr(self) -> Result<StdAddr> {
        self.clone().into_std_addr()
    }
}

impl IntoSubscriptionAccount for &str {
    fn into_std_addr(self) -> Result<StdAddr> {
        self.parse()
            .with_context(|| format!("Failed to parse account address {self}"))
    }
}

impl IntoSubscriptionAccount for String {
    fn into_std_addr(self) -> Result<StdAddr> {
        self.as_str().into_std_addr()
    }
}
