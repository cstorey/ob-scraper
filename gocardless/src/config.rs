use std::{collections::HashMap, fs, net::SocketAddr, path::PathBuf, time::Duration};

use again::RetryPolicy;
use chrono::Days;
use clap::Args;
use color_eyre::{eyre::Context, Result};
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use tracing::{instrument, Span};
use uuid::Uuid;

use crate::{connect::Requisition, files::write_json_atomically};

#[derive(Debug, Clone, Args)]
pub(crate) struct ConfigArg {
    #[clap(short = 'c', long = "config", help = "Configuration file")]
    config: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ProviderConfig {
    pub(crate) institution_id: String,
    pub(crate) output: PathBuf,
    pub(crate) history_days: Option<u64>,
    pub(crate) state: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct RetryConfig {
    delay_s: Option<u64>,
    max_delay_s: Option<u64>,
    max_retries: Option<usize>,
}
impl RetryConfig {
    pub(crate) fn as_retry_policy(&self) -> again::RetryPolicy {
        let mut retry_policy = RetryPolicy::exponential(
            self.delay_s
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(1)),
        );
        if let Some(max_retries) = self.max_retries {
            retry_policy = retry_policy.with_max_retries(max_retries)
        }
        if let Some(max_delay_s) = self.max_delay_s {
            retry_policy = retry_policy.with_max_delay(Duration::from_secs(max_delay_s))
        }

        retry_policy
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ScraperConfig {
    pub(crate) provider: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub(crate) retries: RetryConfig,
    pub(crate) http: HttpListenerConfig,
}
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct HttpListenerConfig {
    pub(crate) bind_address: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProviderState {
    pub(crate) requisition_id: Uuid,
}
impl ConfigArg {
    pub(crate) async fn load(&self) -> Result<ScraperConfig> {
        let content = tokio::fs::read_to_string(&self.config)
            .await
            .wrap_err_with(|| format!("Reading config file: {:?}", self.config))?;
        let config = toml::from_str(&content).context("Parse toml")?;

        Ok(config)
    }
}

impl ProviderConfig {
    pub(crate) fn history_days(&self) -> Days {
        Days::new(self.history_days.unwrap_or(90))
    }

    pub(crate) async fn write_state(&self, state: &ProviderState) -> Result<()> {
        write_json_atomically(&self.state, state.clone()).await
    }

    #[instrument(skip_all, fields(path=?self.state))]
    pub(crate) async fn load_state(&self) -> Result<ProviderState> {
        let span = Span::current();
        let path = self.state.to_owned();
        spawn_blocking(move || -> Result<_> {
            let _entered = span.enter();
            let f = fs::File::open(&path).wrap_err_with(|| format!("Open state file: {path:?}"))?;
            let state = serde_json::from_reader(f)?;

            Ok(state)
        })
        .await?
    }
}

impl ProviderState {
    pub(crate) fn from_requisition(requisition: &Requisition) -> Self {
        ProviderState {
            requisition_id: requisition.id,
        }
    }
}
