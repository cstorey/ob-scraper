use std::{cmp::Ordering, collections::HashMap};

use chrono::{DateTime, Datelike, Days, Local, Months, NaiveDate, Utc};
use clap::Parser;
use color_eyre::{
    eyre::{bail, eyre},
    Result,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::{
    accounts::{Account, AccountStatus, Balances},
    auth::AuthArgs,
    client::BankDataClient,
    config::{ConfigArg, ProviderConfig, ScraperConfig},
    connect::Requisition,
    files::write_json_lines,
    transactions::{Transaction, Transactions, TransactionsQuery},
};

#[derive(Debug, Parser)]
pub struct Cmd {
    #[clap(flatten)]
    auth: AuthArgs,
    #[clap(flatten)]
    config: ConfigArg,
    #[clap(short = 'p', long = "provider", help = "Provider name")]
    provider: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status")]
enum TransactionWithStatus {
    #[serde(rename = "pending")]
    Pending(Transaction),
    #[serde(rename = "booked")]
    Booked(Transaction),
}

impl TransactionWithStatus {
    fn timestamp_best_effort(&self) -> Option<DateTime<Utc>> {
        match self {
            TransactionWithStatus::Pending(transaction) => transaction.timestamp_best_effort(),
            TransactionWithStatus::Booked(transaction) => transaction.timestamp_best_effort(),
        }
    }

    fn transaction_id(&self) -> Option<&str> {
        match self {
            TransactionWithStatus::Pending(transaction) => transaction.transaction_id.as_deref(),
            TransactionWithStatus::Booked(transaction) => transaction.transaction_id.as_deref(),
        }
    }
    fn internal_transaction_id(&self) -> Option<&str> {
        match self {
            TransactionWithStatus::Pending(transaction) => {
                transaction.internal_transaction_id.as_deref()
            }
            TransactionWithStatus::Booked(transaction) => {
                transaction.internal_transaction_id.as_deref()
            }
        }
    }
}

impl Cmd {
    #[instrument("sync", skip_all, fields(provider = %self.provider))]
    pub(crate) async fn run(&self) -> Result<()> {
        let config: ScraperConfig = self.config.load().await?;
        let token = self.auth.load_token().await?;

        let Some(provider_config) = config.provider.get(&self.provider) else {
            return Err(eyre!("Unrecognised provider: {}", self.provider));
        };

        let client = BankDataClient::new(token, &config.retries);

        let state = provider_config.load_state().await?;

        let requisition = client
            .get::<Requisition>(&format!("/api/v2/requisitions/{}/", state.requisition_id))
            .await?;

        debug!(?requisition, "Got requisition",);

        if !requisition.is_linked() {
            return Err(eyre!("Requisition not linked"));
        }

        let end_date = Local::now().date_naive();
        let mut start_date = end_date - provider_config.history_days() + Days::new(1);
        if start_date.day() > 1 {
            start_date = start_date + Months::new(1);
            start_date = start_date - Days::new(start_date.day0().into());
        }
        debug!(%start_date, %end_date, "Scanning date range");

        for acc in requisition.accounts.iter().cloned() {
            self.list_account(provider_config, &client, acc, start_date, end_date)
                .await?;
        }
        Ok(())
    }

    #[instrument(skip_all,fields(%account_id))]
    async fn list_account(
        &self,
        provider_config: &ProviderConfig,
        client: &BankDataClient,
        account_id: Uuid,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<()> {
        let details = fetch_account(client, account_id).await?;

        let account_base = provider_config.output.join(&details.iban);

        let status = details.status.clone();

        write_json_lines(&account_base.join("account-details.json"), [details]).await?;

        if status != AccountStatus::Ready {
            bail!("Account status is not ready: {status:?}")
        }

        let balances = fetch_balances(client, account_id).await?;

        write_json_lines(&account_base.join("balances.jsonl"), balances.balances).await?;

        let transactions = fetch_transactions(client, account_id, start_date, end_date).await?;

        let mut by_month = HashMap::<_, Vec<_>>::new();

        for booked in transactions.transactions.booked {
            let date = booked.date_best_effort();
            let start_of_month = date.map(|d| d.with_day(1).expect("valid date"));

            by_month
                .entry(start_of_month)
                .or_default()
                .push(TransactionWithStatus::Booked(booked))
        }

        for pending in transactions.transactions.pending {
            let date = pending.date_best_effort();
            let start_of_month = date.map(|d| d.with_day(1).expect("valid date"));

            by_month
                .entry(start_of_month)
                .or_default()
                .push(TransactionWithStatus::Pending(pending))
        }

        for (month, mut transactions) in by_month {
            let fname = month
                .map(|month| month.format("%Y-%m.jsonl").to_string())
                .unwrap_or_else(|| "undated.json".to_owned());

            transactions.sort_by(|a, b| {
                let cmp = if let (Some(left), Some(right)) =
                    (a.timestamp_best_effort(), b.timestamp_best_effort())
                {
                    left.cmp(&right)
                } else {
                    Ordering::Equal
                };

                cmp.then_with(|| a.transaction_id().cmp(&b.transaction_id()))
                    .then_with(|| {
                        a.internal_transaction_id()
                            .cmp(&b.internal_transaction_id())
                    })
            });

            let path = account_base.join(fname);
            write_json_lines(&path, transactions).await?;
        }

        Ok(())
    }
}

#[instrument(skip_all)]
async fn fetch_account(
    client: &BankDataClient,
    account_id: Uuid,
) -> Result<Account, color_eyre::eyre::Error> {
    let details = client
        .get::<Account>(&format!("/api/v2/accounts/{}/", account_id))
        .await?;
    Ok(details)
}

#[instrument(skip_all)]
async fn fetch_balances(client: &BankDataClient, account_id: Uuid) -> Result<Balances> {
    let balances = client
        .get::<Balances>(&format!("/api/v2/accounts/{}/balances/", account_id,))
        .await?;
    Ok(balances)
}

#[instrument(skip_all)]
async fn fetch_transactions(
    client: &BankDataClient,
    account_id: Uuid,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Transactions> {
    debug!(%start_date, %end_date, "scanning dates");
    let transactions = client
        .get::<Transactions>(&format!(
            "/api/v2/accounts/{}/transactions/?{}",
            account_id,
            serde_urlencoded::to_string(TransactionsQuery {
                date_from: start_date,
                date_to: end_date,
            })?
        ))
        .await?;
    Ok(transactions)
}
