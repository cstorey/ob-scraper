use std::sync::LazyLock;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tzfile::Tz;

#[derive(Debug, Serialize)]
pub(crate) struct TransactionsQuery {
    pub(crate) date_from: NaiveDate,
    pub(crate) date_to: NaiveDate,
}

#[derive(Debug, Serialize, Deserialize, Default)]

pub(crate) struct Transactions {
    pub(crate) transactions: TransactionsInner,
    #[serde(flatten)]
    pub(crate) other: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Default)]

pub(crate) struct TransactionsInner {
    pub(crate) booked: Vec<Transaction>,
    pub(crate) pending: Vec<Transaction>,
    #[serde(flatten)]
    pub(crate) other: serde_json::Value,
}

static EUROPE_LONDON: LazyLock<Tz> =
    LazyLock::new(|| Tz::named("Europe/London").expect("Europe/London timezone"));

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Transaction {
    #[serde(
        rename = "bookingDate",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) booking_date: Option<NaiveDate>,
    #[serde(
        rename = "bookingDateTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) booking_date_time: Option<DateTime<Utc>>,
    #[serde(rename = "valueDate", default, skip_serializing_if = "Option::is_none")]
    pub(crate) value_date: Option<NaiveDate>,
    #[serde(
        rename = "valueDateTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) value_date_time: Option<DateTime<Utc>>,
    #[serde(
        rename = "transactionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) transaction_id: Option<String>,
    #[serde(
        rename = "internalTransactionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) internal_transaction_id: Option<String>,
    #[serde(flatten)]
    pub(crate) other: serde_json::Value,
}

impl Transaction {
    pub(crate) fn date_best_effort(&self) -> Option<NaiveDate> {
        self.booking_date
            .or(self.booking_date_time.map(|dt| dt.date_naive()))
            .or(self.value_date)
    }

    pub(crate) fn timestamp_best_effort(&self) -> Option<DateTime<Utc>> {
        self.booking_date_time
            .or_else(|| {
                self.booking_date.map(|dt| {
                    dt.and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_local_timezone(&*EUROPE_LONDON)
                        .single()
                        .expect("to Europe/London")
                        .to_utc()
                })
            })
            .or(self.value_date_time)
            .or_else(|| {
                self.value_date.map(|dt| {
                    dt.and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_local_timezone(&*EUROPE_LONDON)
                        .single()
                        .expect("to Europe/London")
                        .to_utc()
                })
            })
    }
}
