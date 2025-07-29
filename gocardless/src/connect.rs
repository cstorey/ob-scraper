use axum::{
    debug_handler,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use color_eyre::{
    eyre::{eyre, Context},
    Report, Result,
};
use http::{header::LOCATION, HeaderValue};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, field, info, instrument, warn, Span};
use uuid::Uuid;

use crate::{
    auth::AuthArgs,
    client::BankDataClient,
    config::{ConfigArg, ProviderState},
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

#[derive(Debug, Serialize)]
struct RequisitionReq {
    institution_id: String,
    redirect: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Requisition {
    pub(crate) id: Uuid,
    pub(crate) link: String,
    pub(crate) status: RequisitionStatus,
    pub(crate) accounts: Vec<Uuid>,
    #[serde(flatten)]
    pub(crate) other: serde_json::Value,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum RequisitionStatus {
    // Requisition has been successfully created
    #[serde(rename = "CR")]
    Created,
    // End-user is giving consent at GoCardless's consent screen
    #[serde(rename = "GC")]
    GivingConsent,
    // End-user is redirected to the financial institution for authentication
    #[serde(rename = "UA")]
    UndergoingAuthentication,
    // Either SSN verification has failed or end-user has entered incorrect credentials
    #[serde(rename = "RJ")]
    Rejected,
    // End-user is selecting accounts
    #[serde(rename = "SA")]
    SelectingAccounts,
    // End-user is granting access to their account information
    #[serde(rename = "GA")]
    GrantingAccess,
    // Account has been successfully linked to requisition
    #[serde(rename = "LN")]
    Linked,
    // Access to accounts has expired as set in End User Agreement
    #[serde(rename = "EX")]
    Expired,
}

impl Cmd {
    #[instrument("auth", skip_all, fields(provider = %self.provider, institution_id, requisition_id))]
    pub(crate) async fn run(&self) -> Result<()> {
        let config = self.config.load().await?;
        let token = self.auth.load_token().await?;

        let Some(provider_config) = config.provider.get(&self.provider) else {
            return Err(eyre!("Unrecognised provider: {}", self.provider));
        };

        Span::current().record("institution_id", &provider_config.institution_id);

        let client = BankDataClient::new(token, &config.retries);

        let cnx = CancellationToken::new();
        let listener = TcpListener::bind(config.http.bind_address)
            .await
            .with_context(|| format!("Bind to address: {}", config.http.bind_address))?;

        let base_url = config
            .http
            .client_facing_url_builder()
            .path_and_query("")
            .build()
            .context("Build base URI")?;

        let req = RequisitionReq {
            institution_id: provider_config.institution_id.clone(),
            redirect: base_url.to_string(),
        };

        debug!(?req);

        let requisition = client
            .post::<Requisition>("/api/v2/requisitions/", &req)
            .await?;

        Span::current().record("requisition_id", field::display(&requisition.id));

        debug!(?requisition, "Got requisition");

        let app = Router::new().merge(routes(cnx.clone(), client.clone(), requisition.id));

        let auth_url = config
            .http
            .client_facing_url_builder()
            .path_and_query(format!(
                "?{}",
                serde_urlencoded::to_string(RequisitionCallbackQuery { id: requisition.id })
                    .context("encode query")?,
            ))
            .build()
            .context("Build auth URI")?;

        println!("Go to link: {}", auth_url);
        info!("Awaiting response");

        axum::serve(listener, app)
            .with_graceful_shutdown(cnx.clone().cancelled_owned())
            .await
            .context("Running server")?;

        let requisition = client
            .get::<Requisition>(&format!("/api/v2/requisitions/{}/", requisition.id))
            .await?;

        debug!(?requisition, "Got requisition",);

        let state = ProviderState::from_requisition(&requisition);

        provider_config.write_state(&state).await?;

        Ok(())
    }
}

impl Requisition {
    pub(crate) fn is_linked(&self) -> bool {
        self.status == RequisitionStatus::Linked
    }
}

#[derive(Clone)]
struct AxumState {
    cnx: CancellationToken,
    expected_requisition_id: Uuid,
    client: BankDataClient,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequisitionCallbackQuery {
    #[serde(rename = "ref")]
    id: Uuid,
}

struct WebError(Report);

type WebResult<T> = std::result::Result<T, WebError>;

impl From<Report> for WebError {
    fn from(value: Report) -> Self {
        WebError(value)
    }
}

fn routes(cnx: CancellationToken, client: BankDataClient, expected_requisition_id: Uuid) -> Router {
    Router::new()
        .route("/", get(handle_redirect))
        .with_state(AxumState {
            cnx,
            client,
            expected_requisition_id,
        })
}

#[instrument(skip_all, fields(
    requisition_id=%q.id,
))]
#[debug_handler]
async fn handle_redirect(
    State(state): State<AxumState>,
    Query(q): Query<RequisitionCallbackQuery>,
) -> WebResult<Response> {
    if q.id != state.expected_requisition_id {
        warn!("Unexpected requisition token");
        return Ok((StatusCode::NOT_FOUND, "Unexpected requisition").into_response());
    }

    let requisition = state
        .client
        .get::<Requisition>(&format!("/api/v2/requisitions/{}/", q.id))
        .await?;

    debug!(?requisition, "Got requisition",);

    match requisition.status {
        RequisitionStatus::Created
        | RequisitionStatus::GivingConsent
        | RequisitionStatus::UndergoingAuthentication
        | RequisitionStatus::SelectingAccounts
        | RequisitionStatus::GrantingAccess => {
            debug!(link=?requisition.link, "Created; redirecting");
            let mut headers = HeaderMap::new();
            headers.insert(
                LOCATION,
                HeaderValue::from_str(&requisition.link).context("location header value")?,
            );
            let resp = (StatusCode::SEE_OTHER, headers).into_response();
            return Ok(resp);
        }
        RequisitionStatus::Linked => {
            info!("Received confirmation");
            state.cnx.cancel();
        }
        status => warn!(?status, "Other requisition status"),
    }

    Ok((StatusCode::OK, "Ok").into_response())
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        error!(error=?self.0, "Error handling request");
        (StatusCode::INTERNAL_SERVER_ERROR, "Error handling response").into_response()
    }
}
