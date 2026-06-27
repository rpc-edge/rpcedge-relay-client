pub use rpcedge_relay_protocol::{
    RelayMethod, RelayRoute, RouteSet, RouteSetMode, SubmitRequest, TransactionEncoding, VERSION,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(2);
const SUBMIT_PATH: &str = "/v1/submit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayClientConfig {
    pub endpoint: String,
    pub api_key: String,
    pub timeout: Duration,
}

impl RelayClientConfig {
    pub fn new(endpoint: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            api_key: api_key.into(),
            timeout: DEFAULT_HTTP_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RelayClient {
    config: RelayClientConfig,
    http: reqwest::Client,
}

impl RelayClient {
    pub fn new(config: RelayClientConfig) -> Result<Self, RelayClientError> {
        if config.endpoint.trim().is_empty() {
            return Err(RelayClientError::InvalidConfig("endpoint is empty"));
        }
        if config.api_key.trim().is_empty() {
            return Err(RelayClientError::InvalidConfig("api_key is empty"));
        }
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(RelayClientError::HttpClient)?;
        Ok(Self { config, http })
    }

    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }

    pub fn api_key(&self) -> &str {
        &self.config.api_key
    }

    pub async fn submit(
        &self,
        request: &SubmitRequest,
    ) -> Result<SubmitResponse, RelayClientError> {
        request
            .validate_shape()
            .map_err(RelayClientError::Protocol)?;
        let response = self
            .http
            .post(self.submit_url())
            .bearer_auth(self.config.api_key.trim())
            .header("x-api-key", self.config.api_key.trim())
            .json(request)
            .send()
            .await
            .map_err(RelayClientError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(RelayClientError::Status {
                status: status.as_u16(),
                body,
            });
        }
        response.json().await.map_err(RelayClientError::Http)
    }

    pub async fn send_transaction_base64(
        &self,
        transaction_base64: impl Into<String>,
        route_set: RouteSet,
    ) -> Result<SubmitResponse, RelayClientError> {
        let request = self.build_send_transaction_request(transaction_base64, route_set);
        self.submit(&request).await
    }

    pub async fn send_transaction_fast_base64(
        &self,
        transaction_base64: impl Into<String>,
        route_set: RouteSet,
    ) -> Result<SubmitResponse, RelayClientError> {
        let mut request = self.build_send_transaction_request(transaction_base64, route_set);
        request.method = RelayMethod::SendTransactionFast;
        self.submit(&request).await
    }

    pub async fn send_bundle_base64(
        &self,
        transactions_base64: Vec<String>,
        route_set: RouteSet,
    ) -> Result<SubmitResponse, RelayClientError> {
        let request = SubmitRequest::send_bundle_base64(transactions_base64, route_set);
        self.submit(&request).await
    }

    pub fn build_send_transaction_request(
        &self,
        transaction_base64: impl Into<String>,
        route_set: RouteSet,
    ) -> SubmitRequest {
        SubmitRequest::send_transaction_base64(transaction_base64, route_set)
    }

    fn submit_url(&self) -> String {
        let endpoint = self.config.endpoint.trim().trim_end_matches('/');
        if endpoint.ends_with(SUBMIT_PATH) {
            endpoint.to_string()
        } else {
            format!("{endpoint}{SUBMIT_PATH}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SubmitResponse {
    pub accepted: bool,
    pub request_id: String,
    pub signature: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayClientError {
    #[error("invalid relay client config: {0}")]
    InvalidConfig(&'static str),
    #[error("failed to build HTTP client: {0}")]
    HttpClient(reqwest::Error),
    #[error("HTTP request failed: {0}")]
    Http(reqwest::Error),
    #[error("relay returned HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("invalid relay protocol request: {0}")]
    Protocol(rpcedge_relay_protocol::ProtocolError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{Method::POST, MockServer};
    use serde_json::json;

    #[test]
    fn rejects_empty_config() {
        assert!(RelayClient::new(RelayClientConfig::new("", "key")).is_err());
    }

    #[test]
    fn builds_send_transaction_request() {
        let client = RelayClient::new(RelayClientConfig::new(
            "https://relay.rpcedge.com",
            "00000000-0000-4000-8000-000000000000",
        ))
        .unwrap();

        let request = client.build_send_transaction_request(
            "AA==",
            RouteSet::default_plus([RelayRoute::HeliusSenderSwqos]),
        );

        assert_eq!(request.version, VERSION);
        assert_eq!(request.method, RelayMethod::SendTransaction);
    }

    #[tokio::test]
    async fn submit_posts_route_aware_envelope_with_bearer_auth() {
        let server = MockServer::start();
        let relay_key = "00000000-0000-4000-8000-000000000000";
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/submit")
                .header("authorization", format!("Bearer {relay_key}"))
                .header("x-api-key", relay_key)
                .body_contains(r#""method":"sendTransaction""#)
                .body_contains(r#""transaction":"AA==""#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "accepted": true,
                    "request_id": "relay-req-1",
                    "signature": "sig-1"
                }));
        });
        let client =
            RelayClient::new(RelayClientConfig::new(server.base_url(), relay_key)).unwrap();

        let response = client
            .send_transaction_base64(
                "AA==",
                RouteSet::default_plus([RelayRoute::HeliusSenderSwqos]),
            )
            .await
            .unwrap();

        mock.assert();
        assert_eq!(
            response,
            SubmitResponse {
                accepted: true,
                request_id: "relay-req-1".to_string(),
                signature: "sig-1".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn submit_returns_status_error_body() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/submit");
            then.status(403).body("route denied");
        });
        let client = RelayClient::new(RelayClientConfig::new(
            server.base_url(),
            "00000000-0000-4000-8000-000000000000",
        ))
        .unwrap();

        let error = client
            .send_bundle_base64(
                vec!["AA==".to_string()],
                RouteSet::only([RelayRoute::JitoBundle]),
            )
            .await
            .expect_err("relay status should fail");

        mock.assert();
        assert!(matches!(
            error,
            RelayClientError::Status { status: 403, .. }
        ));
    }
}
