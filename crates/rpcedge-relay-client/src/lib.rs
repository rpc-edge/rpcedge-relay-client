use quinn::{ClientConfig, Endpoint};
pub use rpcedge_relay_protocol::{
    decode_transaction_base64, encode_quic_frame, QuicPayloadKind, QuicSubmitHeader, RelayMethod,
    RelayRoute, RouteSet, RouteSetMode, SubmitRequest, TransactionEncoding, VERSION,
};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    DigitallySignedStruct, SignatureScheme,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(2);
const SUBMIT_PATH: &str = "/v1/submit";
const JSON_RPC_PATH: &str = "/v1/sendTransaction";
const RAW_TRANSACTION_PATH: &str = "/v1/transactions";
const DEFAULT_QUIC_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_QUIC_SERVER_NAME: &str = "relay.rpcedge.com";
const DEFAULT_QUIC_MAX_RESPONSE_BYTES: usize = 4096;

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

    pub async fn send_transaction_json_rpc_base64(
        &self,
        transaction_base64: impl Into<String>,
    ) -> Result<SubmitResponse, RelayClientError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [transaction_base64.into(), {"encoding": "base64"}],
        });
        let response = self
            .authed(self.http.post(self.json_rpc_url()))
            .json(&body)
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
        let response: JsonRpcSubmitResponse =
            response.json().await.map_err(RelayClientError::Http)?;
        Ok(SubmitResponse {
            accepted: true,
            request_id: response.request_id,
            signature: response.result,
        })
    }

    pub async fn send_transaction_raw_bytes(
        &self,
        transaction: impl AsRef<[u8]>,
    ) -> Result<SubmitResponse, RelayClientError> {
        let response = self
            .authed(self.http.post(self.raw_transaction_url()))
            .header("content-type", "application/octet-stream")
            .body(transaction.as_ref().to_vec())
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

    pub async fn send_transaction_raw_base64(
        &self,
        transaction_base64: &str,
    ) -> Result<SubmitResponse, RelayClientError> {
        let transaction =
            decode_transaction_base64(transaction_base64).map_err(RelayClientError::Protocol)?;
        self.send_transaction_raw_bytes(transaction).await
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
        self.url_for_path(SUBMIT_PATH)
    }

    fn json_rpc_url(&self) -> String {
        self.url_for_path(JSON_RPC_PATH)
    }

    fn raw_transaction_url(&self) -> String {
        self.url_for_path(RAW_TRANSACTION_PATH)
    }

    fn url_for_path(&self, path: &str) -> String {
        let endpoint = self.config.endpoint.trim().trim_end_matches('/');
        if endpoint.ends_with(path) {
            endpoint.to_string()
        } else {
            format!("{endpoint}{path}")
        }
    }

    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .bearer_auth(self.config.api_key.trim())
            .header("x-api-key", self.config.api_key.trim())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicRelayClientConfig {
    pub endpoint: SocketAddr,
    pub api_key: String,
    pub server_name: String,
    pub timeout: Duration,
    pub max_response_bytes: usize,
    pub insecure_skip_server_certificate_verification: bool,
}

impl QuicRelayClientConfig {
    pub fn new(endpoint: SocketAddr, api_key: impl Into<String>) -> Self {
        Self {
            endpoint,
            api_key: api_key.into(),
            server_name: DEFAULT_QUIC_SERVER_NAME.to_string(),
            timeout: DEFAULT_QUIC_TIMEOUT,
            max_response_bytes: DEFAULT_QUIC_MAX_RESPONSE_BYTES,
            insecure_skip_server_certificate_verification: true,
        }
    }

    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = server_name.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuicRelayClient {
    config: QuicRelayClientConfig,
    connection: quinn::Connection,
}

impl QuicRelayClient {
    pub async fn connect(config: QuicRelayClientConfig) -> Result<Self, RelayClientError> {
        if config.api_key.trim().is_empty() {
            return Err(RelayClientError::InvalidConfig("api_key is empty"));
        }
        if config.server_name.trim().is_empty() {
            return Err(RelayClientError::InvalidConfig("server_name is empty"));
        }

        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().expect("valid wildcard addr"))
            .map_err(RelayClientError::QuicEndpoint)?;
        endpoint.set_default_client_config(build_quic_client_config(
            config.insecure_skip_server_certificate_verification,
        )?);

        let connecting = endpoint
            .connect(config.endpoint, config.server_name.trim())
            .map_err(RelayClientError::QuicConnectStart)?;
        let connection = tokio::time::timeout(config.timeout, connecting)
            .await
            .map_err(|_| RelayClientError::Timeout("QUIC connect"))?
            .map_err(RelayClientError::QuicConnect)?;

        Ok(Self { config, connection })
    }

    pub async fn send_transaction_raw_bytes(
        &self,
        transaction: impl AsRef<[u8]>,
    ) -> Result<SubmitResponse, RelayClientError> {
        self.send_transaction_raw_bytes_with_route_set(transaction, RouteSet::server_default())
            .await
    }

    pub async fn send_transaction_raw_bytes_with_route_set(
        &self,
        transaction: impl AsRef<[u8]>,
        route_set: RouteSet,
    ) -> Result<SubmitResponse, RelayClientError> {
        self.send_transaction_raw_bytes_with_request_id(transaction, route_set, None)
            .await
    }

    pub async fn send_transaction_raw_bytes_with_request_id(
        &self,
        transaction: impl AsRef<[u8]>,
        route_set: RouteSet,
        request_id: Option<String>,
    ) -> Result<SubmitResponse, RelayClientError> {
        let (mut send, mut recv) =
            tokio::time::timeout(self.config.timeout, self.connection.open_bi())
                .await
                .map_err(|_| RelayClientError::Timeout("QUIC open stream"))?
                .map_err(RelayClientError::QuicOpenStream)?;
        let frame = encode_routed_quic_frame(
            self.config.api_key.trim(),
            transaction.as_ref(),
            route_set,
            request_id,
        )
        .map_err(RelayClientError::Protocol)?;
        tokio::time::timeout(self.config.timeout, send.write_all(&frame))
            .await
            .map_err(|_| RelayClientError::Timeout("QUIC write"))?
            .map_err(RelayClientError::QuicWrite)?;
        send.finish().map_err(RelayClientError::QuicFinish)?;

        let response = tokio::time::timeout(
            self.config.timeout,
            recv.read_to_end(self.config.max_response_bytes),
        )
        .await
        .map_err(|_| RelayClientError::Timeout("QUIC read"))?
        .map_err(RelayClientError::QuicRead)?;
        parse_quic_response(&response)
    }

    pub async fn send_transaction_raw_base64(
        &self,
        transaction_base64: &str,
    ) -> Result<SubmitResponse, RelayClientError> {
        self.send_transaction_raw_base64_with_route_set(
            transaction_base64,
            RouteSet::server_default(),
        )
        .await
    }

    pub async fn send_transaction_raw_base64_with_route_set(
        &self,
        transaction_base64: &str,
        route_set: RouteSet,
    ) -> Result<SubmitResponse, RelayClientError> {
        let transaction =
            decode_transaction_base64(transaction_base64).map_err(RelayClientError::Protocol)?;
        self.send_transaction_raw_bytes_with_route_set(transaction, route_set)
            .await
    }
}

fn build_quic_client_config(
    skip_cert_verification: bool,
) -> Result<ClientConfig, RelayClientError> {
    let mut crypto = if skip_cert_verification {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
            .with_no_client_auth()
    } else {
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth()
    };
    crypto.enable_early_data = false;
    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(RelayClientError::QuicClientConfig)?,
    )))
}

fn encode_routed_quic_frame(
    api_key: &str,
    transaction: &[u8],
    route_set: RouteSet,
    request_id: Option<String>,
) -> Result<Vec<u8>, rpcedge_relay_protocol::ProtocolError> {
    let header = QuicSubmitHeader {
        version: VERSION,
        method: RelayMethod::SendTransaction,
        payload_kind: QuicPayloadKind::SingleRawTransaction,
        request_id,
        route_set,
        signature: None,
    };
    let payload = encode_quic_frame(&header, transaction)?;
    let mut frame = Vec::with_capacity("api-key: \n".len() + api_key.len() + payload.len());
    frame.extend_from_slice(b"api-key: ");
    frame.extend_from_slice(api_key.as_bytes());
    frame.push(b'\n');
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn parse_quic_response(response: &[u8]) -> Result<SubmitResponse, RelayClientError> {
    if response.starts_with(b"error:") {
        let body = String::from_utf8_lossy(response).trim().to_string();
        return Err(RelayClientError::QuicStatus(body));
    }
    serde_json::from_slice(response).map_err(RelayClientError::Json)
}

#[derive(Debug)]
struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SubmitResponse {
    pub accepted: bool,
    pub request_id: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct JsonRpcSubmitResponse {
    pub result: String,
    pub request_id: String,
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
    #[error("failed to build QUIC endpoint: {0}")]
    QuicEndpoint(std::io::Error),
    #[error("failed to start QUIC connection: {0}")]
    QuicConnectStart(quinn::ConnectError),
    #[error("failed to connect QUIC: {0}")]
    QuicConnect(quinn::ConnectionError),
    #[error("failed to build QUIC client config: {0}")]
    QuicClientConfig(quinn::crypto::rustls::NoInitialCipherSuite),
    #[error("failed to open QUIC stream: {0}")]
    QuicOpenStream(quinn::ConnectionError),
    #[error("failed to write QUIC stream: {0}")]
    QuicWrite(quinn::WriteError),
    #[error("failed to finish QUIC stream: {0}")]
    QuicFinish(quinn::ClosedStream),
    #[error("failed to read QUIC response: {0}")]
    QuicRead(quinn::ReadToEndError),
    #[error("QUIC request timed out during {0}")]
    Timeout(&'static str),
    #[error("relay returned QUIC error: {0}")]
    QuicStatus(String),
    #[error("invalid relay response json: {0}")]
    Json(serde_json::Error),
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

    #[test]
    fn quic_frame_encoder_carries_route_set_after_api_key_prelude() {
        let frame = encode_routed_quic_frame(
            "plab_test_key",
            b"tx-bytes",
            RouteSet::only([RelayRoute::TpuQuic]),
            Some("req-1".to_string()),
        )
        .unwrap();
        let newline = frame
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("api key prelude");

        assert_eq!(&frame[..newline], b"api-key: plab_test_key");
        let (header, payload) =
            rpcedge_relay_protocol::decode_quic_frame(&frame[newline + 1..]).unwrap();
        assert_eq!(header.route_set, RouteSet::only([RelayRoute::TpuQuic]));
        assert_eq!(header.request_id.as_deref(), Some("req-1"));
        assert_eq!(payload, b"tx-bytes");
    }
}
