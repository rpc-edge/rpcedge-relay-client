use crate::{
    encode_transaction_base64, QuicRelayClient, QuicRelayClientConfig, RelayClient,
    RelayClientError, ResponseMode, RouteSet, SubmitRequest, SubmitResponse,
};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayTransport {
    Quic,
    Http,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayFallbackReason {
    QuicDisconnected,
    QuicOpenFailed,
    QuicResponseAmbiguous,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdaptiveRelayClientConfig {
    pub retry_ambiguous_with_idempotency: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuicSupervisorConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for QuicSupervisorConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuicReadiness {
    pub ready: bool,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelaySubmitReceipt {
    pub response: SubmitResponse,
    pub transport: RelayTransport,
    pub fallback_reason: Option<RelayFallbackReason>,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayAttemptError {
    #[error("relay {transport:?} server rejected the request: {message}")]
    Server {
        transport: RelayTransport,
        message: String,
    },
    #[error("relay {transport:?} transport failed: {message}")]
    Transport {
        transport: RelayTransport,
        message: String,
    },
    #[error("relay {transport:?} result is ambiguous: {message}")]
    Ambiguous {
        transport: RelayTransport,
        message: String,
    },
    #[error("relay {transport:?} protocol failed: {message}")]
    Protocol {
        transport: RelayTransport,
        message: String,
    },
}

#[derive(Debug, Clone)]
struct AdaptiveRelayRequest<'a> {
    transaction: &'a [u8],
    route_set: RouteSet,
    request_id: String,
    response_mode: ResponseMode,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct RelayRequestSnapshot {
    transaction: Vec<u8>,
    route_set: RouteSet,
    request_id: String,
    response_mode: ResponseMode,
}

#[cfg(test)]
impl From<&AdaptiveRelayRequest<'_>> for RelayRequestSnapshot {
    fn from(request: &AdaptiveRelayRequest<'_>) -> Self {
        Self {
            transaction: request.transaction.to_vec(),
            route_set: request.route_set.clone(),
            request_id: request.request_id.clone(),
            response_mode: request.response_mode,
        }
    }
}

#[derive(Debug)]
enum TransportAttemptError {
    PreWrite(String),
    Ambiguous(String),
    Server(String),
    Protocol(String),
    Transport(String),
}

#[async_trait]
trait SubmitTransport: Send + Sync {
    fn ready(&self) -> bool;

    async fn submit(
        &self,
        request: &AdaptiveRelayRequest<'_>,
    ) -> Result<SubmitResponse, TransportAttemptError>;
}

struct HttpSubmitTransport {
    client: RelayClient,
}

#[async_trait]
impl SubmitTransport for HttpSubmitTransport {
    fn ready(&self) -> bool {
        true
    }

    async fn submit(
        &self,
        request: &AdaptiveRelayRequest<'_>,
    ) -> Result<SubmitResponse, TransportAttemptError> {
        let mut envelope = SubmitRequest::send_transaction_base64(
            encode_transaction_base64(request.transaction),
            request.route_set.clone(),
        );
        envelope.request_id = Some(request.request_id.clone());
        envelope.response_mode = Some(request.response_mode);
        self.client
            .submit(&envelope)
            .await
            .map_err(classify_http_error)
    }
}

struct QuicSubmitTransport {
    client: Arc<ArcSwapOption<QuicRelayClient>>,
}

#[async_trait]
impl SubmitTransport for QuicSubmitTransport {
    fn ready(&self) -> bool {
        self.client
            .load_full()
            .is_some_and(|client| client.connection.close_reason().is_none())
    }

    async fn submit(
        &self,
        request: &AdaptiveRelayRequest<'_>,
    ) -> Result<SubmitResponse, TransportAttemptError> {
        let client = self
            .client
            .load_full()
            .ok_or_else(|| TransportAttemptError::PreWrite("QUIC is disconnected".to_string()))?;
        client
            .send_transaction_raw_bytes_with_request_id_and_response_mode(
                request.transaction,
                request.route_set.clone(),
                Some(request.request_id.clone()),
                Some(request.response_mode),
            )
            .await
            .map_err(classify_quic_error)
    }
}

pub struct AdaptiveRelayClient {
    quic: Arc<dyn SubmitTransport>,
    http: Arc<dyn SubmitTransport>,
    quic_client: Option<Arc<ArcSwapOption<QuicRelayClient>>>,
    quic_generation: Option<Arc<AtomicU64>>,
    config: AdaptiveRelayClientConfig,
}

pub struct AdaptiveRelaySupervisor {
    task: Option<JoinHandle<()>>,
}

impl AdaptiveRelaySupervisor {
    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for AdaptiveRelaySupervisor {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl AdaptiveRelayClient {
    pub fn new(
        quic: QuicRelayClient,
        http: RelayClient,
        config: AdaptiveRelayClientConfig,
    ) -> Self {
        let quic_client = Arc::new(ArcSwapOption::from(Some(Arc::new(quic))));
        let quic_generation = Arc::new(AtomicU64::new(1));
        Self {
            quic: Arc::new(QuicSubmitTransport {
                client: quic_client.clone(),
            }),
            http: Arc::new(HttpSubmitTransport { client: http }),
            quic_client: Some(quic_client),
            quic_generation: Some(quic_generation),
            config,
        }
    }

    pub fn spawn_supervised(
        quic_config: QuicRelayClientConfig,
        http: RelayClient,
        config: AdaptiveRelayClientConfig,
        supervisor_config: QuicSupervisorConfig,
    ) -> Result<(Self, AdaptiveRelaySupervisor), RelayClientError> {
        validate_supervisor_config(supervisor_config)?;
        tokio::runtime::Handle::try_current().map_err(|_| {
            RelayClientError::InvalidConfig("QUIC supervisor requires a Tokio runtime")
        })?;
        let quic_client = Arc::new(ArcSwapOption::empty());
        let quic_generation = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(run_quic_supervisor(
            quic_config,
            supervisor_config,
            quic_client.clone(),
            quic_generation.clone(),
        ));
        Ok((
            Self {
                quic: Arc::new(QuicSubmitTransport {
                    client: quic_client.clone(),
                }),
                http: Arc::new(HttpSubmitTransport { client: http }),
                quic_client: Some(quic_client),
                quic_generation: Some(quic_generation),
                config,
            },
            AdaptiveRelaySupervisor { task: Some(task) },
        ))
    }

    #[must_use]
    pub fn quic_readiness(&self) -> QuicReadiness {
        QuicReadiness {
            ready: self.quic_client.as_ref().is_some_and(|client| {
                client
                    .load_full()
                    .is_some_and(|client| client.connection.close_reason().is_none())
            }),
            generation: self
                .quic_generation
                .as_ref()
                .map_or(0, |generation| generation.load(Ordering::Acquire)),
        }
    }

    #[cfg(test)]
    fn from_transports<Q, H>(quic: Arc<Q>, http: Arc<H>, config: AdaptiveRelayClientConfig) -> Self
    where
        Q: SubmitTransport + 'static,
        H: SubmitTransport + 'static,
    {
        Self {
            quic,
            http,
            quic_client: None,
            quic_generation: None,
            config,
        }
    }

    pub async fn send_transaction_raw_bytes(
        &self,
        transaction: impl AsRef<[u8]>,
        route_set: RouteSet,
        request_id: impl Into<String>,
        response_mode: ResponseMode,
    ) -> Result<RelaySubmitReceipt, RelayAttemptError> {
        self.submit(&AdaptiveRelayRequest {
            transaction: transaction.as_ref(),
            route_set,
            request_id: request_id.into(),
            response_mode,
        })
        .await
    }

    async fn submit(
        &self,
        request: &AdaptiveRelayRequest<'_>,
    ) -> Result<RelaySubmitReceipt, RelayAttemptError> {
        if !self.quic.ready() {
            return self
                .submit_http(request, RelayFallbackReason::QuicDisconnected)
                .await;
        }

        match self.quic.submit(request).await {
            Ok(response) => Ok(RelaySubmitReceipt {
                response,
                transport: RelayTransport::Quic,
                fallback_reason: None,
            }),
            Err(TransportAttemptError::PreWrite(message)) => {
                let _ = message;
                self.submit_http(request, RelayFallbackReason::QuicOpenFailed)
                    .await
            }
            Err(TransportAttemptError::Ambiguous(_message))
                if self.config.retry_ambiguous_with_idempotency =>
            {
                self.submit_http(request, RelayFallbackReason::QuicResponseAmbiguous)
                    .await
            }
            Err(error) => Err(map_attempt_error(RelayTransport::Quic, error)),
        }
    }

    async fn submit_http(
        &self,
        request: &AdaptiveRelayRequest<'_>,
        fallback_reason: RelayFallbackReason,
    ) -> Result<RelaySubmitReceipt, RelayAttemptError> {
        self.http
            .submit(request)
            .await
            .map(|response| RelaySubmitReceipt {
                response,
                transport: RelayTransport::Http,
                fallback_reason: Some(fallback_reason),
            })
            .map_err(|error| map_attempt_error(RelayTransport::Http, error))
    }
}

async fn run_quic_supervisor(
    quic_config: QuicRelayClientConfig,
    supervisor_config: QuicSupervisorConfig,
    published: Arc<ArcSwapOption<QuicRelayClient>>,
    generation: Arc<AtomicU64>,
) {
    let mut failures = 0_u32;
    loop {
        match QuicRelayClient::connect(quic_config.clone()).await {
            Ok(client) => {
                failures = 0;
                let connection = client.connection.clone();
                published.store(Some(Arc::new(client)));
                generation.fetch_add(1, Ordering::Release);
                let _ = connection.closed().await;
                published.store(None);
                tokio::time::sleep(reconnect_backoff(supervisor_config, 0)).await;
            }
            Err(_) => {
                published.store(None);
                let delay = reconnect_backoff(supervisor_config, failures);
                failures = failures.saturating_add(1);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn validate_supervisor_config(config: QuicSupervisorConfig) -> Result<(), RelayClientError> {
    if config.initial_backoff.is_zero() {
        return Err(RelayClientError::InvalidConfig(
            "QUIC supervisor initial_backoff is zero",
        ));
    }
    if config.max_backoff < config.initial_backoff {
        return Err(RelayClientError::InvalidConfig(
            "QUIC supervisor max_backoff is below initial_backoff",
        ));
    }
    Ok(())
}

fn reconnect_backoff(config: QuicSupervisorConfig, failures: u32) -> Duration {
    let multiplier = 1_u32.checked_shl(failures.min(16)).unwrap_or(u32::MAX);
    let base = config
        .initial_backoff
        .saturating_mul(multiplier)
        .min(config.max_backoff);
    // Deterministic 87.5%-112.5% jitter avoids synchronized reconnect storms
    // without adding RNG or shared state to the submission path.
    let jitter_bucket = u64::from(failures.wrapping_mul(1_103_515_245) % 3);
    let numerator = 7_u32.saturating_add(jitter_bucket as u32);
    (base.saturating_mul(numerator) / 8).min(config.max_backoff)
}

fn classify_http_error(error: RelayClientError) -> TransportAttemptError {
    match error {
        RelayClientError::Status { .. } | RelayClientError::QuicStatus(_) => {
            TransportAttemptError::Server(error.to_string())
        }
        RelayClientError::Protocol(_) | RelayClientError::Json(_) => {
            TransportAttemptError::Protocol(error.to_string())
        }
        _ => TransportAttemptError::Transport(error.to_string()),
    }
}

fn classify_quic_error(error: RelayClientError) -> TransportAttemptError {
    match error {
        RelayClientError::QuicStatus(_) | RelayClientError::Status { .. } => {
            TransportAttemptError::Server(error.to_string())
        }
        RelayClientError::QuicOpenStream(_) => TransportAttemptError::PreWrite(error.to_string()),
        RelayClientError::Timeout("QUIC open stream") => {
            TransportAttemptError::PreWrite(error.to_string())
        }
        RelayClientError::Protocol(_) => TransportAttemptError::Protocol(error.to_string()),
        RelayClientError::QuicWrite(_)
        | RelayClientError::QuicFinish(_)
        | RelayClientError::QuicRead(_)
        | RelayClientError::Json(_)
        | RelayClientError::Timeout(_) => TransportAttemptError::Ambiguous(error.to_string()),
        _ => TransportAttemptError::Transport(error.to_string()),
    }
}

fn map_attempt_error(transport: RelayTransport, error: TransportAttemptError) -> RelayAttemptError {
    match error {
        TransportAttemptError::PreWrite(message) | TransportAttemptError::Transport(message) => {
            RelayAttemptError::Transport { transport, message }
        }
        TransportAttemptError::Ambiguous(message) => {
            RelayAttemptError::Ambiguous { transport, message }
        }
        TransportAttemptError::Server(message) => RelayAttemptError::Server { transport, message },
        TransportAttemptError::Protocol(message) => {
            RelayAttemptError::Protocol { transport, message }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelayRoute;
    use async_trait::async_trait;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    #[derive(Debug)]
    struct FakeTransport {
        ready: bool,
        results: Mutex<VecDeque<Result<SubmitResponse, TransportAttemptError>>>,
        requests: Mutex<Vec<RelayRequestSnapshot>>,
    }

    impl FakeTransport {
        fn new(
            ready: bool,
            results: impl IntoIterator<Item = Result<SubmitResponse, TransportAttemptError>>,
        ) -> Self {
            Self {
                ready,
                results: Mutex::new(results.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl SubmitTransport for FakeTransport {
        fn ready(&self) -> bool {
            self.ready
        }

        async fn submit(
            &self,
            request: &AdaptiveRelayRequest<'_>,
        ) -> Result<SubmitResponse, TransportAttemptError> {
            self.requests.lock().unwrap().push(request.into());
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("fake result")
        }
    }

    fn response() -> SubmitResponse {
        SubmitResponse {
            accepted: true,
            request_id: "request-1".to_string(),
            signature: "signature-1".to_string(),
            route_results: Vec::new(),
            route_results_complete: None,
        }
    }

    fn request() -> AdaptiveRelayRequest<'static> {
        AdaptiveRelayRequest {
            transaction: &[1, 2, 3],
            route_set: RouteSet::only([RelayRoute::TpuQuic]),
            request_id: "request-1".to_string(),
            response_mode: ResponseMode::Fast,
        }
    }

    #[test]
    fn quic_supervisor_config_is_bounded_and_backoff_is_capped() {
        assert!(validate_supervisor_config(QuicSupervisorConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::from_secs(1),
        })
        .is_err());
        assert!(validate_supervisor_config(QuicSupervisorConfig {
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(1),
        })
        .is_err());

        let config = QuicSupervisorConfig {
            initial_backoff: Duration::from_millis(8),
            max_backoff: Duration::from_millis(64),
        };
        assert!(reconnect_backoff(config, 0) >= Duration::from_millis(7));
        assert!(reconnect_backoff(config, 16) <= config.max_backoff);
    }

    #[tokio::test]
    async fn healthy_quic_is_attempted_once_without_http() {
        let quic = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let http = Arc::new(FakeTransport::new(true, []));
        let client = AdaptiveRelayClient::from_transports(
            quic.clone(),
            http.clone(),
            AdaptiveRelayClientConfig::default(),
        );

        let receipt = client.submit(&request()).await.unwrap();

        assert_eq!(receipt.transport, RelayTransport::Quic);
        assert_eq!(receipt.fallback_reason, None);
        assert_eq!(quic.requests.lock().unwrap().len(), 1);
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn disconnected_quic_uses_warm_http_without_waiting() {
        let quic = Arc::new(FakeTransport::new(false, []));
        let http = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let client = AdaptiveRelayClient::from_transports(
            quic.clone(),
            http.clone(),
            AdaptiveRelayClientConfig::default(),
        );

        let receipt = client.submit(&request()).await.unwrap();

        assert_eq!(receipt.transport, RelayTransport::Http);
        assert_eq!(
            receipt.fallback_reason,
            Some(RelayFallbackReason::QuicDisconnected)
        );
        assert!(quic.requests.lock().unwrap().is_empty());
        assert_eq!(http.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn explicit_quic_server_rejection_never_falls_back() {
        let quic = Arc::new(FakeTransport::new(
            true,
            [Err(TransportAttemptError::Server("rejected".to_string()))],
        ));
        let http = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let client = AdaptiveRelayClient::from_transports(
            quic,
            http.clone(),
            AdaptiveRelayClientConfig::default(),
        );

        assert!(matches!(
            client.submit(&request()).await,
            Err(RelayAttemptError::Server {
                transport: RelayTransport::Quic,
                ..
            })
        ));
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ambiguous_quic_failure_requires_idempotency_before_http_retry() {
        let quic = Arc::new(FakeTransport::new(
            true,
            [Err(TransportAttemptError::Ambiguous(
                "read lost".to_string(),
            ))],
        ));
        let http = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let client = AdaptiveRelayClient::from_transports(
            quic,
            http.clone(),
            AdaptiveRelayClientConfig {
                retry_ambiguous_with_idempotency: false,
            },
        );

        assert!(matches!(
            client.submit(&request()).await,
            Err(RelayAttemptError::Ambiguous {
                transport: RelayTransport::Quic,
                ..
            })
        ));
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn idempotent_ambiguous_retry_preserves_the_complete_request() {
        let quic = Arc::new(FakeTransport::new(
            true,
            [Err(TransportAttemptError::Ambiguous(
                "read lost".to_string(),
            ))],
        ));
        let http = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let client = AdaptiveRelayClient::from_transports(
            quic.clone(),
            http.clone(),
            AdaptiveRelayClientConfig {
                retry_ambiguous_with_idempotency: true,
            },
        );

        let receipt = client.submit(&request()).await.unwrap();

        assert_eq!(receipt.transport, RelayTransport::Http);
        assert_eq!(
            receipt.fallback_reason,
            Some(RelayFallbackReason::QuicResponseAmbiguous)
        );
        assert_eq!(
            quic.requests.lock().unwrap().as_slice(),
            http.requests.lock().unwrap().as_slice()
        );
    }
}
