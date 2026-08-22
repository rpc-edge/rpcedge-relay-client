use crate::{
    encode_transaction_base64, QuicRelayClient, QuicRelayClientConfig, RelayClient,
    RelayClientError, ResponseMode, RouteSet, SubmitRequest, SubmitResponse, TransactionVersion,
    VERSION,
};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use rpcedge_relay_protocol::{classify_transaction_wire, ProtocolError};
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
    transaction_version: TransactionVersion,
    route_set: RouteSet,
    request_id: String,
    response_mode: ResponseMode,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct RelayRequestSnapshot {
    transaction: Vec<u8>,
    transaction_version: TransactionVersion,
    route_set: RouteSet,
    request_id: String,
    response_mode: ResponseMode,
}

#[cfg(test)]
impl From<&AdaptiveRelayRequest<'_>> for RelayRequestSnapshot {
    fn from(request: &AdaptiveRelayRequest<'_>) -> Self {
        Self {
            transaction: request.transaction.to_vec(),
            transaction_version: request.transaction_version,
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
            .send_transaction_raw_bytes_with_request_id_and_response_mode_versioned(
                request.transaction,
                request.transaction_version,
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
        self.send_transaction_raw_bytes_versioned(
            transaction,
            TransactionVersion::Legacy,
            route_set,
            request_id,
            response_mode,
        )
        .await
    }

    pub async fn send_transaction_raw_bytes_versioned(
        &self,
        transaction: impl AsRef<[u8]>,
        transaction_version: TransactionVersion,
        route_set: RouteSet,
        request_id: impl Into<String>,
        response_mode: ResponseMode,
    ) -> Result<RelaySubmitReceipt, RelayAttemptError> {
        self.submit(&AdaptiveRelayRequest {
            transaction: transaction.as_ref(),
            transaction_version,
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
        validate_transaction_wire_request(request).map_err(|error| {
            RelayAttemptError::Protocol {
                transport: RelayTransport::Quic,
                message: error.to_string(),
            }
        })?;

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
        validate_protocol_v1_compatibility(request).map_err(|error| {
            RelayAttemptError::Protocol {
                transport: RelayTransport::Http,
                message: error.to_string(),
            }
        })?;
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

fn validate_transaction_wire_request(
    request: &AdaptiveRelayRequest<'_>,
) -> Result<(), ProtocolError> {
    let classified = classify_transaction_wire(request.transaction)?;
    if classified != request.transaction_version {
        return Err(ProtocolError::TransactionVersionMismatch {
            declared: request.transaction_version,
            classified,
        });
    }
    let max = classified.max_transaction_bytes();
    if request.transaction.len() > max {
        return Err(ProtocolError::TransactionTooLarge {
            actual: request.transaction.len(),
            max,
        });
    }
    Ok(())
}

fn validate_protocol_v1_compatibility(
    request: &AdaptiveRelayRequest<'_>,
) -> Result<(), rpcedge_relay_protocol::ProtocolError> {
    if !request.transaction_version.supports_protocol_v1() {
        return Err(
            rpcedge_relay_protocol::ProtocolError::UnsupportedTransactionVersion {
                protocol_version: VERSION,
                transaction_version: request.transaction_version,
            },
        );
    }
    Ok(())
}

async fn run_quic_supervisor(
    quic_config: QuicRelayClientConfig,
    supervisor_config: QuicSupervisorConfig,
    published: Arc<ArcSwapOption<QuicRelayClient>>,
    generation: Arc<AtomicU64>,
) {
    run_quic_supervisor_with(
        RealQuicConnector { quic_config },
        supervisor_config,
        published,
        generation,
    )
    .await;
}

#[async_trait]
trait SupervisedQuicConnector: Send + Sync + 'static {
    type Client: Send + Sync + 'static;

    async fn connect(&self) -> Result<Self::Client, ()>;
    async fn wait_closed(&self, client: &Self::Client);
}

struct RealQuicConnector {
    quic_config: QuicRelayClientConfig,
}

#[async_trait]
impl SupervisedQuicConnector for RealQuicConnector {
    type Client = QuicRelayClient;

    async fn connect(&self) -> Result<Self::Client, ()> {
        QuicRelayClient::connect(self.quic_config.clone())
            .await
            .map_err(|_| ())
    }

    async fn wait_closed(&self, client: &Self::Client) {
        let _ = client.connection.closed().await;
    }
}

async fn run_quic_supervisor_with<C: SupervisedQuicConnector>(
    connector: C,
    supervisor_config: QuicSupervisorConfig,
    published: Arc<ArcSwapOption<C::Client>>,
    generation: Arc<AtomicU64>,
) {
    let mut failures = 0_u32;
    loop {
        match connector.connect().await {
            Ok(client) => {
                failures = 0;
                published.store(Some(Arc::new(client)));
                generation.fetch_add(1, Ordering::Release);
                let connected = published
                    .load_full()
                    .expect("client was published immediately above");
                connector.wait_closed(connected.as_ref()).await;
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
        sync::{
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
            Arc, Mutex,
        },
    };
    use tokio::sync::watch;

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

    #[derive(Debug)]
    struct FakeSupervisedClient {
        id: u64,
        closed: watch::Receiver<bool>,
    }

    struct FakeConnector {
        attempts: Arc<AtomicUsize>,
        results: Mutex<VecDeque<Result<FakeSupervisedClient, ()>>>,
    }

    #[async_trait]
    impl SupervisedQuicConnector for FakeConnector {
        type Client = FakeSupervisedClient;

        async fn connect(&self) -> Result<Self::Client, ()> {
            self.attempts.fetch_add(1, AtomicOrdering::Relaxed);
            self.results.lock().unwrap().pop_front().unwrap_or(Err(()))
        }

        async fn wait_closed(&self, client: &Self::Client) {
            let mut closed = client.closed.clone();
            if !*closed.borrow() {
                let _ = closed.changed().await;
            }
        }
    }

    async fn wait_for_generation(generation: &AtomicU64, expected: u64) {
        tokio::time::timeout(Duration::from_millis(250), async {
            while generation.load(Ordering::Acquire) < expected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("supervisor generation did not advance");
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
        static LEGACY_TRANSACTION: [u8; 66] = {
            let mut transaction = [0; 66];
            transaction[0] = 1;
            transaction
        };
        AdaptiveRelayRequest {
            transaction: &LEGACY_TRANSACTION,
            transaction_version: TransactionVersion::Legacy,
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
    async fn supervisor_recovers_after_connect_failure_and_closed_generation() {
        let (close_first, first_closed) = watch::channel(false);
        let (_close_second, second_closed) = watch::channel(false);
        let attempts = Arc::new(AtomicUsize::new(0));
        let connector = FakeConnector {
            attempts: attempts.clone(),
            results: Mutex::new(VecDeque::from([
                Err(()),
                Ok(FakeSupervisedClient {
                    id: 1,
                    closed: first_closed,
                }),
                Ok(FakeSupervisedClient {
                    id: 2,
                    closed: second_closed,
                }),
            ])),
        };
        let published = Arc::new(ArcSwapOption::empty());
        let generation = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(run_quic_supervisor_with(
            connector,
            QuicSupervisorConfig {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(2),
            },
            published.clone(),
            generation.clone(),
        ));

        wait_for_generation(&generation, 1).await;
        assert_eq!(published.load_full().unwrap().id, 1);
        close_first.send(true).unwrap();
        wait_for_generation(&generation, 2).await;
        assert_eq!(published.load_full().unwrap().id, 2);
        assert_eq!(attempts.load(AtomicOrdering::Relaxed), 3);

        task.abort();
        let _ = task.await;
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

    #[tokio::test]
    async fn v1_transaction_never_downgrades_to_http() {
        let quic = Arc::new(FakeTransport::new(false, []));
        let http = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let client = AdaptiveRelayClient::from_transports(
            quic.clone(),
            http.clone(),
            AdaptiveRelayClientConfig::default(),
        );
        let mut v1 = request();
        v1.transaction = &[0x81];
        v1.transaction_version = TransactionVersion::V1;

        assert!(matches!(
            client.submit(&v1).await,
            Err(RelayAttemptError::Protocol { .. })
        ));
        assert!(quic.requests.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mislabeled_transaction_is_rejected_before_ready_quic() {
        let quic = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let http = Arc::new(FakeTransport::new(true, []));
        let client = AdaptiveRelayClient::from_transports(
            quic.clone(),
            http.clone(),
            AdaptiveRelayClientConfig::default(),
        );
        let mut mislabeled = request();
        mislabeled.transaction = &[0x81];
        mislabeled.transaction_version = TransactionVersion::Legacy;

        assert!(matches!(
            client.submit(&mislabeled).await,
            Err(RelayAttemptError::Protocol {
                transport: RelayTransport::Quic,
                ..
            })
        ));
        assert!(quic.requests.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mislabeled_small_v1_is_rejected_before_http_fallback() {
        let quic = Arc::new(FakeTransport::new(false, []));
        let http = Arc::new(FakeTransport::new(true, [Ok(response())]));
        let client = AdaptiveRelayClient::from_transports(
            quic.clone(),
            http.clone(),
            AdaptiveRelayClientConfig::default(),
        );
        let mut mislabeled = request();
        mislabeled.transaction = &[0x81];
        mislabeled.transaction_version = TransactionVersion::Legacy;

        assert!(matches!(
            client.submit(&mislabeled).await,
            Err(RelayAttemptError::Protocol {
                transport: RelayTransport::Quic,
                ..
            })
        ));
        assert!(quic.requests.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }
}
