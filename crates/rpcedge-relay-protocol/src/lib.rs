use base64::Engine;
use serde::{Deserialize, Serialize};

pub const VERSION: u8 = 1;
pub const DEFAULT_ALPN: &str = "rpcedge-submit-v1";
pub const VERSION_V2: u8 = 2;
pub const ALPN_V2: &str = "rpcedge-submit-v2";
pub const DEFAULT_MAX_FRAME_HEADER_BYTES: usize = 4096;
pub const DEFAULT_MAX_TRANSACTION_BYTES: usize = 1232;
pub const MAX_V1_TRANSACTION_BYTES: usize = 4096;
pub const DEFAULT_MAX_BUNDLE_TRANSACTIONS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelayMethod {
    #[serde(rename = "sendTransaction")]
    SendTransaction,
    #[serde(rename = "sendTransactionFast")]
    SendTransactionFast,
    #[serde(rename = "sendBundle")]
    SendBundle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayRoute {
    TpuQuic,
    TpuUdp,
    HarmonicBundle,
    HeliusSenderSwqos,
    RpcFallback,
    JitoTransaction,
    JitoBundle,
}

impl RelayRoute {
    pub fn is_jito(self) -> bool {
        matches!(self, Self::JitoTransaction | Self::JitoBundle)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteSetMode {
    ServerDefault,
    Only,
    DefaultPlus,
    DefaultMinus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseMode {
    Fast,
    RouteResults,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteSet {
    pub mode: RouteSetMode,
    #[serde(default)]
    pub routes: Vec<RelayRoute>,
}

impl Default for RouteSet {
    fn default() -> Self {
        Self {
            mode: RouteSetMode::ServerDefault,
            routes: Vec::new(),
        }
    }
}

impl RouteSet {
    pub fn server_default() -> Self {
        Self::default()
    }

    pub fn only<I>(routes: I) -> Self
    where
        I: IntoIterator<Item = RelayRoute>,
    {
        Self {
            mode: RouteSetMode::Only,
            routes: routes.into_iter().collect(),
        }
    }

    pub fn default_plus<I>(routes: I) -> Self
    where
        I: IntoIterator<Item = RelayRoute>,
    {
        Self {
            mode: RouteSetMode::DefaultPlus,
            routes: routes.into_iter().collect(),
        }
    }

    pub fn default_minus<I>(routes: I) -> Self
    where
        I: IntoIterator<Item = RelayRoute>,
    {
        Self {
            mode: RouteSetMode::DefaultMinus,
            routes: routes.into_iter().collect(),
        }
    }

    pub fn resolve(&self, defaults: &[RelayRoute]) -> Vec<RelayRoute> {
        let mut out = match self.mode {
            RouteSetMode::ServerDefault
            | RouteSetMode::DefaultPlus
            | RouteSetMode::DefaultMinus => defaults.to_vec(),
            RouteSetMode::Only => Vec::new(),
        };

        match self.mode {
            RouteSetMode::ServerDefault => {}
            RouteSetMode::Only => {
                push_unique_all(&mut out, &self.routes);
            }
            RouteSetMode::DefaultPlus => {
                push_unique_all(&mut out, &self.routes);
            }
            RouteSetMode::DefaultMinus => {
                out.retain(|route| !self.routes.contains(route));
            }
        }

        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionEncoding {
    Base64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitRequest {
    pub version: u8,
    pub method: RelayMethod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub route_set: RouteSet,
    #[serde(
        default,
        rename = "responseMode",
        alias = "response_mode",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_mode: Option<ResponseMode>,
    #[serde(default)]
    pub transaction_encoding: Option<TransactionEncoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<String>>,
}

impl SubmitRequest {
    pub fn send_transaction_base64(transaction: impl Into<String>, route_set: RouteSet) -> Self {
        Self {
            version: VERSION,
            method: RelayMethod::SendTransaction,
            request_id: None,
            route_set,
            response_mode: None,
            transaction_encoding: Some(TransactionEncoding::Base64),
            transaction: Some(transaction.into()),
            transactions: None,
        }
    }

    pub fn send_bundle_base64(transactions: Vec<String>, route_set: RouteSet) -> Self {
        Self {
            version: VERSION,
            method: RelayMethod::SendBundle,
            request_id: None,
            route_set,
            response_mode: None,
            transaction_encoding: Some(TransactionEncoding::Base64),
            transaction: None,
            transactions: Some(transactions),
        }
    }

    pub fn validate_shape(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        match self.method {
            RelayMethod::SendTransaction | RelayMethod::SendTransactionFast => {
                if self.transaction.is_none() || self.transactions.is_some() {
                    return Err(ProtocolError::InvalidPayload(
                        "single transaction methods require transaction only",
                    ));
                }
            }
            RelayMethod::SendBundle => {
                let count = self.transactions.as_ref().map_or(0, Vec::len);
                if self.transaction.is_some()
                    || count == 0
                    || count > DEFAULT_MAX_BUNDLE_TRANSACTIONS
                {
                    return Err(ProtocolError::InvalidPayload(
                        "sendBundle requires 1..=5 transactions",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuicSubmitHeader {
    pub version: u8,
    pub method: RelayMethod,
    pub payload_kind: QuicPayloadKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub route_set: RouteSet,
    #[serde(
        default,
        rename = "responseMode",
        alias = "response_mode",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_mode: Option<ResponseMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Version-two QUIC submission metadata.
///
/// V2 is intentionally a separate type so the established V1 wire codec stays
/// byte-for-byte stable and remains available for rollback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuicSubmitHeaderV2 {
    pub version: u8,
    pub method: RelayMethod,
    pub payload_kind: QuicPayloadKind,
    pub transaction_version: TransactionVersion,
    pub raw_transaction_length: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub route_set: RouteSet,
    #[serde(
        default,
        rename = "responseMode",
        alias = "response_mode",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_mode: Option<ResponseMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionVersion {
    Legacy,
    V0,
    V1,
}

impl TransactionVersion {
    pub const fn max_transaction_bytes(self) -> usize {
        match self {
            Self::Legacy | Self::V0 => DEFAULT_MAX_TRANSACTION_BYTES,
            Self::V1 => MAX_V1_TRANSACTION_BYTES,
        }
    }

    pub const fn supports_protocol_v1(self) -> bool {
        matches!(self, Self::Legacy | Self::V0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuicPayloadKind {
    SingleRawTransaction,
    BundleJson,
}

pub fn encode_quic_frame(
    header: &QuicSubmitHeader,
    payload: &[u8],
) -> Result<Vec<u8>, ProtocolError> {
    validate_version(header.version)?;
    let header_bytes = serde_json::to_vec(header).map_err(ProtocolError::Json)?;
    if header_bytes.len() > DEFAULT_MAX_FRAME_HEADER_BYTES {
        return Err(ProtocolError::HeaderTooLarge {
            actual: header_bytes.len(),
            max: DEFAULT_MAX_FRAME_HEADER_BYTES,
        });
    }
    if payload.len() > DEFAULT_MAX_TRANSACTION_BYTES
        && header.payload_kind == QuicPayloadKind::SingleRawTransaction
    {
        return Err(ProtocolError::TransactionTooLarge {
            actual: payload.len(),
            max: DEFAULT_MAX_TRANSACTION_BYTES,
        });
    }

    let mut out = Vec::with_capacity(4 + header_bytes.len() + payload.len());
    out.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_quic_frame(frame: &[u8]) -> Result<(QuicSubmitHeader, Vec<u8>), ProtocolError> {
    if frame.len() < 4 {
        return Err(ProtocolError::FrameTooShort);
    }
    let header_len = u32::from_be_bytes(frame[0..4].try_into().expect("fixed length")) as usize;
    if header_len > DEFAULT_MAX_FRAME_HEADER_BYTES {
        return Err(ProtocolError::HeaderTooLarge {
            actual: header_len,
            max: DEFAULT_MAX_FRAME_HEADER_BYTES,
        });
    }
    if frame.len() < 4 + header_len {
        return Err(ProtocolError::FrameTooShort);
    }

    let header: QuicSubmitHeader =
        serde_json::from_slice(&frame[4..4 + header_len]).map_err(ProtocolError::Json)?;
    validate_version(header.version)?;
    let payload = frame[4 + header_len..].to_vec();
    if payload.len() > DEFAULT_MAX_TRANSACTION_BYTES
        && header.payload_kind == QuicPayloadKind::SingleRawTransaction
    {
        return Err(ProtocolError::TransactionTooLarge {
            actual: payload.len(),
            max: DEFAULT_MAX_TRANSACTION_BYTES,
        });
    }
    Ok((header, payload))
}

pub fn encode_quic_frame_v2(
    header: &QuicSubmitHeaderV2,
    payload: &[u8],
) -> Result<Vec<u8>, ProtocolError> {
    validate_version_v2(header.version)?;
    validate_v2_transaction(header, payload)?;
    let header_bytes = serde_json::to_vec(header).map_err(ProtocolError::Json)?;
    if header_bytes.len() > DEFAULT_MAX_FRAME_HEADER_BYTES {
        return Err(ProtocolError::HeaderTooLarge {
            actual: header_bytes.len(),
            max: DEFAULT_MAX_FRAME_HEADER_BYTES,
        });
    }

    let mut out = Vec::with_capacity(4 + header_bytes.len() + payload.len());
    out.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_quic_frame_v2(frame: &[u8]) -> Result<(QuicSubmitHeaderV2, Vec<u8>), ProtocolError> {
    if frame.len() < 4 {
        return Err(ProtocolError::FrameTooShort);
    }
    let header_len = u32::from_be_bytes(frame[0..4].try_into().expect("fixed length")) as usize;
    if header_len > DEFAULT_MAX_FRAME_HEADER_BYTES {
        return Err(ProtocolError::HeaderTooLarge {
            actual: header_len,
            max: DEFAULT_MAX_FRAME_HEADER_BYTES,
        });
    }
    if frame.len() < 4 + header_len {
        return Err(ProtocolError::FrameTooShort);
    }

    let header: QuicSubmitHeaderV2 =
        serde_json::from_slice(&frame[4..4 + header_len]).map_err(ProtocolError::Json)?;
    validate_version_v2(header.version)?;
    let payload = frame[4 + header_len..].to_vec();
    validate_v2_transaction(&header, &payload)?;
    Ok((header, payload))
}

fn validate_v2_transaction(
    header: &QuicSubmitHeaderV2,
    payload: &[u8],
) -> Result<(), ProtocolError> {
    if header.payload_kind != QuicPayloadKind::SingleRawTransaction {
        return Err(ProtocolError::InvalidPayload(
            "protocol v2 supports single raw transactions only",
        ));
    }
    let actual = payload.len();
    let declared = header.raw_transaction_length as usize;
    if declared != actual {
        return Err(ProtocolError::TransactionLengthMismatch { declared, actual });
    }
    let max = header.transaction_version.max_transaction_bytes();
    if actual > max {
        return Err(ProtocolError::TransactionTooLarge { actual, max });
    }
    Ok(())
}

pub fn encode_transaction_base64(transaction: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(transaction)
}

pub fn decode_transaction_base64(transaction: &str) -> Result<Vec<u8>, ProtocolError> {
    base64::engine::general_purpose::STANDARD
        .decode(transaction)
        .map_err(ProtocolError::Base64)
}

fn push_unique_all(out: &mut Vec<RelayRoute>, routes: &[RelayRoute]) {
    for route in routes {
        if !out.contains(route) {
            out.push(*route);
        }
    }
}

fn validate_version(version: u8) -> Result<(), ProtocolError> {
    if version == VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion(version))
    }
}

fn validate_version_v2(version: u8) -> Result<(), ProtocolError> {
    if version == VERSION_V2 {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion(version))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("invalid payload: {0}")]
    InvalidPayload(&'static str),
    #[error("frame is too short")]
    FrameTooShort,
    #[error("frame header is too large: {actual} > {max}")]
    HeaderTooLarge { actual: usize, max: usize },
    #[error("transaction is too large: {actual} > {max}")]
    TransactionTooLarge { actual: usize, max: usize },
    #[error("transaction length does not match header: declared {declared}, actual {actual}")]
    TransactionLengthMismatch { declared: usize, actual: usize },
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid base64 transaction: {0}")]
    Base64(#[from] base64::DecodeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_set_resolves_modes() {
        let defaults = [RelayRoute::TpuQuic, RelayRoute::TpuUdp];

        assert_eq!(RouteSet::server_default().resolve(&defaults), defaults);
        assert_eq!(
            RouteSet::only([RelayRoute::JitoBundle]).resolve(&defaults),
            [RelayRoute::JitoBundle]
        );
        assert_eq!(
            RouteSet::default_plus([RelayRoute::JitoBundle]).resolve(&defaults),
            [
                RelayRoute::TpuQuic,
                RelayRoute::TpuUdp,
                RelayRoute::JitoBundle
            ]
        );
        assert_eq!(
            RouteSet::default_minus([RelayRoute::TpuUdp]).resolve(&defaults),
            [RelayRoute::TpuQuic]
        );
    }

    #[test]
    fn route_set_serde_names_are_stable() {
        let encoded =
            serde_json::to_string(&RouteSet::default_plus([RelayRoute::JitoBundle])).unwrap();
        assert_eq!(
            encoded,
            r#"{"mode":"default_plus","routes":["jito_bundle"]}"#
        );
        let decoded: RouteSet = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, RouteSet::default_plus([RelayRoute::JitoBundle]));
    }

    #[test]
    fn validates_single_tx_and_bundle_shapes() {
        SubmitRequest::send_transaction_base64("AA==", RouteSet::server_default())
            .validate_shape()
            .unwrap();
        SubmitRequest::send_bundle_base64(
            vec!["AA==".to_string(), "AQ==".to_string()],
            RouteSet::only([RelayRoute::JitoBundle]),
        )
        .validate_shape()
        .unwrap();

        let invalid = SubmitRequest::send_bundle_base64(Vec::new(), RouteSet::server_default());
        assert!(matches!(
            invalid.validate_shape(),
            Err(ProtocolError::InvalidPayload(_))
        ));
    }

    #[test]
    fn quic_frame_round_trips() {
        let header = QuicSubmitHeader {
            version: VERSION,
            method: RelayMethod::SendTransaction,
            payload_kind: QuicPayloadKind::SingleRawTransaction,
            request_id: Some("req-1".to_string()),
            route_set: RouteSet::server_default(),
            response_mode: None,
            signature: Some("sig".to_string()),
        };
        let encoded = encode_quic_frame(&header, b"tx").unwrap();
        let (decoded_header, decoded_payload) = decode_quic_frame(&encoded).unwrap();

        assert_eq!(decoded_header, header);
        assert_eq!(decoded_payload, b"tx");
    }

    #[test]
    fn quic_frame_preserves_route_set_and_request_id() {
        let header = QuicSubmitHeader {
            version: VERSION,
            method: RelayMethod::SendTransaction,
            payload_kind: QuicPayloadKind::SingleRawTransaction,
            request_id: Some("bench-req-1".to_string()),
            route_set: RouteSet::only([RelayRoute::TpuQuic]),
            response_mode: Some(ResponseMode::RouteResults),
            signature: None,
        };
        let payload = b"unique-transaction-bytes";
        let encoded = encode_quic_frame(&header, payload).unwrap();
        let (decoded_header, decoded_payload) = decode_quic_frame(&encoded).unwrap();

        assert_eq!(
            decoded_header.route_set,
            RouteSet::only([RelayRoute::TpuQuic])
        );
        assert_eq!(decoded_header.request_id.as_deref(), Some("bench-req-1"));
        assert_eq!(
            decoded_header.response_mode,
            Some(ResponseMode::RouteResults)
        );
        assert_eq!(decoded_payload, payload);
    }

    fn v2_header(
        transaction_version: TransactionVersion,
        raw_transaction_length: u32,
    ) -> QuicSubmitHeaderV2 {
        QuicSubmitHeaderV2 {
            version: VERSION_V2,
            method: RelayMethod::SendTransaction,
            payload_kind: QuicPayloadKind::SingleRawTransaction,
            transaction_version,
            raw_transaction_length,
            request_id: Some("req-v2".to_string()),
            route_set: RouteSet::server_default(),
            response_mode: Some(ResponseMode::Fast),
            signature: None,
        }
    }

    #[test]
    fn v1_codec_retains_1232_byte_ceiling() {
        let header = QuicSubmitHeader {
            version: VERSION,
            method: RelayMethod::SendTransaction,
            payload_kind: QuicPayloadKind::SingleRawTransaction,
            request_id: None,
            route_set: RouteSet::server_default(),
            response_mode: None,
            signature: None,
        };

        let frame = encode_quic_frame(&header, &vec![0; 1_232]).unwrap();
        assert_eq!(decode_quic_frame(&frame).unwrap().1.len(), 1_232);
        assert!(matches!(
            encode_quic_frame(&header, &vec![0; 1_233]),
            Err(ProtocolError::TransactionTooLarge { max: 1_232, .. })
        ));
    }

    #[test]
    fn v2_accepts_v1_through_4096_and_rejects_4097() {
        let header = v2_header(TransactionVersion::V1, 4_096);
        let frame = encode_quic_frame_v2(&header, &vec![0; 4_096]).unwrap();
        let (decoded, payload) = decode_quic_frame_v2(&frame).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(payload.len(), 4_096);

        let oversized = v2_header(TransactionVersion::V1, 4_097);
        assert!(matches!(
            encode_quic_frame_v2(&oversized, &vec![0; 4_097]),
            Err(ProtocolError::TransactionTooLarge { max: 4_096, .. })
        ));
    }

    #[test]
    fn v2_retains_1232_ceiling_for_legacy_and_v0() {
        for version in [TransactionVersion::Legacy, TransactionVersion::V0] {
            let header = v2_header(version, 1_233);
            assert!(matches!(
                encode_quic_frame_v2(&header, &vec![0; 1_233]),
                Err(ProtocolError::TransactionTooLarge { max: 1_232, .. })
            ));
        }
    }

    #[test]
    fn v2_rejects_declared_length_mismatch() {
        let header = v2_header(TransactionVersion::V1, 4);
        assert!(matches!(
            encode_quic_frame_v2(&header, &[1, 2, 3]),
            Err(ProtocolError::TransactionLengthMismatch {
                declared: 4,
                actual: 3
            })
        ));
    }

    #[test]
    fn v2_rejects_future_version_and_truncated_frames() {
        let mut future = serde_json::to_value(v2_header(TransactionVersion::V1, 1)).unwrap();
        future["version"] = serde_json::json!(VERSION_V2 + 1);
        let header = serde_json::to_vec(&future).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(&(header.len() as u32).to_be_bytes());
        frame.extend_from_slice(&header);
        frame.push(0);

        assert!(matches!(
            decode_quic_frame_v2(&frame),
            Err(ProtocolError::UnsupportedVersion(version)) if version == VERSION_V2 + 1
        ));
        assert!(matches!(
            decode_quic_frame_v2(&[0, 0, 0]),
            Err(ProtocolError::FrameTooShort)
        ));
        assert!(matches!(
            decode_quic_frame_v2(&[0, 0, 0, 8, b'{']),
            Err(ProtocolError::FrameTooShort)
        ));
    }
}
