pub use rpcedge_relay_protocol::{
    RelayMethod, RelayRoute, RouteSet, RouteSetMode, SubmitRequest, VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayClientConfig {
    pub endpoint: String,
    pub api_key: String,
}

#[derive(Debug, Clone)]
pub struct RelayClient {
    config: RelayClientConfig,
}

impl RelayClient {
    pub fn new(config: RelayClientConfig) -> Result<Self, RelayClientError> {
        if config.endpoint.trim().is_empty() {
            return Err(RelayClientError::InvalidConfig("endpoint is empty"));
        }
        if config.api_key.trim().is_empty() {
            return Err(RelayClientError::InvalidConfig("api_key is empty"));
        }
        Ok(Self { config })
    }

    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }

    pub fn api_key(&self) -> &str {
        &self.config.api_key
    }

    pub fn build_send_transaction_request(
        &self,
        transaction_base64: impl Into<String>,
        route_set: RouteSet,
    ) -> SubmitRequest {
        SubmitRequest::send_transaction_base64(transaction_base64, route_set)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RelayClientError {
    #[error("invalid relay client config: {0}")]
    InvalidConfig(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_config() {
        assert!(RelayClient::new(RelayClientConfig {
            endpoint: String::new(),
            api_key: "key".to_string(),
        })
        .is_err());
    }

    #[test]
    fn builds_send_transaction_request() {
        let client = RelayClient::new(RelayClientConfig {
            endpoint: "https://relay.rpcedge.com".to_string(),
            api_key: "00000000-0000-4000-8000-000000000000".to_string(),
        })
        .unwrap();

        let request = client.build_send_transaction_request(
            "AA==",
            RouteSet::default_plus([RelayRoute::HeliusSenderSwqos]),
        );

        assert_eq!(request.version, VERSION);
        assert_eq!(request.method, RelayMethod::SendTransaction);
    }
}
