# RPCEdge Relay Client

Rust client and shared protocol types for RPCEdge transaction relay.

This repository is intentionally separate from the private Polaris monorepo.
The intended upstream is:

```text
https://github.com/rpc-edge/rpcedge-relay-client.git
```

Push access is required before Polaris can pin this client by Git revision.

## Current Status

Public Rust client and shared protocol types:

- `rpcedge-relay-protocol`: public method, route, route-set, HTTP envelope, and
  QUIC frame types.
- `rpcedge-relay-client`: async HTTP client for the route-aware
  `POST /v1/submit` endpoint.

Public launch scope is single-transaction submission:

- `sendTransaction`
- `sendTransactionFast`

`sendBundle` is represented in the protocol for Polaris internal use only. The
server must deny it for public standard and premium keys.

## HTTP Usage

```rust
use rpcedge_relay_client::{RelayClient, RelayClientConfig, RouteSet};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RelayClient::new(RelayClientConfig::new(
        "https://relay.rpcedge.com",
        "00000000-0000-4000-8000-000000000000",
    ))?;

    let response = client
        .send_transaction_base64("BASE64_TRANSACTION", RouteSet::server_default())
        .await?;

    println!("accepted={} signature={}", response.accepted, response.signature);
    Ok(())
}
```

The client sends both `Authorization: Bearer <api-key>` and `x-api-key:
<api-key>` headers. The endpoint may be either a base URL such as
`https://relay.rpcedge.com` or the full submit URL
`https://relay.rpcedge.com/v1/submit`.

## Route Sets

```rust
use rpcedge_relay_protocol::{RelayRoute, RouteSet};

let route_set = RouteSet::default_plus([RelayRoute::JitoBundle]);
```

The server resolves route sets after authentication and policy lookup. A client
request for an unauthorized route must fail explicitly; it must not silently
fall back to server defaults.

## Transports

- HTTP JSON envelope: `POST /v1/submit`
- Raw HTTP compatibility: `POST /v1/transactions`
- QUIC framed v1 with ALPN `rpcedge-submit-v1` is planned.

See the Polaris architecture docs for the rollout plan.
