# RPCEdge Relay Client

Rust client and shared protocol types for RPCEdge transaction relay.

This repository is intentionally separate from the private Polaris monorepo.
The intended upstream is:

```text
https://github.com/rpc-edge/rpcedge-relay-client.git
```

Push access is required before Polaris can pin this client by Git revision.

## Current Status

Draft scaffold:

- `rpcedge-relay-protocol`: public method, route, route-set, HTTP envelope, and
  QUIC frame types.
- `rpcedge-relay-client`: placeholder client API surface that will wrap HTTP and
  QUIC transports once the route-aware gateway endpoint lands.

Public launch scope is single-transaction submission:

- `sendTransaction`
- `sendTransactionFast`

`sendBundle` is represented in the protocol for Polaris internal use only. The
server must deny it for public standard and premium keys.

## Route Sets

```rust
use rpcedge_relay_protocol::{RelayRoute, RouteSet};

let route_set = RouteSet::default_plus([RelayRoute::JitoBundle]);
```

The server resolves route sets after authentication and policy lookup. A client
request for an unauthorized route must fail explicitly; it must not silently
fall back to server defaults.

## Planned Transports

- HTTP JSON envelope: `POST /v1/submit`
- Raw HTTP compatibility: `POST /v1/transactions`
- QUIC framed v1 with ALPN `rpcedge-submit-v1`

See the Polaris architecture docs for the rollout plan.
