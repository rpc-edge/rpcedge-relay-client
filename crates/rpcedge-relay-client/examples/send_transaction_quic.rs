use rpcedge_relay_client::{QuicRelayClient, QuicRelayClientConfig};
use std::{net::SocketAddr, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint: SocketAddr = std::env::var("RPCEDGE_RELAY_QUIC_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:4433".into())
        .parse()?;
    let api_key = std::env::var("RPCEDGE_API_KEY")
        .unwrap_or_else(|_| "00000000-0000-4000-8000-000000000000".into());
    let transaction_base64 = std::env::var("RPCEDGE_TRANSACTION_BASE64").unwrap_or("AA==".into());

    let client = QuicRelayClient::connect(
        QuicRelayClientConfig::new(endpoint, api_key).with_timeout(Duration::from_secs(2)),
    )
    .await?;
    let response = client
        .send_transaction_raw_base64(&transaction_base64)
        .await?;

    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
