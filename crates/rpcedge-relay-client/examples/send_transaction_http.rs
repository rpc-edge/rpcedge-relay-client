use rpcedge_relay_client::{RelayClient, RelayClientConfig, RouteSet};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("RPCEDGE_RELAY_URL").unwrap_or_else(|_| "https://relay.rpcedge.com".into());
    let api_key = std::env::var("RPCEDGE_API_KEY")
        .unwrap_or_else(|_| "00000000-0000-4000-8000-000000000000".into());
    let transaction_base64 = std::env::var("RPCEDGE_TRANSACTION_BASE64").unwrap_or("AA==".into());

    let client = RelayClient::new(RelayClientConfig::new(endpoint, api_key))?;
    let response = client
        .send_transaction_base64(transaction_base64, RouteSet::server_default())
        .await?;

    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
