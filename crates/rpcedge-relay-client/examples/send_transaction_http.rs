use rpcedge_relay_client::{RelayClient, RelayClientConfig, RouteSet};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RelayClient::new(RelayClientConfig {
        endpoint: "https://relay.rpcedge.com".to_string(),
        api_key: "00000000-0000-4000-8000-000000000000".to_string(),
    })?;

    let request = client.build_send_transaction_request("AA==", RouteSet::server_default());
    println!("{}", serde_json::to_string_pretty(&request)?);
    Ok(())
}
