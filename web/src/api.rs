use serde_json::Value;

pub const DEFAULT_RPC_ENDPOINT: &str = "ws://127.0.0.1:8787";
#[cfg(target_arch = "wasm32")]
pub const RPC_ENDPOINT_STORAGE_KEY: &str = "quant-trader.rpc-endpoint";

#[cfg(target_arch = "wasm32")]
use jsonrpsee::{
    core::{client::ClientT, traits::ToRpcParams},
    wasm_client::WasmClientBuilder,
};
#[cfg(target_arch = "wasm32")]
use serde_json::Map;

#[derive(Clone, Debug, PartialEq)]
pub struct DashboardData {
    pub system: Value,
    pub positions: Value,
    pub strategies: Value,
    pub execution_configs: Value,
    pub alerts: Value,
    pub metrics: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PerformanceData {
    pub report: Value,
    pub snapshots: Value,
}

#[cfg(target_arch = "wasm32")]
pub async fn load_dashboard(endpoint: &str) -> Result<DashboardData, String> {
    let client = WasmClientBuilder::default()
        .build(endpoint)
        .await
        .map_err(|error| error.to_string())?;
    Ok(DashboardData {
        system: call(&client, "system.status", Map::new()).await?,
        positions: call(&client, "portfolio.positions", Map::new()).await?,
        strategies: call(&client, "strategy.list", Map::new()).await?,
        execution_configs: call(&client, "strategy.execution.list", Map::new()).await?,
        alerts: call(
            &client,
            "monitor.alerts",
            object([
                ("active_only", Value::Bool(true)),
                ("limit", Value::from(100)),
            ]),
        )
        .await?,
        metrics: call(&client, "monitor.metrics", Map::new()).await?,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn load_dashboard(_endpoint: &str) -> Result<DashboardData, String> {
    Err("Web UI must run as wasm32".into())
}

#[cfg(target_arch = "wasm32")]
pub async fn call_method(endpoint: &str, method: &str, params: Value) -> Result<Value, String> {
    let client = WasmClientBuilder::default()
        .build(endpoint)
        .await
        .map_err(|error| error.to_string())?;
    let params = params
        .as_object()
        .cloned()
        .ok_or_else(|| "RPC params must be a JSON object".to_owned())?;
    call(&client, method, params).await
}

#[cfg(target_arch = "wasm32")]
pub async fn load_performance(
    endpoint: &str,
    strategy_id: &str,
    initial_capital: f64,
) -> Result<PerformanceData, String> {
    let client = WasmClientBuilder::default()
        .build(endpoint)
        .await
        .map_err(|error| error.to_string())?;
    Ok(PerformanceData {
        report: call(
            &client,
            "performance.report",
            object([
                ("strategy_id", Value::from(strategy_id)),
                ("initial_capital", Value::from(initial_capital)),
                ("benchmark_conid", Value::Null),
            ]),
        )
        .await?,
        snapshots: call(
            &client,
            "performance.snapshots",
            object([
                ("strategy_id", Value::from(strategy_id)),
                ("limit", Value::from(100)),
            ]),
        )
        .await?,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn load_performance(
    _endpoint: &str,
    _strategy_id: &str,
    _initial_capital: f64,
) -> Result<PerformanceData, String> {
    Err("Web UI must run as wasm32".into())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn call_method(_endpoint: &str, _method: &str, _params: Value) -> Result<Value, String> {
    Err("Web UI must run as wasm32".into())
}

pub fn validate_rpc_endpoint(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim().trim_end_matches('/').to_owned();
    if !(endpoint.starts_with("ws://") || endpoint.starts_with("wss://")) {
        return Err("RPC 地址必须以 ws:// 或 wss:// 开头".into());
    }
    let remainder = endpoint
        .split_once("://")
        .map(|(_, remainder)| remainder)
        .unwrap_or_default();
    let authority = remainder.split('/').next().unwrap_or_default();
    if authority.is_empty()
        || endpoint.contains(char::is_whitespace)
        || endpoint.contains('#')
        || authority.contains('@')
    {
        return Err(
            "RPC 地址格式无效，例如 ws://192.168.1.10:8787 或 wss://example.com/rpc".into(),
        );
    }
    Ok(endpoint)
}

#[cfg(target_arch = "wasm32")]
pub fn load_rpc_endpoint() -> String {
    let configured_default = option_env!("QUANT_RPC_WS_URL").unwrap_or(DEFAULT_RPC_ENDPOINT);
    web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(RPC_ENDPOINT_STORAGE_KEY).ok().flatten())
        .and_then(|value| validate_rpc_endpoint(&value).ok())
        .unwrap_or_else(|| configured_default.to_owned())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_rpc_endpoint() -> String {
    DEFAULT_RPC_ENDPOINT.into()
}

#[cfg(target_arch = "wasm32")]
pub fn save_rpc_endpoint(endpoint: &str) -> Result<String, String> {
    let endpoint = validate_rpc_endpoint(endpoint)?;
    let storage = web_sys::window()
        .ok_or_else(|| "浏览器 Window 不可用".to_owned())?
        .local_storage()
        .map_err(|_| "无法访问 LocalStorage".to_owned())?
        .ok_or_else(|| "浏览器禁用了 LocalStorage".to_owned())?;
    storage
        .set_item(RPC_ENDPOINT_STORAGE_KEY, &endpoint)
        .map_err(|_| "无法写入 LocalStorage".to_owned())?;
    Ok(endpoint)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn save_rpc_endpoint(endpoint: &str) -> Result<String, String> {
    validate_rpc_endpoint(endpoint)
}

#[cfg(target_arch = "wasm32")]
async fn call<C, P>(client: &C, method: &str, params: P) -> Result<Value, String>
where
    C: ClientT + Sync,
    P: ToRpcParams + Send,
{
    client
        .request(method, params)
        .await
        .map_err(|error| format!("{method}: {error}"))
}

#[cfg(target_arch = "wasm32")]
fn object<const N: usize>(values: [(&str, Value); N]) -> Map<String, Value> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::validate_rpc_endpoint;

    #[test]
    fn rpc_endpoint_accepts_websocket_urls_and_normalizes_trailing_slash() {
        assert_eq!(
            validate_rpc_endpoint(" ws://192.168.1.20:8787/ ").unwrap(),
            "ws://192.168.1.20:8787"
        );
        assert_eq!(
            validate_rpc_endpoint("wss://quant.example.test").unwrap(),
            "wss://quant.example.test"
        );
        assert_eq!(
            validate_rpc_endpoint("wss://quant.example.test/rpc").unwrap(),
            "wss://quant.example.test/rpc"
        );
        assert!(validate_rpc_endpoint("http://127.0.0.1:8787").is_err());
        assert!(validate_rpc_endpoint("ws://user@host/rpc").is_err());
    }
}
