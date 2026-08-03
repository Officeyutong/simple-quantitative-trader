//! Shared JSON-RPC contract used by the daemon, CLI and WebAssembly UI.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const RPC_VERSION: u32 = 2;

pub const ALL_METHODS: &[&str] = &[
    "system.status",
    "system.health",
    "system.version",
    "system.shutdown",
    "ibkr.status",
    "ibkr.connect",
    "ibkr.disconnect",
    "account.managed",
    "account.summary",
    "account.pnl",
    "instrument.search",
    "instrument.list",
    "portfolio.positions",
    "data.backfill",
    "data.jobs",
    "data.job.cancel",
    "data.coverage",
    "data.verify",
    "data.snapshot.create",
    "data.snapshot.list",
    "market_data.subscribe",
    "market_data.unsubscribe",
    "market_data.subscriptions",
    "market_data.quote",
    "market_data.health",
    "market_data.bars",
    "strategy.create",
    "strategy.kinds",
    "strategy.list",
    "strategy.rename",
    "strategy.start",
    "strategy.pause",
    "strategy.stop",
    "strategy.delete",
    "strategy.signals",
    "strategy.execution.configure",
    "strategy.execution.configure_portfolio",
    "strategy.execution.enable",
    "strategy.execution.disable",
    "strategy.execution.list",
    "strategy.execution.actions",
    "execution_cost.model.upsert",
    "execution_cost.model.list",
    "execution_cost.model.delete",
    "execution_cost.control.configure",
    "execution_cost.control.list",
    "performance.report",
    "performance.snapshots",
    "fx.set",
    "fx.list",
    "calendar.add",
    "calendar.refresh",
    "calendar.list",
    "calendar.status",
    "monitor.metrics",
    "monitor.alerts",
    "monitor.acknowledge",
    "logs.tail",
    "backtest.run",
    "backtest.list",
    "backtest.get",
    "backup.create",
    "backup.list",
    "safety.status",
    "safety.set",
    "safety.live_approve",
    "safety.live_revoke",
    "order.preview",
    "order.submit",
    "order.cancel",
    "order.intent.resolve",
    "order.list",
    "execution.list",
    "reconcile.run",
    "reconcile.status",
    "reconcile.differences",
    "reconcile.acknowledge",
];

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EmptyParams {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DynamicParams {
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub rpc_version: u32,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Accepted {
    pub accepted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyIdParams {
    pub strategy_id: uuid::Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyDeleteParams {
    pub strategy_id: uuid::Uuid,
    pub confirm: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstrumentSearchParams {
    pub pattern: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelOrderParams {
    pub broker_order_id: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcknowledgeDifferenceParams {
    pub difference_id: uuid::Uuid,
    pub note: String,
}

/// Manually resolves an order intent stuck in 'unknown' after the operator
/// has confirmed the true outcome against IBKR (via reconcile and the open
/// orders / executions views). Unknown intents block automatic execution for
/// their contract and occupy risk headroom until resolved.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolveOrderIntentParams {
    pub order_intent_id: uuid::Uuid,
    pub note: String,
    pub confirm: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketDataConidParams {
    pub conid: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketDataBarsParams {
    pub conid: i32,
    #[serde(default = "default_live_bar_timeframe")]
    pub timeframe: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataCoverageParams {
    pub conid: i32,
    pub timeframe: String,
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub outside_rth: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyCreateParams {
    pub name: String,
    pub kind: String,
    pub config: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyRenameParams {
    pub strategy_id: uuid::Uuid,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategySignalsParams {
    pub strategy_id: uuid::Uuid,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogsTailParams {
    #[serde(default)]
    pub after_cursor: u64,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyExecutionToggleParams {
    pub strategy_id: uuid::Uuid,
    pub confirm: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyExecutionActionsParams {
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// Backward-compatible first-page size used by older CLI clients.
    pub limit: Option<usize>,
}

pub type PaginationParams = StrategyExecutionActionsParams;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerformanceReportParams {
    pub strategy_id: uuid::Uuid,
    pub initial_capital: f64,
    pub benchmark_conid: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerformanceSnapshotsParams {
    pub strategy_id: uuid::Uuid,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalendarListParams {
    pub exchange: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalendarStatusParams {
    pub exchange: String,
    #[serde(default)]
    pub outside_rth: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitoringAlertsParams {
    #[serde(default = "default_true")]
    pub active_only: bool,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitoringAcknowledgeParams {
    pub alert_id: uuid::Uuid,
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetSnapshotParams {
    pub name: String,
    #[serde(default = "default_bars_dataset")]
    pub dataset: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SafetyModeParams {
    pub mode: String,
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveApprovalParams {
    pub conids: Vec<i32>,
    pub note: String,
    pub confirm_live_risk: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SafetyNoteParams {
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataJobIdParams {
    pub job_id: uuid::Uuid,
}

fn default_true() -> bool {
    true
}

fn default_page() -> usize {
    1
}

fn default_page_size() -> usize {
    25
}

fn default_limit() -> usize {
    100
}

fn default_live_bar_timeframe() -> String {
    "1m".into()
}

fn default_bars_dataset() -> String {
    "bars".into()
}

pub mod method {
    pub const SYSTEM_STATUS: &str = "system.status";
    pub const SYSTEM_HEALTH: &str = "system.health";
    pub const SYSTEM_VERSION: &str = "system.version";
    pub const SYSTEM_SHUTDOWN: &str = "system.shutdown";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_names_are_unique() {
        let mut methods = ALL_METHODS.to_vec();
        methods.sort_unstable();
        methods.dedup();
        assert_eq!(methods.len(), ALL_METHODS.len());
    }
}
