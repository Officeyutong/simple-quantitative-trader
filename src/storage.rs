use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use duckdb::{Connection, OptionalExt, params};
use serde::Serialize;

use crate::error::{AppError, Result};

const MIGRATIONS: &[(i64, &str)] = &[
    (
        1,
        r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS system_events (
    event_seq BIGINT PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR NOT NULL,
    payload_json JSON NOT NULL,
    event_time TIMESTAMPTZ NOT NULL
);
"#,
    ),
    (
        2,
        r#"
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id UUID PRIMARY KEY,
    conid BIGINT NOT NULL UNIQUE,
    symbol VARCHAR NOT NULL,
    security_type VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS positions_current (
    account_id VARCHAR NOT NULL,
    conid BIGINT NOT NULL,
    quantity DOUBLE NOT NULL,
    average_cost DOUBLE NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, conid)
);
CREATE TABLE IF NOT EXISTS order_intents (
    order_intent_id UUID PRIMARY KEY,
    idempotency_key VARCHAR NOT NULL UNIQUE,
    account_id VARCHAR NOT NULL,
    conid BIGINT NOT NULL,
    payload_json JSON NOT NULL,
    status VARCHAR NOT NULL,
    rejection_reason VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    order_intent_id UUID NOT NULL,
    broker_order_id BIGINT,
    broker_perm_id BIGINT,
    status VARCHAR NOT NULL,
    filled_quantity DOUBLE NOT NULL DEFAULT 0,
    average_fill_price DOUBLE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS order_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    event_type VARCHAR NOT NULL,
    payload_json JSON NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS executions (
    execution_id UUID PRIMARY KEY,
    broker_execution_id VARCHAR NOT NULL UNIQUE,
    order_id UUID,
    conid BIGINT NOT NULL,
    side VARCHAR NOT NULL,
    quantity DOUBLE NOT NULL,
    price DOUBLE NOT NULL,
    commission DOUBLE,
    currency VARCHAR,
    executed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS risk_decisions (
    decision_id UUID PRIMARY KEY,
    order_intent_id UUID NOT NULL,
    outcome VARCHAR NOT NULL,
    reason_code VARCHAR NOT NULL,
    detail VARCHAR NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS dataset_files (
    file_id UUID PRIMARY KEY,
    dataset VARCHAR NOT NULL,
    relative_path VARCHAR NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    conid BIGINT,
    timeframe VARCHAR,
    min_time TIMESTAMPTZ,
    max_time TIMESTAMPTZ,
    row_count BIGINT NOT NULL,
    byte_size BIGINT NOT NULL,
    active BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
"#,
    ),
    (
        3,
        r#"
ALTER TABLE orders ADD COLUMN connection_session_id UUID;
ALTER TABLE executions ADD COLUMN connection_session_id UUID;
ALTER TABLE executions ADD COLUMN broker_perm_id BIGINT;
CREATE INDEX orders_session_order_id_idx
    ON orders (connection_session_id, broker_order_id);
CREATE INDEX orders_perm_id_idx ON orders (broker_perm_id);
"#,
    ),
    (
        4,
        r#"
CREATE TABLE reconciliation_runs (
    reconciliation_id UUID PRIMARY KEY,
    connection_session_id UUID NOT NULL,
    status VARCHAR NOT NULL,
    open_order_count BIGINT NOT NULL,
    completed_order_count BIGINT NOT NULL,
    recovered_event_count BIGINT NOT NULL,
    external_order_count BIGINT NOT NULL,
    blocking_difference_count BIGINT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE reconciliation_differences (
    difference_id UUID PRIMARY KEY,
    reconciliation_id UUID NOT NULL,
    difference_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    broker_order_id BIGINT,
    broker_perm_id BIGINT,
    local_order_id UUID,
    detail VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE broker_order_snapshots (
    snapshot_id UUID PRIMARY KEY,
    reconciliation_id UUID NOT NULL,
    source VARCHAR NOT NULL,
    broker_order_id BIGINT,
    broker_perm_id BIGINT,
    client_id BIGINT NOT NULL,
    account_id VARCHAR NOT NULL,
    conid BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    quantity DOUBLE NOT NULL,
    order_type VARCHAR NOT NULL,
    limit_price DOUBLE,
    status VARCHAR NOT NULL,
    completed_time VARCHAR,
    local_order_id UUID,
    is_external BOOLEAN NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX reconciliation_runs_session_idx
    ON reconciliation_runs (connection_session_id, completed_at);
CREATE INDEX broker_order_snapshots_perm_idx
    ON broker_order_snapshots (broker_perm_id);
"#,
    ),
    (
        5,
        r#"
CREATE TABLE account_summary_current (
    account_id VARCHAR NOT NULL,
    tag VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, tag, currency)
);
CREATE TABLE account_pnl_current (
    account_id VARCHAR PRIMARY KEY,
    daily_pnl DOUBLE NOT NULL,
    unrealized_pnl DOUBLE,
    realized_pnl DOUBLE,
    observed_at TIMESTAMPTZ NOT NULL
);
"#,
    ),
    (
        6,
        r#"
ALTER TABLE reconciliation_differences
    ADD COLUMN disposition VARCHAR;
ALTER TABLE reconciliation_differences ADD COLUMN disposition_note VARCHAR;
ALTER TABLE reconciliation_differences ADD COLUMN disposition_at TIMESTAMPTZ;
UPDATE reconciliation_differences SET disposition = 'open';
"#,
    ),
    (
        7,
        r#"
CREATE TABLE market_data_subscriptions (
    conid BIGINT PRIMARY KEY,
    symbol VARCHAR NOT NULL,
    security_type VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    primary_exchange VARCHAR NOT NULL,
    local_symbol VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE market_ticks_current (
    conid BIGINT NOT NULL,
    tick_type VARCHAR NOT NULL,
    numeric_value DOUBLE,
    text_value VARCHAR,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (conid, tick_type)
);
"#,
    ),
    (
        8,
        r#"
CREATE TABLE market_data_status (
    conid BIGINT PRIMARY KEY,
    state VARCHAR NOT NULL,
    last_error VARCHAR,
    observed_at TIMESTAMPTZ NOT NULL
);
"#,
    ),
    (
        9,
        r#"
CREATE TABLE market_minute_bars (
    conid BIGINT NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    tick_count BIGINT NOT NULL,
    final BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (conid, bar_time)
);
"#,
    ),
    (
        10,
        r#"
CREATE TABLE data_jobs (
    job_id UUID PRIMARY KEY,
    job_type VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    request_json JSON NOT NULL,
    cursor_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    attempts BIGINT NOT NULL,
    completed_slices BIGINT NOT NULL,
    last_error VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX data_jobs_state_created_idx ON data_jobs (state, created_at);
"#,
    ),
    (
        11,
        r#"
CREATE TABLE strategies (
    strategy_id UUID PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    kind VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    config_json JSON NOT NULL,
    last_evaluated_bar TIMESTAMPTZ,
    last_error VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE strategy_evaluations (
    evaluation_id UUID PRIMARY KEY,
    strategy_id UUID NOT NULL,
    conid BIGINT NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    short_value DOUBLE NOT NULL,
    long_value DOUBLE NOT NULL,
    previous_short_value DOUBLE NOT NULL,
    previous_long_value DOUBLE NOT NULL,
    signal VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(strategy_id, bar_time)
);
CREATE INDEX strategies_state_idx ON strategies (state, updated_at);
CREATE INDEX strategy_evaluations_strategy_time_idx
    ON strategy_evaluations (strategy_id, bar_time);
"#,
    ),
    (
        12,
        r#"
CREATE TABLE backtest_runs (
    backtest_id UUID PRIMARY KEY,
    strategy_kind VARCHAR NOT NULL,
    parameters_json JSON NOT NULL,
    dataset_file_ids_json JSON NOT NULL,
    seed BIGINT NOT NULL,
    state VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    metrics_json JSON,
    error VARCHAR
);
CREATE TABLE backtest_trades (
    trade_id UUID PRIMARY KEY,
    backtest_id UUID NOT NULL,
    conid BIGINT NOT NULL,
    signal_time TIMESTAMPTZ NOT NULL,
    fill_time TIMESTAMPTZ NOT NULL,
    side VARCHAR NOT NULL,
    quantity DOUBLE NOT NULL,
    price DOUBLE NOT NULL,
    commission DOUBLE NOT NULL,
    slippage DOUBLE NOT NULL
);
CREATE TABLE backtest_equity (
    backtest_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    cash DOUBLE NOT NULL,
    position DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    equity DOUBLE NOT NULL,
    PRIMARY KEY(backtest_id, observed_at)
);
CREATE INDEX backtest_runs_started_idx ON backtest_runs (started_at);
"#,
    ),
    (
        13,
        r#"
ALTER TABLE dataset_files ADD COLUMN checksum VARCHAR;
CREATE TABLE dataset_snapshots (
    snapshot_id UUID PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    dataset VARCHAR NOT NULL,
    file_ids_json JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
"#,
    ),
    (
        14,
        r#"
CREATE TABLE trading_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    reject_new_orders BOOLEAN NOT NULL,
    pause_strategies BOOLEAN NOT NULL,
    emergency_stop BOOLEAN NOT NULL,
    live_approved BOOLEAN NOT NULL,
    live_conid_whitelist_json JSON NOT NULL,
    operator_note VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL
);
INSERT INTO trading_control VALUES
    (true, false, false, false, false, '[]', 'safe default', now());
"#,
    ),
    (
        15,
        r#"
ALTER TABLE instruments ADD COLUMN primary_exchange VARCHAR;
ALTER TABLE instruments ADD COLUMN local_symbol VARCHAR;
ALTER TABLE instruments ADD COLUMN description VARCHAR;
CREATE TABLE position_history (
    snapshot_id UUID PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    conid BIGINT NOT NULL,
    quantity DOUBLE NOT NULL,
    average_cost DOUBLE NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE account_pnl_history (
    snapshot_id UUID PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    daily_pnl DOUBLE NOT NULL,
    unrealized_pnl DOUBLE,
    realized_pnl DOUBLE,
    observed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE pending_commissions (
    broker_execution_id VARCHAR PRIMARY KEY,
    commission DOUBLE NOT NULL,
    currency VARCHAR NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX position_history_account_time_idx
    ON position_history(account_id, observed_at);
CREATE INDEX account_pnl_history_account_time_idx
    ON account_pnl_history(account_id, observed_at);
"#,
    ),
    (
        16,
        r#"
ALTER TABLE strategy_evaluations ADD COLUMN output_json JSON;
"#,
    ),
    (
        17,
        r#"
CREATE TABLE strategy_execution_configs (
    strategy_id UUID PRIMARY KEY,
    enabled BOOLEAN NOT NULL,
    paper_only BOOLEAN NOT NULL,
    account_id VARCHAR NOT NULL,
    target_quantity DOUBLE NOT NULL,
    order_type VARCHAR NOT NULL,
    contract_json JSON NOT NULL,
    enabled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE strategy_execution_actions (
    action_id UUID PRIMARY KEY,
    strategy_id UUID NOT NULL,
    evaluation_id UUID NOT NULL UNIQUE,
    idempotency_key VARCHAR NOT NULL UNIQUE,
    signal VARCHAR NOT NULL,
    requested_quantity DOUBLE,
    state VARCHAR NOT NULL,
    order_intent_id UUID,
    broker_order_id BIGINT,
    detail VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX strategy_execution_actions_state_idx
    ON strategy_execution_actions(state, created_at);
"#,
    ),
    (
        18,
        r#"
CREATE TABLE position_sync_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    state VARCHAR NOT NULL,
    observed_at TIMESTAMPTZ
);
INSERT INTO position_sync_state VALUES (true, 'ready', NULL);
"#,
    ),
    (
        19,
        r#"
ALTER TABLE strategy_execution_configs
    ADD COLUMN short_target_quantity DOUBLE DEFAULT 0;
ALTER TABLE strategy_execution_configs
    ADD COLUMN allow_short BOOLEAN DEFAULT false;

CREATE TABLE fx_rates (
    base_currency VARCHAR NOT NULL,
    quote_currency VARCHAR NOT NULL,
    rate DOUBLE NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source VARCHAR NOT NULL,
    PRIMARY KEY (base_currency, quote_currency)
);

CREATE TABLE market_sessions (
    exchange VARCHAR NOT NULL,
    trading_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    state VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, trading_date)
);

CREATE TABLE strategy_performance_snapshots (
    strategy_id UUID NOT NULL,
    account_id VARCHAR NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    base_currency VARCHAR NOT NULL,
    gross_pnl DOUBLE NOT NULL,
    commissions DOUBLE NOT NULL,
    net_pnl DOUBLE NOT NULL,
    turnover DOUBLE NOT NULL,
    realized_trade_count BIGINT NOT NULL,
    winning_trade_count BIGINT NOT NULL,
    losing_trade_count BIGINT NOT NULL,
    open_position_count BIGINT NOT NULL,
    PRIMARY KEY (strategy_id, account_id, observed_at)
);
CREATE INDEX strategy_performance_snapshots_time_idx
    ON strategy_performance_snapshots(strategy_id, observed_at);

CREATE TABLE monitoring_alerts (
    alert_id UUID PRIMARY KEY,
    alert_key VARCHAR NOT NULL UNIQUE,
    severity VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    message VARCHAR NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_note VARCHAR
);
CREATE INDEX monitoring_alerts_state_idx
    ON monitoring_alerts(state, severity, last_observed_at);

CREATE TABLE strategy_execution_action_legs (
    action_id UUID NOT NULL,
    leg_index INTEGER NOT NULL,
    conid INTEGER NOT NULL,
    symbol VARCHAR NOT NULL,
    target_quantity DOUBLE NOT NULL,
    requested_side VARCHAR,
    requested_quantity DOUBLE,
    order_intent_id UUID,
    broker_order_id BIGINT,
    state VARCHAR NOT NULL,
    detail VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (action_id, leg_index)
);
CREATE TABLE strategy_execution_portfolio_legs (
    strategy_id UUID NOT NULL,
    leg_index INTEGER NOT NULL,
    contract_json JSON NOT NULL,
    buy_target_quantity DOUBLE NOT NULL,
    sell_target_quantity DOUBLE NOT NULL,
    PRIMARY KEY (strategy_id, leg_index)
);
"#,
    ),
    (
        20,
        r#"
CREATE TABLE market_five_second_bars (
    conid BIGINT NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    tick_count BIGINT NOT NULL,
    final BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (conid, bar_time)
);
CREATE INDEX market_five_second_bars_final_time_idx
    ON market_five_second_bars(conid, final, bar_time);
"#,
    ),
    (
        21,
        r#"
ALTER TABLE orders ADD COLUMN remaining_quantity DOUBLE;
ALTER TABLE orders ADD COLUMN last_fill_price DOUBLE;
ALTER TABLE orders ADD COLUMN why_held VARCHAR;
ALTER TABLE orders ADD COLUMN market_cap_price DOUBLE;

CREATE TABLE broker_order_events (
    event_id UUID PRIMARY KEY,
    connection_session_id UUID,
    broker_order_id BIGINT NOT NULL,
    broker_perm_id BIGINT,
    event_type VARCHAR NOT NULL,
    payload_json JSON NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX broker_order_events_order_idx
    ON broker_order_events(connection_session_id, broker_order_id, received_at);
CREATE INDEX broker_order_events_perm_idx
    ON broker_order_events(broker_perm_id, received_at);
"#,
    ),
    (
        22,
        r#"
CREATE TABLE execution_cost_models (
    cost_model_id UUID PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    currency VARCHAR NOT NULL,
    buy_fixed_fee DOUBLE NOT NULL,
    buy_rate_bps DOUBLE NOT NULL,
    buy_min_fee DOUBLE NOT NULL,
    sell_fixed_fee DOUBLE NOT NULL,
    sell_rate_bps DOUBLE NOT NULL,
    sell_min_fee DOUBLE NOT NULL,
    sell_tax_bps DOUBLE NOT NULL,
    estimated_spread_bps DOUBLE NOT NULL,
    estimated_slippage_bps DOUBLE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE strategy_cost_controls (
    strategy_id UUID PRIMARY KEY,
    enabled BOOLEAN NOT NULL,
    cost_model_id UUID NOT NULL,
    minimum_cost_multiple DOUBLE NOT NULL,
    maximum_commission_to_gross_profit_ratio DOUBLE NOT NULL,
    minimum_completed_trades BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE strategy_execution_actions ADD COLUMN estimated_notional DOUBLE;
ALTER TABLE strategy_execution_actions ADD COLUMN estimated_round_trip_cost DOUBLE;
ALTER TABLE strategy_execution_actions ADD COLUMN required_edge_bps DOUBLE;
ALTER TABLE strategy_execution_actions ADD COLUMN signal_edge_bps DOUBLE;
ALTER TABLE strategy_execution_actions ADD COLUMN cost_gate_result VARCHAR;
"#,
    ),
    (
        23,
        r#"
ALTER TABLE execution_cost_models ADD COLUMN buy_per_share_fee DOUBLE DEFAULT 0;
ALTER TABLE execution_cost_models ADD COLUMN sell_per_share_fee DOUBLE DEFAULT 0;
"#,
    ),
    (
        24,
        r#"
ALTER TABLE strategy_execution_configs
    ADD COLUMN outside_rth BOOLEAN DEFAULT false;
"#,
    ),
    (
        25,
        r#"
CREATE TABLE market_session_intervals (
    exchange VARCHAR NOT NULL,
    session_kind VARCHAR NOT NULL,
    trading_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    state VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, session_kind, opens_at, closes_at)
);
INSERT INTO market_session_intervals
SELECT exchange, 'regular', trading_date, opens_at, closes_at, state, source, updated_at
FROM market_sessions;
"#,
    ),
    (
        26,
        r#"
CREATE TABLE strategy_runtime_states (
    strategy_id UUID PRIMARY KEY,
    state_version BIGINT NOT NULL,
    state_json JSON NOT NULL,
    revision BIGINT NOT NULL,
    last_transition_bar TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
INSERT INTO strategy_runtime_states
SELECT strategy_id, 1, '{}', 0, NULL, created_at, updated_at
FROM strategies;
"#,
    ),
];

const MAX_STRATEGY_STATE_BYTES: usize = 1024 * 1024;

pub struct Storage {
    connection: Connection,
    database_path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct DatasetFile {
    pub file_id: uuid::Uuid,
    pub relative_path: PathBuf,
    pub row_count: usize,
    pub byte_size: u64,
    pub min_time: DateTime<Utc>,
    pub max_time: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ReconciliationReport {
    pub reconciliation_id: uuid::Uuid,
    pub healthy: bool,
    pub open_order_count: usize,
    pub completed_order_count: usize,
    pub recovered_event_count: usize,
    pub external_order_count: usize,
    pub blocking_difference_count: usize,
    pub unresolved_local_count: usize,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReconciliationHealth {
    pub state: &'static str,
    pub reconciliation_id: Option<uuid::Uuid>,
    pub connection_session_id: Option<uuid::Uuid>,
    pub blocking_difference_count: usize,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CloseOnlyDecision {
    pub allowed: bool,
    pub current_quantity: Option<f64>,
    pub maximum_closing_quantity: f64,
    pub reason: String,
    pub position_observed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketDataHealth {
    pub state: &'static str,
    pub conid: i32,
    pub subscription_state: Option<String>,
    pub latest_price: Option<f64>,
    pub latest_price_type: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub age_seconds: Option<i64>,
    pub maximum_age_seconds: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct PortfolioRiskDecision {
    pub allowed: bool,
    pub reason_code: &'static str,
    pub detail: String,
    pub current_position: f64,
    pub positions_observed_at: Option<DateTime<Utc>>,
    pub projected_position: f64,
    pub projected_gross_exposure: f64,
    pub projected_net_exposure: f64,
    pub active_order_count: usize,
    pub recent_order_count: usize,
    pub daily_pnl: Option<f64>,
    pub daily_pnl_observed_at: Option<DateTime<Utc>>,
    pub price_deviation_bps: Option<f64>,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct BackfillJobRequest {
    pub contract: crate::ibkr::ContractCandidate,
    pub timeframe: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub outside_rth: bool,
}

#[derive(Clone, Debug)]
pub struct ClaimedBackfillJob {
    pub job_id: uuid::Uuid,
    pub request: BackfillJobRequest,
    pub cursor_time: DateTime<Utc>,
    pub attempts: i64,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct BacktestRequest {
    #[serde(default)]
    pub strategy_id: Option<uuid::Uuid>,
    pub conid: i32,
    pub timeframe: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub short_window: Option<usize>,
    pub long_window: Option<usize>,
    #[serde(default = "default_strategy_kind")]
    pub strategy_kind: String,
    #[serde(default)]
    pub strategy_config: Option<serde_json::Value>,
    pub quantity: f64,
    pub initial_cash: f64,
    #[serde(default)]
    pub slippage_bps: f64,
    #[serde(default)]
    pub commission_per_order: f64,
    #[serde(default)]
    pub seed: i64,
}

fn default_strategy_kind() -> String {
    "moving_average_cross".into()
}

#[derive(Clone, Debug)]
struct BacktestBar {
    open_time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Clone, Debug)]
struct SimulatedTrade {
    signal_time: DateTime<Utc>,
    fill_time: DateTime<Utc>,
    side: &'static str,
    quantity: f64,
    price: f64,
    commission: f64,
    slippage: f64,
}

#[derive(Clone, Debug)]
struct EquityPoint {
    observed_at: DateTime<Utc>,
    cash: f64,
    position: f64,
    close: f64,
    equity: f64,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct StrategyExecutionConfig {
    pub strategy_id: uuid::Uuid,
    pub account: String,
    pub target_quantity: f64,
    #[serde(default)]
    pub short_target_quantity: f64,
    #[serde(default)]
    pub allow_short: bool,
    #[serde(default = "default_execution_order_type")]
    pub order_type: String,
    #[serde(default = "default_true")]
    pub paper_only: bool,
    #[serde(default)]
    pub outside_rth: bool,
    pub contract: crate::ibkr::ContractCandidate,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct StrategyExecutionLegConfig {
    pub contract: crate::ibkr::ContractCandidate,
    pub buy_target_quantity: f64,
    pub sell_target_quantity: f64,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct ExecutionCostModelInput {
    pub cost_model_id: Option<uuid::Uuid>,
    pub name: String,
    pub currency: String,
    pub buy_fixed_fee: f64,
    #[serde(default)]
    pub buy_per_share_fee: f64,
    pub buy_rate_bps: f64,
    pub buy_min_fee: f64,
    pub sell_fixed_fee: f64,
    #[serde(default)]
    pub sell_per_share_fee: f64,
    pub sell_rate_bps: f64,
    pub sell_min_fee: f64,
    pub sell_tax_bps: f64,
    pub estimated_spread_bps: f64,
    pub estimated_slippage_bps: f64,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct StrategyCostControlInput {
    pub strategy_id: uuid::Uuid,
    pub enabled: bool,
    pub cost_model_id: uuid::Uuid,
    pub minimum_cost_multiple: f64,
    pub maximum_commission_to_gross_profit_ratio: f64,
    pub minimum_completed_trades: usize,
}

#[derive(Clone, Debug)]
pub struct ClaimedCostControl {
    pub currency: String,
    pub buy_fixed_fee: f64,
    pub buy_per_share_fee: f64,
    pub buy_rate_bps: f64,
    pub buy_min_fee: f64,
    pub sell_fixed_fee: f64,
    pub sell_per_share_fee: f64,
    pub sell_rate_bps: f64,
    pub sell_min_fee: f64,
    pub sell_tax_bps: f64,
    pub estimated_spread_bps: f64,
    pub estimated_slippage_bps: f64,
    pub minimum_cost_multiple: f64,
    pub maximum_commission_to_gross_profit_ratio: f64,
    pub minimum_completed_trades: usize,
    pub actual_fee_bps_p90: Option<f64>,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct StrategyPortfolioExecutionConfig {
    pub strategy_id: uuid::Uuid,
    pub account: String,
    #[serde(default = "default_execution_order_type")]
    pub order_type: String,
    #[serde(default = "default_true")]
    pub paper_only: bool,
    #[serde(default)]
    pub outside_rth: bool,
    pub legs: Vec<StrategyExecutionLegConfig>,
}

fn default_execution_order_type() -> String {
    "market".into()
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ClaimedStrategyAction {
    pub action_id: uuid::Uuid,
    pub strategy_id: uuid::Uuid,
    pub evaluation_id: uuid::Uuid,
    pub signal: String,
    pub side: String,
    pub account: String,
    pub quantity: f64,
    pub order_type: String,
    pub paper_only: bool,
    pub outside_rth: bool,
    pub contract: crate::ibkr::ContractCandidate,
    pub idempotency_key: String,
    pub legs: Vec<ClaimedStrategyLeg>,
    pub signal_edge_bps: Option<f64>,
    pub cost_control: Option<ClaimedCostControl>,
}

#[derive(Clone, Debug)]
pub struct ClaimedStrategyLeg {
    pub leg_index: i32,
    pub side: String,
    pub quantity: f64,
    pub current_quantity: f64,
    pub target_quantity: f64,
    pub contract: crate::ibkr::ContractCandidate,
    pub idempotency_key: String,
}

impl ClaimedStrategyLeg {
    pub fn is_risk_reducing(&self) -> bool {
        position_change_is_risk_reducing(self.current_quantity, self.target_quantity)
    }
}

pub(crate) fn position_change_is_risk_reducing(current: f64, target: f64) -> bool {
    target.abs() < current.abs()
        && (target.abs() <= f64::EPSILON || target.signum() == current.signum())
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct FxRateInput {
    pub base_currency: String,
    pub quote_currency: String,
    pub rate: f64,
    #[serde(default = "default_manual_source")]
    pub source: String,
    #[serde(default = "Utc::now")]
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct MarketSessionInput {
    pub exchange: String,
    pub trading_date: chrono::NaiveDate,
    pub opens_at: DateTime<Utc>,
    pub closes_at: DateTime<Utc>,
    #[serde(default = "default_open_state")]
    pub state: String,
    #[serde(default = "default_manual_source")]
    pub source: String,
}

fn default_manual_source() -> String {
    "manual".into()
}

fn default_open_state() -> String {
    "open".into()
}

#[derive(Clone, Debug, Default)]
struct PerformancePosition {
    quantity: f64,
    average_price: f64,
}

/// Recovers the storage mutex even if a previous holder panicked.
///
/// Without this every later access would panic on the poisoned lock and the
/// daemon would degrade into an unusable zombie process. DuckDB transactions
/// keep the on-disk state consistent, so recovering the guard is safe; the
/// recovery is logged so the operator can investigate the original panic.
pub trait StorageMutexExt {
    fn lock_safe(&self) -> std::sync::MutexGuard<'_, Storage>;
}

impl StorageMutexExt for std::sync::Mutex<Storage> {
    fn lock_safe(&self) -> std::sync::MutexGuard<'_, Storage> {
        self.lock().unwrap_or_else(|poisoned| {
            tracing::error!("storage mutex was poisoned by a panicking task; recovering the lock");
            poisoned.into_inner()
        })
    }
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection =
            Connection::open(path).map_err(|error| AppError::Storage(error.to_string()))?;
        let mut storage = Self {
            connection,
            database_path: path.to_path_buf(),
        };
        storage.migrate()?;
        storage.recover_interrupted_jobs()?;
        Ok(storage)
    }

    fn migrate(&mut self) -> Result<()> {
        self.connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version BIGINT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL
                );",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;

        let supported = MIGRATIONS
            .iter()
            .map(|(version, _)| *version)
            .max()
            .unwrap_or(0);
        let applied_max: i64 = self
            .connection
            .query_row(
                "SELECT coalesce(max(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if applied_max > supported {
            return Err(AppError::Storage(format!(
                "database schema version {applied_max} is newer than the maximum supported \
                 version {supported}; refusing to start with an older binary"
            )));
        }

        for (version, sql) in MIGRATIONS {
            let applied: bool = self
                .connection
                .query_row(
                    "SELECT count(*) > 0 FROM schema_migrations WHERE version = ?",
                    params![version],
                    |row| row.get(0),
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            if applied {
                continue;
            }
            let transaction = self
                .connection
                .transaction()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute_batch(sql)
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let now: DateTime<Utc> = Utc::now();
            transaction
                .execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    params![version, now],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .commit()
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    pub fn schema_version(&self) -> Result<i64> {
        self.connection
            .query_row(
                "SELECT coalesce(max(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn recover_interrupted_jobs(&mut self) -> Result<()> {
        self.connection
            .execute(
                "UPDATE data_jobs SET state = 'retrying',
                   last_error = 'daemon stopped while slice was running',
                   updated_at = ?
                 WHERE state = 'running'",
                params![Utc::now()],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.connection
            .execute(
                "UPDATE strategy_execution_actions SET state = 'failed',
                    detail = 'daemon stopped while action outcome was unknown; manual review required',
                    updated_at = ?
                 WHERE state = 'processing'",
                params![Utc::now()],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn create_strategy(
        &mut self,
        name: &str,
        kind: &str,
        config: &serde_json::Value,
    ) -> Result<uuid::Uuid> {
        if name.trim().is_empty() {
            return Err(AppError::Storage("strategy name cannot be empty".into()));
        }
        let strategy = crate::strategy::build(kind, config.clone()).map_err(AppError::Storage)?;
        let state_version = i64::from(strategy.state_version());
        if state_version <= 0 {
            return Err(AppError::Storage(
                "strategy state version must be greater than zero".into(),
            ));
        }
        let initial_state = serialize_strategy_state(&strategy.initial_state())?;
        let strategy_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO strategies VALUES
                 (?, ?, ?, 'stopped', ?, NULL, NULL, ?, ?)",
                params![
                    strategy_id,
                    name.trim(),
                    kind,
                    serde_json::to_string(config)?,
                    now,
                    now
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO strategy_runtime_states
                 (strategy_id, state_version, state_json, revision,
                  last_transition_bar, created_at, updated_at)
                 VALUES (?, ?, ?, 0, NULL, ?, ?)",
                params![strategy_id, state_version, initial_state, now, now],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(strategy_id)
    }

    pub fn set_strategy_state(&mut self, strategy_id: uuid::Uuid, state: &str) -> Result<bool> {
        if !matches!(state, "running" | "paused" | "stopped") {
            return Err(AppError::Storage("invalid strategy state".into()));
        }
        self.connection
            .execute(
                "UPDATE strategies SET state = ?, last_error = NULL, updated_at = ?
                 WHERE strategy_id = ?",
                params![state, Utc::now(), strategy_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn rename_strategy(&mut self, strategy_id: uuid::Uuid, name: &str) -> Result<bool> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::Storage("strategy name cannot be empty".into()));
        }
        let duplicate: bool = self
            .connection
            .query_row(
                "SELECT count(*) > 0 FROM strategies
                 WHERE name = ? AND strategy_id <> ?",
                params![name, strategy_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if duplicate {
            return Err(AppError::Storage("strategy name already exists".into()));
        }
        self.connection
            .execute(
                "UPDATE strategies SET name = ?, updated_at = ? WHERE strategy_id = ?",
                params![name, Utc::now(), strategy_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn delete_strategy(&mut self, strategy_id: uuid::Uuid) -> Result<bool> {
        let current = self
            .connection
            .query_row(
                "SELECT s.state, coalesce(c.enabled, false)
                 FROM strategies s
                 LEFT JOIN strategy_execution_configs c USING (strategy_id)
                 WHERE s.strategy_id = ?",
                params![strategy_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some((state, execution_enabled)) = current else {
            return Ok(false);
        };
        if state != "stopped" {
            return Err(AppError::Storage(
                "strategy must be stopped before deletion".into(),
            ));
        }
        if execution_enabled {
            return Err(AppError::Storage(
                "strategy execution must be disabled before deletion".into(),
            ));
        }

        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM strategy_execution_action_legs
                 WHERE action_id IN (
                    SELECT action_id FROM strategy_execution_actions WHERE strategy_id = ?
                 )",
                params![strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for table in [
            "strategy_execution_portfolio_legs",
            "strategy_execution_actions",
            "strategy_execution_configs",
            "strategy_cost_controls",
            "strategy_performance_snapshots",
            "strategy_evaluations",
            "strategy_runtime_states",
        ] {
            transaction
                .execute(
                    &format!("DELETE FROM {table} WHERE strategy_id = ?"),
                    params![strategy_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        let deleted = transaction
            .execute(
                "DELETE FROM strategies WHERE strategy_id = ?",
                params![strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            > 0;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(deleted)
    }

    pub fn configure_strategy_execution(&mut self, config: &StrategyExecutionConfig) -> Result<()> {
        if config.account.trim().is_empty()
            || !config.target_quantity.is_finite()
            || config.target_quantity <= 0.0
            || !config.short_target_quantity.is_finite()
            || config.short_target_quantity > 0.0
            || (!config.allow_short && config.short_target_quantity < 0.0)
            || !matches!(config.order_type.as_str(), "market" | "limit")
            || (config.outside_rth && config.order_type != "limit")
            || !config.paper_only
            || config.contract.conid <= 0
        {
            return Err(AppError::Storage(
                "execution requires account, target_quantity > 0, a supported order type, \
                 limit orders for outside-RTH execution, paper_only=true and a valid contract"
                    .into(),
            ));
        }
        let (kind, config_json): (String, String) = self
            .connection
            .query_row(
                "SELECT kind, config_json::VARCHAR FROM strategies WHERE strategy_id = ?",
                params![config.strategy_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| AppError::Storage("strategy not found".into()))?;
        let strategy = crate::strategy::build(&kind, serde_json::from_str(&config_json)?)
            .map_err(AppError::Storage)?;
        if strategy.conid() != config.contract.conid {
            return Err(AppError::Storage(
                "execution contract conid does not match strategy conid".into(),
            ));
        }
        let now = Utc::now();
        self.connection
            .execute(
                "INSERT INTO strategy_execution_configs
                 (strategy_id, enabled, paper_only, account_id, target_quantity,
                  order_type, contract_json, enabled_at, created_at, updated_at,
                  short_target_quantity, allow_short, outside_rth)
                 VALUES (?, false, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                 ON CONFLICT (strategy_id) DO UPDATE SET
                   enabled = false, paper_only = excluded.paper_only,
                   account_id = excluded.account_id,
                   target_quantity = excluded.target_quantity,
                   order_type = excluded.order_type,
                   contract_json = excluded.contract_json,
                   short_target_quantity = excluded.short_target_quantity,
                   allow_short = excluded.allow_short,
                   outside_rth = excluded.outside_rth,
                   enabled_at = NULL, updated_at = excluded.updated_at",
                params![
                    config.strategy_id,
                    config.paper_only,
                    config.account.trim(),
                    config.target_quantity,
                    config.order_type,
                    serde_json::to_string(&config.contract)?,
                    now,
                    now,
                    config.short_target_quantity,
                    config.allow_short,
                    config.outside_rth
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn configure_strategy_portfolio_execution(
        &mut self,
        config: &StrategyPortfolioExecutionConfig,
    ) -> Result<()> {
        if config.legs.is_empty() {
            return Err(AppError::Storage(
                "portfolio execution requires at least one leg".into(),
            ));
        }
        for leg in &config.legs {
            if leg.contract.conid <= 0
                || !leg.buy_target_quantity.is_finite()
                || !leg.sell_target_quantity.is_finite()
            {
                return Err(AppError::Storage(
                    "portfolio legs require valid contracts and finite targets".into(),
                ));
            }
        }
        let first = &config.legs[0];
        self.configure_strategy_execution(&StrategyExecutionConfig {
            strategy_id: config.strategy_id,
            account: config.account.clone(),
            target_quantity: first.buy_target_quantity,
            short_target_quantity: first.sell_target_quantity,
            allow_short: first.sell_target_quantity < 0.0,
            order_type: config.order_type.clone(),
            paper_only: config.paper_only,
            outside_rth: config.outside_rth,
            contract: first.contract.clone(),
        })?;
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM strategy_execution_portfolio_legs WHERE strategy_id = ?",
                params![config.strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for (index, leg) in config.legs.iter().enumerate() {
            transaction
                .execute(
                    "INSERT INTO strategy_execution_portfolio_legs
                     VALUES (?, ?, ?, ?, ?)",
                    params![
                        config.strategy_id,
                        index as i32,
                        serde_json::to_string(&leg.contract)?,
                        leg.buy_target_quantity,
                        leg.sell_target_quantity
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn set_strategy_execution_enabled(
        &mut self,
        strategy_id: uuid::Uuid,
        enabled: bool,
    ) -> Result<bool> {
        let now = Utc::now();
        self.connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET enabled = ?, enabled_at = CASE WHEN ? THEN ? ELSE NULL END,
                     updated_at = ?
                 WHERE strategy_id = ?",
                params![enabled, enabled, now, now, strategy_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_strategy_execution_configs(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT strategy_id, enabled, paper_only, account_id, target_quantity,
                    order_type, contract_json::VARCHAR, enabled_at, created_at, updated_at,
                    short_target_quantity, allow_short, outside_rth
             FROM strategy_execution_configs ORDER BY created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement.query_map([], |row| {
            let contract: String = row.get(6)?;
            Ok(serde_json::json!({
                "strategy_id": row.get::<_, uuid::Uuid>(0)?,
                "enabled": row.get::<_, bool>(1)?,
                "paper_only": row.get::<_, bool>(2)?,
                "account": row.get::<_, String>(3)?,
                "target_quantity": row.get::<_, f64>(4)?,
                "order_type": row.get::<_, String>(5)?,
                "contract": serde_json::from_str::<serde_json::Value>(&contract).unwrap_or_default(),
                "enabled_at": row.get::<_, Option<DateTime<Utc>>>(7)?,
                "created_at": row.get::<_, DateTime<Utc>>(8)?,
                "updated_at": row.get::<_, DateTime<Utc>>(9)?
                ,"short_target_quantity": row.get::<_, f64>(10)?
                ,"allow_short": row.get::<_, bool>(11)?
                ,"outside_rth": row.get::<_, bool>(12)?
            }))
        }).map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn enabled_strategy_accounts(&self) -> Result<Vec<(uuid::Uuid, String)>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT strategy_id, account_id FROM strategy_execution_configs
                 WHERE enabled = true ORDER BY strategy_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, uuid::Uuid>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn claim_strategy_action(&mut self) -> Result<Option<ClaimedStrategyAction>> {
        self.record_disabled_strategy_actions()?;
        let candidate = self
            .connection
            .query_row(
                "SELECT e.evaluation_id, e.strategy_id, e.signal,
                    c.account_id, c.target_quantity, c.order_type,
                    c.paper_only, c.contract_json::VARCHAR,
                    c.short_target_quantity, c.allow_short, e.output_json::VARCHAR,
                    e.short_value, e.long_value, s.kind, c.outside_rth
             FROM strategy_evaluations e
             JOIN strategy_execution_configs c ON c.strategy_id = e.strategy_id
             JOIN strategies s ON s.strategy_id = e.strategy_id
             LEFT JOIN strategy_execution_actions a ON a.evaluation_id = e.evaluation_id
             WHERE c.enabled = true AND c.enabled_at IS NOT NULL
               AND e.created_at >= c.enabled_at AND e.signal IN ('buy', 'sell')
               AND a.evaluation_id IS NULL
             ORDER BY e.created_at LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, uuid::Uuid>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, f64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, bool>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, f64>(8)?,
                        row.get::<_, bool>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, f64>(11)?,
                        row.get::<_, f64>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, bool>(14)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some((
            evaluation_id,
            strategy_id,
            signal,
            account,
            target_quantity,
            order_type,
            paper_only,
            contract_json,
            short_target_quantity,
            allow_short,
            output_json,
            indicator_a,
            indicator_b,
            strategy_kind,
            outside_rth,
        )) = candidate
        else {
            return Ok(None);
        };
        let signal_edge_bps = if strategy_kind == "paper_round_trip" {
            None
        } else {
            output_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                .and_then(|output| {
                    let price = output
                        .pointer("/bar/close")
                        .and_then(serde_json::Value::as_f64)
                        .or_else(|| output.get("close").and_then(serde_json::Value::as_f64))?;
                    let reference = if strategy_kind == "close_threshold" {
                        output
                            .get(if signal == "buy" {
                                "buy_below"
                            } else {
                                "sell_above"
                            })
                            .and_then(serde_json::Value::as_f64)?
                    } else {
                        indicator_b
                    };
                    (price > 0.0).then_some((indicator_a - reference).abs() / price * 10_000.0)
                })
        };
        let mut cost_control = self
            .connection
            .query_row(
                "SELECT m.currency, m.buy_fixed_fee, m.buy_per_share_fee,
                        m.buy_rate_bps, m.buy_min_fee,
                        m.sell_fixed_fee, m.sell_per_share_fee,
                        m.sell_rate_bps, m.sell_min_fee,
                        m.sell_tax_bps, m.estimated_spread_bps,
                        m.estimated_slippage_bps, c.minimum_cost_multiple,
                        c.maximum_commission_to_gross_profit_ratio,
                        c.minimum_completed_trades
                 FROM strategy_cost_controls c
                 JOIN execution_cost_models m USING (cost_model_id)
                 WHERE c.strategy_id = ? AND c.enabled = true",
                params![strategy_id],
                |row| {
                    Ok(ClaimedCostControl {
                        currency: row.get(0)?,
                        buy_fixed_fee: row.get(1)?,
                        buy_per_share_fee: row.get(2)?,
                        buy_rate_bps: row.get(3)?,
                        buy_min_fee: row.get(4)?,
                        sell_fixed_fee: row.get(5)?,
                        sell_per_share_fee: row.get(6)?,
                        sell_rate_bps: row.get(7)?,
                        sell_min_fee: row.get(8)?,
                        sell_tax_bps: row.get(9)?,
                        estimated_spread_bps: row.get(10)?,
                        estimated_slippage_bps: row.get(11)?,
                        minimum_cost_multiple: row.get(12)?,
                        maximum_commission_to_gross_profit_ratio: row.get(13)?,
                        minimum_completed_trades: row.get::<_, i64>(14)?.max(0) as usize,
                        actual_fee_bps_p90: None,
                    })
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if let Some(control) = &mut cost_control {
            control.actual_fee_bps_p90 = self
                .connection
                .query_row(
                    "SELECT quantile_cont(
                         abs(e.commission) / nullif(e.quantity * e.price, 0) * 10000, 0.9)
                     FROM executions e
                     JOIN orders o ON o.order_id = e.order_id
                     JOIN strategy_execution_actions a
                       ON a.order_intent_id = o.order_intent_id
                     WHERE a.strategy_id = ? AND upper(e.currency) = upper(?)
                       AND e.commission IS NOT NULL
                       AND e.quantity > 0 AND e.price > 0",
                    params![strategy_id, control.currency],
                    |row| row.get::<_, Option<f64>>(0),
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        if let Some(control) = &cost_control {
            let performance = self
                .connection
                .query_row(
                    "SELECT gross_pnl, commissions, realized_trade_count
                     FROM strategy_performance_snapshots
                     WHERE strategy_id = ? AND account_id = ?
                     ORDER BY observed_at DESC LIMIT 1",
                    params![strategy_id, account],
                    |row| {
                        Ok((
                            row.get::<_, f64>(0)?,
                            row.get::<_, f64>(1)?,
                            row.get::<_, i64>(2)?.max(0) as usize,
                        ))
                    },
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            if let Some((gross_pnl, commissions, trades)) = performance {
                let ratio = if gross_pnl > 0.0 {
                    commissions / gross_pnl
                } else if commissions > 0.0 {
                    f64::INFINITY
                } else {
                    0.0
                };
                if trades >= control.minimum_completed_trades
                    && ratio > control.maximum_commission_to_gross_profit_ratio
                {
                    let now = Utc::now();
                    let action_id = uuid::Uuid::now_v7();
                    let detail = format!(
                        "execution automatically paused: commission/gross-profit ratio {:.4} \
                         exceeds configured {:.4} after {} completed trades",
                        ratio, control.maximum_commission_to_gross_profit_ratio, trades
                    );
                    let transaction = self
                        .connection
                        .transaction()
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                    transaction
                        .execute(
                            "UPDATE strategy_execution_configs
                             SET enabled = false, updated_at = ? WHERE strategy_id = ?",
                            params![now, strategy_id],
                        )
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                    transaction
                        .execute(
                            "INSERT INTO strategy_execution_actions
                             (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                              requested_quantity, state, detail, created_at, updated_at,
                              cost_gate_result)
                             VALUES (?, ?, ?, ?, ?, NULL, 'skipped', ?, ?, ?, 'auto_paused')",
                            params![
                                action_id,
                                strategy_id,
                                evaluation_id,
                                format!("strategy:{strategy_id}:{evaluation_id}"),
                                signal,
                                detail,
                                now,
                                now
                            ],
                        )
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                    transaction
                        .commit()
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                    return Ok(None);
                }
            }
        }
        let mut leg_statement = self
            .connection
            .prepare(
                "SELECT leg_index, contract_json::VARCHAR,
                        buy_target_quantity, sell_target_quantity
                 FROM strategy_execution_portfolio_legs
                 WHERE strategy_id = ? ORDER BY leg_index",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut configured_legs = leg_statement
            .query_map(params![strategy_id], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(leg_statement);
        if configured_legs.is_empty() {
            configured_legs.push((
                0,
                contract_json,
                target_quantity,
                if allow_short {
                    short_target_quantity
                } else {
                    0.0
                },
            ));
        }
        let mut legs = Vec::new();
        let mut active_order_details = Vec::new();
        for (leg_index, leg_contract_json, buy_target, sell_target) in configured_legs {
            let leg_contract: crate::ibkr::ContractCandidate =
                serde_json::from_str(&leg_contract_json)?;
            let current_position: f64 = self
                .connection
                .query_row(
                    "SELECT coalesce((SELECT quantity FROM positions_current
                                  WHERE account_id = ? AND conid = ?), 0)",
                    params![account, leg_contract.conid],
                    |row| row.get(0),
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let mut active_statement = self
                .connection
                .prepare(
                    "SELECT o.order_id, o.broker_order_id, o.status
                     FROM orders o JOIN order_intents i
                       ON i.order_intent_id = o.order_intent_id
                     WHERE i.account_id = ? AND i.conid = ?
                       AND lower(o.status) IN ('submitted','presubmitted','pendingsubmit',
                                               'pendingcancel','cancel_pending','apipending')
                     ORDER BY o.created_at",
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let active_orders = active_statement
                .query_map(params![account, leg_contract.conid], |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(|error| AppError::Storage(error.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            drop(active_statement);
            if !active_orders.is_empty() {
                for (order_id, broker_order_id, status) in active_orders {
                    let broker = broker_order_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "尚未分配".into());
                    active_order_details.push(format!(
                        "{}（Conid {}，账户 {}）已有活动订单：Broker Order ID {}，状态 {}，本地订单 UUID {}",
                        leg_contract.symbol,
                        leg_contract.conid,
                        account,
                        broker,
                        status,
                        order_id
                    ));
                }
                continue;
            }
            let target = if signal == "buy" {
                buy_target
            } else {
                sell_target
            };
            let delta = target - current_position;
            let side = if delta > f64::EPSILON {
                Some("buy")
            } else if delta < -f64::EPSILON {
                Some("sell")
            } else {
                None
            };
            if let Some(side) = side {
                legs.push(ClaimedStrategyLeg {
                    leg_index,
                    side: side.into(),
                    quantity: delta.abs(),
                    current_quantity: current_position,
                    target_quantity: target,
                    contract: leg_contract,
                    idempotency_key: format!(
                        "strategy:{strategy_id}:{evaluation_id}:leg:{leg_index}"
                    ),
                });
            }
        }
        let quantity = (active_order_details.is_empty() && !legs.is_empty())
            .then_some(legs.iter().map(|leg| leg.quantity).sum::<f64>());
        let action_id = uuid::Uuid::now_v7();
        let idempotency_key = format!("strategy:{strategy_id}:{evaluation_id}");
        let now = Utc::now();
        let (state, detail) = match quantity {
            Some(_) => ("processing", None),
            None if !active_order_details.is_empty() => (
                "skipped",
                Some(format!(
                    "未提交新订单，以避免同一证券重复或冲突下单。{}。请等待订单结束，或在“订单与成交”页面手动取消。",
                    active_order_details.join("；")
                )),
            ),
            None => (
                "skipped",
                Some("signal requires no position change".to_owned()),
            ),
        };
        self.connection
            .execute(
                "INSERT INTO strategy_execution_actions
                 (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                  requested_quantity, state, order_intent_id, broker_order_id,
                  detail, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)",
                params![
                    action_id,
                    strategy_id,
                    evaluation_id,
                    idempotency_key,
                    signal,
                    quantity,
                    state,
                    detail,
                    now,
                    now
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some(quantity) = quantity else {
            return Ok(None);
        };
        for leg in &legs {
            self.connection
                .execute(
                    "INSERT INTO strategy_execution_action_legs
                     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'processing', NULL, ?, ?)",
                    params![
                        action_id,
                        leg.leg_index,
                        leg.contract.conid,
                        leg.contract.symbol,
                        leg.target_quantity,
                        leg.side,
                        leg.quantity,
                        now,
                        now
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        let first = legs
            .first()
            .cloned()
            .expect("non-empty legs imply claimed quantity");
        Ok(Some(ClaimedStrategyAction {
            action_id,
            strategy_id,
            evaluation_id,
            signal,
            side: first.side.clone(),
            account,
            quantity,
            order_type,
            paper_only,
            outside_rth,
            contract: first.contract.clone(),
            idempotency_key,
            legs,
            signal_edge_bps,
            cost_control,
        }))
    }

    fn record_disabled_strategy_actions(&mut self) -> Result<usize> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.evaluation_id, e.strategy_id, e.signal
                 FROM strategy_evaluations e
                 JOIN strategy_execution_configs c USING (strategy_id)
                 LEFT JOIN strategy_execution_actions a USING (evaluation_id)
                 WHERE c.enabled = false
                   AND e.created_at >= c.updated_at
                   AND e.signal IN ('buy', 'sell')
                   AND a.evaluation_id IS NULL
                 ORDER BY e.created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let evaluations = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, uuid::Uuid>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        if evaluations.is_empty() {
            return Ok(0);
        }
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let now = Utc::now();
        for (evaluation_id, strategy_id, signal) in &evaluations {
            transaction
                .execute(
                    "INSERT INTO strategy_execution_actions
                     (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                      requested_quantity, state, detail, created_at, updated_at,
                      cost_gate_result)
                     VALUES (?, ?, ?, ?, ?, NULL, 'skipped', ?, ?, ?,
                             'execution_disabled')",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        evaluation_id,
                        format!("strategy:{strategy_id}:{evaluation_id}"),
                        signal,
                        "automatic execution skipped: strategy execution is disabled; enable \
                         Paper execution to process future signals",
                        now,
                        now
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(evaluations.len())
    }

    pub fn record_strategy_cost_gate(
        &mut self,
        action_id: uuid::Uuid,
        state: &str,
        estimated_notional: f64,
        estimated_round_trip_cost: f64,
        required_edge_bps: f64,
        signal_edge_bps: Option<f64>,
        detail: &str,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE strategy_execution_actions SET state = ?,
                    estimated_notional = ?, estimated_round_trip_cost = ?,
                    required_edge_bps = ?, signal_edge_bps = ?,
                    cost_gate_result = ?, detail = ?, updated_at = ?
                 WHERE action_id = ?",
                params![
                    state,
                    estimated_notional,
                    estimated_round_trip_cost,
                    required_edge_bps,
                    signal_edge_bps,
                    if state == "processing" {
                        "passed"
                    } else {
                        "blocked"
                    },
                    detail,
                    Utc::now(),
                    action_id
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn finish_strategy_action(
        &mut self,
        action_id: uuid::Uuid,
        state: &str,
        order_intent_id: Option<uuid::Uuid>,
        broker_order_id: Option<i32>,
        detail: Option<&str>,
    ) -> Result<()> {
        if !matches!(state, "submitted" | "rejected" | "failed" | "skipped") {
            return Err(AppError::Storage("invalid strategy action state".into()));
        }
        self.connection
            .execute(
                "UPDATE strategy_execution_actions SET state = ?, order_intent_id = ?,
                broker_order_id = ?, detail = ?, updated_at = ? WHERE action_id = ?",
                params![
                    state,
                    order_intent_id,
                    broker_order_id,
                    detail,
                    Utc::now(),
                    action_id
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn finish_strategy_action_leg(
        &mut self,
        action_id: uuid::Uuid,
        leg_index: i32,
        state: &str,
        order_intent_id: Option<uuid::Uuid>,
        broker_order_id: Option<i32>,
        detail: Option<&str>,
    ) -> Result<()> {
        if !matches!(state, "submitted" | "rejected" | "failed" | "skipped") {
            return Err(AppError::Storage(
                "invalid strategy action leg state".into(),
            ));
        }
        self.connection
            .execute(
                "UPDATE strategy_execution_action_legs
                 SET state = ?, order_intent_id = ?, broker_order_id = ?,
                     detail = ?, updated_at = ?
                 WHERE action_id = ? AND leg_index = ?",
                params![
                    state,
                    order_intent_id,
                    broker_order_id,
                    detail,
                    Utc::now(),
                    action_id,
                    leg_index
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    #[cfg(test)]
    pub fn list_strategy_execution_actions(&self, limit: usize) -> Result<Vec<serde_json::Value>> {
        self.list_strategy_execution_actions_page(1, limit)
            .map(|(rows, _)| rows)
    }

    pub fn list_strategy_execution_actions_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let total: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_actions",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let page_size = page_size.clamp(1, 500);
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        let mut statement = self
            .connection
            .prepare(
                "SELECT action_id, strategy_id, evaluation_id, idempotency_key, signal,
                    requested_quantity, state, order_intent_id, broker_order_id,
                    detail, created_at, updated_at, estimated_notional,
                    estimated_round_trip_cost, required_edge_bps,
                    signal_edge_bps, cost_gate_result
             FROM strategy_execution_actions ORDER BY created_at DESC LIMIT ? OFFSET ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![page_size as i64, offset as i64], |row| {
                Ok(serde_json::json!({
                    "action_id": row.get::<_, uuid::Uuid>(0)?,
                    "strategy_id": row.get::<_, uuid::Uuid>(1)?,
                    "evaluation_id": row.get::<_, uuid::Uuid>(2)?,
                    "idempotency_key": row.get::<_, String>(3)?,
                    "signal": row.get::<_, String>(4)?,
                    "requested_quantity": row.get::<_, Option<f64>>(5)?,
                    "state": row.get::<_, String>(6)?,
                    "order_intent_id": row.get::<_, Option<uuid::Uuid>>(7)?,
                    "broker_order_id": row.get::<_, Option<i64>>(8)?,
                    "detail": row.get::<_, Option<String>>(9)?,
                    "created_at": row.get::<_, DateTime<Utc>>(10)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(11)?,
                    "estimated_notional": row.get::<_, Option<f64>>(12)?,
                    "estimated_round_trip_cost": row.get::<_, Option<f64>>(13)?,
                    "required_edge_bps": row.get::<_, Option<f64>>(14)?,
                    "signal_edge_bps": row.get::<_, Option<f64>>(15)?,
                    "cost_gate_result": row.get::<_, Option<String>>(16)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut actions = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        for action in &mut actions {
            let Some(action_id) = action["action_id"]
                .as_str()
                .and_then(|value| uuid::Uuid::parse_str(value).ok())
            else {
                continue;
            };
            let mut leg_statement = self
                .connection
                .prepare(
                    "SELECT l.leg_index, l.conid, l.symbol, l.target_quantity,
                            l.requested_side, l.requested_quantity, l.order_intent_id,
                            l.broker_order_id, l.state, l.detail, l.created_at, l.updated_at,
                            i.description, i.exchange, i.primary_exchange
                     FROM strategy_execution_action_legs l
                     LEFT JOIN instruments i ON i.conid = l.conid
                     WHERE l.action_id = ? ORDER BY l.leg_index",
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let legs = leg_statement
                .query_map(params![action_id], |row| {
                    Ok(serde_json::json!({
                        "leg_index": row.get::<_, i32>(0)?,
                        "conid": row.get::<_, i32>(1)?,
                        "symbol": row.get::<_, String>(2)?,
                        "target_quantity": row.get::<_, f64>(3)?,
                        "requested_side": row.get::<_, Option<String>>(4)?,
                        "requested_quantity": row.get::<_, Option<f64>>(5)?,
                        "order_intent_id": row.get::<_, Option<uuid::Uuid>>(6)?,
                        "broker_order_id": row.get::<_, Option<i64>>(7)?,
                        "state": row.get::<_, String>(8)?,
                        "detail": row.get::<_, Option<String>>(9)?,
                        "created_at": row.get::<_, DateTime<Utc>>(10)?,
                        "updated_at": row.get::<_, DateTime<Utc>>(11)?,
                        "description": row.get::<_, Option<String>>(12)?,
                        "exchange": row.get::<_, Option<String>>(13)?,
                        "primary_exchange": row.get::<_, Option<String>>(14)?,
                    }))
                })
                .map_err(|error| AppError::Storage(error.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            action["legs"] = serde_json::Value::Array(legs);
        }
        Ok((actions, total.max(0) as usize))
    }

    pub fn list_strategies(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT s.strategy_id, s.name, s.kind, s.state, s.config_json::VARCHAR,
                    s.last_evaluated_bar, s.last_error, s.created_at, s.updated_at,
                    i.conid, i.symbol, i.description, i.exchange, i.primary_exchange,
                    i.security_type, i.currency, i.local_symbol
             FROM strategies s
             LEFT JOIN instruments i
               ON i.conid = try_cast(json_extract_string(s.config_json, '$.conid') AS BIGINT)
             ORDER BY s.created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                let config: String = row.get(4)?;
                Ok(serde_json::json!({
                    "strategy_id": row.get::<_, uuid::Uuid>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "kind": row.get::<_, String>(2)?,
                    "state": row.get::<_, String>(3)?,
                    "config": serde_json::from_str::<serde_json::Value>(&config)
                        .unwrap_or(serde_json::Value::Null),
                    "last_evaluated_bar": row.get::<_, Option<DateTime<Utc>>>(5)?,
                    "last_error": row.get::<_, Option<String>>(6)?,
                    "created_at": row.get::<_, DateTime<Utc>>(7)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(8)?,
                    "conid": row.get::<_, Option<i64>>(9)?,
                    "symbol": row.get::<_, Option<String>>(10)?,
                    "description": row.get::<_, Option<String>>(11)?,
                    "exchange": row.get::<_, Option<String>>(12)?,
                    "primary_exchange": row.get::<_, Option<String>>(13)?,
                    "security_type": row.get::<_, Option<String>>(14)?,
                    "currency": row.get::<_, Option<String>>(15)?,
                    "local_symbol": row.get::<_, Option<String>>(16)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_strategy_evaluations(
        &self,
        strategy_id: uuid::Uuid,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT evaluation_id, conid, bar_time, short_value, long_value,
                    previous_short_value, previous_long_value, signal, created_at,
                    output_json::VARCHAR
             FROM strategy_evaluations WHERE strategy_id = ?
             ORDER BY bar_time DESC LIMIT ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![strategy_id, limit.min(10_000) as i64], |row| {
                let output: Option<String> = row.get(9)?;
                Ok(serde_json::json!({
                    "evaluation_id": row.get::<_, uuid::Uuid>(0)?,
                    "strategy_id": strategy_id,
                    "conid": row.get::<_, i64>(1)?,
                    "bar_time": row.get::<_, DateTime<Utc>>(2)?,
                    "short_value": row.get::<_, f64>(3)?,
                    "long_value": row.get::<_, f64>(4)?,
                    "previous_short_value": row.get::<_, f64>(5)?,
                    "previous_long_value": row.get::<_, f64>(6)?,
                    "signal": row.get::<_, String>(7)?,
                    "created_at": row.get::<_, DateTime<Utc>>(8)?,
                    "output": output.and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn evaluate_running_strategies(&mut self) -> Result<usize> {
        let paused: bool = self
            .connection
            .query_row(
                "SELECT pause_strategies OR emergency_stop
                 FROM trading_control WHERE singleton",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if paused {
            return Ok(0);
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT strategy_id, kind, config_json::VARCHAR, last_evaluated_bar
             FROM strategies WHERE state = 'running' ORDER BY created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<DateTime<Utc>>>(3)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        let mut evaluated = 0;
        for (strategy_id, kind, config_json, last_evaluated) in rows {
            // One broken strategy must not stop evaluation of the others: record
            // the failure on that strategy and continue with the remaining ones.
            match self.evaluate_single_strategy(strategy_id, &kind, &config_json, last_evaluated) {
                Ok(true) => evaluated += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::error!(%error, %strategy_id, "strategy evaluation failed");
                    let _ = self.connection.execute(
                        "UPDATE strategies SET last_error = ?, updated_at = ?
                         WHERE strategy_id = ?",
                        params![error.to_string(), Utc::now(), strategy_id],
                    );
                }
            }
        }
        Ok(evaluated)
    }

    fn evaluate_single_strategy(
        &mut self,
        strategy_id: uuid::Uuid,
        kind: &str,
        config_json: &str,
        last_evaluated: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let config: serde_json::Value = serde_json::from_str(config_json)?;
        let strategy = crate::strategy::build(kind, config).map_err(AppError::Storage)?;
        let bar_table = match strategy.bar_timeframe() {
            "1m" => "market_minute_bars",
            "5s" => "market_five_second_bars",
            timeframe => {
                return Err(AppError::Storage(format!(
                    "strategy {} requests unsupported live Bar timeframe {timeframe}",
                    strategy.kind()
                )));
            }
        };
        let mut bars = self
            .connection
            .prepare(&format!(
                "SELECT bar_time, open, high, low, close, tick_count
                     FROM {bar_table}
                 WHERE conid = ? AND final = true
                 ORDER BY bar_time DESC LIMIT ?"
            ))
            .map_err(|error| AppError::Storage(error.to_string()))?
            .query_map(
                params![strategy.conid(), strategy.minimum_history() as i64],
                |row| {
                    Ok(crate::strategy::StrategyBar {
                        time: row.get(0)?,
                        open: row.get(1)?,
                        high: row.get(2)?,
                        low: row.get(3)?,
                        close: row.get(4)?,
                        volume: row.get::<_, i64>(5)? as f64,
                    })
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if bars.len() < strategy.minimum_history() {
            return Ok(false);
        }
        bars.reverse();
        let bar_time = bars.last().expect("bars non-empty").time;
        if last_evaluated.is_some_and(|time| time >= bar_time) {
            return Ok(false);
        }
        let (stored_state_version, state_json, state_revision) = self
            .connection
            .query_row(
                "SELECT state_version, state_json::VARCHAR, revision
                 FROM strategy_runtime_states WHERE strategy_id = ?",
                params![strategy_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| {
                AppError::Storage(format!(
                    "persisted runtime state is missing for strategy {strategy_id}"
                ))
            })?;
        let expected_state_version = i64::from(strategy.state_version());
        if stored_state_version != expected_state_version {
            return Err(AppError::Storage(format!(
                "strategy {strategy_id} runtime state version {stored_state_version} does not \
                 match engine version {expected_state_version}; migrate or reset the state before \
                 resuming"
            )));
        }
        let current_state: serde_json::Value = serde_json::from_str(&state_json)?;
        let transition = strategy
            .evaluate_with_state(&bars, &current_state)
            .map_err(AppError::Storage)?;
        let next_state = serialize_strategy_state(&transition.next_state)?;
        let output = transition.output;
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let inserted = transaction
            .execute(
                "INSERT INTO strategy_evaluations
                     (evaluation_id, strategy_id, conid, bar_time, short_value, long_value,
                      previous_short_value, previous_long_value, signal, created_at, output_json)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (strategy_id, bar_time) DO NOTHING",
                params![
                    uuid::Uuid::now_v7(),
                    strategy_id,
                    strategy.conid(),
                    bar_time,
                    output.indicator_a,
                    output.indicator_b,
                    output.previous_indicator_a,
                    output.previous_indicator_b,
                    output.signal.as_str(),
                    now,
                    serde_json::to_string(&output.details)?
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if inserted == 0 {
            transaction
                .commit()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Ok(false);
        }
        transaction
            .execute(
                "UPDATE strategies SET last_evaluated_bar = ?, last_error = NULL, updated_at = ?
                 WHERE strategy_id = ?",
                params![bar_time, now, strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let state_updated = transaction
            .execute(
                "UPDATE strategy_runtime_states
                 SET state_json = ?, revision = revision + 1,
                     last_transition_bar = ?, updated_at = ?
                 WHERE strategy_id = ? AND state_version = ? AND revision = ?",
                params![
                    next_state,
                    bar_time,
                    now,
                    strategy_id,
                    expected_state_version,
                    state_revision
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if state_updated != 1 {
            return Err(AppError::Storage(format!(
                "strategy {strategy_id} runtime state changed concurrently; evaluation was rolled back"
            )));
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(true)
    }

    pub fn trading_control(&self) -> Result<serde_json::Value> {
        self.connection
            .query_row(
                "SELECT reject_new_orders, pause_strategies, emergency_stop,
                        live_approved, live_conid_whitelist_json::VARCHAR,
                        operator_note, updated_at
                 FROM trading_control WHERE singleton",
                [],
                |row| {
                    let whitelist: String = row.get(4)?;
                    Ok(serde_json::json!({
                        "reject_new_orders": row.get::<_, bool>(0)?,
                        "pause_strategies": row.get::<_, bool>(1)?,
                        "emergency_stop": row.get::<_, bool>(2)?,
                        "live_approved": row.get::<_, bool>(3)?,
                        "live_conid_whitelist": serde_json::from_str::<serde_json::Value>(&whitelist)
                            .unwrap_or_else(|_| serde_json::json!([])),
                        "operator_note": row.get::<_, Option<String>>(5)?,
                        "updated_at": row.get::<_, DateTime<Utc>>(6)?
                    }))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn set_trading_control(&mut self, mode: &str, note: &str) -> Result<serde_json::Value> {
        let (reject, pause, emergency) = match mode {
            "normal" => (false, false, false),
            "reject_new_orders" => (true, false, false),
            "pause_strategies" => (false, true, false),
            "emergency_stop" => (true, true, true),
            _ => return Err(AppError::Storage("invalid trading control mode".into())),
        };
        self.connection
            .execute(
                "UPDATE trading_control SET reject_new_orders = ?, pause_strategies = ?,
                emergency_stop = ?, operator_note = ?, updated_at = ? WHERE singleton",
                params![reject, pause, emergency, note, Utc::now()],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.trading_control()
    }

    pub fn approve_live_trading(
        &mut self,
        conids: &[i32],
        note: &str,
    ) -> Result<serde_json::Value> {
        if conids.is_empty() || conids.iter().any(|conid| *conid <= 0) || note.trim().is_empty() {
            return Err(AppError::Storage(
                "live approval requires a non-empty conid whitelist and operator note".into(),
            ));
        }
        self.connection
            .execute(
                "UPDATE trading_control SET live_approved = true,
                live_conid_whitelist_json = ?, operator_note = ?, updated_at = ?
             WHERE singleton",
                params![serde_json::to_string(conids)?, note.trim(), Utc::now()],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.trading_control()
    }

    pub fn revoke_live_trading(&mut self, note: &str) -> Result<serde_json::Value> {
        self.connection
            .execute(
                "UPDATE trading_control SET live_approved = false,
                reject_new_orders = true, pause_strategies = true,
                operator_note = ?, updated_at = ? WHERE singleton",
                params![note, Utc::now()],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.trading_control()
    }

    fn upsert_position(&mut self, position: &crate::ibkr::PositionSnapshot) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO instruments
                 (instrument_id, conid, symbol, security_type, currency, exchange,
                  created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (conid) DO UPDATE SET
                   symbol = excluded.symbol,
                   security_type = excluded.security_type,
                   currency = excluded.currency,
                   exchange = excluded.exchange,
                   updated_at = excluded.updated_at",
                params![
                    uuid::Uuid::now_v7(),
                    position.conid,
                    position.symbol,
                    position.security_type,
                    position.currency,
                    position.exchange,
                    position.observed_at,
                    position.observed_at
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.connection
            .execute(
                "INSERT INTO positions_current
                 (account_id, conid, quantity, average_cost, observed_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (account_id, conid) DO UPDATE SET
                   quantity = excluded.quantity,
                   average_cost = excluded.average_cost,
                   observed_at = excluded.observed_at",
                params![
                    position.account,
                    position.conid,
                    position.quantity,
                    position.average_cost,
                    position.observed_at
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.connection
            .execute(
                "INSERT INTO position_history VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    uuid::Uuid::now_v7(),
                    position.account,
                    position.conid,
                    position.quantity,
                    position.average_cost,
                    position.observed_at
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn upsert_instrument(
        &mut self,
        contract: &crate::ibkr::ContractCandidate,
    ) -> Result<uuid::Uuid> {
        if contract.conid <= 0 {
            return Err(AppError::Storage(
                "instrument conid must be positive".into(),
            ));
        }
        let now = Utc::now();
        let instrument_id = self
            .connection
            .query_row(
                "INSERT INTO instruments
             (instrument_id, conid, symbol, security_type, currency, exchange,
              created_at, updated_at, primary_exchange, local_symbol, description)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (conid) DO UPDATE SET
               symbol = excluded.symbol, security_type = excluded.security_type,
               currency = excluded.currency, exchange = excluded.exchange,
               primary_exchange = excluded.primary_exchange,
               local_symbol = excluded.local_symbol,
               description = excluded.description, updated_at = excluded.updated_at
             RETURNING instrument_id",
                params![
                    uuid::Uuid::now_v7(),
                    contract.conid,
                    contract.symbol,
                    contract.security_type,
                    contract.currency,
                    contract.exchange,
                    now,
                    now,
                    contract.primary_exchange,
                    contract.local_symbol,
                    contract.description
                ],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(instrument_id)
    }

    pub fn list_instruments(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT instrument_id, conid, symbol, security_type, currency, exchange,
                    primary_exchange, local_symbol, description, created_at, updated_at
             FROM instruments ORDER BY symbol, conid",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "instrument_id": row.get::<_, uuid::Uuid>(0)?,
                    "conid": row.get::<_, i64>(1)?,
                    "symbol": row.get::<_, String>(2)?,
                    "security_type": row.get::<_, String>(3)?,
                    "currency": row.get::<_, String>(4)?,
                    "exchange": row.get::<_, String>(5)?,
                    "primary_exchange": row.get::<_, Option<String>>(6)?,
                    "local_symbol": row.get::<_, Option<String>>(7)?,
                    "description": row.get::<_, Option<String>>(8)?,
                    "created_at": row.get::<_, DateTime<Utc>>(9)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(10)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_positions(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT p.account_id, p.conid, i.symbol, i.security_type, i.currency,
                        i.exchange, p.quantity, p.average_cost, p.observed_at,
                        i.description, i.primary_exchange
                 FROM positions_current p
                 LEFT JOIN instruments i USING (conid)
                 ORDER BY p.account_id, p.conid",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "account": row.get::<_, String>(0)?,
                    "conid": row.get::<_, i64>(1)?,
                    "symbol": row.get::<_, Option<String>>(2)?,
                    "security_type": row.get::<_, Option<String>>(3)?,
                    "currency": row.get::<_, Option<String>>(4)?,
                    "exchange": row.get::<_, Option<String>>(5)?,
                    "quantity": row.get::<_, f64>(6)?,
                    "average_cost": row.get::<_, f64>(7)?,
                    "observed_at": row.get::<_, DateTime<Utc>>(8)?,
                    "description": row.get::<_, Option<String>>(9)?,
                    "primary_exchange": row.get::<_, Option<String>>(10)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_account_summary(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT account_id, tag, value, currency, observed_at
                 FROM account_summary_current ORDER BY account_id, tag, currency",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "account": row.get::<_, String>(0)?,
                    "tag": row.get::<_, String>(1)?,
                    "value": row.get::<_, String>(2)?,
                    "currency": row.get::<_, String>(3)?,
                    "observed_at": row.get::<_, DateTime<Utc>>(4)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_account_pnl(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT account_id, daily_pnl, unrealized_pnl, realized_pnl, observed_at
                 FROM account_pnl_current ORDER BY account_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "account": row.get::<_, String>(0)?,
                    "daily_pnl": row.get::<_, f64>(1)?,
                    "unrealized_pnl": row.get::<_, Option<f64>>(2)?,
                    "realized_pnl": row.get::<_, Option<f64>>(3)?,
                    "observed_at": row.get::<_, DateTime<Utc>>(4)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn evaluate_close_only(
        &self,
        account: &str,
        conid: i32,
        side: &str,
        quantity: f64,
        session_connected_at: DateTime<Utc>,
    ) -> Result<CloseOnlyDecision> {
        let sync_state: String = self
            .connection
            .query_row(
                "SELECT state FROM position_sync_state WHERE singleton",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if sync_state != "ready" {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: None,
                maximum_closing_quantity: 0.0,
                reason: "IBKR position snapshot is still synchronizing".into(),
                position_observed_at: None,
            });
        }
        let position = self
            .connection
            .query_row(
                "SELECT quantity, observed_at FROM positions_current
                 WHERE account_id = ? AND conid = ?",
                params![account, conid],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, DateTime<Utc>>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some((current_quantity, observed_at)) = position else {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: None,
                maximum_closing_quantity: 0.0,
                reason: "no local position snapshot exists".into(),
                position_observed_at: None,
            });
        };
        if observed_at < session_connected_at {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: Some(current_quantity),
                maximum_closing_quantity: 0.0,
                reason: "position snapshot predates the active IBKR session".into(),
                position_observed_at: Some(observed_at),
            });
        }
        let maximum_closing_quantity = current_quantity.abs();
        let closing_direction = match side {
            "sell" => current_quantity > 0.0,
            "buy" => current_quantity < 0.0,
            _ => false,
        };
        let allowed = closing_direction
            && quantity.is_finite()
            && quantity > 0.0
            && quantity <= maximum_closing_quantity;
        let reason = if !closing_direction {
            "order side does not reduce the current position"
        } else if quantity > maximum_closing_quantity {
            "order quantity would cross through flat and open a reverse position"
        } else if !quantity.is_finite() || quantity <= 0.0 {
            "order quantity must be positive and finite"
        } else {
            "order strictly reduces the current position"
        };
        Ok(CloseOnlyDecision {
            allowed,
            current_quantity: Some(current_quantity),
            maximum_closing_quantity,
            reason: reason.into(),
            position_observed_at: Some(observed_at),
        })
    }

    pub fn acknowledge_reconciliation_difference(
        &mut self,
        difference_id: uuid::Uuid,
        note: &str,
    ) -> Result<()> {
        if note.trim().is_empty() {
            return Err(AppError::Storage(
                "difference acknowledgement note cannot be empty".into(),
            ));
        }
        let changed = self
            .connection
            .execute(
                "UPDATE reconciliation_differences
                 SET disposition = 'acknowledged', disposition_note = ?,
                     disposition_at = ?
                 WHERE difference_id = ? AND disposition = 'open'",
                params![note.trim(), Utc::now(), difference_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if changed == 0 {
            return Err(AppError::Storage(
                "difference does not exist or is already disposed".into(),
            ));
        }
        Ok(())
    }

    pub fn upsert_execution_cost_model(
        &mut self,
        input: &ExecutionCostModelInput,
    ) -> Result<uuid::Uuid> {
        let values = [
            input.buy_fixed_fee,
            input.buy_per_share_fee,
            input.buy_rate_bps,
            input.buy_min_fee,
            input.sell_fixed_fee,
            input.sell_per_share_fee,
            input.sell_rate_bps,
            input.sell_min_fee,
            input.sell_tax_bps,
            input.estimated_spread_bps,
            input.estimated_slippage_bps,
        ];
        if input.name.trim().is_empty()
            || input.currency.trim().is_empty()
            || values
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(AppError::Storage(
                "cost model requires a name, currency, and finite non-negative fees".into(),
            ));
        }
        let id = input.cost_model_id.unwrap_or_else(uuid::Uuid::now_v7);
        let now = Utc::now();
        self.connection
            .execute(
                "INSERT INTO execution_cost_models
                 (cost_model_id, name, currency, buy_fixed_fee, buy_rate_bps,
                  buy_min_fee, sell_fixed_fee, sell_rate_bps, sell_min_fee,
                  sell_tax_bps, estimated_spread_bps, estimated_slippage_bps,
                  created_at, updated_at, buy_per_share_fee, sell_per_share_fee)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (cost_model_id) DO UPDATE SET
                   name = excluded.name, currency = excluded.currency,
                   buy_fixed_fee = excluded.buy_fixed_fee,
                   buy_per_share_fee = excluded.buy_per_share_fee,
                   buy_rate_bps = excluded.buy_rate_bps,
                   buy_min_fee = excluded.buy_min_fee,
                   sell_fixed_fee = excluded.sell_fixed_fee,
                   sell_per_share_fee = excluded.sell_per_share_fee,
                   sell_rate_bps = excluded.sell_rate_bps,
                   sell_min_fee = excluded.sell_min_fee,
                   sell_tax_bps = excluded.sell_tax_bps,
                   estimated_spread_bps = excluded.estimated_spread_bps,
                   estimated_slippage_bps = excluded.estimated_slippage_bps,
                   updated_at = excluded.updated_at",
                params![
                    id,
                    input.name.trim(),
                    input.currency.trim().to_ascii_uppercase(),
                    input.buy_fixed_fee,
                    input.buy_rate_bps,
                    input.buy_min_fee,
                    input.sell_fixed_fee,
                    input.sell_rate_bps,
                    input.sell_min_fee,
                    input.sell_tax_bps,
                    input.estimated_spread_bps,
                    input.estimated_slippage_bps,
                    now,
                    now,
                    input.buy_per_share_fee,
                    input.sell_per_share_fee
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(id)
    }

    pub fn list_execution_cost_models(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT cost_model_id, name, currency, buy_fixed_fee,
                        buy_per_share_fee, buy_rate_bps,
                        buy_min_fee, sell_fixed_fee, sell_rate_bps, sell_min_fee,
                        sell_per_share_fee, sell_tax_bps,
                        estimated_spread_bps, estimated_slippage_bps,
                        created_at, updated_at
                 FROM execution_cost_models ORDER BY name",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "cost_model_id": row.get::<_, uuid::Uuid>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "currency": row.get::<_, String>(2)?,
                    "buy_fixed_fee": row.get::<_, f64>(3)?,
                    "buy_per_share_fee": row.get::<_, f64>(4)?,
                    "buy_rate_bps": row.get::<_, f64>(5)?,
                    "buy_min_fee": row.get::<_, f64>(6)?,
                    "sell_fixed_fee": row.get::<_, f64>(7)?,
                    "sell_rate_bps": row.get::<_, f64>(8)?,
                    "sell_min_fee": row.get::<_, f64>(9)?,
                    "sell_per_share_fee": row.get::<_, f64>(10)?,
                    "sell_tax_bps": row.get::<_, f64>(11)?,
                    "estimated_spread_bps": row.get::<_, f64>(12)?,
                    "estimated_slippage_bps": row.get::<_, f64>(13)?,
                    "created_at": row.get::<_, DateTime<Utc>>(14)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(15)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn delete_execution_cost_model(&mut self, id: uuid::Uuid) -> Result<bool> {
        let used: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_cost_controls WHERE cost_model_id = ?",
                params![id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if used > 0 {
            return Err(AppError::Storage(
                "cost model is assigned to a strategy and cannot be deleted".into(),
            ));
        }
        self.connection
            .execute(
                "DELETE FROM execution_cost_models WHERE cost_model_id = ?",
                params![id],
            )
            .map(|count| count > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn configure_strategy_cost_control(
        &mut self,
        input: &StrategyCostControlInput,
    ) -> Result<()> {
        if !input.minimum_cost_multiple.is_finite()
            || input.minimum_cost_multiple < 1.0
            || !input.maximum_commission_to_gross_profit_ratio.is_finite()
            || input.maximum_commission_to_gross_profit_ratio <= 0.0
        {
            return Err(AppError::Storage(
                "cost control requires minimum_cost_multiple >= 1 and a positive ratio".into(),
            ));
        }
        let references: i64 = self
            .connection
            .query_row(
                "SELECT
                    (SELECT count(*) FROM strategies WHERE strategy_id = ?)
                  + (SELECT count(*) FROM execution_cost_models WHERE cost_model_id = ?)",
                params![input.strategy_id, input.cost_model_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if references != 2 {
            return Err(AppError::Storage(
                "strategy and execution cost model must both exist".into(),
            ));
        }
        let now = Utc::now();
        self.connection
            .execute(
                "INSERT INTO strategy_cost_controls VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (strategy_id) DO UPDATE SET
                   enabled = excluded.enabled, cost_model_id = excluded.cost_model_id,
                   minimum_cost_multiple = excluded.minimum_cost_multiple,
                   maximum_commission_to_gross_profit_ratio =
                       excluded.maximum_commission_to_gross_profit_ratio,
                   minimum_completed_trades = excluded.minimum_completed_trades,
                   updated_at = excluded.updated_at",
                params![
                    input.strategy_id,
                    input.enabled,
                    input.cost_model_id,
                    input.minimum_cost_multiple,
                    input.maximum_commission_to_gross_profit_ratio,
                    input.minimum_completed_trades as i64,
                    now,
                    now
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_strategy_cost_controls(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT c.strategy_id, s.name, c.enabled, c.cost_model_id, m.name,
                        c.minimum_cost_multiple,
                        c.maximum_commission_to_gross_profit_ratio,
                        c.minimum_completed_trades, c.updated_at
                 FROM strategy_cost_controls c
                 JOIN strategies s USING (strategy_id)
                 JOIN execution_cost_models m USING (cost_model_id)
                 ORDER BY s.name",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "strategy_id": row.get::<_, uuid::Uuid>(0)?,
                    "strategy_name": row.get::<_, String>(1)?,
                    "enabled": row.get::<_, bool>(2)?,
                    "cost_model_id": row.get::<_, uuid::Uuid>(3)?,
                    "cost_model_name": row.get::<_, String>(4)?,
                    "minimum_cost_multiple": row.get::<_, f64>(5)?,
                    "maximum_commission_to_gross_profit_ratio": row.get::<_, f64>(6)?,
                    "minimum_completed_trades": row.get::<_, i64>(7)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(8)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn add_market_data_subscription(
        &mut self,
        contract: &crate::ibkr::ContractCandidate,
    ) -> Result<()> {
        let mut contract = contract.clone();
        contract.normalize_streaming_subscription();
        contract
            .validate_streaming_subscription()
            .map_err(AppError::Storage)?;
        self.upsert_instrument(&contract)?;
        self.connection
            .execute(
                "INSERT INTO market_data_subscriptions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (conid) DO UPDATE SET
                   symbol = excluded.symbol,
                   security_type = excluded.security_type,
                   currency = excluded.currency,
                   exchange = excluded.exchange,
                   primary_exchange = excluded.primary_exchange,
                   local_symbol = excluded.local_symbol,
                   description = excluded.description",
                params![
                    contract.conid,
                    contract.symbol,
                    contract.security_type,
                    contract.currency,
                    contract.exchange,
                    contract.primary_exchange,
                    contract.local_symbol,
                    contract.description,
                    Utc::now()
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn remove_market_data_subscription(&mut self, conid: i32) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM market_data_subscriptions WHERE conid = ?",
                params![conid],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM market_data_status WHERE conid = ?",
                params![conid],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn market_data_subscriptions(&self) -> Result<Vec<crate::ibkr::ContractCandidate>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT conid, symbol, security_type, currency, exchange,
                        primary_exchange, local_symbol, description
                 FROM market_data_subscriptions ORDER BY symbol, conid",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(crate::ibkr::ContractCandidate {
                    conid: row.get(0)?,
                    symbol: row.get(1)?,
                    security_type: row.get(2)?,
                    currency: row.get(3)?,
                    exchange: row.get(4)?,
                    primary_exchange: row.get(5)?,
                    local_symbol: row.get(6)?,
                    description: row.get(7)?,
                    derivative_security_types: Vec::new(),
                })
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut contracts = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for contract in &mut contracts {
            contract.normalize_streaming_subscription();
        }
        Ok(contracts)
    }

    pub fn latest_quote(&self, conid: i32) -> Result<serde_json::Value> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT tick_type, numeric_value, text_value, observed_at
                 FROM market_ticks_current WHERE conid = ? ORDER BY tick_type",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![conid], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, DateTime<Utc>>(3)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut ticks = serde_json::Map::new();
        let mut latest_observed_at: Option<DateTime<Utc>> = None;
        for (tick_type, numeric_value, text_value, observed_at) in rows {
            latest_observed_at =
                Some(latest_observed_at.map_or(observed_at, |latest| latest.max(observed_at)));
            ticks.insert(
                tick_type,
                serde_json::json!({
                    "numeric_value": numeric_value,
                    "text_value": text_value,
                    "observed_at": observed_at
                }),
            );
        }
        let status = self
            .connection
            .query_row(
                "SELECT state, last_error, observed_at
                 FROM market_data_status WHERE conid = ?",
                params![conid],
                |row| {
                    Ok(serde_json::json!({
                        "state": row.get::<_, String>(0)?,
                        "last_error": row.get::<_, Option<String>>(1)?,
                        "observed_at": row.get::<_, DateTime<Utc>>(2)?
                    }))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(serde_json::json!({
            "conid": conid,
            "subscription_status": status,
            "latest_observed_at": latest_observed_at,
            "ticks": ticks
        }))
    }

    pub fn market_data_health(
        &self,
        conid: i32,
        maximum_age_seconds: u64,
        now: DateTime<Utc>,
    ) -> Result<MarketDataHealth> {
        let subscription = self
            .connection
            .query_row(
                "SELECT state FROM market_data_status WHERE conid = ?",
                params![conid],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let price = self
            .connection
            .query_row(
                "SELECT tick_type, numeric_value, observed_at
                 FROM market_ticks_current
                 WHERE conid = ?
                   AND tick_type IN ('Bid', 'Ask', 'Last', 'DelayedBid',
                                     'DelayedAsk', 'DelayedLast', 'LastRthTrade')
                   AND numeric_value IS NOT NULL AND numeric_value > 0
                 ORDER BY
                   (observed_at >= ?) DESC,
                   CASE tick_type
                     WHEN 'Bid' THEN 0
                     WHEN 'Ask' THEN 1
                     WHEN 'Last' THEN 2
                     WHEN 'LastRthTrade' THEN 3
                     WHEN 'DelayedBid' THEN 4
                     WHEN 'DelayedAsk' THEN 5
                     WHEN 'DelayedLast' THEN 6
                     ELSE 7
                   END,
                   observed_at DESC
                 LIMIT 1",
                params![
                    conid,
                    now - chrono::Duration::seconds(maximum_age_seconds as i64)
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, DateTime<Utc>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let (price_type, latest_price, observed_at) = match price {
            Some((price_type, price, observed_at)) => {
                (Some(price_type), Some(price), Some(observed_at))
            }
            None => (None, None, None),
        };
        let age_seconds = observed_at.map(|time| (now - time).num_seconds().max(0));
        let delayed = price_type
            .as_deref()
            .is_some_and(|tick_type| tick_type.starts_with("Delayed"));
        let state = if delayed {
            "delayed"
        } else if subscription.as_deref() == Some("active")
            && age_seconds.is_some_and(|age| age <= maximum_age_seconds as i64)
        {
            "fresh"
        } else if observed_at.is_some() {
            "stale"
        } else {
            "missing"
        };
        Ok(MarketDataHealth {
            state,
            conid,
            subscription_state: subscription,
            latest_price,
            latest_price_type: price_type,
            observed_at,
            age_seconds,
            maximum_age_seconds,
        })
    }

    pub fn evaluate_portfolio_risk(
        &self,
        config: &crate::config::RiskConfig,
        account: &str,
        request: &crate::ibkr::BrokerOrderRequest,
        estimated_price: Option<f64>,
        market_price: Option<f64>,
        close_only: bool,
        now: DateTime<Utc>,
    ) -> Result<PortfolioRiskDecision> {
        let (position_sync_state, position_snapshot_observed_at) = self
            .connection
            .query_row(
                "SELECT state, observed_at FROM position_sync_state WHERE singleton",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<DateTime<Utc>>>(1)?,
                    ))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let (current_position, current_average_cost) = self
            .connection
            .query_row(
                "SELECT quantity, average_cost FROM positions_current
                 WHERE account_id = ? AND conid = ?",
                params![account, request.contract.conid],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, f64>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .unwrap_or((0.0, 0.0));
        let side = request.side.to_ascii_lowercase();
        let signed_quantity = match side.as_str() {
            "buy" => request.quantity,
            "sell" => -request.quantity,
            _ => 0.0,
        };
        let projected_position = current_position + signed_quantity;
        let mut position_statement = self
            .connection
            .prepare(
                "SELECT p.quantity, p.average_cost,
                        coalesce(i.currency, ?)
                 FROM positions_current p
                 LEFT JOIN instruments i ON i.conid = p.conid
                 WHERE p.account_id = ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let position_rows = position_statement
            .query_map(params![config.base_currency, account], |row| {
                Ok((
                    row.get::<_, f64>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(position_statement);
        let mut gross_exposure = 0.0;
        let mut net_exposure = 0.0;
        for (quantity, average_cost, currency) in position_rows {
            let rate = self
                .currency_conversion_rate(
                    &currency,
                    &config.base_currency,
                    config.max_fx_rate_age_seconds,
                    now,
                )?
                .unwrap_or(0.0);
            let exposure = quantity * average_cost * rate;
            gross_exposure += exposure.abs();
            net_exposure += exposure;
        }
        let request_fx_rate = self
            .currency_conversion_rate(
                &request.contract.currency,
                &config.base_currency,
                config.max_fx_rate_age_seconds,
                now,
            )?
            .unwrap_or(0.0);
        let latest_position_observed_at = self
            .connection
            .query_row(
                "SELECT max(observed_at) FROM positions_current WHERE account_id = ?",
                params![account],
                |row| row.get::<_, Option<DateTime<Utc>>>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // A completed IBKR snapshot is authoritative even when the account is flat and
        // therefore contains no position rows. Position updates received after the
        // snapshot can advance freshness further.
        let positions_observed_at =
            match (position_snapshot_observed_at, latest_position_observed_at) {
                (Some(snapshot), Some(position)) => Some(snapshot.max(position)),
                (snapshot, position) => snapshot.or(position),
            };
        let reference_price = market_price
            .or(request.limit_price)
            .or(estimated_price)
            .unwrap_or(0.0);
        let current_contribution = current_position * current_average_cost * request_fx_rate;
        let projected_contribution = projected_position * reference_price * request_fx_rate;
        let projected_gross_exposure =
            (gross_exposure - current_contribution.abs() + projected_contribution.abs()).max(0.0);
        let projected_net_exposure = net_exposure - current_contribution + projected_contribution;
        let active_order_count: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM orders o
                 JOIN order_intents i USING (order_intent_id)
                 WHERE i.account_id = ?
                   AND lower(o.status) IN
                       ('submitted', 'presubmitted', 'pending_submit',
                        'pendingsubmit', 'pending_cancel', 'pendingcancel',
                        'cancel_pending')",
                params![account],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let recent_cutoff = now - chrono::Duration::minutes(1);
        let recent_order_count: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM order_intents
                 WHERE account_id = ? AND created_at >= ?",
                params![account, recent_cutoff],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let pnl_snapshot = self
            .connection
            .query_row(
                "SELECT daily_pnl, observed_at FROM account_pnl_current WHERE account_id = ?",
                params![account],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, DateTime<Utc>>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let daily_pnl_observed_at = pnl_snapshot.map(|(_, observed_at)| observed_at);
        let daily_pnl = pnl_snapshot
            .filter(|(_, observed_at)| {
                (now - *observed_at).num_seconds().max(0)
                    <= config.max_account_data_age_seconds as i64
            })
            .map(|(pnl, _)| pnl);
        let order_price = request.limit_price.or(estimated_price);
        let price_deviation_bps = order_price.zip(market_price).map(|(order, market)| {
            if market > 0.0 {
                ((order - market).abs() / market) * 10_000.0
            } else {
                f64::INFINITY
            }
        });
        let decision = PortfolioRiskDecision {
            allowed: true,
            reason_code: "ALLOWED",
            detail: "all configured portfolio risk checks passed".into(),
            current_position,
            positions_observed_at,
            projected_position,
            projected_gross_exposure,
            projected_net_exposure,
            active_order_count: active_order_count as usize,
            recent_order_count: recent_order_count as usize,
            daily_pnl,
            daily_pnl_observed_at,
            price_deviation_bps,
        };
        if !close_only && position_sync_state != "ready" {
            return Ok(portfolio_reject(
                decision,
                "POSITION_SYNC_INCOMPLETE",
                "IBKR position snapshot is still synchronizing".into(),
            ));
        }
        if !close_only
            && !positions_observed_at.is_some_and(|observed_at| {
                (now - observed_at).num_seconds().max(0)
                    <= config.max_account_data_age_seconds as i64
            })
        {
            return Ok(portfolio_reject(
                decision,
                "POSITION_DATA_UNAVAILABLE",
                "position data is missing or stale; opening risk is blocked".into(),
            ));
        }
        if !close_only && projected_position.abs() > config.max_position_quantity {
            return Ok(portfolio_reject(
                decision,
                "MAX_POSITION_QUANTITY",
                format!(
                    "projected position {} exceeds maximum absolute quantity {}",
                    projected_position, config.max_position_quantity
                ),
            ));
        }
        if !close_only && projected_gross_exposure > config.max_gross_exposure {
            return Ok(portfolio_reject(
                decision,
                "MAX_GROSS_EXPOSURE",
                format!(
                    "projected gross exposure {} exceeds maximum {}",
                    projected_gross_exposure, config.max_gross_exposure
                ),
            ));
        }
        if !close_only && projected_net_exposure.abs() > config.max_net_exposure {
            return Ok(portfolio_reject(
                decision,
                "MAX_NET_EXPOSURE",
                format!(
                    "projected net exposure {} exceeds maximum absolute value {}",
                    projected_net_exposure, config.max_net_exposure
                ),
            ));
        }
        if !close_only && active_order_count as usize >= config.max_open_orders {
            return Ok(portfolio_reject(
                decision,
                "MAX_OPEN_ORDERS",
                format!("active order count reached {}", config.max_open_orders),
            ));
        }
        if recent_order_count as usize >= config.max_orders_per_minute {
            return Ok(portfolio_reject(
                decision,
                "ORDER_RATE_LIMIT",
                format!(
                    "orders in the last minute reached {}",
                    config.max_orders_per_minute
                ),
            ));
        }
        if !close_only && daily_pnl.is_none() {
            return Ok(portfolio_reject(
                decision,
                "DAILY_PNL_UNAVAILABLE",
                "account PnL is unavailable; opening risk is blocked".into(),
            ));
        }
        if !close_only && daily_pnl.is_some_and(|pnl| pnl <= -config.max_daily_loss) {
            return Ok(portfolio_reject(
                decision,
                "MAX_DAILY_LOSS",
                format!("daily PnL reached the loss limit {}", config.max_daily_loss),
            ));
        }
        if !close_only
            && price_deviation_bps
                .is_some_and(|deviation| deviation > config.max_price_deviation_bps)
        {
            return Ok(portfolio_reject(
                decision,
                "MAX_PRICE_DEVIATION",
                format!(
                    "order price deviation exceeds {} bps",
                    config.max_price_deviation_bps
                ),
            ));
        }
        Ok(decision)
    }

    pub fn list_market_bars(
        &self,
        conid: i32,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let table = match timeframe {
            "1m" => "market_minute_bars",
            "5s" => "market_five_second_bars",
            _ => {
                return Err(AppError::Storage(format!(
                    "unsupported live Bar timeframe: {timeframe}; expected 1m or 5s"
                )));
            }
        };
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT bar_time, open, high, low, close, tick_count, final, updated_at
                 FROM {table} WHERE conid = ?
                 ORDER BY bar_time DESC LIMIT ?"
            ))
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![conid, limit.min(10_000) as i64], |row| {
                Ok(serde_json::json!({
                    "conid": conid,
                    "timeframe": timeframe,
                    "bar_time": row.get::<_, DateTime<Utc>>(0)?,
                    "open": row.get::<_, f64>(1)?,
                    "high": row.get::<_, f64>(2)?,
                    "low": row.get::<_, f64>(3)?,
                    "close": row.get::<_, f64>(4)?,
                    "tick_count": row.get::<_, i64>(5)?,
                    "final": row.get::<_, bool>(6)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(7)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn update_market_minute_bar(
        &mut self,
        conid: i32,
        price: f64,
        observed_at: DateTime<Utc>,
    ) -> Result<()> {
        self.update_market_bar("market_minute_bars", 60, conid, price, observed_at)
    }

    fn update_market_five_second_bar(
        &mut self,
        conid: i32,
        price: f64,
        observed_at: DateTime<Utc>,
    ) -> Result<()> {
        self.update_market_bar("market_five_second_bars", 5, conid, price, observed_at)
    }

    fn update_market_bar(
        &mut self,
        table: &'static str,
        interval_seconds: i64,
        conid: i32,
        price: f64,
        observed_at: DateTime<Utc>,
    ) -> Result<()> {
        let bucket_timestamp =
            observed_at.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bar_time = DateTime::from_timestamp(bucket_timestamp, 0)
            .ok_or_else(|| AppError::Storage("market tick timestamp is out of range".into()))?;
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                &format!(
                    "UPDATE {table} SET final = true, updated_at = ?
                 WHERE conid = ? AND bar_time < ? AND final = false",
                ),
                params![observed_at, conid, bar_time],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                &format!(
                    "INSERT INTO {table}
                 VALUES (?, ?, ?, ?, ?, ?, 1, false, ?)
                 ON CONFLICT (conid, bar_time) DO UPDATE SET
                   high = greatest({table}.high, excluded.close),
                   low = least({table}.low, excluded.close),
                   close = excluded.close,
                   tick_count = {table}.tick_count + 1,
                   updated_at = excluded.updated_at"
                ),
                params![conid, bar_time, price, price, price, price, observed_at],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn create_backfill_job(&mut self, request: &BackfillJobRequest) -> Result<uuid::Uuid> {
        if request.end <= request.start {
            return Err(AppError::Storage("backfill end must be after start".into()));
        }
        let job_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        let request_json = serde_json::to_string(request)?;
        self.connection
            .execute(
                "INSERT INTO data_jobs VALUES
                 (?, 'historical_backfill', 'pending', ?, ?, ?, 0, 0, NULL, ?, ?)",
                params![job_id, request_json, request.start, request.end, now, now],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(job_id)
    }

    pub fn claim_backfill_job(&mut self) -> Result<Option<ClaimedBackfillJob>> {
        let row = self
            .connection
            .query_row(
                "SELECT job_id, request_json::VARCHAR, cursor_time, attempts
                 FROM data_jobs
                 WHERE state IN ('pending', 'retrying') AND attempts < 3
                 ORDER BY created_at LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, DateTime<Utc>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some((job_id, request_json, cursor_time, attempts)) = row else {
            return Ok(None);
        };
        let request = serde_json::from_str(&request_json)?;
        self.connection
            .execute(
                "UPDATE data_jobs SET state = 'running', updated_at = ? WHERE job_id = ?",
                params![Utc::now(), job_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(Some(ClaimedBackfillJob {
            job_id,
            request,
            cursor_time,
            attempts,
        }))
    }

    pub fn advance_backfill_job(
        &mut self,
        job_id: uuid::Uuid,
        next_cursor: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<()> {
        let state = if next_cursor >= end_time {
            "completed"
        } else {
            "pending"
        };
        self.connection
            .execute(
                "UPDATE data_jobs SET state = ?, cursor_time = ?, attempts = 0,
                   completed_slices = completed_slices + 1, last_error = NULL,
                   updated_at = ? WHERE job_id = ?",
                params![state, next_cursor.min(end_time), Utc::now(), job_id],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn fail_backfill_job(
        &mut self,
        job_id: uuid::Uuid,
        attempts: i64,
        error: &str,
    ) -> Result<()> {
        let next_attempts = attempts + 1;
        let state = if next_attempts >= 3 {
            "failed"
        } else {
            "retrying"
        };
        self.connection
            .execute(
                "UPDATE data_jobs SET state = ?, attempts = ?, last_error = ?,
                   updated_at = ? WHERE job_id = ?",
                params![state, next_attempts, error, Utc::now(), job_id],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_data_jobs(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT job_id, job_type, state, request_json::VARCHAR, cursor_time, end_time, attempts,
                        completed_slices, last_error, created_at, updated_at
                 FROM data_jobs ORDER BY created_at DESC LIMIT 200",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "job_id": row.get::<_, uuid::Uuid>(0)?,
                    "job_type": row.get::<_, String>(1)?,
                    "state": row.get::<_, String>(2)?,
                    "request": serde_json::from_str::<serde_json::Value>(
                        &row.get::<_, String>(3)?
                    ).unwrap_or(serde_json::Value::Null),
                    "cursor_time": row.get::<_, DateTime<Utc>>(4)?,
                    "end_time": row.get::<_, DateTime<Utc>>(5)?,
                    "attempts": row.get::<_, i64>(6)?,
                    "completed_slices": row.get::<_, i64>(7)?,
                    "last_error": row.get::<_, Option<String>>(8)?,
                    "created_at": row.get::<_, DateTime<Utc>>(9)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(10)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn cancel_data_job(&mut self, job_id: uuid::Uuid) -> Result<bool> {
        self.connection
            .execute(
                "UPDATE data_jobs SET state = 'cancelled',
                    last_error = 'cancelled by operator', updated_at = ?
                 WHERE job_id = ? AND state IN ('pending', 'retrying')",
                params![Utc::now(), job_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn historical_coverage(
        &self,
        conid: i32,
        timeframe: &str,
        requested_start: DateTime<Utc>,
        requested_end: DateTime<Utc>,
    ) -> Result<serde_json::Value> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT min_time, max_time, row_count, relative_path
                 FROM dataset_files
                 WHERE dataset = 'bars' AND conid = ? AND timeframe = ? AND active = true
                 ORDER BY min_time",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let files = statement
            .query_map(params![conid, timeframe], |row| {
                Ok((
                    row.get::<_, DateTime<Utc>>(0)?,
                    row.get::<_, DateTime<Utc>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let step = timeframe_duration(timeframe)?;
        let mut gaps = Vec::new();
        let mut cursor = requested_start;
        let mut rows = 0_i64;
        let mut file_values = Vec::new();
        for (min_time, max_time, row_count, path) in files {
            if max_time < requested_start || min_time >= requested_end {
                continue;
            }
            if min_time > cursor {
                gaps.push(serde_json::json!({"start": cursor, "end": min_time}));
            }
            cursor = cursor.max(max_time + step);
            rows += row_count;
            file_values.push(serde_json::json!({
                "path": path,
                "min_time": min_time,
                "max_time": max_time,
                "row_count": row_count
            }));
        }
        if cursor < requested_end {
            gaps.push(serde_json::json!({"start": cursor, "end": requested_end}));
        }
        Ok(serde_json::json!({
            "conid": conid,
            "timeframe": timeframe,
            "requested_start": requested_start,
            "requested_end": requested_end,
            "covered": gaps.is_empty(),
            "row_count": rows,
            "files": file_values,
            "raw_gaps": gaps,
            "calendar_adjusted": false
        }))
    }

    pub fn verify_dataset_files(&mut self, lake_dir: &Path) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id, relative_path, byte_size, checksum
                 FROM dataset_files WHERE active = true ORDER BY created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let files = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        let mut results = Vec::new();
        for (file_id, relative_path, expected_size, expected_checksum) in files {
            let path = lake_dir.join(&relative_path);
            let result = match fs::metadata(&path) {
                Ok(metadata) => {
                    let checksum = file_checksum(&path)?;
                    if expected_checksum.is_none() {
                        self.connection
                            .execute(
                                "UPDATE dataset_files SET checksum = ? WHERE file_id = ?",
                                params![checksum, file_id],
                            )
                            .map_err(|error| AppError::Storage(error.to_string()))?;
                    }
                    let healthy = metadata.len() as i64 == expected_size
                        && expected_checksum
                            .as_ref()
                            .is_none_or(|expected| expected == &checksum);
                    serde_json::json!({
                        "file_id": file_id,
                        "relative_path": relative_path,
                        "healthy": healthy,
                        "byte_size": metadata.len(),
                        "checksum": checksum,
                        "expected_checksum": expected_checksum
                    })
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => serde_json::json!({
                    "file_id": file_id,
                    "relative_path": relative_path,
                    "healthy": false,
                    "error": "file_not_found"
                }),
                Err(error) => return Err(error.into()),
            };
            results.push(result);
        }
        Ok(results)
    }

    pub fn create_dataset_snapshot(&mut self, name: &str, dataset: &str) -> Result<uuid::Uuid> {
        if name.trim().is_empty() || dataset.trim().is_empty() {
            return Err(AppError::Storage(
                "snapshot name and dataset cannot be empty".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id FROM dataset_files
                 WHERE dataset = ? AND active = true ORDER BY created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let file_ids = statement
            .query_map(params![dataset], |row| row.get::<_, uuid::Uuid>(0))
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        if file_ids.is_empty() {
            return Err(AppError::Storage(
                "cannot snapshot a dataset without active files".into(),
            ));
        }
        let snapshot_id = uuid::Uuid::now_v7();
        self.connection
            .execute(
                "INSERT INTO dataset_snapshots VALUES (?, ?, ?, ?, ?)",
                params![
                    snapshot_id,
                    name.trim(),
                    dataset.trim(),
                    serde_json::to_string(&file_ids)?,
                    Utc::now()
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(snapshot_id)
    }

    pub fn list_dataset_snapshots(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT snapshot_id, name, dataset, file_ids_json::VARCHAR, created_at
                 FROM dataset_snapshots ORDER BY created_at DESC",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                let file_ids: String = row.get(3)?;
                Ok(serde_json::json!({
                    "snapshot_id": row.get::<_, uuid::Uuid>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "dataset": row.get::<_, String>(2)?,
                    "file_ids": serde_json::from_str::<serde_json::Value>(&file_ids).unwrap_or_default(),
                    "created_at": row.get::<_, DateTime<Utc>>(4)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn operational_health(
        &self,
        lake_dir: &Path,
        staging_dir: &Path,
    ) -> Result<serde_json::Value> {
        let pending_jobs: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM data_jobs WHERE state IN ('pending', 'retrying', 'running')",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let running_strategies: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM strategies WHERE state = 'running'",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let failed_subscriptions: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM market_data_status
                 WHERE state IN ('failed', 'retrying')",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let database_bytes = fs::metadata(&self.database_path)
            .map(|item| item.len())
            .unwrap_or(0);
        Ok(serde_json::json!({
            "healthy": true,
            "database_bytes": database_bytes,
            "lake_bytes": directory_size(lake_dir)?,
            "staging_bytes": directory_size(staging_dir)?,
            "pending_data_jobs": pending_jobs,
            "running_strategies": running_strategies,
            "failed_market_data_subscriptions": failed_subscriptions,
            "checked_at": Utc::now()
        }))
    }

    pub fn monitoring_facts(&self, now: DateTime<Utc>) -> Result<serde_json::Value> {
        let failed_market_data: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM market_data_status
                 WHERE state IN ('failed', 'retrying')",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let competing_live_session_conids = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT conid FROM market_data_status
                     WHERE state IN ('failed', 'retrying')
                       AND (
                         last_error LIKE '%10197%'
                         OR lower(last_error) LIKE '%competing live session%'
                       )
                     ORDER BY conid",
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            statement
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(|error| AppError::Storage(error.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AppError::Storage(error.to_string()))?
        };
        let delayed_market_data: i64 = self
            .connection
            .query_row(
                "SELECT count(DISTINCT t.conid)
                 FROM market_ticks_current t
                 JOIN market_data_status s ON s.conid = t.conid
                 WHERE s.state = 'active' AND t.tick_type LIKE 'Delayed%'
                   AND t.observed_at >= ?",
                params![now - chrono::Duration::minutes(5)],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let uncertain_orders: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM orders WHERE lower(status) = 'unknown'",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let failed_actions: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_actions
                 WHERE state = 'failed'",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(serde_json::json!({
            "failed_market_data": failed_market_data,
            "competing_live_session_count": competing_live_session_conids.len(),
            "competing_live_session_conids": competing_live_session_conids,
            "delayed_market_data": delayed_market_data,
            "uncertain_orders": uncertain_orders,
            "failed_strategy_actions": failed_actions,
        }))
    }

    pub fn create_backup(
        &mut self,
        backup_dir: &Path,
        lake_dir: &Path,
    ) -> Result<serde_json::Value> {
        self.connection
            .execute_batch("CHECKPOINT")
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let backup_id = uuid::Uuid::now_v7();
        let destination = backup_dir.join(backup_id.to_string());
        let files_dir = destination.join("lake");
        fs::create_dir_all(&files_dir)?;
        fs::copy(&self.database_path, destination.join("state.duckdb"))?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id, relative_path, checksum FROM dataset_files
             WHERE active = true ORDER BY created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let files = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        let mut manifest_files = Vec::new();
        for (file_id, relative_path, checksum) in files {
            let source = lake_dir.join(&relative_path);
            let target = files_dir.join(&relative_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source, &target)?;
            manifest_files.push(serde_json::json!({
                "file_id": file_id,
                "relative_path": relative_path,
                "checksum": checksum.or_else(|| file_checksum(&source).ok())
            }));
        }
        let manifest = serde_json::json!({
            "backup_id": backup_id,
            "created_at": Utc::now(),
            "schema_version": self.schema_version()?,
            "database": "state.duckdb",
            "files": manifest_files
        });
        fs::write(
            destination.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        Ok(serde_json::json!({
            "backup_id": backup_id,
            "path": destination,
            "file_count": manifest_files.len(),
            "manifest": manifest
        }))
    }

    pub fn list_backups(backup_dir: &Path) -> Result<Vec<serde_json::Value>> {
        let mut backups = Vec::new();
        match fs::read_dir(backup_dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let manifest_path = entry.path().join("manifest.json");
                    if manifest_path.is_file() {
                        let value =
                            serde_json::from_slice::<serde_json::Value>(&fs::read(manifest_path)?)?;
                        backups.push(value);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        backups.sort_by(|left, right| {
            right["created_at"]
                .as_str()
                .cmp(&left["created_at"].as_str())
        });
        Ok(backups)
    }

    pub fn run_moving_average_backtest(
        &mut self,
        lake_dir: &Path,
        request: &BacktestRequest,
    ) -> Result<serde_json::Value> {
        let request = self.resolve_backtest_request(request)?;
        validate_backtest_request(&request)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id, relative_path FROM dataset_files
                 WHERE dataset = 'bars' AND conid = ? AND timeframe = ? AND active = true
                   AND max_time >= ? AND min_time < ?
                 ORDER BY min_time",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let files = statement
            .query_map(
                params![request.conid, request.timeframe, request.start, request.end],
                |row| Ok((row.get::<_, uuid::Uuid>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        if files.is_empty() {
            return Err(AppError::Storage(
                "no active Parquet files cover the requested backtest range".into(),
            ));
        }
        // Let DuckDB plan and scan every selected Parquet fragment together.
        // Preparing and executing one query per small backfill file adds
        // substantial fixed overhead as the lake becomes fragmented.
        let parquet_files = files
            .iter()
            .map(|(_, relative_path)| format!("'{}'", sql_path(&lake_dir.join(relative_path))))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT open_time, open, high, low, close, volume
             FROM read_parquet([{parquet_files}])
             WHERE open_time >= ? AND open_time < ?
             ORDER BY open_time"
        );
        let mut statement = self
            .connection
            .prepare(&sql)
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut bars = statement
            .query_map(params![request.start, request.end], |row| {
                Ok(BacktestBar {
                    open_time: row.get(0)?,
                    open: row.get(1)?,
                    high: row.get(2)?,
                    low: row.get(3)?,
                    close: row.get(4)?,
                    volume: row.get(5)?,
                })
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        bars.dedup_by_key(|bar| bar.open_time);
        let backtest_id = uuid::Uuid::now_v7();
        let started_at = Utc::now();
        let file_ids: Vec<uuid::Uuid> = files.iter().map(|(file_id, _)| *file_id).collect();
        // Simulate before writing the run row: a failure is recorded as a
        // terminal 'failed' run instead of leaving a zombie 'running' record.
        let simulation = build_backtest_strategy(&request)
            .and_then(|strategy| simulate_strategy(&request, strategy.as_ref(), &bars));
        let (trades, equity, metrics) = match simulation {
            Ok(result) => result,
            Err(error) => {
                self.connection
                    .execute(
                        "INSERT INTO backtest_runs VALUES
                         (?, ?, ?, ?, ?, 'failed', ?, ?, NULL, ?)",
                        params![
                            backtest_id,
                            request.strategy_kind,
                            serde_json::to_string(&request)?,
                            serde_json::to_string(&file_ids)?,
                            request.seed,
                            started_at,
                            Utc::now(),
                            error.to_string(),
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                return Err(error);
            }
        };
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO backtest_runs VALUES
                 (?, ?, ?, ?, ?, 'completed', ?, ?, ?, NULL)",
                params![
                    backtest_id,
                    request.strategy_kind,
                    serde_json::to_string(&request)?,
                    serde_json::to_string(&file_ids)?,
                    request.seed,
                    started_at,
                    Utc::now(),
                    serde_json::to_string(&metrics)?,
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        {
            let mut appender = transaction
                .appender("backtest_trades")
                .map_err(|error| AppError::Storage(error.to_string()))?;
            for trade in &trades {
                appender
                    .append_row(params![
                        uuid::Uuid::now_v7(),
                        backtest_id,
                        request.conid,
                        trade.signal_time,
                        trade.fill_time,
                        trade.side,
                        trade.quantity,
                        trade.price,
                        trade.commission,
                        trade.slippage
                    ])
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            }
            appender
                .flush()
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        {
            let mut appender = transaction
                .appender("backtest_equity")
                .map_err(|error| AppError::Storage(error.to_string()))?;
            for point in &equity {
                appender
                    .append_row(params![
                        backtest_id,
                        point.observed_at,
                        point.cash,
                        point.position,
                        point.close,
                        point.equity
                    ])
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            }
            appender
                .flush()
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(serde_json::json!({
            "backtest_id": backtest_id,
            "state": "completed",
            "metrics": metrics,
            "dataset_file_ids": file_ids
        }))
    }

    fn resolve_backtest_request(&self, request: &BacktestRequest) -> Result<BacktestRequest> {
        let Some(strategy_id) = request.strategy_id else {
            return Ok(request.clone());
        };
        let stored = self
            .connection
            .query_row(
                "SELECT kind, config_json::VARCHAR FROM strategies WHERE strategy_id = ?",
                params![strategy_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| AppError::Storage("backtest strategy not found".into()))?;
        let strategy_config: serde_json::Value = serde_json::from_str(&stored.1)?;
        let strategy = crate::strategy::build(&stored.0, strategy_config.clone())
            .map_err(AppError::Storage)?;
        let mut resolved = request.clone();
        resolved.conid = strategy.conid();
        resolved.timeframe = strategy.bar_timeframe().to_owned();
        resolved.strategy_kind = stored.0;
        resolved.strategy_config = Some(strategy_config);
        resolved.short_window = None;
        resolved.long_window = None;
        Ok(resolved)
    }

    pub fn list_backtests(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT b.backtest_id, b.strategy_kind, b.parameters_json::VARCHAR,
                    b.dataset_file_ids_json::VARCHAR, b.seed, b.state, b.started_at,
                    b.completed_at, b.metrics_json::VARCHAR, b.error,
                    i.symbol, i.description, i.exchange, i.primary_exchange
             FROM backtest_runs b
             LEFT JOIN instruments i
               ON i.conid = try_cast(json_extract_string(b.parameters_json, '$.conid') AS BIGINT)
             ORDER BY b.started_at DESC LIMIT 200",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement.query_map([], |row| {
            let parameters: String = row.get(2)?;
            let files: String = row.get(3)?;
            let metrics: Option<String> = row.get(8)?;
            Ok(serde_json::json!({
                "backtest_id": row.get::<_, uuid::Uuid>(0)?,
                "strategy_kind": row.get::<_, String>(1)?,
                "parameters": serde_json::from_str::<serde_json::Value>(&parameters).unwrap_or_default(),
                "dataset_file_ids": serde_json::from_str::<serde_json::Value>(&files).unwrap_or_default(),
                "seed": row.get::<_, i64>(4)?,
                "state": row.get::<_, String>(5)?,
                "started_at": row.get::<_, DateTime<Utc>>(6)?,
                "completed_at": row.get::<_, Option<DateTime<Utc>>>(7)?,
                "metrics": metrics.and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()),
                "error": row.get::<_, Option<String>>(9)?,
                "symbol": row.get::<_, Option<String>>(10)?,
                "description": row.get::<_, Option<String>>(11)?,
                "exchange": row.get::<_, Option<String>>(12)?,
                "primary_exchange": row.get::<_, Option<String>>(13)?
            }))
        }).map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn backtest_details(&self, backtest_id: uuid::Uuid) -> Result<Option<serde_json::Value>> {
        let run = self
            .connection
            .query_row(
                "SELECT b.strategy_kind, b.parameters_json::VARCHAR,
                        b.dataset_file_ids_json::VARCHAR, b.seed, b.state, b.started_at,
                        b.completed_at, b.metrics_json::VARCHAR, b.error,
                        i.symbol, i.description, i.exchange, i.primary_exchange
                 FROM backtest_runs b
                 LEFT JOIN instruments i
                   ON i.conid = try_cast(json_extract_string(b.parameters_json, '$.conid') AS BIGINT)
                 WHERE b.backtest_id = ?",
                params![backtest_id],
                |row| {
                    let parameters: String = row.get(1)?;
                    let files: String = row.get(2)?;
                    let metrics: Option<String> = row.get(7)?;
                    Ok(serde_json::json!({
                        "backtest_id": backtest_id,
                        "strategy_kind": row.get::<_, String>(0)?,
                        "parameters": serde_json::from_str::<serde_json::Value>(&parameters).unwrap_or_default(),
                        "dataset_file_ids": serde_json::from_str::<serde_json::Value>(&files).unwrap_or_default(),
                        "seed": row.get::<_, i64>(3)?,
                        "state": row.get::<_, String>(4)?,
                        "started_at": row.get::<_, DateTime<Utc>>(5)?,
                        "completed_at": row.get::<_, Option<DateTime<Utc>>>(6)?,
                        "metrics": metrics.and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()),
                        "error": row.get::<_, Option<String>>(8)?,
                        "symbol": row.get::<_, Option<String>>(9)?,
                        "description": row.get::<_, Option<String>>(10)?,
                        "exchange": row.get::<_, Option<String>>(11)?,
                        "primary_exchange": row.get::<_, Option<String>>(12)?
                    }))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some(mut run) = run else {
            return Ok(None);
        };
        let mut trade_statement = self
            .connection
            .prepare(
                "SELECT conid, signal_time, fill_time, side, quantity, price,
                        commission, slippage
                 FROM backtest_trades WHERE backtest_id = ? ORDER BY fill_time",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let trades = trade_statement
            .query_map(params![backtest_id], |row| {
                Ok(serde_json::json!({
                    "conid": row.get::<_, i64>(0)?,
                    "signal_time": row.get::<_, DateTime<Utc>>(1)?,
                    "fill_time": row.get::<_, DateTime<Utc>>(2)?,
                    "side": row.get::<_, String>(3)?,
                    "quantity": row.get::<_, f64>(4)?,
                    "price": row.get::<_, f64>(5)?,
                    "commission": row.get::<_, f64>(6)?,
                    "slippage": row.get::<_, f64>(7)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut equity_statement = self
            .connection
            .prepare(
                "SELECT observed_at, cash, position, close, equity
                 FROM backtest_equity WHERE backtest_id = ? ORDER BY observed_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let equity = equity_statement
            .query_map(params![backtest_id], |row| {
                Ok(serde_json::json!({
                    "observed_at": row.get::<_, DateTime<Utc>>(0)?,
                    "cash": row.get::<_, f64>(1)?,
                    "position": row.get::<_, f64>(2)?,
                    "close": row.get::<_, f64>(3)?,
                    "equity": row.get::<_, f64>(4)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        run["trades"] = serde_json::Value::Array(trades);
        run["equity"] = serde_json::Value::Array(equity);
        Ok(Some(run))
    }

    pub fn write_historical_bars(
        &mut self,
        lake_dir: &Path,
        staging_dir: &Path,
        bars: &[crate::ibkr::HistoricalBar],
    ) -> Result<DatasetFile> {
        let first = bars
            .first()
            .ok_or_else(|| AppError::Storage("IBKR returned no historical bars".into()))?;
        validate_bars(bars)?;
        fs::create_dir_all(staging_dir)?;
        let file_id = uuid::Uuid::now_v7();
        let staging_path = staging_dir.join(format!("{file_id}.parquet.tmp"));
        let final_dir = lake_dir
            .join("bars")
            .join(format!("timeframe={}", first.timeframe))
            .join(format!("conid={}", first.conid));
        fs::create_dir_all(&final_dir)?;
        let final_path = final_dir.join(format!("part-{file_id}.parquet"));

        self.connection
            .execute_batch(
                "DROP TABLE IF EXISTS temp_bars;
                 CREATE TEMP TABLE temp_bars (
                    conid INTEGER, timeframe VARCHAR, open_time TIMESTAMPTZ,
                    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                    volume DOUBLE, wap DOUBLE, trade_count INTEGER
                 );",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for bar in bars {
            transaction
                .execute(
                    "INSERT INTO temp_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        bar.conid,
                        bar.timeframe,
                        bar.open_time,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                        bar.wap,
                        bar.trade_count,
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let copy_sql = format!(
            "COPY (SELECT * FROM temp_bars ORDER BY open_time) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            sql_path(&staging_path)
        );
        self.connection
            .execute_batch(&copy_sql)
            .map_err(|error| AppError::Storage(error.to_string()))?;
        fs::rename(&staging_path, &final_path)?;
        let metadata = fs::metadata(&final_path)?;
        let checksum = file_checksum(&final_path)?;
        let min_time = bars
            .iter()
            .map(|bar| bar.open_time)
            .min()
            .expect("not empty");
        let max_time = bars
            .iter()
            .map(|bar| bar.open_time)
            .max()
            .expect("not empty");
        let relative_path = final_path
            .strip_prefix(lake_dir)
            .unwrap_or(&final_path)
            .to_path_buf();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO dataset_files
                 (file_id, dataset, relative_path, schema_version, conid, timeframe,
                  min_time, max_time, row_count, byte_size, active, created_at, checksum)
                 VALUES (?, 'bars', ?, 1, ?, ?, ?, ?, ?, ?, true, ?, ?)",
                params![
                    file_id,
                    relative_path.to_string_lossy(),
                    first.conid,
                    first.timeframe,
                    min_time,
                    max_time,
                    bars.len() as i64,
                    metadata.len() as i64,
                    Utc::now(),
                    checksum,
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // Retire older manifest entries whose time range is fully covered by the
        // new file so repeated backfills do not accumulate duplicate active data.
        transaction
            .execute(
                "UPDATE dataset_files SET active = false
                 WHERE dataset = 'bars' AND conid = ? AND timeframe = ?
                   AND active = true AND file_id <> ?
                   AND min_time >= ? AND max_time <= ?",
                params![first.conid, first.timeframe, file_id, min_time, max_time],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(DatasetFile {
            file_id,
            relative_path,
            row_count: bars.len(),
            byte_size: metadata.len(),
            min_time,
            max_time,
        })
    }

    pub fn create_order_intent(
        &mut self,
        idempotency_key: &str,
        account: &str,
        request: &crate::ibkr::BrokerOrderRequest,
        status: &str,
        rejection_reason: Option<&str>,
    ) -> Result<uuid::Uuid> {
        self.upsert_instrument(&request.contract)?;
        let id = uuid::Uuid::now_v7();
        let now = Utc::now();
        let payload =
            serde_json::to_string(request).map_err(|error| AppError::Storage(error.to_string()))?;
        let changed = self
            .connection
            .execute(
                "INSERT INTO order_intents
                 (order_intent_id, idempotency_key, account_id, conid, payload_json,
                  status, rejection_reason, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                params![
                    id,
                    idempotency_key,
                    account,
                    request.contract.conid,
                    payload,
                    status,
                    rejection_reason,
                    now,
                    now,
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if changed == 0 {
            let existing: (uuid::Uuid, String) = self
                .connection
                .query_row(
                    "SELECT order_intent_id, status FROM order_intents
                     WHERE idempotency_key = ?",
                    params![idempotency_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Err(AppError::Storage(format!(
                "idempotency key already exists: {idempotency_key} \
                 (intent {} with status '{}'); query the existing intent instead of retrying \
                 with a new key",
                existing.0, existing.1
            )));
        }
        Ok(id)
    }

    /// Marks an intent whose broker outcome could not be confirmed, for example
    /// after an acknowledgement timeout. The order may or may not be live at
    /// IBKR; it must never be resubmitted automatically. Reconciliation against
    /// IBKR open orders is the only path that resolves the true state.
    pub fn mark_order_intent_unknown(&mut self, intent_id: uuid::Uuid, reason: &str) -> Result<()> {
        self.connection
            .execute(
                "UPDATE order_intents
                 SET status = 'unknown', rejection_reason = ?, updated_at = ?
                 WHERE order_intent_id = ?",
                params![reason, Utc::now(), intent_id],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn record_risk_decision(
        &mut self,
        intent_id: uuid::Uuid,
        outcome: &str,
        reason_code: &str,
        detail: &str,
    ) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO risk_decisions VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    uuid::Uuid::now_v7(),
                    intent_id,
                    outcome,
                    reason_code,
                    detail,
                    Utc::now(),
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn record_submitted_order(
        &mut self,
        intent_id: uuid::Uuid,
        broker_order_id: i32,
        connection_session_id: uuid::Uuid,
    ) -> Result<uuid::Uuid> {
        let order_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE order_intents SET status = 'submitted', updated_at = ?
                 WHERE order_intent_id = ?",
                params![now, intent_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO orders
                 (order_id, order_intent_id, broker_order_id, connection_session_id,
                  status, filled_quantity, created_at, updated_at)
                 VALUES (?, ?, ?, ?, 'submitted', 0, ?, ?)",
                params![
                    order_id,
                    intent_id,
                    broker_order_id,
                    connection_session_id,
                    now,
                    now
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(order_id)
    }

    pub fn mark_cancel_pending(
        &mut self,
        broker_order_id: i32,
        connection_session_id: uuid::Uuid,
    ) -> Result<String> {
        let current = self
            .connection
            .query_row(
                "SELECT status FROM orders
                 WHERE connection_session_id = ? AND broker_order_id = ?",
                params![connection_session_id, broker_order_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let previous_status = match current {
            Some(status) => status,
            None => {
                let previous = self
                    .connection
                    .query_row(
                        "SELECT status, connection_session_id
                         FROM orders WHERE broker_order_id = ?
                         ORDER BY updated_at DESC LIMIT 1",
                        params![broker_order_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Option<uuid::Uuid>>(1)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                return match previous {
                    Some((status, session)) => Err(AppError::Storage(format!(
                        "order {broker_order_id} has local status {status} in previous connection session {}; \
                         the latest IBKR reconciliation did not report it as an open order in the current session, \
                         so it cannot be cancelled",
                        session
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "unknown".into())
                    ))),
                    None => Err(AppError::Storage(format!(
                        "broker order id {broker_order_id} is not present in local order history"
                    ))),
                };
            }
        };
        if !matches!(
            previous_status.to_ascii_lowercase().as_str(),
            "submitted" | "presubmitted" | "pendingsubmit" | "apipending"
        ) {
            return Err(AppError::Storage(format!(
                "order {broker_order_id} cannot be cancelled from status {previous_status}"
            )));
        }
        self.connection
            .execute(
                "UPDATE orders SET status = 'cancel_pending', updated_at = ?
                 WHERE connection_session_id = ? AND broker_order_id = ?",
                params![Utc::now(), connection_session_id, broker_order_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(previous_status)
    }

    pub fn restore_cancel_status(
        &mut self,
        broker_order_id: i32,
        connection_session_id: uuid::Uuid,
        previous_status: &str,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE orders SET status = ?, updated_at = ?
                 WHERE connection_session_id = ? AND broker_order_id = ?
                   AND lower(status) = 'cancel_pending'",
                params![
                    previous_status,
                    Utc::now(),
                    connection_session_id,
                    broker_order_id
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn mark_previous_session_order_not_open(
        &mut self,
        broker_order_id: i32,
        current_session_id: uuid::Uuid,
    ) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT order_id FROM orders
                 WHERE broker_order_id = ?
                   AND connection_session_id IS DISTINCT FROM ?
                   AND lower(status) IN (
                       'submitted','presubmitted','pendingsubmit',
                       'pendingcancel','cancel_pending','apipending'
                   )",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let order_ids = statement
            .query_map(params![broker_order_id, current_session_id], |row| {
                row.get::<_, uuid::Uuid>(0)
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        if order_ids.is_empty() {
            return Ok(false);
        }
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for order_id in order_ids {
            transaction
                .execute(
                    "UPDATE orders SET status = 'not_open', updated_at = ?
                     WHERE order_id = ?",
                    params![now, order_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute(
                    "INSERT INTO order_events VALUES (?, ?, 'reconciliation_not_open', ?, ?, ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        order_id,
                        serde_json::json!({
                            "broker_order_id": broker_order_id,
                            "current_connection_session_id": current_session_id,
                            "detail": "IBKR all-open-orders did not report this previous-session order during an explicit cancellation attempt"
                        })
                        .to_string(),
                        now,
                        now
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(true)
    }

    pub fn mark_order_intent_rejected(
        &mut self,
        intent_id: uuid::Uuid,
        reason: &str,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE order_intents
                 SET status = 'broker_rejected', rejection_reason = ?, updated_at = ?
                 WHERE order_intent_id = ?",
                params![reason, Utc::now(), intent_id],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn apply_broker_event(&mut self, event: &crate::ibkr::BrokerEvent) -> Result<()> {
        match event {
            crate::ibkr::BrokerEvent::OrderStatus {
                connection_session_id,
                broker_order_id,
                status,
                filled,
                remaining,
                average_fill_price,
                last_fill_price,
                perm_id,
                why_held,
                market_cap_price,
            } => {
                let now = Utc::now();
                let payload = serde_json::json!({
                    "status": status,
                    "filled": filled,
                    "remaining": remaining,
                    "average_fill_price": average_fill_price,
                    "last_fill_price": last_fill_price,
                    "perm_id": perm_id,
                    "why_held": why_held,
                    "market_cap_price": market_cap_price,
                });
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO broker_order_events
                         VALUES (?, ?, ?, ?, 'order_status', ?, ?)",
                        params![
                            uuid::Uuid::now_v7(),
                            connection_session_id,
                            broker_order_id,
                            perm_id,
                            payload.to_string(),
                            now
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "UPDATE orders SET status = ?, filled_quantity = ?,
                             remaining_quantity = ?, average_fill_price = ?,
                             last_fill_price = ?, broker_perm_id = ?, why_held = ?,
                             market_cap_price = ?, updated_at = ?
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)",
                        params![
                            status,
                            filled,
                            remaining,
                            average_fill_price,
                            last_fill_price,
                            perm_id,
                            why_held,
                            market_cap_price,
                            now,
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id,
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO order_events
                         SELECT ?, order_id, 'ibkr_order_status', ?, ?, ?
                         FROM orders
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)
                         ORDER BY created_at DESC LIMIT 1",
                        params![
                            uuid::Uuid::now_v7(),
                            payload.to_string(),
                            now,
                            now,
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id,
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id,
                broker_order_id,
                perm_id,
                status,
                reject_reason,
                warning_text,
                completed_time,
                completed_status,
            } => {
                let now = Utc::now();
                let payload = serde_json::json!({
                    "status": status,
                    "perm_id": perm_id,
                    "reject_reason": reject_reason,
                    "warning_text": warning_text,
                    "completed_time": completed_time,
                    "completed_status": completed_status,
                });
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO broker_order_events
                         VALUES (?, ?, ?, ?, 'open_order', ?, ?)",
                        params![
                            uuid::Uuid::now_v7(),
                            connection_session_id,
                            broker_order_id,
                            perm_id,
                            payload.to_string(),
                            now
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "UPDATE orders SET
                           status = CASE
                             WHEN lower(status) IN
                               ('filled','cancelled','canceled','inactive','rejected')
                             THEN status
                             ELSE ?
                           END,
                           broker_perm_id = ?, updated_at = ?
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)",
                        params![
                            status,
                            perm_id,
                            now,
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id,
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO order_events
                         SELECT ?, order_id, 'ibkr_open_order', ?, ?, ?
                         FROM orders
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)
                         ORDER BY created_at DESC LIMIT 1",
                        params![
                            uuid::Uuid::now_v7(),
                            payload.to_string(),
                            now,
                            now,
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id,
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::Execution {
                connection_session_id,
                broker_order_id,
                perm_id,
                execution_id,
                conid,
                side,
                quantity,
                price,
                ..
            } => {
                self.connection
                    .execute(
                        "INSERT INTO executions
                             (execution_id, broker_execution_id, order_id, conid, side,
                              quantity, price, executed_at, received_at,
                              connection_session_id, broker_perm_id)
                             SELECT ?, ?, order_id, ?, ?, ?, ?, ?, ?, ?, ?
                             FROM orders
                             WHERE (connection_session_id = ? AND broker_order_id = ?)
                                OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)
                             LIMIT 1
                             ON CONFLICT (broker_execution_id) DO NOTHING",
                        params![
                            uuid::Uuid::now_v7(),
                            execution_id,
                            conid,
                            side,
                            quantity,
                            price,
                            Utc::now(),
                            Utc::now(),
                            connection_session_id,
                            perm_id,
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id,
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                self.connection
                    .execute(
                        "UPDATE executions SET
                           commission = (SELECT commission FROM pending_commissions
                                         WHERE broker_execution_id = ?),
                           currency = (SELECT currency FROM pending_commissions
                                       WHERE broker_execution_id = ?)
                         WHERE broker_execution_id = ?
                           AND EXISTS (SELECT 1 FROM pending_commissions
                                       WHERE broker_execution_id = ?)",
                        params![execution_id, execution_id, execution_id, execution_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                self.connection
                    .execute(
                        "DELETE FROM pending_commissions WHERE broker_execution_id = ?",
                        params![execution_id],
                    )
                    .map(|_| ())
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::Commission {
                execution_id,
                commission,
                currency,
                ..
            } => {
                let changed = self
                    .connection
                    .execute(
                        "UPDATE executions SET commission = ?, currency = ?
                     WHERE broker_execution_id = ?",
                        params![commission, currency, execution_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if changed == 0 {
                    self.connection
                        .execute(
                            "INSERT INTO pending_commissions VALUES (?, ?, ?, ?)
                         ON CONFLICT (broker_execution_id) DO UPDATE SET
                           commission = excluded.commission,
                           currency = excluded.currency,
                           received_at = excluded.received_at",
                            params![execution_id, commission, currency, Utc::now()],
                        )
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                }
                Ok(())
            }
            crate::ibkr::BrokerEvent::AccountSummary {
                account,
                tag,
                value,
                currency,
                observed_at,
            } => self
                .connection
                .execute(
                    "INSERT INTO account_summary_current VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (account_id, tag, currency) DO UPDATE SET
                       value = excluded.value, observed_at = excluded.observed_at",
                    params![account, tag, currency, value, observed_at],
                )
                .map(|_| ())
                .map_err(|error| AppError::Storage(error.to_string())),
            crate::ibkr::BrokerEvent::PositionSnapshotStarted { observed_at } => {
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "UPDATE position_sync_state SET state = 'syncing', observed_at = ?
                         WHERE singleton",
                        params![observed_at],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "UPDATE positions_current SET quantity = 0, average_cost = 0",
                        [],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::Position { position } => self.upsert_position(position),
            crate::ibkr::BrokerEvent::PositionSnapshotCompleted { observed_at } => {
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "UPDATE positions_current SET observed_at = ?",
                        params![observed_at],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO position_history
                         SELECT uuid(), account_id, conid, quantity, average_cost, ?
                         FROM positions_current",
                        params![observed_at],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .execute(
                        "UPDATE position_sync_state SET state = 'ready', observed_at = ?
                         WHERE singleton",
                        params![observed_at],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::Pnl {
                account,
                daily_pnl,
                unrealized_pnl,
                realized_pnl,
                observed_at,
            } => {
                self.connection
                    .execute(
                        "INSERT INTO account_pnl_current VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (account_id) DO UPDATE SET
                       daily_pnl = excluded.daily_pnl,
                       unrealized_pnl = excluded.unrealized_pnl,
                       realized_pnl = excluded.realized_pnl,
                       observed_at = excluded.observed_at",
                        params![
                            account,
                            daily_pnl,
                            unrealized_pnl,
                            realized_pnl,
                            observed_at
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                self.connection
                    .execute(
                        "INSERT INTO account_pnl_history VALUES (?, ?, ?, ?, ?, ?)",
                        params![
                            uuid::Uuid::now_v7(),
                            account,
                            daily_pnl,
                            unrealized_pnl,
                            realized_pnl,
                            observed_at
                        ],
                    )
                    .map(|_| ())
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::MarketDataTick {
                conid,
                tick_type,
                numeric_value,
                text_value,
                observed_at,
            } => {
                self.connection
                    .execute(
                        "INSERT INTO market_ticks_current VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (conid, tick_type) DO UPDATE SET
                       numeric_value = excluded.numeric_value,
                       text_value = excluded.text_value,
                       observed_at = excluded.observed_at",
                        params![conid, tick_type, numeric_value, text_value, observed_at],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if matches!(tick_type.as_str(), "Last" | "DelayedLast" | "LastRthTrade")
                    && numeric_value.is_some_and(|price| price.is_finite() && price > 0.0)
                {
                    self.update_market_minute_bar(
                        *conid,
                        numeric_value.expect("validated market price"),
                        *observed_at,
                    )?;
                    self.update_market_five_second_bar(
                        *conid,
                        numeric_value.expect("validated market price"),
                        *observed_at,
                    )?;
                }
                Ok(())
            }
            crate::ibkr::BrokerEvent::MarketDataStatus {
                conid,
                state,
                error,
                observed_at,
            } => self
                .connection
                .execute(
                    "INSERT INTO market_data_status VALUES (?, ?, ?, ?)
                     ON CONFLICT (conid) DO UPDATE SET
                       state = excluded.state,
                       last_error = excluded.last_error,
                       observed_at = excluded.observed_at",
                    params![conid, state, error, observed_at],
                )
                .map(|_| ())
                .map_err(|error| AppError::Storage(error.to_string())),
        }
    }

    pub fn upsert_fx_rate(&mut self, input: &FxRateInput) -> Result<()> {
        let base = input.base_currency.trim().to_ascii_uppercase();
        let quote = input.quote_currency.trim().to_ascii_uppercase();
        if base.len() != 3
            || quote.len() != 3
            || base == quote
            || !input.rate.is_finite()
            || input.rate <= 0.0
        {
            return Err(AppError::Storage(
                "FX rate requires distinct three-letter currencies and rate > 0".into(),
            ));
        }
        self.connection
            .execute(
                "INSERT INTO fx_rates VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (base_currency, quote_currency) DO UPDATE SET
                   rate = excluded.rate, observed_at = excluded.observed_at,
                   source = excluded.source",
                params![
                    base,
                    quote,
                    input.rate,
                    input.observed_at,
                    input.source.trim()
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_fx_rates(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT base_currency, quote_currency, rate, observed_at, source
                 FROM fx_rates ORDER BY base_currency, quote_currency",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "base_currency": row.get::<_, String>(0)?,
                    "quote_currency": row.get::<_, String>(1)?,
                    "rate": row.get::<_, f64>(2)?,
                    "observed_at": row.get::<_, DateTime<Utc>>(3)?,
                    "source": row.get::<_, String>(4)?,
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn currency_conversion_rate(
        &self,
        from: &str,
        to: &str,
        maximum_age_seconds: u64,
        now: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        let from = from.trim().to_ascii_uppercase();
        let to = to.trim().to_ascii_uppercase();
        if from == to {
            return Ok(Some(1.0));
        }
        let direct = self
            .connection
            .query_row(
                "SELECT rate, observed_at FROM fx_rates
                 WHERE base_currency = ? AND quote_currency = ?",
                params![from, to],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, DateTime<Utc>>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if let Some((rate, observed_at)) = direct {
            return Ok(
                ((now - observed_at).num_seconds().max(0) <= maximum_age_seconds as i64)
                    .then_some(rate),
            );
        }
        let inverse = self
            .connection
            .query_row(
                "SELECT rate, observed_at FROM fx_rates
                 WHERE base_currency = ? AND quote_currency = ?",
                params![to, from],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, DateTime<Utc>>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(inverse.and_then(|(rate, observed_at)| {
            ((now - observed_at).num_seconds().max(0) <= maximum_age_seconds as i64 && rate > 0.0)
                .then_some(1.0 / rate)
        }))
    }

    pub fn upsert_market_session(&mut self, input: &MarketSessionInput) -> Result<()> {
        let exchange = input.exchange.trim().to_ascii_uppercase();
        if exchange.is_empty()
            || input.opens_at >= input.closes_at
            || !matches!(input.state.as_str(), "open" | "closed")
        {
            return Err(AppError::Storage(
                "market session requires exchange, opens_at < closes_at and state open/closed"
                    .into(),
            ));
        }
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM market_session_intervals
                 WHERE exchange = ? AND session_kind = 'regular' AND trading_date = ?",
                params![exchange, input.trading_date],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO market_session_intervals
                 VALUES (?, 'regular', ?, ?, ?, ?, ?, ?)",
                params![
                    exchange,
                    input.trading_date,
                    input.opens_at,
                    input.closes_at,
                    input.state,
                    input.source.trim(),
                    Utc::now()
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn replace_ibkr_market_sessions(
        &mut self,
        schedule: &crate::ibkr::ContractSchedule,
    ) -> Result<usize> {
        if schedule.exchange.trim().is_empty() || schedule.regular_sessions.is_empty() {
            return Err(AppError::Storage(
                "IBKR market schedule requires an exchange and regular sessions".into(),
            ));
        }
        let exchange = schedule.exchange.trim().to_ascii_uppercase();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM market_session_intervals
                 WHERE exchange = ? AND source LIKE 'ibkr_contract_details:%'",
                params![exchange],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for (kind, sessions) in [
            ("regular", schedule.regular_sessions.as_slice()),
            ("extended", schedule.extended_sessions.as_slice()),
        ] {
            let dates = sessions
                .iter()
                .map(|session| session.trading_date)
                .collect::<std::collections::BTreeSet<_>>();
            for date in dates {
                transaction
                    .execute(
                        "DELETE FROM market_session_intervals
                         WHERE exchange = ? AND session_kind = ? AND trading_date = ?",
                        params![exchange, kind, date],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            }
            for session in sessions {
                transaction
                    .execute(
                        "INSERT INTO market_session_intervals
                         VALUES (?, ?, ?, ?, ?, 'open', ?, ?)",
                        params![
                            exchange,
                            kind,
                            session.trading_date,
                            session.opens_at,
                            session.closes_at,
                            format!(
                                "ibkr_contract_details:{}:{}",
                                schedule.conid, schedule.time_zone_id
                            ),
                            schedule.fetched_at
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(schedule.regular_sessions.len() + schedule.extended_sessions.len())
    }

    pub fn market_calendar_needs_refresh(
        &self,
        exchange: &str,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let last_update = self
            .connection
            .query_row(
                "SELECT max(updated_at) FROM market_session_intervals
                 WHERE exchange = ? AND source LIKE 'ibkr_contract_details:%'",
                params![exchange.trim().to_ascii_uppercase()],
                |row| row.get::<_, Option<DateTime<Utc>>>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(last_update
            .map(|updated_at| now - updated_at >= chrono::Duration::hours(6))
            .unwrap_or(true))
    }

    pub fn list_market_sessions(
        &self,
        exchange: Option<&str>,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let exchange = exchange.map(|value| value.trim().to_ascii_uppercase());
        let mut statement = self
            .connection
            .prepare(
                "SELECT exchange, trading_date, opens_at, closes_at, state, source, updated_at,
                        session_kind
                 FROM market_session_intervals
                 WHERE (? IS NULL OR exchange = ?)
                 ORDER BY trading_date DESC, exchange, session_kind, opens_at LIMIT ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(
                params![exchange, exchange, limit.min(10_000) as i64],
                |row| {
                    Ok(serde_json::json!({
                        "exchange": row.get::<_, String>(0)?,
                        "trading_date": row.get::<_, chrono::NaiveDate>(1)?,
                        "opens_at": row.get::<_, DateTime<Utc>>(2)?,
                        "closes_at": row.get::<_, DateTime<Utc>>(3)?,
                        "state": row.get::<_, String>(4)?,
                        "source": row.get::<_, String>(5)?,
                        "updated_at": row.get::<_, DateTime<Utc>>(6)?,
                        "session_kind": row.get::<_, String>(7)?,
                    }))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    /// Returns `None` when no calendar has been loaded for the exchange.
    #[cfg(test)]
    pub fn market_session_is_open(
        &self,
        exchange: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<bool>> {
        self.market_session_is_open_for(exchange, now, false)
    }

    pub fn market_session_is_open_for(
        &self,
        exchange: &str,
        now: DateTime<Utc>,
        outside_rth: bool,
    ) -> Result<Option<bool>> {
        let exchange = exchange.trim().to_ascii_uppercase();
        let session_kind = if outside_rth { "extended" } else { "regular" };
        let configured: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM market_session_intervals
                 WHERE exchange = ? AND session_kind = ?",
                params![exchange, session_kind],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if configured == 0 {
            return Ok(None);
        }
        let open: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM market_session_intervals
                 WHERE exchange = ? AND session_kind = ? AND state = 'open'
                   AND opens_at <= ? AND closes_at > ?",
                params![exchange, session_kind, now, now],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(Some(open > 0))
    }

    pub fn upsert_monitoring_alert(
        &mut self,
        alert_key: &str,
        severity: &str,
        message: &str,
        active: bool,
    ) -> Result<()> {
        let now = Utc::now();
        if active {
            self.connection
                .execute(
                    "INSERT INTO monitoring_alerts VALUES
                     (?, ?, ?, 'active', ?, ?, ?, NULL, NULL)
                     ON CONFLICT (alert_key) DO UPDATE SET
                       severity = excluded.severity,
                       state = CASE
                         WHEN monitoring_alerts.state = 'acknowledged'
                         THEN 'acknowledged'
                         ELSE 'active'
                       END,
                       message = excluded.message,
                       last_observed_at = excluded.last_observed_at,
                       acknowledged_at = CASE
                         WHEN monitoring_alerts.state = 'acknowledged'
                         THEN monitoring_alerts.acknowledged_at
                         ELSE NULL
                       END,
                       acknowledged_note = CASE
                         WHEN monitoring_alerts.state = 'acknowledged'
                         THEN monitoring_alerts.acknowledged_note
                         ELSE NULL
                       END",
                    params![uuid::Uuid::now_v7(), alert_key, severity, message, now, now],
                )
                .map(|_| ())
                .map_err(|error| AppError::Storage(error.to_string()))
        } else {
            self.connection
                .execute(
                    "UPDATE monitoring_alerts SET state = 'resolved',
                     last_observed_at = ? WHERE alert_key = ?
                     AND state IN ('active', 'acknowledged')",
                    params![now, alert_key],
                )
                .map(|_| ())
                .map_err(|error| AppError::Storage(error.to_string()))
        }
    }

    pub fn list_monitoring_alerts(
        &self,
        active_only: bool,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT alert_id, alert_key, severity, state, message,
                        first_observed_at, last_observed_at,
                        acknowledged_at, acknowledged_note
                 FROM monitoring_alerts
                 WHERE (? = false OR state = 'active')
                 ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                          last_observed_at DESC LIMIT ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![active_only, limit.min(10_000) as i64], |row| {
                Ok(serde_json::json!({
                    "alert_id": row.get::<_, uuid::Uuid>(0)?,
                    "alert_key": row.get::<_, String>(1)?,
                    "severity": row.get::<_, String>(2)?,
                    "state": row.get::<_, String>(3)?,
                    "message": row.get::<_, String>(4)?,
                    "first_observed_at": row.get::<_, DateTime<Utc>>(5)?,
                    "last_observed_at": row.get::<_, DateTime<Utc>>(6)?,
                    "acknowledged_at": row.get::<_, Option<DateTime<Utc>>>(7)?,
                    "acknowledged_note": row.get::<_, Option<String>>(8)?,
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn acknowledge_monitoring_alert(
        &mut self,
        alert_id: uuid::Uuid,
        note: &str,
    ) -> Result<bool> {
        self.connection
            .execute(
                "UPDATE monitoring_alerts SET state = 'acknowledged',
                 acknowledged_at = ?, acknowledged_note = ?
                 WHERE alert_id = ? AND state = 'active'",
                params![Utc::now(), note.trim(), alert_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn strategy_performance_report(
        &self,
        strategy_id: uuid::Uuid,
        initial_capital: f64,
        base_currency: &str,
        maximum_fx_age_seconds: u64,
        benchmark_conid: Option<i32>,
        now: DateTime<Utc>,
    ) -> Result<serde_json::Value> {
        if !initial_capital.is_finite() || initial_capital <= 0.0 {
            return Err(AppError::Storage(
                "initial_capital must be finite and greater than zero".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.executed_at, lower(e.side), e.quantity, e.price,
                        coalesce(e.commission, 0), e.currency, e.conid
                 FROM executions e
                 JOIN orders o ON o.order_id = e.order_id
                 JOIN (
                    SELECT strategy_id, order_intent_id
                    FROM strategy_execution_actions
                    WHERE order_intent_id IS NOT NULL
                    UNION
                    SELECT a.strategy_id, l.order_intent_id
                    FROM strategy_execution_action_legs l
                    JOIN strategy_execution_actions a USING (action_id)
                    WHERE l.order_intent_id IS NOT NULL
                 ) attributed
                   ON attributed.order_intent_id = o.order_intent_id
                 WHERE attributed.strategy_id = ?
                 ORDER BY e.executed_at, e.broker_execution_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![strategy_id], |row| {
                Ok((
                    row.get::<_, DateTime<Utc>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i32>(6)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;

        let mut positions: HashMap<i32, PerformancePosition> = HashMap::new();
        let mut gross_pnl = 0.0;
        let mut commissions = 0.0;
        let mut turnover = 0.0;
        let mut realized_trade_count = 0_i64;
        let mut winning_trade_count = 0_i64;
        let mut losing_trade_count = 0_i64;
        let mut peak_equity = initial_capital;
        let mut maximum_drawdown = 0.0_f64;
        let mut daily_equity: BTreeMap<chrono::NaiveDate, f64> = BTreeMap::new();
        let mut first_execution_at = None;
        let mut last_execution_at = None;

        for (executed_at, side, quantity, price, commission, currency, conid) in rows {
            first_execution_at.get_or_insert(executed_at);
            last_execution_at = Some(executed_at);
            let fx = self
                .currency_conversion_rate(&currency, base_currency, maximum_fx_age_seconds, now)?
                .ok_or_else(|| {
                    AppError::Storage(format!(
                        "fresh FX rate is required to convert {currency} to {base_currency}"
                    ))
                })?;
            commissions += commission * fx;
            turnover += quantity * price * fx;
            let position = positions.entry(conid).or_default();
            let mut realized = 0.0;
            if side.starts_with("bought") || side == "buy" {
                if position.quantity < 0.0 {
                    let closing = quantity.min(-position.quantity);
                    realized += (position.average_price - price) * closing;
                    position.quantity += closing;
                    let remaining = quantity - closing;
                    if position.quantity.abs() <= f64::EPSILON {
                        position.quantity = remaining;
                        position.average_price = if remaining > 0.0 { price } else { 0.0 };
                    }
                } else {
                    let next = position.quantity + quantity;
                    position.average_price = if next > 0.0 {
                        (position.average_price * position.quantity + price * quantity) / next
                    } else {
                        0.0
                    };
                    position.quantity = next;
                }
            } else if side.starts_with("sold") || side == "sell" {
                if position.quantity > 0.0 {
                    let closing = quantity.min(position.quantity);
                    realized += (price - position.average_price) * closing;
                    position.quantity -= closing;
                    let remaining = quantity - closing;
                    if position.quantity.abs() <= f64::EPSILON {
                        position.quantity = -remaining;
                        position.average_price = if remaining > 0.0 { price } else { 0.0 };
                    }
                } else {
                    let current_abs = -position.quantity;
                    let next_abs = current_abs + quantity;
                    position.average_price = if next_abs > 0.0 {
                        (position.average_price * current_abs + price * quantity) / next_abs
                    } else {
                        0.0
                    };
                    position.quantity = -next_abs;
                }
            }
            if realized.abs() > f64::EPSILON {
                realized_trade_count += 1;
                if realized > 0.0 {
                    winning_trade_count += 1;
                } else {
                    losing_trade_count += 1;
                }
            }
            gross_pnl += realized * fx;
            let equity = initial_capital + gross_pnl - commissions;
            peak_equity = peak_equity.max(equity);
            maximum_drawdown = maximum_drawdown.max(peak_equity - equity);
            daily_equity.insert(executed_at.date_naive(), equity);
        }

        let mut returns = Vec::new();
        let mut previous = initial_capital;
        for equity in daily_equity.values() {
            returns.push((*equity - previous) / previous.max(f64::EPSILON));
            previous = *equity;
        }
        let mean = if returns.is_empty() {
            0.0
        } else {
            returns.iter().sum::<f64>() / returns.len() as f64
        };
        let variance = if returns.len() > 1 {
            returns
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / (returns.len() - 1) as f64
        } else {
            0.0
        };
        let downside = returns
            .iter()
            .filter(|value| **value < 0.0)
            .map(|value| value.powi(2))
            .sum::<f64>();
        let downside_deviation = if returns.is_empty() {
            0.0
        } else {
            (downside / returns.len() as f64).sqrt()
        };
        let sharpe = (variance > 0.0).then_some(mean / variance.sqrt() * 252.0_f64.sqrt());
        let sortino =
            (downside_deviation > 0.0).then_some(mean / downside_deviation * 252.0_f64.sqrt());
        let net_pnl = gross_pnl - commissions;
        let open_position_count = positions
            .values()
            .filter(|position| position.quantity.abs() > f64::EPSILON)
            .count();
        let benchmark_return = match (benchmark_conid, first_execution_at, last_execution_at) {
            (Some(conid), Some(start), Some(end)) => {
                let first = self
                    .connection
                    .query_row(
                        "SELECT close FROM market_minute_bars
                     WHERE conid = ? AND minute >= ? AND minute <= ?
                     ORDER BY minute LIMIT 1",
                        params![conid, start, end],
                        |row| row.get::<_, f64>(0),
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let last = self
                    .connection
                    .query_row(
                        "SELECT close FROM market_minute_bars
                     WHERE conid = ? AND minute >= ? AND minute <= ?
                     ORDER BY minute DESC LIMIT 1",
                        params![conid, start, end],
                        |row| row.get::<_, f64>(0),
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                first
                    .zip(last)
                    .and_then(|(first, last)| (first > 0.0).then_some(last / first - 1.0))
            }
            _ => None,
        };
        let strategy_return = net_pnl / initial_capital;
        Ok(serde_json::json!({
            "strategy_id": strategy_id,
            "base_currency": base_currency.to_ascii_uppercase(),
            "initial_capital": initial_capital,
            "gross_pnl": gross_pnl,
            "commissions": commissions,
            "net_pnl": net_pnl,
            "return": strategy_return,
            "benchmark_conid": benchmark_conid,
            "benchmark_return": benchmark_return,
            "excess_return": benchmark_return.map(|value| strategy_return - value),
            "turnover": turnover,
            "maximum_drawdown": maximum_drawdown,
            "maximum_drawdown_pct": maximum_drawdown / initial_capital,
            "sharpe": sharpe,
            "sortino": sortino,
            "realized_trade_count": realized_trade_count,
            "winning_trade_count": winning_trade_count,
            "losing_trade_count": losing_trade_count,
            "win_rate": (realized_trade_count > 0)
                .then_some(winning_trade_count as f64 / realized_trade_count as f64),
            "open_position_count": open_position_count,
            "daily_equity": daily_equity.into_iter().map(|(date, equity)| {
                serde_json::json!({"date": date, "equity": equity})
            }).collect::<Vec<_>>(),
            "generated_at": now,
        }))
    }

    pub fn persist_strategy_performance_snapshot(
        &mut self,
        strategy_id: uuid::Uuid,
        account: &str,
        report: &serde_json::Value,
    ) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO strategy_performance_snapshots VALUES
                 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    strategy_id,
                    account,
                    Utc::now(),
                    report["base_currency"].as_str().unwrap_or("USD"),
                    report["gross_pnl"].as_f64().unwrap_or(0.0),
                    report["commissions"].as_f64().unwrap_or(0.0),
                    report["net_pnl"].as_f64().unwrap_or(0.0),
                    report["turnover"].as_f64().unwrap_or(0.0),
                    report["realized_trade_count"].as_i64().unwrap_or(0),
                    report["winning_trade_count"].as_i64().unwrap_or(0),
                    report["losing_trade_count"].as_i64().unwrap_or(0),
                    report["open_position_count"].as_i64().unwrap_or(0)
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_strategy_performance_snapshots(
        &self,
        strategy_id: uuid::Uuid,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT account_id, observed_at, base_currency, gross_pnl,
                        commissions, net_pnl, turnover, realized_trade_count,
                        winning_trade_count, losing_trade_count, open_position_count
                 FROM strategy_performance_snapshots WHERE strategy_id = ?
                 ORDER BY observed_at DESC LIMIT ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![strategy_id, limit.min(10_000) as i64], |row| {
                Ok(serde_json::json!({
                    "strategy_id": strategy_id,
                    "account": row.get::<_, String>(0)?,
                    "observed_at": row.get::<_, DateTime<Utc>>(1)?,
                    "base_currency": row.get::<_, String>(2)?,
                    "gross_pnl": row.get::<_, f64>(3)?,
                    "commissions": row.get::<_, f64>(4)?,
                    "net_pnl": row.get::<_, f64>(5)?,
                    "turnover": row.get::<_, f64>(6)?,
                    "realized_trade_count": row.get::<_, i64>(7)?,
                    "winning_trade_count": row.get::<_, i64>(8)?,
                    "losing_trade_count": row.get::<_, i64>(9)?,
                    "open_position_count": row.get::<_, i64>(10)?,
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_orders_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let total: i64 = self
            .connection
            .query_row("SELECT count(*) FROM orders", [], |row| row.get(0))
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let page_size = page_size.clamp(1, 500);
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        let mut statement = self
            .connection
            .prepare(
                "SELECT o.order_id, o.order_intent_id, o.broker_order_id, o.broker_perm_id,
                        o.connection_session_id, o.status, o.filled_quantity,
                        o.average_fill_price, o.created_at, o.updated_at,
                        oi.conid, i.symbol, i.description, i.exchange, i.primary_exchange,
                        o.remaining_quantity, o.last_fill_price, o.why_held,
                        o.market_cap_price,
                        (SELECT b.payload_json::VARCHAR
                         FROM broker_order_events b
                         WHERE ((b.connection_session_id = o.connection_session_id
                                 AND b.broker_order_id = o.broker_order_id)
                             OR (o.broker_perm_id IS NOT NULL AND o.broker_perm_id <> 0
                                 AND b.broker_perm_id = o.broker_perm_id))
                           AND b.event_type = 'open_order'
                         ORDER BY b.received_at DESC LIMIT 1)
                 FROM orders o
                 LEFT JOIN order_intents oi USING (order_intent_id)
                 LEFT JOIN instruments i ON i.conid = oi.conid
                 ORDER BY o.created_at DESC LIMIT ? OFFSET ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![page_size as i64, offset as i64], |row| {
                let order_id: uuid::Uuid = row.get(0)?;
                let intent_id: uuid::Uuid = row.get(1)?;
                let created_at: DateTime<Utc> = row.get(8)?;
                let updated_at: DateTime<Utc> = row.get(9)?;
                Ok(serde_json::json!({
                    "order_id": order_id,
                    "order_intent_id": intent_id,
                    "broker_order_id": row.get::<_, Option<i64>>(2)?,
                    "broker_perm_id": row.get::<_, Option<i64>>(3)?,
                    "connection_session_id": row.get::<_, Option<uuid::Uuid>>(4)?,
                    "status": row.get::<_, String>(5)?,
                    "filled_quantity": row.get::<_, f64>(6)?,
                    "average_fill_price": row.get::<_, Option<f64>>(7)?,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "conid": row.get::<_, Option<i64>>(10)?,
                    "symbol": row.get::<_, Option<String>>(11)?,
                    "description": row.get::<_, Option<String>>(12)?,
                    "exchange": row.get::<_, Option<String>>(13)?,
                    "primary_exchange": row.get::<_, Option<String>>(14)?,
                    "remaining_quantity": row.get::<_, Option<f64>>(15)?,
                    "last_fill_price": row.get::<_, Option<f64>>(16)?,
                    "why_held": row.get::<_, Option<String>>(17)?,
                    "market_cap_price": row.get::<_, Option<f64>>(18)?,
                    "latest_broker_event": row
                        .get::<_, Option<String>>(19)?
                        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok((rows, total.max(0) as usize))
    }

    pub fn list_executions_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let total: i64 = self
            .connection
            .query_row("SELECT count(*) FROM executions", [], |row| row.get(0))
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let page_size = page_size.clamp(1, 500);
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.broker_execution_id, e.order_id, e.conid, e.side, e.quantity,
                        e.price, e.commission, e.currency, e.executed_at, e.received_at,
                        e.connection_session_id, e.broker_perm_id,
                        i.symbol, i.description, i.exchange, i.primary_exchange
                 FROM executions e
                 LEFT JOIN instruments i ON i.conid = e.conid
                 ORDER BY e.received_at DESC LIMIT ? OFFSET ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![page_size as i64, offset as i64], |row| {
                let executed_at: DateTime<Utc> = row.get(8)?;
                let received_at: DateTime<Utc> = row.get(9)?;
                Ok(serde_json::json!({
                    "broker_execution_id": row.get::<_, String>(0)?,
                    "order_id": row.get::<_, Option<uuid::Uuid>>(1)?,
                    "conid": row.get::<_, i64>(2)?,
                    "side": row.get::<_, String>(3)?,
                    "quantity": row.get::<_, f64>(4)?,
                    "price": row.get::<_, f64>(5)?,
                    "commission": row.get::<_, Option<f64>>(6)?,
                    "currency": row.get::<_, Option<String>>(7)?,
                    "executed_at": executed_at,
                    "received_at": received_at
                    ,"connection_session_id": row.get::<_, Option<uuid::Uuid>>(10)?
                    ,"broker_perm_id": row.get::<_, Option<i64>>(11)?
                    ,"symbol": row.get::<_, Option<String>>(12)?
                    ,"description": row.get::<_, Option<String>>(13)?
                    ,"exchange": row.get::<_, Option<String>>(14)?
                    ,"primary_exchange": row.get::<_, Option<String>>(15)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok((rows, total.max(0) as usize))
    }

    pub fn reconcile(
        &mut self,
        snapshot: &crate::ibkr::ReconciliationSnapshot,
    ) -> Result<ReconciliationReport> {
        for event in &snapshot.events {
            self.apply_broker_event(event)?;
        }
        let reconciliation_id = uuid::Uuid::now_v7();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // A terminal OpenOrder event is authoritative even if a later IBKR
        // completed-orders snapshot no longer includes the order. Heal orders
        // left active by an older binary before comparing the current snapshot.
        transaction
            .execute(
                "UPDATE orders AS o SET
                   status = json_extract_string(b.payload_json, '$.status'),
                   broker_perm_id = CASE
                     WHEN b.broker_perm_id <> 0 THEN b.broker_perm_id
                     ELSE o.broker_perm_id
                   END,
                   updated_at = greatest(o.updated_at, b.received_at)
                 FROM broker_order_events AS b
                 WHERE b.connection_session_id = o.connection_session_id
                   AND b.broker_order_id = o.broker_order_id
                   AND b.event_type = 'open_order'
                   AND lower(json_extract_string(b.payload_json, '$.status')) IN
                     ('filled','cancelled','canceled','inactive','rejected')
                   AND lower(o.status) NOT IN
                     ('filled','cancelled','canceled','inactive','rejected')
                   AND b.received_at = (
                     SELECT max(latest.received_at)
                     FROM broker_order_events AS latest
                     WHERE latest.connection_session_id = o.connection_session_id
                       AND latest.broker_order_id = o.broker_order_id
                       AND latest.event_type = 'open_order'
                   )",
                [],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut external_order_count = 0;
        let mut blocking_difference_count = 0;

        for (source, order) in snapshot
            .open_orders
            .iter()
            .map(|order| ("open", order))
            .chain(
                snapshot
                    .completed_orders
                    .iter()
                    .map(|order| ("completed", order)),
            )
        {
            let mut local_order_id: Option<uuid::Uuid> = transaction
                .query_row(
                    "SELECT order_id FROM orders
                     WHERE (broker_perm_id = ? AND ? <> 0)
                        OR (connection_session_id = ? AND broker_order_id = ?)
                     ORDER BY created_at DESC LIMIT 1",
                    params![
                        order.perm_id,
                        order.perm_id,
                        snapshot.connection_session_id,
                        order.broker_order_id
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            // A completed-orders snapshot may report broker_order_id=-1 after
            // reconnect. Recover the local order through an OpenOrder event
            // that captured the stable Perm ID while the original session was
            // still active.
            if local_order_id.is_none() && order.perm_id != 0 {
                let mut event_statement = transaction
                    .prepare(
                        "SELECT DISTINCT o.order_id
                         FROM orders o
                         JOIN broker_order_events b
                           ON b.connection_session_id = o.connection_session_id
                          AND b.broker_order_id = o.broker_order_id
                         WHERE b.broker_perm_id = ?
                         ORDER BY o.order_id
                         LIMIT 2",
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let candidates = event_statement
                    .query_map(params![order.perm_id], |row| row.get::<_, uuid::Uuid>(0))
                    .map_err(|error| AppError::Storage(error.to_string()))?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                drop(event_statement);
                if candidates.len() == 1 {
                    local_order_id = candidates.first().copied();
                }
            }
            // Some orders created before a disconnect may not have received an
            // OrderStatus event carrying IBKR's stable Perm ID. For an order
            // that IBKR currently reports as open or completed, allow a conservative
            // fallback match only when broker id, account, contract, side and
            // quantity identify exactly one locally active order. Ambiguous
            // candidates remain external and cannot be cancelled implicitly.
            if local_order_id.is_none() {
                let mut fallback_statement = transaction
                    .prepare(
                        "SELECT o.order_id
                         FROM orders o
                         JOIN order_intents i USING (order_intent_id)
                         WHERE o.broker_order_id = ?
                           AND i.account_id = ?
                           AND i.conid = ?
                           AND lower(json_extract_string(i.payload_json, '$.side')) = lower(?)
                           AND abs(
                               try_cast(json_extract_string(i.payload_json, '$.quantity') AS DOUBLE)
                               - ?
                           ) < 0.000000001
                           AND lower(o.status) IN (
                               'submitted','presubmitted','pendingsubmit',
                               'pendingcancel','cancel_pending','apipending'
                           )
                         ORDER BY o.created_at DESC
                         LIMIT 2",
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let candidates = fallback_statement
                    .query_map(
                        params![
                            order.broker_order_id,
                            order.account,
                            order.conid,
                            order.side,
                            order.quantity
                        ],
                        |row| row.get::<_, uuid::Uuid>(0),
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                drop(fallback_statement);
                if candidates.len() == 1 {
                    local_order_id = candidates.first().copied();
                }
            }
            let is_external = local_order_id.is_none();
            if is_external {
                external_order_count += 1;
                let severity = if source == "open" {
                    blocking_difference_count += 1;
                    "blocking"
                } else {
                    "informational"
                };
                transaction
                    .execute(
                        "INSERT INTO reconciliation_differences
                         (difference_id, reconciliation_id, difference_type, severity,
                          broker_order_id, broker_perm_id, local_order_id, detail, created_at,
                          disposition)
                         VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 'open')",
                        params![
                            uuid::Uuid::now_v7(),
                            reconciliation_id,
                            format!("external_{source}_order"),
                            severity,
                            order.broker_order_id,
                            order.perm_id,
                            format!(
                                "IBKR {source} order is not known locally: {} {} {}",
                                order.symbol, order.side, order.quantity
                            ),
                            snapshot.completed_at
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            } else {
                transaction
                    .execute(
                        "UPDATE orders SET status = ?, broker_perm_id = ?,
                             connection_session_id = CASE WHEN ? = 'open' THEN ? ELSE connection_session_id END,
                             broker_order_id = CASE WHEN ? = 'open' THEN ? ELSE broker_order_id END,
                             updated_at = ?
                         WHERE order_id = ?",
                        params![
                            order.status,
                            order.perm_id,
                            source,
                            snapshot.connection_session_id,
                            source,
                            order.broker_order_id,
                            snapshot.completed_at,
                            local_order_id
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            }
            transaction
                .execute(
                    "INSERT INTO broker_order_snapshots
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        reconciliation_id,
                        source,
                        order.broker_order_id,
                        order.perm_id,
                        order.client_id,
                        order.account,
                        order.conid,
                        order.symbol,
                        order.side,
                        order.quantity,
                        order.order_type,
                        order.limit_price,
                        order.status,
                        order.completed_time,
                        local_order_id,
                        is_external,
                        snapshot.completed_at
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }

        let open_ids: std::collections::HashSet<i32> = snapshot
            .open_orders
            .iter()
            .map(|order| order.broker_order_id)
            .collect();
        let open_perm_ids: std::collections::HashSet<i64> = snapshot
            .open_orders
            .iter()
            .filter_map(|order| (order.perm_id != 0).then_some(order.perm_id))
            .collect();
        let mut statement = transaction
            .prepare(
                "SELECT order_id, connection_session_id, broker_order_id, broker_perm_id FROM orders
                 WHERE broker_order_id IS NOT NULL
                   AND lower(status) NOT IN ('filled', 'cancelled', 'canceled', 'inactive', 'rejected')",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let candidates: Vec<(uuid::Uuid, Option<uuid::Uuid>, i32, Option<i64>)> = statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        let unresolved: Vec<_> = candidates
            .iter()
            .filter(|(_, connection_session_id, broker_order_id, perm_id)| {
                !(connection_session_id == &Some(snapshot.connection_session_id)
                    && open_ids.contains(broker_order_id))
                    && !perm_id.is_some_and(|id| open_perm_ids.contains(&id))
            })
            .collect();
        let unresolved_local_count = unresolved.len();
        blocking_difference_count += unresolved_local_count;
        for (order_id, _, broker_order_id, perm_id) in unresolved {
            transaction
                .execute(
                    "INSERT INTO reconciliation_differences
                     (difference_id, reconciliation_id, difference_type, severity,
                      broker_order_id, broker_perm_id, local_order_id, detail, created_at,
                      disposition)
                     VALUES (?, ?, 'missing_broker_order', 'blocking', ?, ?, ?, ?, ?, 'open')",
                    params![
                        uuid::Uuid::now_v7(),
                        reconciliation_id,
                        broker_order_id,
                        perm_id,
                        order_id,
                        "locally active order is absent from IBKR open orders",
                        snapshot.completed_at
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        let healthy = blocking_difference_count == 0;
        transaction
            .execute(
                "INSERT INTO reconciliation_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    reconciliation_id,
                    snapshot.connection_session_id,
                    if healthy { "healthy" } else { "degraded" },
                    snapshot.open_orders.len() as i64,
                    snapshot.completed_orders.len() as i64,
                    snapshot.events.len() as i64,
                    external_order_count as i64,
                    blocking_difference_count as i64,
                    snapshot.completed_at
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(ReconciliationReport {
            reconciliation_id,
            healthy,
            open_order_count: snapshot.open_orders.len(),
            completed_order_count: snapshot.completed_orders.len(),
            recovered_event_count: snapshot.events.len(),
            external_order_count,
            blocking_difference_count,
            unresolved_local_count,
            completed_at: snapshot.completed_at,
        })
    }

    pub fn reconciliation_health(
        &self,
        connection_session_id: Option<uuid::Uuid>,
    ) -> Result<ReconciliationHealth> {
        let Some(connection_session_id) = connection_session_id else {
            return Ok(ReconciliationHealth {
                state: "pending",
                reconciliation_id: None,
                connection_session_id: None,
                blocking_difference_count: 0,
                completed_at: None,
            });
        };
        let row = self
            .connection
            .query_row(
                "SELECT reconciliation_id, status, blocking_difference_count, completed_at
                 FROM reconciliation_runs WHERE connection_session_id = ?
                 ORDER BY completed_at DESC LIMIT 1",
                params![connection_session_id],
                |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, DateTime<Utc>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(match row {
            Some((id, status, count, completed_at)) => ReconciliationHealth {
                state: if status == "healthy" {
                    "healthy"
                } else {
                    "degraded"
                },
                reconciliation_id: Some(id),
                connection_session_id: Some(connection_session_id),
                blocking_difference_count: count as usize,
                completed_at: Some(completed_at),
            },
            None => ReconciliationHealth {
                state: "pending",
                reconciliation_id: None,
                connection_session_id: Some(connection_session_id),
                blocking_difference_count: 0,
                completed_at: None,
            },
        })
    }

    pub fn list_reconciliation_differences(&self) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT difference_id, reconciliation_id, difference_type, severity,
                        broker_order_id, broker_perm_id, local_order_id, detail, created_at,
                        disposition, disposition_note, disposition_at
                 FROM reconciliation_differences ORDER BY created_at DESC LIMIT 200",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok(serde_json::json!({
                    "difference_id": row.get::<_, uuid::Uuid>(0)?,
                    "reconciliation_id": row.get::<_, uuid::Uuid>(1)?,
                    "difference_type": row.get::<_, String>(2)?,
                    "severity": row.get::<_, String>(3)?,
                    "broker_order_id": row.get::<_, Option<i64>>(4)?,
                    "broker_perm_id": row.get::<_, Option<i64>>(5)?,
                    "local_order_id": row.get::<_, Option<uuid::Uuid>>(6)?,
                    "detail": row.get::<_, String>(7)?,
                    "created_at": row.get::<_, DateTime<Utc>>(8)?,
                    "disposition": row.get::<_, String>(9)?,
                    "disposition_note": row.get::<_, Option<String>>(10)?,
                    "disposition_at": row.get::<_, Option<DateTime<Utc>>>(11)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }
}

fn portfolio_reject(
    mut decision: PortfolioRiskDecision,
    reason_code: &'static str,
    detail: String,
) -> PortfolioRiskDecision {
    decision.allowed = false;
    decision.reason_code = reason_code;
    decision.detail = detail;
    decision
}

fn timeframe_duration(timeframe: &str) -> Result<chrono::Duration> {
    match timeframe {
        "5s" => Ok(chrono::Duration::seconds(5)),
        "1m" => Ok(chrono::Duration::minutes(1)),
        "5m" => Ok(chrono::Duration::minutes(5)),
        "15m" => Ok(chrono::Duration::minutes(15)),
        "30m" => Ok(chrono::Duration::minutes(30)),
        "1h" => Ok(chrono::Duration::hours(1)),
        "1d" => Ok(chrono::Duration::days(1)),
        _ => Err(AppError::Storage(format!(
            "unsupported timeframe: {timeframe}"
        ))),
    }
}

fn serialize_strategy_state(state: &serde_json::Value) -> Result<String> {
    let serialized = serde_json::to_string(state)?;
    if serialized.len() > MAX_STRATEGY_STATE_BYTES {
        return Err(AppError::Storage(format!(
            "strategy runtime state is {} bytes; maximum is {} bytes",
            serialized.len(),
            MAX_STRATEGY_STATE_BYTES
        )));
    }
    Ok(serialized)
}

fn validate_backtest_request(request: &BacktestRequest) -> Result<()> {
    if request.conid <= 0
        || request.end <= request.start
        || request.quantity <= 0.0
        || request.initial_cash <= 0.0
        || request.slippage_bps < 0.0
        || request.commission_per_order < 0.0
    {
        return Err(AppError::Storage("invalid backtest parameters".into()));
    }
    let strategy = build_backtest_strategy(request)?;
    if strategy.conid() != request.conid {
        return Err(AppError::Storage(
            "backtest conid must match strategy config conid".into(),
        ));
    }
    Ok(())
}

fn build_backtest_strategy(
    request: &BacktestRequest,
) -> Result<Box<dyn crate::strategy::Strategy>> {
    let config = match &request.strategy_config {
        Some(config) => config.clone(),
        None if request.strategy_kind == "moving_average_cross" => serde_json::json!({
            "conid": request.conid,
            "short_window": request.short_window.ok_or_else(|| {
                AppError::Storage("short_window is required for moving_average_cross".into())
            })?,
            "long_window": request.long_window.ok_or_else(|| {
                AppError::Storage("long_window is required for moving_average_cross".into())
            })?
        }),
        None => {
            return Err(AppError::Storage(
                "strategy_config is required for this strategy kind".into(),
            ));
        }
    };
    crate::strategy::build(&request.strategy_kind, config).map_err(AppError::Storage)
}

fn simulate_strategy(
    request: &BacktestRequest,
    strategy: &dyn crate::strategy::Strategy,
    bars: &[BacktestBar],
) -> Result<(Vec<SimulatedTrade>, Vec<EquityPoint>, serde_json::Value)> {
    if bars.len() < strategy.minimum_history() + 1 {
        return Err(AppError::Storage(format!(
            "backtest needs at least {} bars, found {}",
            strategy.minimum_history() + 1,
            bars.len()
        )));
    }
    let mut cash: f64 = request.initial_cash;
    let mut position: f64 = 0.0;
    let mut trades = Vec::new();
    let mut equity = Vec::new();
    let mut pending: Option<(&'static str, DateTime<Utc>)> = None;
    let mut history = Vec::with_capacity(bars.len());
    let mut strategy_state = strategy.initial_state();
    for bar in bars {
        if let Some((side, signal_time)) = pending.take() {
            let direction = if side == "buy" { 1.0 } else { -1.0 };
            let slippage = bar.open * request.slippage_bps / 10_000.0 * direction;
            let fill_price = bar.open + slippage;
            let quantity = if side == "buy" {
                request.quantity
            } else {
                position.min(request.quantity)
            };
            if quantity > 0.0 {
                let cash_change = fill_price * quantity;
                if side == "buy"
                    && cash >= cash_change + request.commission_per_order
                    && position == 0.0
                {
                    cash -= cash_change + request.commission_per_order;
                    position += quantity;
                } else if side == "sell" && position > 0.0 {
                    cash += cash_change - request.commission_per_order;
                    position -= quantity;
                } else {
                    history.push(crate::strategy::StrategyBar {
                        time: bar.open_time,
                        open: bar.open,
                        high: bar.high,
                        low: bar.low,
                        close: bar.close,
                        volume: bar.volume,
                    });
                    equity.push(EquityPoint {
                        observed_at: bar.open_time,
                        cash,
                        position,
                        close: bar.close,
                        equity: cash + position * bar.close,
                    });
                    continue;
                }
                trades.push(SimulatedTrade {
                    signal_time,
                    fill_time: bar.open_time,
                    side,
                    quantity,
                    price: fill_price,
                    commission: request.commission_per_order,
                    slippage: slippage.abs() * quantity,
                });
            }
        }
        history.push(crate::strategy::StrategyBar {
            time: bar.open_time,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
        });
        if history.len() >= strategy.minimum_history() {
            // Live evaluation intentionally supplies exactly minimum_history
            // bars. Match that contract here so an expanding backtest does not
            // repeatedly copy and scan its entire history on every bar.
            let evaluation_start = history.len() - strategy.minimum_history();
            let transition = strategy
                .evaluate_with_state(&history[evaluation_start..], &strategy_state)
                .map_err(AppError::Storage)?;
            strategy_state = transition.next_state;
            let output = transition.output;
            if output.signal == crate::strategy::StrategySignal::Buy && position == 0.0 {
                pending = Some(("buy", bar.open_time));
            } else if output.signal == crate::strategy::StrategySignal::Sell && position > 0.0 {
                pending = Some(("sell", bar.open_time));
            }
        }
        equity.push(EquityPoint {
            observed_at: bar.open_time,
            cash,
            position,
            close: bar.close,
            equity: cash + position * bar.close,
        });
    }
    let final_equity = equity.last().expect("validated bars").equity;
    let total_return = final_equity / request.initial_cash - 1.0;
    let mut peak = request.initial_cash;
    let mut maximum_drawdown = 0.0_f64;
    let mut returns = Vec::new();
    for window in equity.windows(2) {
        peak = peak.max(window[1].equity);
        maximum_drawdown = maximum_drawdown.max((peak - window[1].equity) / peak.max(f64::EPSILON));
        if window[0].equity != 0.0 {
            returns.push(window[1].equity / window[0].equity - 1.0);
        }
    }
    let mean_return = if returns.is_empty() {
        0.0
    } else {
        returns.iter().sum::<f64>() / returns.len() as f64
    };
    let volatility = if returns.len() < 2 {
        0.0
    } else {
        (returns
            .iter()
            .map(|value| (value - mean_return).powi(2))
            .sum::<f64>()
            / (returns.len() - 1) as f64)
            .sqrt()
    };
    let traded_notional = trades
        .iter()
        .map(|trade| trade.price * trade.quantity)
        .sum::<f64>();
    let metrics = serde_json::json!({
        "bar_count": bars.len(),
        "trade_count": trades.len(),
        "initial_cash": request.initial_cash,
        "final_equity": final_equity,
        "total_return": total_return,
        "bar_return_volatility": volatility,
        "maximum_drawdown": maximum_drawdown,
        "turnover": traded_notional / request.initial_cash,
        "open_position": position,
        "pending_signal_discarded_at_end": pending.is_some()
    });
    Ok((trades, equity, metrics))
}

fn validate_bars(bars: &[crate::ibkr::HistoricalBar]) -> Result<()> {
    let first = &bars[0];
    let mut previous = None;
    for bar in bars {
        if bar.conid != first.conid || bar.timeframe != first.timeframe {
            return Err(AppError::Storage(
                "historical batch mixes contracts or timeframes".into(),
            ));
        }
        if ![bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap]
            .iter()
            .all(|value| value.is_finite())
            || bar.low > bar.open
            || bar.low > bar.close
            || bar.high < bar.open
            || bar.high < bar.close
            || bar.volume < 0.0
        {
            return Err(AppError::Storage(format!(
                "invalid bar at {}",
                bar.open_time
            )));
        }
        if previous.is_some_and(|time| time >= bar.open_time) {
            return Err(AppError::Storage(
                "historical bars are duplicated or not ordered".into(),
            ));
        }
        previous = Some(bar.open_time);
    }
    Ok(())
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn file_checksum(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = 0xcbf29ce484222325_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        for byte in &buffer[..count] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    Ok(format!("fnv1a64:{hash:016x}"))
}

fn directory_size(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        match fs::read_dir(directory) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let metadata = entry.metadata()?;
                    if metadata.is_dir() {
                        pending.push(entry.path());
                    } else {
                        total = total.saturating_add(metadata.len());
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_migrates_database() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let current_version = MIGRATIONS.last().map(|(version, _)| *version).unwrap_or(0);
        assert_eq!(storage.schema_version().unwrap(), current_version);
    }

    #[test]
    fn refuses_to_open_a_database_from_a_newer_binary() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.duckdb");
        {
            let storage = Storage::open(&path).unwrap();
            storage
                .connection
                .execute(
                    "INSERT INTO schema_migrations VALUES (9999, ?)",
                    params![Utc::now()],
                )
                .unwrap();
        }
        let error = match Storage::open(&path) {
            Ok(_) => panic!("opening a newer schema version must fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("newer than the maximum supported")
        );
    }

    fn test_bar(open_time: DateTime<Utc>, close: f64) -> crate::ibkr::HistoricalBar {
        crate::ibkr::HistoricalBar {
            conid: 756733,
            timeframe: "1d".into(),
            open_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            wap: close,
            trade_count: 1,
        }
    }

    #[test]
    fn repeated_backfill_deactivates_fully_covered_files() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let lake = directory.path().join("lake");
        let staging = directory.path().join("staging");
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars: Vec<_> = (0..3)
            .map(|day| test_bar(start + chrono::Duration::days(day), 100.0 + day as f64))
            .collect();
        let first = storage
            .write_historical_bars(&lake, &staging, &bars)
            .unwrap();
        let second = storage
            .write_historical_bars(&lake, &staging, &bars)
            .unwrap();
        assert_ne!(first.file_id, second.file_id);
        let active: Vec<uuid::Uuid> = {
            let mut statement = storage
                .connection
                .prepare("SELECT file_id FROM dataset_files WHERE active = true")
                .unwrap();
            let rows = statement
                .query_map([], |row| row.get::<_, uuid::Uuid>(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            rows
        };
        assert_eq!(active, vec![second.file_id]);
    }

    #[test]
    fn failed_backtest_is_recorded_as_failed_not_running() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let lake = directory.path().join("lake");
        let staging = directory.path().join("staging");
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars: Vec<_> = (0..3)
            .map(|day| test_bar(start + chrono::Duration::days(day), 100.0 + day as f64))
            .collect();
        storage
            .write_historical_bars(&lake, &staging, &bars)
            .unwrap();
        let request = BacktestRequest {
            strategy_id: None,
            conid: 756733,
            timeframe: "1d".into(),
            start,
            end: start + chrono::Duration::days(3),
            // Not enough bars for the requested windows: the simulation fails
            // after request validation, exercising the failed-run audit path.
            short_window: Some(2),
            long_window: Some(10),
            strategy_kind: "moving_average_cross".into(),
            strategy_config: None,
            quantity: 1.0,
            initial_cash: 100.0,
            slippage_bps: 0.0,
            commission_per_order: 0.0,
            seed: 1,
        };
        assert!(
            storage
                .run_moving_average_backtest(&lake, &request)
                .is_err()
        );
        let runs = storage.list_backtests().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["state"], "failed");
        assert!(runs[0]["error"].as_str().is_some());
        let backtest_id = runs[0]["backtest_id"]
            .as_str()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .unwrap();
        let details = storage.backtest_details(backtest_id).unwrap().unwrap();
        assert_eq!(details["state"], "failed");
        assert_eq!(details["trades"], serde_json::json!([]));
        assert_eq!(details["equity"], serde_json::json!([]));
    }

    #[test]
    fn successful_backtest_bulk_persists_trades_and_equity() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let lake = directory.path().join("lake");
        let staging = directory.path().join("staging");
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [3.0, 2.0, 1.0, 4.0, 5.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| test_bar(start + chrono::Duration::days(index as i64), close))
            .collect::<Vec<_>>();
        // Two fragments exercise the combined read_parquet([...]) scan.
        storage
            .write_historical_bars(&lake, &staging, &bars[..3])
            .unwrap();
        storage
            .write_historical_bars(&lake, &staging, &bars[3..])
            .unwrap();
        let request = BacktestRequest {
            strategy_id: None,
            conid: 756733,
            timeframe: "1d".into(),
            start,
            end: start + chrono::Duration::days(5),
            short_window: Some(2),
            long_window: Some(3),
            strategy_kind: "moving_average_cross".into(),
            strategy_config: None,
            quantity: 1.0,
            initial_cash: 100.0,
            slippage_bps: 0.0,
            commission_per_order: 1.0,
            seed: 1,
        };

        let result = storage
            .run_moving_average_backtest(&lake, &request)
            .unwrap();
        let backtest_id = uuid::Uuid::parse_str(result["backtest_id"].as_str().unwrap()).unwrap();
        let details = storage.backtest_details(backtest_id).unwrap().unwrap();
        assert_eq!(details["state"], "completed");
        assert_eq!(details["metrics"]["bar_count"], 5);
        assert_eq!(details["equity"].as_array().unwrap().len(), 5);
        assert_eq!(details["trades"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn existing_strategy_is_authoritative_for_backtest_security_and_timeframe() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "msft-v2",
                "moving_average_cross_v2",
                &serde_json::json!({
                    "conid": 272093,
                    "short_window": 5,
                    "long_window": 20,
                    "bar_timeframe": "5s"
                }),
            )
            .unwrap();
        let start = Utc::now();
        let request = BacktestRequest {
            strategy_id: Some(strategy_id),
            conid: 756733,
            timeframe: "1d".into(),
            start,
            end: start + chrono::Duration::hours(1),
            short_window: Some(1),
            long_window: Some(2),
            strategy_kind: "close_threshold".into(),
            strategy_config: Some(serde_json::json!({
                "conid": 756733,
                "buy_below": 1,
                "sell_above": 2
            })),
            quantity: 1.0,
            initial_cash: 100_000.0,
            slippage_bps: 0.0,
            commission_per_order: 0.0,
            seed: 42,
        };

        let resolved = storage.resolve_backtest_request(&request).unwrap();
        assert_eq!(resolved.strategy_id, Some(strategy_id));
        assert_eq!(resolved.conid, 272093);
        assert_eq!(resolved.timeframe, "5s");
        assert_eq!(resolved.strategy_kind, "moving_average_cross_v2");
        assert_eq!(resolved.strategy_config.unwrap()["conid"], 272093);
        assert_eq!(resolved.short_window, None);
        assert_eq!(resolved.long_window, None);
    }

    #[test]
    fn execution_cost_models_are_database_managed() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let model_id = storage
            .upsert_execution_cost_model(&ExecutionCostModelInput {
                cost_model_id: None,
                name: "mixed-fees".into(),
                currency: "hkd".into(),
                buy_fixed_fee: 1.0,
                buy_per_share_fee: 0.005,
                buy_rate_bps: 5.0,
                buy_min_fee: 15.0,
                sell_fixed_fee: 1.0,
                sell_per_share_fee: 0.005,
                sell_rate_bps: 5.0,
                sell_min_fee: 15.0,
                sell_tax_bps: 10.0,
                estimated_spread_bps: 4.0,
                estimated_slippage_bps: 3.0,
            })
            .unwrap();
        let strategy_id = storage
            .create_strategy(
                "cost-aware",
                "moving_average_cross",
                &serde_json::json!({
                    "conid": 272093,
                    "short_window": 5,
                    "long_window": 20
                }),
            )
            .unwrap();
        storage
            .configure_strategy_cost_control(&StrategyCostControlInput {
                strategy_id,
                enabled: true,
                cost_model_id: model_id,
                minimum_cost_multiple: 2.0,
                maximum_commission_to_gross_profit_ratio: 0.5,
                minimum_completed_trades: 5,
            })
            .unwrap();

        let models = storage.list_execution_cost_models().unwrap();
        assert_eq!(models[0]["currency"], "HKD");
        assert_eq!(models[0]["buy_min_fee"], 15.0);
        assert_eq!(models[0]["buy_per_share_fee"], 0.005);
        let controls = storage.list_strategy_cost_controls().unwrap();
        assert_eq!(controls[0]["strategy_id"], strategy_id.to_string());
        assert_eq!(controls[0]["cost_model_id"], model_id.to_string());
        assert!(storage.delete_execution_cost_model(model_id).is_err());
    }

    #[test]
    fn completed_position_snapshot_refreshes_absent_positions_to_zero() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let old = Utc::now() - chrono::Duration::hours(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 2.0,
                    average_cost: 500.0,
                    observed_at: old,
                },
            })
            .unwrap();
        let started = Utc::now();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                observed_at: started,
            })
            .unwrap();
        assert_eq!(storage.list_positions().unwrap()[0]["quantity"], 0.0);
        let completed = started + chrono::Duration::milliseconds(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                observed_at: completed,
            })
            .unwrap();
        let position = &storage.list_positions().unwrap()[0];
        assert_eq!(position["quantity"], 0.0);
        assert_eq!(
            serde_json::from_value::<DateTime<Utc>>(position["observed_at"].clone())
                .unwrap()
                .timestamp_micros(),
            completed.timestamp_micros()
        );
    }

    #[test]
    fn completed_empty_position_snapshot_allows_opening_risk() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Pnl {
                account: "DU123".into(),
                daily_pnl: 0.0,
                unrealized_pnl: Some(0.0),
                realized_pnl: Some(0.0),
                observed_at: now,
            })
            .unwrap();

        let request = crate::ibkr::BrokerOrderRequest {
            contract: crate::ibkr::ContractCandidate {
                conid: 272093,
                symbol: "MSFT".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "NASDAQ".into(),
                local_symbol: "MSFT".into(),
                description: String::new(),
                derivative_security_types: Vec::new(),
            },
            side: "buy".into(),
            quantity: 10.0,
            order_type: "market".into(),
            limit_price: None,
            outside_rth: false,
        };
        let config = crate::config::RiskConfig::default();
        let decision = storage
            .evaluate_portfolio_risk(
                &config,
                "DU123",
                &request,
                Some(100.0),
                Some(100.0),
                false,
                now,
            )
            .unwrap();

        assert!(decision.allowed, "{decision:?}");
        assert_eq!(
            decision
                .positions_observed_at
                .map(|time| time.timestamp_micros()),
            Some(now.timestamp_micros())
        );

        let stale = storage
            .evaluate_portfolio_risk(
                &config,
                "DU123",
                &request,
                Some(100.0),
                Some(100.0),
                false,
                now + chrono::Duration::seconds(config.max_account_data_age_seconds as i64 + 1),
            )
            .unwrap();
        assert_eq!(stale.reason_code, "POSITION_DATA_UNAVAILABLE");
    }

    #[test]
    fn emergency_stop_is_persistent_and_pauses_strategy_work() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let control = storage
            .set_trading_control("emergency_stop", "test drill")
            .unwrap();
        assert_eq!(control["emergency_stop"], true);
        assert_eq!(control["reject_new_orders"], true);
        assert_eq!(control["pause_strategies"], true);
        assert_eq!(storage.evaluate_running_strategies().unwrap(), 0);
        drop(storage);
        let mut reopened = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        assert_eq!(reopened.trading_control().unwrap()["emergency_stop"], true);
        assert_eq!(
            reopened
                .set_trading_control("normal", "test reset")
                .unwrap()["emergency_stop"],
            false
        );
    }

    #[test]
    fn strategy_deletion_requires_stopped_state_and_removes_definition() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "delete test",
                "moving_average_cross",
                &serde_json::json!({
                    "conid": 756733,
                    "short_window": 2,
                    "long_window": 3
                }),
            )
            .unwrap();

        storage.set_strategy_state(strategy_id, "running").unwrap();
        assert!(storage.delete_strategy(strategy_id).is_err());
        storage.set_strategy_state(strategy_id, "stopped").unwrap();
        assert!(storage.delete_strategy(strategy_id).unwrap());
        assert!(
            !storage
                .list_strategies()
                .unwrap()
                .iter()
                .any(|strategy| strategy["strategy_id"] == strategy_id.to_string())
        );
        assert!(!storage.delete_strategy(strategy_id).unwrap());
    }

    #[test]
    fn strategy_rename_preserves_identity_and_requires_a_unique_non_empty_name() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let config = serde_json::json!({
            "conid": 756733,
            "short_window": 2,
            "long_window": 3
        });
        let strategy_id = storage
            .create_strategy("original name", "moving_average_cross", &config)
            .unwrap();
        storage
            .create_strategy("existing name", "moving_average_cross", &config)
            .unwrap();

        assert!(
            storage
                .rename_strategy(strategy_id, "  renamed strategy  ")
                .unwrap()
        );
        let renamed = storage
            .list_strategies()
            .unwrap()
            .into_iter()
            .find(|strategy| strategy["strategy_id"] == strategy_id.to_string())
            .unwrap();
        assert_eq!(renamed["name"], "renamed strategy");
        assert!(storage.rename_strategy(strategy_id, " ").is_err());
        assert!(
            storage
                .rename_strategy(strategy_id, "existing name")
                .is_err()
        );
        assert!(
            !storage
                .rename_strategy(uuid::Uuid::now_v7(), "missing strategy")
                .unwrap()
        );
    }

    #[test]
    fn moving_average_strategy_persists_once_per_final_bar() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "test crossover",
                "moving_average_cross",
                &serde_json::json!({
                    "conid": 756733,
                    "short_window": 2,
                    "long_window": 3
                }),
            )
            .unwrap();
        assert!(storage.set_strategy_state(strategy_id, "running").unwrap());
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for (offset, close) in [3.0, 2.0, 1.0, 4.0].into_iter().enumerate() {
            let time = start + chrono::Duration::minutes(offset as i64);
            storage
                .connection
                .execute(
                    "INSERT INTO market_minute_bars VALUES (?, ?, ?, ?, ?, ?, 1, true, ?)",
                    params![756733, time, close, close, close, close, time],
                )
                .unwrap();
        }
        assert_eq!(storage.evaluate_running_strategies().unwrap(), 1);
        assert_eq!(storage.evaluate_running_strategies().unwrap(), 0);
        let evaluations = storage.list_strategy_evaluations(strategy_id, 10).unwrap();
        assert_eq!(evaluations.len(), 1);
        assert_eq!(evaluations[0]["signal"], "buy");
        assert_eq!(evaluations[0]["output"]["short_window"], 2);
    }

    #[test]
    fn strategy_runtime_state_survives_database_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("state.duckdb");
        let strategy_id;
        {
            let mut storage = Storage::open(&database).unwrap();
            strategy_id = storage
                .create_strategy(
                    "stateful paper strategy",
                    "paper_round_trip",
                    &serde_json::json!({"conid": 12087792, "phase_bars": 1}),
                )
                .unwrap();
            storage.set_strategy_state(strategy_id, "running").unwrap();
            let time = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
            storage
                .connection
                .execute(
                    "INSERT INTO market_minute_bars
                     VALUES (12087792, ?, 1, 1, 1, 1, 1, true, ?)",
                    params![time, time],
                )
                .unwrap();
            assert_eq!(storage.evaluate_running_strategies().unwrap(), 1);
        }

        let mut storage = Storage::open(&database).unwrap();
        let next_time = DateTime::from_timestamp(1_700_000_060, 0).unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO market_minute_bars
                 VALUES (12087792, ?, 1, 1, 1, 1, 1, true, ?)",
                params![next_time, next_time],
            )
            .unwrap();
        assert_eq!(storage.evaluate_running_strategies().unwrap(), 1);
        let (state, version, revision, last_bar): (String, i64, i64, DateTime<Utc>) = storage
            .connection
            .query_row(
                "SELECT state_json::VARCHAR, state_version, revision, last_transition_bar
                 FROM strategy_runtime_states WHERE strategy_id = ?",
                params![strategy_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let state: serde_json::Value = serde_json::from_str(&state).unwrap();
        assert_eq!(version, 1);
        assert_eq!(revision, 2);
        assert_eq!(state["evaluation_count"], 2);
        assert_eq!(last_bar, next_time);
        assert_eq!(
            storage
                .list_strategy_evaluations(strategy_id, 10)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn strategy_runtime_state_version_mismatch_fails_without_advancing_state() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "incompatible state",
                "paper_round_trip",
                &serde_json::json!({"conid": 12087792, "phase_bars": 1}),
            )
            .unwrap();
        storage.set_strategy_state(strategy_id, "running").unwrap();
        storage
            .connection
            .execute(
                "UPDATE strategy_runtime_states SET state_version = 999
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let time = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO market_minute_bars
                 VALUES (12087792, ?, 1, 1, 1, 1, 1, true, ?)",
                params![time, time],
            )
            .unwrap();

        assert_eq!(storage.evaluate_running_strategies().unwrap(), 0);
        assert!(
            storage
                .list_strategy_evaluations(strategy_id, 10)
                .unwrap()
                .is_empty()
        );
        let revision: i64 = storage
            .connection
            .query_row(
                "SELECT revision FROM strategy_runtime_states WHERE strategy_id = ?",
                params![strategy_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revision, 0);
        let strategy = storage
            .list_strategies()
            .unwrap()
            .into_iter()
            .find(|strategy| strategy["strategy_id"] == strategy_id.to_string())
            .unwrap();
        assert!(
            strategy["last_error"]
                .as_str()
                .unwrap()
                .contains("does not match engine version")
        );
    }

    #[test]
    fn five_second_moving_average_reads_only_final_five_second_bars() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "five second crossover",
                "moving_average_cross_5s",
                &serde_json::json!({
                    "conid": 756733,
                    "short_window": 2,
                    "long_window": 3
                }),
            )
            .unwrap();
        storage.set_strategy_state(strategy_id, "running").unwrap();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for (offset, close) in [3.0, 2.0, 1.0, 4.0].into_iter().enumerate() {
            let time = start + chrono::Duration::seconds(offset as i64 * 5);
            storage
                .connection
                .execute(
                    "INSERT INTO market_five_second_bars
                     VALUES (?, ?, ?, ?, ?, ?, 1, true, ?)",
                    params![756733, time, close, close, close, close, time],
                )
                .unwrap();
        }
        // A newer unfinished bucket must not be evaluated.
        let unfinished = start + chrono::Duration::seconds(20);
        storage
            .connection
            .execute(
                "INSERT INTO market_five_second_bars
                 VALUES (756733, ?, 99, 99, 99, 99, 1, false, ?)",
                params![unfinished, unfinished],
            )
            .unwrap();

        assert_eq!(storage.evaluate_running_strategies().unwrap(), 1);
        assert_eq!(storage.evaluate_running_strategies().unwrap(), 0);
        let evaluations = storage.list_strategy_evaluations(strategy_id, 10).unwrap();
        assert_eq!(evaluations.len(), 1);
        assert_eq!(evaluations[0]["signal"], "buy");
        assert_eq!(
            evaluations[0]["bar_time"],
            serde_json::json!(start + chrono::Duration::seconds(15))
        );
        assert_eq!(evaluations[0]["output"]["timeframe"], "5s");
    }

    #[test]
    fn strategy_execution_claims_each_signal_once_and_targets_position() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "execution test",
                "close_threshold",
                &serde_json::json!({
                    "conid": 756733,
                    "buy_below": 100.0,
                    "sell_above": 200.0
                }),
            )
            .unwrap();
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 3.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: crate::ibkr::ContractCandidate {
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "SMART".into(),
                    primary_exchange: "ARCA".into(),
                    local_symbol: "SPY".into(),
                    description: String::new(),
                    derivative_security_types: Vec::new(),
                },
            })
            .unwrap();
        assert!(
            storage
                .set_strategy_execution_enabled(strategy_id, true)
                .unwrap()
        );
        let evaluation_id = uuid::Uuid::now_v7();
        let now = Utc::now() + chrono::Duration::milliseconds(1);
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 90, 100, 90, 200, 'buy', ?, '{}')",
                params![evaluation_id, strategy_id, now, now],
            )
            .unwrap();
        let action = storage.claim_strategy_action().unwrap().unwrap();
        assert_eq!(action.evaluation_id, evaluation_id);
        assert_eq!(action.quantity, 3.0);
        assert!(storage.claim_strategy_action().unwrap().is_none());
        assert_eq!(
            storage.list_strategy_execution_actions(10).unwrap().len(),
            1
        );
        drop(storage);
        let reopened = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let actions = reopened.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["state"], "failed");
        assert!(
            actions[0]["detail"]
                .as_str()
                .unwrap()
                .contains("manual review")
        );
    }

    #[test]
    fn disabled_strategy_execution_persists_skipped_signal_actions() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "disabled execution audit",
                "close_threshold",
                &serde_json::json!({
                    "conid": 756733,
                    "buy_below": 100.0,
                    "sell_above": 200.0
                }),
            )
            .unwrap();
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 3.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: crate::ibkr::ContractCandidate {
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "SMART".into(),
                    primary_exchange: "ARCA".into(),
                    local_symbol: "SPY".into(),
                    description: String::new(),
                    derivative_security_types: Vec::new(),
                },
            })
            .unwrap();
        let evaluation_id = uuid::Uuid::now_v7();
        let now = Utc::now() + chrono::Duration::milliseconds(1);
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 90, 100, 90, 200, 'buy', ?, '{}')",
                params![evaluation_id, strategy_id, now, now],
            )
            .unwrap();

        assert!(storage.claim_strategy_action().unwrap().is_none());
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0]["evaluation_id"],
            serde_json::json!(evaluation_id)
        );
        assert_eq!(actions[0]["state"], "skipped");
        assert_eq!(actions[0]["cost_gate_result"], "execution_disabled");
        assert!(
            actions[0]["detail"]
                .as_str()
                .unwrap()
                .contains("strategy execution is disabled")
        );

        assert!(storage.claim_strategy_action().unwrap().is_none());
        assert_eq!(
            storage.list_strategy_execution_actions(10).unwrap().len(),
            1
        );
    }

    #[test]
    fn strategy_skip_identifies_the_blocking_active_order() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let contract = crate::ibkr::ContractCandidate {
            conid: 756733,
            symbol: "SPY".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "ARCA".into(),
            local_symbol: "SPY".into(),
            description: "SPDR S&P 500 ETF TRUST".into(),
            derivative_security_types: Vec::new(),
        };
        let strategy_id = storage
            .create_strategy(
                "blocked execution test",
                "close_threshold",
                &serde_json::json!({"conid": 756733, "buy_below": 100.0, "sell_above": 200.0}),
            )
            .unwrap();
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 3.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: contract.clone(),
            })
            .unwrap();
        storage
            .set_strategy_execution_enabled(strategy_id, true)
            .unwrap();
        let request = crate::ibkr::BrokerOrderRequest {
            contract,
            side: "BUY".into(),
            quantity: 1.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("blocking-order", "DU123", &request, "accepted", None)
            .unwrap();
        storage
            .record_submitted_order(intent_id, 42, uuid::Uuid::now_v7())
            .unwrap();
        let now = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 90, 100, 90, 200, 'buy', ?, '{}')",
                params![uuid::Uuid::now_v7(), strategy_id, now, now],
            )
            .unwrap();

        assert!(storage.claim_strategy_action().unwrap().is_none());
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["state"], "skipped");
        let detail = actions[0]["detail"].as_str().unwrap();
        assert!(detail.contains("Broker Order ID 42"));
        assert!(detail.contains("SPY"));
        assert!(detail.contains("submitted"));
    }

    #[test]
    fn manual_cancel_only_marks_cancellable_orders_and_can_restore_status() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session_id = uuid::Uuid::now_v7();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: crate::ibkr::ContractCandidate {
                conid: 756733,
                symbol: "SPY".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "ARCA".into(),
                local_symbol: "SPY".into(),
                description: String::new(),
                derivative_security_types: Vec::new(),
            },
            side: "BUY".into(),
            quantity: 1.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("cancel-order", "DU123", &request, "accepted", None)
            .unwrap();
        storage
            .record_submitted_order(intent_id, 42, session_id)
            .unwrap();

        let previous = storage.mark_cancel_pending(42, session_id).unwrap();
        assert_eq!(previous, "submitted");
        assert!(storage.mark_cancel_pending(42, session_id).is_err());
        storage
            .restore_cancel_status(42, session_id, &previous)
            .unwrap();
        assert_eq!(
            storage.list_orders_page(1, 10).unwrap().0[0]["status"],
            "submitted"
        );
        assert!(
            storage
                .mark_previous_session_order_not_open(42, uuid::Uuid::now_v7())
                .unwrap()
        );
        assert_eq!(
            storage.list_orders_page(1, 10).unwrap().0[0]["status"],
            "not_open"
        );
        let event_count: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM order_events
                 WHERE event_type = 'reconciliation_not_open'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);
    }

    #[test]
    fn reconciliation_rebinds_an_open_order_without_perm_id_to_the_current_session() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let old_session = uuid::Uuid::now_v7();
        let current_session = uuid::Uuid::now_v7();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: crate::ibkr::ContractCandidate {
                conid: 756733,
                symbol: "SPY".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "ARCA".into(),
                local_symbol: "SPY".into(),
                description: String::new(),
                derivative_security_types: Vec::new(),
            },
            side: "BUY".into(),
            quantity: 1.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("reconnected-order", "DU123", &request, "accepted", None)
            .unwrap();
        storage
            .record_submitted_order(intent_id, 42, old_session)
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OrderStatus {
                connection_session_id: Some(old_session),
                broker_order_id: 42,
                status: "Submitted".into(),
                filled: 0.0,
                remaining: 1.0,
                average_fill_price: None,
                last_fill_price: None,
                perm_id: 0,
                why_held: "locate".into(),
                market_cap_price: Some(499.5),
            })
            .unwrap();

        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["remaining_quantity"], 1.0);
        assert_eq!(order["why_held"], "locate");
        assert_eq!(order["market_cap_price"], 499.5);
        let event_count: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM broker_order_events
                 WHERE broker_order_id = 42 AND event_type = 'order_status'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);

        storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: current_session,
                open_orders: vec![crate::ibkr::OpenOrderSnapshot {
                    broker_order_id: 42,
                    perm_id: 0,
                    client_id: 0,
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    side: "BUY".into(),
                    quantity: 1.0,
                    order_type: "LMT".into(),
                    limit_price: Some(500.0),
                    status: "Submitted".into(),
                    completed_time: None,
                }],
                completed_orders: Vec::new(),
                events: Vec::new(),
                completed_at: Utc::now(),
            })
            .unwrap();

        assert_eq!(
            storage.mark_cancel_pending(42, current_session).unwrap(),
            "Submitted"
        );
        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["broker_order_id"], 42);
        assert_eq!(order["connection_session_id"], current_session.to_string());
    }

    #[test]
    fn reconciliation_updates_a_completed_order_without_perm_id_from_an_old_session() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let old_session = uuid::Uuid::now_v7();
        let current_session = uuid::Uuid::now_v7();
        let intent_id = uuid::Uuid::now_v7();
        let order_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO order_intents VALUES (?, 'completed-reconnect', 'DU123', 756733,
                    ?, 'submitted', NULL, ?, ?)",
                params![
                    intent_id,
                    serde_json::json!({
                        "side": "BUY",
                        "quantity": 1.0,
                        "contract": {"conid": 756733}
                    })
                    .to_string(),
                    now,
                    now
                ],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO orders
                 (order_id, order_intent_id, broker_order_id, connection_session_id,
                  status, filled_quantity, created_at, updated_at)
                 VALUES (?, ?, 42, ?, 'submitted', 0, ?, ?)",
                params![order_id, intent_id, old_session, now, now],
            )
            .unwrap();

        storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: current_session,
                open_orders: Vec::new(),
                completed_orders: vec![crate::ibkr::OpenOrderSnapshot {
                    broker_order_id: 42,
                    perm_id: 0,
                    client_id: 0,
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    side: "BUY".into(),
                    quantity: 1.0,
                    order_type: "LMT".into(),
                    limit_price: Some(500.0),
                    status: "Cancelled".into(),
                    completed_time: Some(now.to_rfc3339()),
                }],
                events: Vec::new(),
                completed_at: now,
            })
            .unwrap();

        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Cancelled");
        let error = storage
            .mark_cancel_pending(42, current_session)
            .unwrap_err()
            .to_string();
        assert!(error.contains("local status Cancelled"));
        assert!(error.contains("cannot be cancelled"));
    }

    #[test]
    fn completed_open_order_event_moves_a_submitted_order_to_filled() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session = uuid::Uuid::now_v7();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: crate::ibkr::ContractCandidate {
                conid: 272093,
                symbol: "MSFT".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "NASDAQ".into(),
                local_symbol: "MSFT".into(),
                description: "MICROSOFT CORP".into(),
                derivative_security_types: Vec::new(),
            },
            side: "SELL".into(),
            quantity: 100.0,
            order_type: "MKT".into(),
            limit_price: None,
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("completed-open-order", "DU123", &request, "accepted", None)
            .unwrap();
        storage
            .record_submitted_order(intent_id, 22, session)
            .unwrap();

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id: Some(session),
                broker_order_id: 22,
                perm_id: 549849917,
                status: "Filled".into(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: "20260729 14:38:37 US/Eastern".into(),
                completed_status: "Filled Size: 100".into(),
            })
            .unwrap();

        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["broker_perm_id"], 549849917);
    }

    #[test]
    fn reconciliation_recovers_completed_order_through_broker_event_perm_id() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let old_session = uuid::Uuid::now_v7();
        let current_session = uuid::Uuid::now_v7();
        let intent_id = uuid::Uuid::now_v7();
        let order_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO order_intents VALUES (?, 'event-perm-recovery', 'DU123', 272093,
                    ?, 'submitted', NULL, ?, ?)",
                params![
                    intent_id,
                    serde_json::json!({
                        "side": "SELL",
                        "quantity": 100.0,
                        "contract": {"conid": 272093}
                    })
                    .to_string(),
                    now,
                    now
                ],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO orders
                 (order_id, order_intent_id, broker_order_id, connection_session_id,
                  status, filled_quantity, created_at, updated_at)
                 VALUES (?, ?, 22, ?, 'submitted', 0, ?, ?)",
                params![order_id, intent_id, old_session, now, now],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO broker_order_events
                 VALUES (?, ?, 22, 549849917, 'open_order', ?, ?)",
                params![
                    uuid::Uuid::now_v7(),
                    old_session,
                    serde_json::json!({"status": "Submitted"}).to_string(),
                    now
                ],
            )
            .unwrap();

        let report = storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: current_session,
                open_orders: Vec::new(),
                completed_orders: vec![crate::ibkr::OpenOrderSnapshot {
                    broker_order_id: -1,
                    perm_id: 549849917,
                    client_id: 17,
                    account: "DU123".into(),
                    conid: 272093,
                    symbol: "MSFT".into(),
                    side: "SELL".into(),
                    quantity: 100.0,
                    order_type: "MKT".into(),
                    limit_price: None,
                    status: "Filled".into(),
                    completed_time: Some("20260729 14:38:37 US/Eastern".into()),
                }],
                events: Vec::new(),
                completed_at: now,
            })
            .unwrap();

        assert!(report.healthy);
        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["broker_perm_id"], 549849917);

        storage
            .connection
            .execute(
                "UPDATE orders SET status = 'submitted', broker_perm_id = NULL
                 WHERE order_id = ?",
                params![order_id],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "UPDATE broker_order_events
                 SET payload_json = ? WHERE broker_perm_id = 549849917",
                params![serde_json::json!({"status": "Filled"}).to_string()],
            )
            .unwrap();
        let healed = storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: uuid::Uuid::now_v7(),
                open_orders: Vec::new(),
                completed_orders: Vec::new(),
                events: Vec::new(),
                completed_at: now + chrono::Duration::seconds(1),
            })
            .unwrap();
        assert!(healed.healthy);
        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["broker_perm_id"], 549849917);
    }

    #[test]
    fn backtest_fills_a_signal_only_on_the_next_bar() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let closes = [3.0, 2.0, 1.0, 4.0, 5.0];
        let bars: Vec<BacktestBar> = closes
            .into_iter()
            .enumerate()
            .map(|(index, close)| BacktestBar {
                open_time: start + chrono::Duration::minutes(index as i64),
                open: if index == 4 { 10.0 } else { close },
                high: if index == 4 { 10.0 } else { close },
                low: close.min(if index == 4 { 10.0 } else { close }),
                close,
                volume: 1.0,
            })
            .collect();
        let request = BacktestRequest {
            strategy_id: None,
            conid: 756733,
            timeframe: "1m".into(),
            start,
            end: start + chrono::Duration::minutes(5),
            short_window: Some(2),
            long_window: Some(3),
            strategy_kind: "moving_average_cross".into(),
            strategy_config: None,
            quantity: 1.0,
            initial_cash: 100.0,
            slippage_bps: 100.0,
            commission_per_order: 1.0,
            seed: 7,
        };
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, _, metrics) = simulate_strategy(&request, strategy.as_ref(), &bars).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].signal_time, bars[3].open_time);
        assert_eq!(trades[0].fill_time, bars[4].open_time);
        assert_eq!(trades[0].price, 10.1);
        assert_eq!(metrics["bar_count"], 5);
    }

    #[test]
    fn reconciliation_health_tracks_blocking_external_open_orders() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session_id = uuid::Uuid::now_v7();
        let snapshot = crate::ibkr::ReconciliationSnapshot {
            connection_session_id: session_id,
            open_orders: vec![crate::ibkr::OpenOrderSnapshot {
                broker_order_id: 42,
                perm_id: 9001,
                client_id: 0,
                account: "DU123".into(),
                conid: 756733,
                symbol: "SPY".into(),
                side: "BUY".into(),
                quantity: 1.0,
                order_type: "LMT".into(),
                limit_price: Some(1.0),
                status: "Submitted".into(),
                completed_time: None,
            }],
            completed_orders: Vec::new(),
            events: Vec::new(),
            completed_at: Utc::now(),
        };

        let report = storage.reconcile(&snapshot).unwrap();
        assert!(!report.healthy);
        assert_eq!(report.external_order_count, 1);
        assert_eq!(report.blocking_difference_count, 1);
        assert_eq!(
            storage
                .reconciliation_health(Some(session_id))
                .unwrap()
                .state,
            "degraded"
        );
        assert_eq!(storage.list_reconciliation_differences().unwrap().len(), 1);
    }

    #[test]
    fn empty_reconciliation_is_healthy() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session_id = uuid::Uuid::now_v7();
        let report = storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: session_id,
                open_orders: Vec::new(),
                completed_orders: Vec::new(),
                events: Vec::new(),
                completed_at: Utc::now(),
            })
            .unwrap();
        assert!(report.healthy);
        assert_eq!(
            storage
                .reconciliation_health(Some(session_id))
                .unwrap()
                .state,
            "healthy"
        );
    }

    #[test]
    fn persists_continuous_account_events() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let observed_at = Utc::now();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::AccountSummary {
                account: "DU123".into(),
                tag: "NetLiquidation".into(),
                value: "100000".into(),
                currency: "USD".into(),
                observed_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Pnl {
                account: "DU123".into(),
                daily_pnl: 12.5,
                unrealized_pnl: Some(8.0),
                realized_pnl: Some(4.5),
                observed_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 2.0,
                    average_cost: 700.0,
                    observed_at,
                },
            })
            .unwrap();

        assert_eq!(storage.list_account_summary().unwrap().len(), 1);
        assert_eq!(storage.list_account_pnl().unwrap().len(), 1);
        assert_eq!(storage.list_positions().unwrap().len(), 1);
    }

    #[test]
    fn degraded_close_only_never_crosses_through_flat() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let connected_at = Utc::now();
        let observed_at = connected_at + chrono::Duration::milliseconds(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 5.0,
                    average_cost: 700.0,
                    observed_at,
                },
            })
            .unwrap();

        assert!(
            storage
                .evaluate_close_only("DU123", 756733, "sell", 5.0, connected_at)
                .unwrap()
                .allowed
        );
        assert!(
            !storage
                .evaluate_close_only("DU123", 756733, "sell", 6.0, connected_at)
                .unwrap()
                .allowed
        );
        assert!(
            !storage
                .evaluate_close_only("DU123", 756733, "buy", 1.0, connected_at)
                .unwrap()
                .allowed
        );
        assert!(
            !storage
                .evaluate_close_only(
                    "DU123",
                    756733,
                    "sell",
                    1.0,
                    observed_at + chrono::Duration::seconds(1)
                )
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn acknowledging_a_difference_preserves_the_degraded_gate() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session_id = uuid::Uuid::now_v7();
        storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: session_id,
                open_orders: vec![crate::ibkr::OpenOrderSnapshot {
                    broker_order_id: 42,
                    perm_id: 9001,
                    client_id: 0,
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    side: "BUY".into(),
                    quantity: 1.0,
                    order_type: "LMT".into(),
                    limit_price: Some(1.0),
                    status: "Submitted".into(),
                    completed_time: None,
                }],
                completed_orders: Vec::new(),
                events: Vec::new(),
                completed_at: Utc::now(),
            })
            .unwrap();
        let differences = storage.list_reconciliation_differences().unwrap();
        let difference_id: uuid::Uuid =
            serde_json::from_value(differences[0]["difference_id"].clone()).unwrap();
        storage
            .acknowledge_reconciliation_difference(difference_id, "reviewed externally")
            .unwrap();

        assert_eq!(
            storage
                .reconciliation_health(Some(session_id))
                .unwrap()
                .state,
            "degraded"
        );
        assert_eq!(
            storage.list_reconciliation_differences().unwrap()[0]["disposition"],
            "acknowledged"
        );
    }

    #[test]
    fn persists_market_data_subscriptions_and_latest_ticks() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let contract = crate::ibkr::ContractCandidate {
            conid: 756733,
            symbol: "SPY".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: String::new(),
            primary_exchange: "ARCA".into(),
            local_symbol: String::new(),
            description: "SPDR S&P 500 ETF".into(),
            derivative_security_types: Vec::new(),
        };
        storage.add_market_data_subscription(&contract).unwrap();
        let subscriptions = storage.market_data_subscriptions().unwrap();
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].exchange, "SMART");
        assert_eq!(subscriptions[0].local_symbol, "SPY");

        let first =
            DateTime::from_timestamp(Utc::now().timestamp().div_euclid(60) * 60 + 1, 0).unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid: contract.conid,
                tick_type: "Bid".into(),
                numeric_value: Some(700.0),
                text_value: None,
                observed_at: first,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid: contract.conid,
                tick_type: "Bid".into(),
                numeric_value: Some(701.0),
                text_value: None,
                observed_at: first + chrono::Duration::seconds(1),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataStatus {
                conid: contract.conid,
                state: "active".into(),
                error: None,
                observed_at: first,
            })
            .unwrap();
        for (price, observed_at) in [
            (700.0, first),
            (702.0, first + chrono::Duration::seconds(10)),
            (699.0, first + chrono::Duration::seconds(20)),
            (701.0, first + chrono::Duration::minutes(1)),
        ] {
            storage
                .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                    conid: contract.conid,
                    tick_type: "Last".into(),
                    numeric_value: Some(price),
                    text_value: None,
                    observed_at,
                })
                .unwrap();
        }
        let quote = storage.latest_quote(contract.conid).unwrap();
        assert_eq!(quote["ticks"]["Bid"]["numeric_value"], 701.0);
        assert_eq!(quote["subscription_status"]["state"], "active");
        assert_eq!(
            storage
                .market_data_health(contract.conid, 30, first + chrono::Duration::minutes(1))
                .unwrap()
                .state,
            "fresh"
        );
        let bars = storage.list_market_bars(contract.conid, "1m", 10).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[1]["high"], 702.0);
        assert_eq!(bars[1]["low"], 699.0);
        assert_eq!(bars[1]["final"], true);
        let five_second_bars = storage.list_market_bars(contract.conid, "5s", 20).unwrap();
        assert_eq!(five_second_bars.len(), 4);
        assert_eq!(
            five_second_bars
                .iter()
                .filter(|bar| bar["final"] == true)
                .count(),
            3
        );
        assert_eq!(five_second_bars[3]["close"], 700.0);
        assert_eq!(five_second_bars[0]["close"], 701.0);

        let delayed_observed_at = first + chrono::Duration::minutes(2);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid: contract.conid,
                tick_type: "DelayedBid".into(),
                numeric_value: Some(698.0),
                text_value: None,
                observed_at: delayed_observed_at,
            })
            .unwrap();
        let delayed_health = storage
            .market_data_health(contract.conid, 30, delayed_observed_at)
            .unwrap();
        assert_eq!(delayed_health.state, "delayed");
        assert_eq!(
            delayed_health.latest_price_type.as_deref(),
            Some("DelayedBid")
        );
        assert_eq!(delayed_health.age_seconds, Some(0));

        storage
            .remove_market_data_subscription(contract.conid)
            .unwrap();
        assert!(storage.market_data_subscriptions().unwrap().is_empty());
        assert!(storage.latest_quote(contract.conid).unwrap()["subscription_status"].is_null());
    }

    #[test]
    fn rejects_invalid_market_data_subscription_before_persisting_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let contract = crate::ibkr::ContractCandidate {
            conid: 14204,
            symbol: "EUR".into(),
            security_type: "CASH".into(),
            currency: String::new(),
            exchange: String::new(),
            primary_exchange: String::new(),
            local_symbol: String::new(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };

        assert!(storage.add_market_data_subscription(&contract).is_err());
        assert!(storage.market_data_subscriptions().unwrap().is_empty());
    }

    #[test]
    fn portfolio_risk_limits_opening_but_allows_risk_reducing_close() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 5.0,
                    average_cost: 100.0,
                    observed_at: now,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Pnl {
                account: "DU123".into(),
                daily_pnl: -10_000.0,
                unrealized_pnl: Some(-10_000.0),
                realized_pnl: Some(0.0),
                observed_at: now,
            })
            .unwrap();
        let mut config = crate::config::RiskConfig::default();
        config.max_position_quantity = 5.0;
        config.max_daily_loss = 100.0;
        let contract = crate::ibkr::ContractCandidate {
            conid: 756733,
            symbol: "SPY".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "ARCA".into(),
            local_symbol: "SPY".into(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };
        let opening = crate::ibkr::BrokerOrderRequest {
            contract: contract.clone(),
            side: "buy".into(),
            quantity: 1.0,
            order_type: "limit".into(),
            limit_price: Some(100.0),
            outside_rth: false,
        };
        assert_eq!(
            storage
                .evaluate_portfolio_risk(&config, "DU123", &opening, None, Some(100.0), false, now)
                .unwrap()
                .reason_code,
            "MAX_POSITION_QUANTITY"
        );
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Pnl {
                account: "DU123".into(),
                daily_pnl: 0.0,
                unrealized_pnl: Some(0.0),
                realized_pnl: Some(0.0),
                observed_at: now,
            })
            .unwrap();
        config.max_position_quantity = 10.0;
        config.max_price_deviation_bps = 100.0;
        let deviating = crate::ibkr::BrokerOrderRequest {
            limit_price: Some(110.0),
            ..opening.clone()
        };
        assert_eq!(
            storage
                .evaluate_portfolio_risk(
                    &config,
                    "DU123",
                    &deviating,
                    None,
                    Some(100.0),
                    false,
                    now
                )
                .unwrap()
                .reason_code,
            "MAX_PRICE_DEVIATION"
        );
        let closing = crate::ibkr::BrokerOrderRequest {
            side: "sell".into(),
            ..opening
        };
        assert!(
            storage
                .evaluate_portfolio_risk(&config, "DU123", &closing, None, None, true, now)
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn backfill_jobs_persist_progress_and_complete() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = Utc::now();
        let end = start + chrono::Duration::days(2);
        let request = BackfillJobRequest {
            contract: crate::ibkr::ContractCandidate {
                conid: 756733,
                symbol: "SPY".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: "ARCA".into(),
                local_symbol: "SPY".into(),
                description: String::new(),
                derivative_security_types: Vec::new(),
            },
            timeframe: "1m".into(),
            start,
            end,
            outside_rth: false,
        };
        let job_id = storage.create_backfill_job(&request).unwrap();
        let claimed = storage.claim_backfill_job().unwrap().unwrap();
        assert_eq!(claimed.job_id, job_id);
        storage
            .advance_backfill_job(job_id, start + chrono::Duration::days(1), end)
            .unwrap();
        assert_eq!(storage.list_data_jobs().unwrap()[0]["state"], "pending");
        let claimed = storage.claim_backfill_job().unwrap().unwrap();
        storage
            .advance_backfill_job(job_id, end, claimed.request.end)
            .unwrap();
        assert_eq!(storage.list_data_jobs().unwrap()[0]["state"], "completed");
        let coverage = storage
            .historical_coverage(756733, "1m", start, end)
            .unwrap();
        assert_eq!(coverage["covered"], false);
        assert_eq!(coverage["raw_gaps"].as_array().unwrap().len(), 1);
        let five_second_coverage = storage
            .historical_coverage(756733, "5s", start, end)
            .unwrap();
        assert_eq!(five_second_coverage["covered"], false);
        assert_eq!(
            five_second_coverage["raw_gaps"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn fx_rates_and_explicit_market_sessions_are_enforced() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.8,
                source: "test".into(),
                observed_at: now,
            })
            .unwrap();
        assert_eq!(
            storage
                .currency_conversion_rate("HKD", "USD", 60, now)
                .unwrap(),
            Some(1.0 / 7.8)
        );
        storage
            .upsert_market_session(&MarketSessionInput {
                exchange: "SEHK".into(),
                trading_date: now.date_naive(),
                opens_at: now - chrono::Duration::minutes(1),
                closes_at: now + chrono::Duration::minutes(1),
                state: "open".into(),
                source: "test".into(),
            })
            .unwrap();
        assert_eq!(
            storage.market_session_is_open("SEHK", now).unwrap(),
            Some(true)
        );
        assert_eq!(
            storage
                .market_session_is_open("SEHK", now + chrono::Duration::hours(1))
                .unwrap(),
            Some(false)
        );
        assert_eq!(storage.market_session_is_open("ARCA", now).unwrap(), None);
    }

    #[test]
    fn ibkr_calendar_keeps_regular_and_extended_sessions_separate() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = "2026-07-31T10:30:00Z".parse::<DateTime<Utc>>().unwrap();
        storage
            .replace_ibkr_market_sessions(&crate::ibkr::ContractSchedule {
                conid: 29612116,
                exchange: "SBF".into(),
                time_zone_id: "MET".into(),
                regular_sessions: vec![crate::ibkr::ContractSession {
                    trading_date: now.date_naive(),
                    opens_at: "2026-07-31T07:00:00Z".parse().unwrap(),
                    closes_at: "2026-07-31T15:30:00Z".parse().unwrap(),
                }],
                extended_sessions: vec![crate::ibkr::ContractSession {
                    trading_date: now.date_naive(),
                    opens_at: "2026-07-31T05:30:00Z".parse().unwrap(),
                    closes_at: "2026-07-31T18:00:00Z".parse().unwrap(),
                }],
                fetched_at: now,
            })
            .unwrap();
        let before_regular = "2026-07-31T06:00:00Z".parse().unwrap();
        assert_eq!(
            storage
                .market_session_is_open_for("SBF", before_regular, false)
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            storage
                .market_session_is_open_for("SBF", before_regular, true)
                .unwrap(),
            Some(true)
        );
        assert!(!storage.market_calendar_needs_refresh("SBF", now).unwrap());
        assert!(
            storage
                .market_calendar_needs_refresh("SBF", now + chrono::Duration::hours(6))
                .unwrap()
        );
        let sessions = storage.list_market_sessions(Some("SBF"), 10).unwrap();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn automatic_execution_can_target_a_short_position() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "short target",
                "close_threshold",
                &serde_json::json!({
                    "conid": 756733,
                    "buy_below": 100.0,
                    "sell_above": 200.0
                }),
            )
            .unwrap();
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 10.0,
                short_target_quantity: -4.0,
                allow_short: true,
                order_type: "limit".into(),
                paper_only: true,
                outside_rth: true,
                contract: crate::ibkr::ContractCandidate {
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "SMART".into(),
                    primary_exchange: "ARCA".into(),
                    local_symbol: "SPY".into(),
                    description: String::new(),
                    derivative_security_types: Vec::new(),
                },
            })
            .unwrap();
        storage
            .set_strategy_execution_enabled(strategy_id, true)
            .unwrap();
        let now = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 210, 100, 90, 200, 'sell', ?, '{}')",
                params![uuid::Uuid::now_v7(), strategy_id, now, now],
            )
            .unwrap();
        let action = storage.claim_strategy_action().unwrap().unwrap();
        assert_eq!(action.side, "sell");
        assert_eq!(action.quantity, 4.0);
        assert_eq!(action.legs[0].target_quantity, -4.0);
        assert_eq!(action.order_type, "limit");
        assert!(action.outside_rth);
    }

    #[test]
    fn acknowledged_monitoring_alert_stays_acknowledged_until_resolved() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        storage
            .upsert_monitoring_alert("test_alert", "critical", "requires review", true)
            .unwrap();
        let alert = storage.list_monitoring_alerts(false, 10).unwrap().remove(0);
        let alert_id = uuid::Uuid::parse_str(alert["alert_id"].as_str().unwrap()).unwrap();
        assert!(
            storage
                .acknowledge_monitoring_alert(alert_id, "reviewed")
                .unwrap()
        );

        storage
            .upsert_monitoring_alert("test_alert", "critical", "still present", true)
            .unwrap();
        let alert = storage.list_monitoring_alerts(false, 10).unwrap().remove(0);
        assert_eq!(alert["state"], "acknowledged");
        assert_eq!(alert["acknowledged_note"], "reviewed");
        assert!(storage.list_monitoring_alerts(true, 10).unwrap().is_empty());

        storage
            .upsert_monitoring_alert("test_alert", "critical", "resolved", false)
            .unwrap();
        assert_eq!(
            storage.list_monitoring_alerts(false, 10).unwrap()[0]["state"],
            "resolved"
        );

        storage
            .upsert_monitoring_alert("test_alert", "critical", "recurred", true)
            .unwrap();
        let alert = storage.list_monitoring_alerts(false, 10).unwrap().remove(0);
        assert_eq!(alert["state"], "active");
        assert!(alert["acknowledged_at"].is_null());
    }

    #[test]
    fn monitoring_identifies_competing_live_market_data_session_while_retrying() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let observed_at = Utc::now();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataStatus {
                conid: 29_612_116,
                state: "retrying".into(),
                error: Some("[10197] No market data during competing live session".into()),
                observed_at,
            })
            .unwrap();

        let facts = storage.monitoring_facts(observed_at).unwrap();
        assert_eq!(facts["failed_market_data"], 1);
        assert_eq!(facts["competing_live_session_count"], 1);
        assert_eq!(
            facts["competing_live_session_conids"],
            serde_json::json!([29_612_116])
        );

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataStatus {
                conid: 29_612_116,
                state: "active".into(),
                error: None,
                observed_at: observed_at + chrono::Duration::seconds(1),
            })
            .unwrap();
        let recovered = storage
            .monitoring_facts(observed_at + chrono::Duration::seconds(1))
            .unwrap();
        assert_eq!(recovered["failed_market_data"], 0);
        assert_eq!(recovered["competing_live_session_count"], 0);
    }
}
