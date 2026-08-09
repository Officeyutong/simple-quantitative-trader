use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use duckdb::{Connection, OptionalExt, params};
use serde::Serialize;
use serde_json::Value;

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
    (
        27,
        r#"
CREATE TABLE pending_broker_executions (
    broker_execution_id VARCHAR PRIMARY KEY,
    connection_session_id UUID,
    broker_order_id BIGINT NOT NULL,
    broker_perm_id BIGINT NOT NULL,
    conid BIGINT NOT NULL,
    side VARCHAR NOT NULL,
    quantity DOUBLE NOT NULL,
    price DOUBLE NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

-- Older binaries could persist a terminal broker event before the local order
-- row existed and then leave the newly inserted order permanently submitted.
UPDATE orders AS o SET
  status = json_extract_string(b.payload_json, '$.status'),
  broker_perm_id = CASE WHEN b.broker_perm_id <> 0 THEN b.broker_perm_id
                        ELSE o.broker_perm_id END,
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
  );
"#,
    ),
    (
        28,
        r#"
-- Historical bars requested for regular and extended sessions must not share
-- one manifest namespace: otherwise a later backfill can retire files from the
-- other session scope and a backtest can silently mix both data sets.  The Web
-- client historically requested regular-hours data, so existing files retain
-- that interpretation during migration.
ALTER TABLE dataset_files ADD COLUMN session_kind VARCHAR DEFAULT 'regular';
"#,
    ),
    (
        29,
        r#"
-- Keep bid/ask spread impact separate from post-quote slippage in backtests.
-- Existing runs used one manually configured slippage value and therefore
-- have no separately attributable spread cost.
ALTER TABLE backtest_trades ADD COLUMN spread DOUBLE DEFAULT 0;
"#,
    ),
    (
        30,
        r#"
-- Strategy-level limits are persisted separately from daemon-wide risk
-- limits. They block only position-increasing actions; exits remain enabled.
CREATE TABLE strategy_risk_controls (
    strategy_id UUID PRIMARY KEY,
    enabled BOOLEAN NOT NULL,
    strategy_capital DOUBLE NOT NULL,
    maximum_position_capital_ratio DOUBLE NOT NULL,
    maximum_rolling_24h_realized_net_loss_ratio DOUBLE NOT NULL,
    maximum_consecutive_net_losing_trades BIGINT NOT NULL,
    maximum_rolling_24h_completed_trades BIGINT NOT NULL,
    maximum_rolling_24h_turnover_capital_ratio DOUBLE NOT NULL,
    statistics_reset_at TIMESTAMPTZ NOT NULL,
    statistics_reset_note VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
INSERT INTO strategy_risk_controls
SELECT strategy_id, true, 100000, 1.0, 0.02, 3, 10, 10.0,
       current_timestamp, 'initial migration baseline',
       current_timestamp, current_timestamp
FROM strategy_execution_configs;

-- Old snapshots only contain realized accounting. Nullable mark-to-market
-- columns keep that distinction explicit instead of relabeling old values as
-- total performance.
ALTER TABLE strategy_performance_snapshots ADD COLUMN unrealized_pnl DOUBLE;
ALTER TABLE strategy_performance_snapshots ADD COLUMN total_net_pnl DOUBLE;
ALTER TABLE strategy_performance_snapshots ADD COLUMN data_complete BOOLEAN;
ALTER TABLE strategy_performance_snapshots ADD COLUMN valuation_complete BOOLEAN;
ALTER TABLE strategy_performance_snapshots ADD COLUMN warnings_json JSON;

-- V2 previously persisted only an empty v1 state. Resetting those rows while
-- bumping the explicit schema version enables the real state machine without
-- pretending an incompatible state is usable.
UPDATE strategy_runtime_states
SET state_version = 2, state_json = '{}', revision = revision + 1,
    last_transition_bar = NULL, updated_at = current_timestamp
WHERE state_version = 1 AND strategy_id IN (
    SELECT strategy_id FROM strategies WHERE kind = 'moving_average_cross_v2'
);
"#,
    ),
    (
        31,
        r#"
-- Realized PnL must use the FX rate that was known at execution time. Keeping
-- only the latest quote made old USD trades drift whenever USD/HKD changed.
CREATE TABLE fx_rate_history (
    base_currency VARCHAR NOT NULL,
    quote_currency VARCHAR NOT NULL,
    rate DOUBLE NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source VARCHAR NOT NULL,
    PRIMARY KEY (base_currency, quote_currency, observed_at)
);
CREATE INDEX fx_rate_history_pair_time_idx
    ON fx_rate_history(base_currency, quote_currency, observed_at);
INSERT INTO fx_rate_history
SELECT base_currency, quote_currency, rate, observed_at, source FROM fx_rates;
"#,
    ),
    (
        32,
        r#"
-- Reconciliation must retain IBKR's explicit completed-order evidence even
-- when the accompanying status field incorrectly remains `Submitted`.
ALTER TABLE broker_order_snapshots ADD COLUMN completed_status VARCHAR;
"#,
    ),
    (
        33,
        r#"
-- A timestamp is not a safe subscription identity: a delayed event from an
-- older IBKR position stream must never mutate the current snapshot lease.
ALTER TABLE position_sync_state ADD COLUMN subscription_id UUID;
UPDATE position_sync_state
SET state = 'stale', observed_at = NULL, subscription_id = NULL
WHERE singleton;
"#,
    ),
    (
        34,
        r#"
-- Strategy capital is a monetary amount and must retain its unit. Existing
-- rows intentionally remain NULL: only an operator can confirm which daemon
-- base currency an old unitless number represented. Opening actions fail
-- closed until that risk control is saved again.
ALTER TABLE strategy_risk_controls ADD COLUMN capital_currency VARCHAR;
"#,
    ),
    (
        35,
        r#"
-- Heartbeats renew the position subscription lease but do not prove that an
-- absent positions_current row means flat after a locally received fill. Keep
-- the latest completed full snapshot timestamp separately for that purpose.
ALTER TABLE position_sync_state ADD COLUMN snapshot_completed_at TIMESTAMPTZ;
"#,
    ),
    (
        36,
        r#"
-- A signal's desired target outlives an individual broker submission only for
-- protective reductions. Rows are append/audit records: a later real signal
-- supersedes the active row instead of mutating its source identity.
CREATE TABLE strategy_execution_desired_targets (
    desired_target_id UUID PRIMARY KEY,
    strategy_id UUID NOT NULL,
    source_evaluation_id UUID NOT NULL UNIQUE,
    signal VARCHAR NOT NULL,
    targets_json JSON NOT NULL,
    state VARCHAR NOT NULL,
    requires_flatten BOOLEAN NOT NULL DEFAULT false,
    flatten_completed_at TIMESTAMPTZ,
    superseded_by_evaluation_id UUID,
    detail VARCHAR,
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX strategy_execution_desired_targets_state_idx
    ON strategy_execution_desired_targets(state, created_at);
ALTER TABLE strategy_execution_actions ADD COLUMN source_evaluation_id UUID;
UPDATE strategy_execution_actions
SET source_evaluation_id = evaluation_id
WHERE source_evaluation_id IS NULL;
"#,
    ),
    (
        37,
        r#"
-- V2 state v3 separates externally published direction from internal
-- catch-up candidates and introduces a flatten-only protective intent. Old v2
-- state cannot be interpreted safely. Reset it explicitly and fail closed:
-- running strategies are paused and automatic execution is disabled until the
-- operator reviews the position and resumes with freshly initialized state.
UPDATE strategy_execution_desired_targets
SET state = 'cancelled',
    detail = 'cancelled by moving_average_cross_v2 state v3 safety migration',
    updated_at = current_timestamp
WHERE state = 'active' AND strategy_id IN (
    SELECT strategy_id FROM strategies WHERE kind = 'moving_average_cross_v2'
);
UPDATE strategy_execution_configs
SET enabled = false, enabled_at = NULL, updated_at = current_timestamp
WHERE strategy_id IN (
    SELECT strategy_id FROM strategies WHERE kind = 'moving_average_cross_v2'
);
UPDATE strategy_runtime_states
SET state_version = 3, state_json = '{}', revision = revision + 1,
    last_transition_bar = NULL, updated_at = current_timestamp
WHERE strategy_id IN (
    SELECT strategy_id FROM strategies WHERE kind = 'moving_average_cross_v2'
);
UPDATE strategies
SET state = CASE WHEN state = 'running' THEN 'paused' ELSE state END,
    last_evaluated_bar = NULL,
    last_error = 'moving_average_cross_v2 state v3 safety upgrade reset runtime state; review the current position and explicitly resume/re-enable execution',
    updated_at = current_timestamp
WHERE kind = 'moving_average_cross_v2';
"#,
    ),
];

const MAX_STRATEGY_STATE_BYTES: usize = 1024 * 1024;

/// Maximum age of a buy/sell evaluation that automatic execution will still
/// act on. Older signals (typically accumulated while the daemon was stopped)
/// are recorded as skipped and never executed late.
pub const MAX_EXECUTABLE_SIGNAL_AGE_SECONDS: i64 = 900;
const DESIRED_TARGET_RETRY_DELAY_SECONDS: i64 = 30;
const DESIRED_TARGET_POSITION_EVIDENCE_RETRY_SECONDS: i64 = 15;
const POSITION_EVIDENCE_WAIT_DETAIL: &str =
    "waiting for the position stream to catch up before recomputing the target delta";
const POSITION_QUANTITY_EPSILON: f64 = 1.0e-8;
const MARKET_TRADE_PAIR_MAX_RECEIPT_GAP_SECONDS: i64 = 2;
const LIVE_TRADE_MAX_SOURCE_AGE_SECONDS: i64 = 60;
const DELAYED_TRADE_MAX_SOURCE_AGE_SECONDS: i64 = 30 * 60;

#[derive(Clone, Debug)]
struct PendingMarketTradePrice {
    tick_type: String,
    price: f64,
    received_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug)]
struct PendingMarketTradeTimestamp {
    source_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
}

#[derive(Default)]
struct PendingMarketTradePair {
    price: Option<PendingMarketTradePrice>,
    timestamp: Option<PendingMarketTradeTimestamp>,
    last_emitted_receipts: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

pub struct Storage {
    connection: Connection,
    database_path: PathBuf,
    pending_market_trades: HashMap<(i32, bool), PendingMarketTradePair>,
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

#[derive(Clone, Debug)]
struct PositionEvidenceState {
    latest_execution_received_at: Option<DateTime<Utc>>,
    has_incomplete_fill_evidence: bool,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct DesiredTargetLeg {
    leg_index: i32,
    conid: i32,
    target_quantity: f64,
    #[serde(default)]
    requires_flatten: bool,
}

#[derive(Clone, Debug)]
struct DesiredActionContext {
    desired_target_id: uuid::Uuid,
    source_evaluation_id: uuid::Uuid,
    carrier_evaluation_id: uuid::Uuid,
    targets: Vec<DesiredTargetLeg>,
    requires_flatten: bool,
    is_retry: bool,
}

#[derive(Clone, Debug)]
struct DesiredLegInput {
    leg_index: i32,
    contract: crate::ibkr::ContractCandidate,
    current_quantity: f64,
    final_target_quantity: f64,
}

impl PositionEvidenceState {
    fn is_caught_up(&self, position_observed_at: Option<DateTime<Utc>>) -> bool {
        !self.has_incomplete_fill_evidence
            && self
                .latest_execution_received_at
                .is_none_or(|received_at| position_observed_at.is_some_and(|at| at >= received_at))
    }
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
    #[serde(default)]
    pub outside_rth: bool,
    /// When present, historical bars are persisted as completed-period FX
    /// observations instead of market-data Parquet.  Keeping this in the job
    /// payload makes old jobs backward compatible and lets the existing
    /// durable queue/retry machinery repair historical performance data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fx_rate_pair: Option<FxRateBackfillTarget>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, Serialize)]
pub struct FxRateBackfillTarget {
    pub base_currency: String,
    pub quote_currency: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct HistoricalFxGap {
    pub base_currency: String,
    pub quote_currency: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub affected_execution_values: usize,
}

#[derive(Clone, Debug)]
pub struct BackfillJobCreation {
    pub job_id: uuid::Uuid,
    /// True when the request was folded into an existing active job instead of
    /// inserting another queue entry.
    pub reused: bool,
    /// True when reusing the job expanded its requested time range.
    pub range_expanded: bool,
}

#[derive(Clone, Debug)]
struct ActiveBackfillJob {
    job_id: uuid::Uuid,
    state: String,
    request: BackfillJobRequest,
    cursor_time: DateTime<Utc>,
    completed_slices: i64,
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
    /// Explicit database cost model for ad-hoc backtests. Backtests tied to a
    /// persisted strategy always use that strategy's assigned model instead.
    #[serde(default)]
    pub cost_model_id: Option<uuid::Uuid>,
    /// Controls whether a strategy-bound backtest reproduces the strategy's
    /// currently configured cost gates or merely deducts execution costs.
    /// An omitted value defaults to `match_strategy` for persisted strategies
    /// and `fees_only` for ad-hoc requests.
    #[serde(default)]
    pub cost_gate_mode: Option<BacktestCostGateMode>,
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
    /// Target long position for a buy signal. The legacy field name is kept
    /// for wire compatibility with existing RPC/CLI clients.
    pub quantity: f64,
    /// Target position for a sell signal. A negative value is used only when
    /// `allow_short` is true; otherwise sell signals flatten to zero.
    #[serde(default)]
    pub short_target_quantity: f64,
    #[serde(default)]
    pub allow_short: bool,
    pub initial_cash: f64,
    /// Selects the same IBKR historical-data session scope as the strategy's
    /// execution configuration. False means liquidHours (regular session),
    /// true means tradingHours (extended session).
    #[serde(default)]
    pub outside_rth: bool,
    #[serde(default)]
    pub seed: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BacktestCostGateMode {
    MatchStrategy,
    FeesOnly,
}

impl BacktestRequest {
    fn effective_cost_gate_mode(&self) -> BacktestCostGateMode {
        self.cost_gate_mode
            .unwrap_or(if self.strategy_id.is_some() {
                BacktestCostGateMode::MatchStrategy
            } else {
                BacktestCostGateMode::FeesOnly
            })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BacktestDetailOptions {
    pub trade_page: usize,
    pub trade_page_size: usize,
    pub max_equity_points: usize,
}

impl Default for BacktestDetailOptions {
    fn default() -> Self {
        Self {
            trade_page: 1,
            trade_page_size: 200,
            max_equity_points: 2_000,
        }
    }
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
    spread: f64,
    slippage: f64,
}

#[derive(Clone, Debug)]
struct PendingBacktestTarget {
    desired_target: f64,
    signal_time: DateTime<Utc>,
    signal_edge_bps: Option<f64>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct EstimatedExecutionCost {
    pub commission: f64,
    pub spread: f64,
    pub slippage: f64,
}

impl ExecutionCostModelInput {
    pub fn estimated_commission(&self, side: CostSide, notional: f64, quantity: f64) -> f64 {
        let (fixed, per_share, rate_bps, minimum) = match side {
            CostSide::Buy => (
                self.buy_fixed_fee,
                self.buy_per_share_fee,
                self.buy_rate_bps,
                self.buy_min_fee,
            ),
            CostSide::Sell => (
                self.sell_fixed_fee,
                self.sell_per_share_fee,
                self.sell_rate_bps,
                self.sell_min_fee,
            ),
        };
        let broker_fee =
            (fixed + quantity * per_share + notional * rate_bps / 10_000.0).max(minimum);
        let sell_tax = if side == CostSide::Sell {
            notional * self.sell_tax_bps / 10_000.0
        } else {
            0.0
        };
        broker_fee + sell_tax
    }

    pub fn estimated_execution_cost(
        &self,
        side: CostSide,
        notional: f64,
        quantity: f64,
    ) -> EstimatedExecutionCost {
        EstimatedExecutionCost {
            commission: self.estimated_commission(side, notional, quantity),
            // Crossing from a mid/open reference to one side of the quoted
            // market consumes half of the full bid/ask spread on each leg.
            spread: notional * self.estimated_spread_bps / 2.0 / 10_000.0,
            slippage: notional * self.estimated_slippage_bps / 10_000.0,
        }
    }

    pub fn estimated_round_trip_cost(
        &self,
        notional: f64,
        quantity: f64,
        learned_commission_bps_p90: Option<f64>,
    ) -> f64 {
        let buy = self.estimated_execution_cost(CostSide::Buy, notional, quantity);
        let sell = self.estimated_execution_cost(CostSide::Sell, notional, quantity);
        let configured_commissions = buy.commission + sell.commission;
        let learned_commissions = learned_commission_bps_p90
            .map(|bps| 2.0 * notional * bps / 10_000.0)
            .unwrap_or(0.0);
        configured_commissions.max(learned_commissions)
            + buy.spread
            + sell.spread
            + buy.slippage
            + sell.slippage
    }
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

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct StrategyRiskControlInput {
    pub strategy_id: uuid::Uuid,
    pub enabled: bool,
    /// Capital allocated to this strategy, denominated in `capital_currency`.
    pub strategy_capital: f64,
    /// Immutable unit attached to the configured capital. The RPC layer only
    /// accepts the daemon's current risk base currency, so changing the daemon
    /// configuration cannot silently reinterpret an existing numeric budget.
    #[serde(default)]
    pub capital_currency: Option<String>,
    pub maximum_position_capital_ratio: f64,
    pub maximum_rolling_24h_realized_net_loss_ratio: f64,
    pub maximum_consecutive_net_losing_trades: usize,
    pub maximum_rolling_24h_completed_trades: usize,
    pub maximum_rolling_24h_turnover_capital_ratio: f64,
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct StrategyRiskResetInput {
    pub strategy_id: uuid::Uuid,
    pub confirm: bool,
    pub note: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StrategyRiskStatistics {
    pub data_complete: bool,
    pub warning: Option<String>,
    pub rolling_24h_realized_net_pnl: f64,
    pub rolling_24h_turnover: f64,
    pub rolling_24h_completed_trades: usize,
    pub consecutive_net_losing_trades: usize,
    pub gross_pnl_since_reset: f64,
    pub commissions_since_reset: f64,
    pub completed_trades_since_reset: usize,
}

#[derive(Clone, Debug)]
struct StrategyRiskControl {
    enabled: bool,
    strategy_capital: f64,
    capital_currency: Option<String>,
    maximum_position_capital_ratio: f64,
    maximum_rolling_24h_realized_net_loss_ratio: f64,
    maximum_consecutive_net_losing_trades: usize,
    maximum_rolling_24h_completed_trades: usize,
    maximum_rolling_24h_turnover_capital_ratio: f64,
    statistics_reset_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ClaimedCostControl {
    pub model: ExecutionCostModelInput,
    pub minimum_cost_multiple: f64,
    pub maximum_commission_to_gross_profit_ratio: f64,
    pub minimum_completed_trades: usize,
    pub actual_fee_bps_p90: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
struct BacktestCostGateSnapshot {
    mode: BacktestCostGateMode,
    /// None for an ad-hoc backtest, which has no strategy control to mirror.
    strategy_control_enabled: Option<bool>,
    /// True only when `mode=match_strategy` and the saved strategy control is
    /// enabled. Fees are deducted regardless of this flag.
    applied: bool,
    minimum_cost_multiple: Option<f64>,
    maximum_commission_to_gross_profit_ratio: Option<f64>,
    minimum_completed_trades: Option<usize>,
    /// Frozen once at backtest start. This mirrors the live gate's learned
    /// commission floor without allowing later executions to rewrite a run.
    actual_fee_bps_p90: Option<f64>,
    statistics_baseline: &'static str,
    scope: &'static str,
}

#[derive(Clone, Debug)]
struct BacktestCostContext {
    model: ExecutionCostModelInput,
    model_source: &'static str,
    gate: BacktestCostGateSnapshot,
}

impl BacktestCostGateSnapshot {
    fn fees_only(mode: BacktestCostGateMode) -> Self {
        Self {
            mode,
            strategy_control_enabled: None,
            applied: false,
            minimum_cost_multiple: None,
            maximum_commission_to_gross_profit_ratio: None,
            minimum_completed_trades: None,
            actual_fee_bps_p90: None,
            statistics_baseline: "backtest_start",
            scope: "transaction_cost_and_commission_performance_only; strategy risk, account, market-data freshness, order-conflict and trading-calendar gates are not simulated",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CostGateLegEstimate {
    pub quantity: f64,
    pub price: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionCostGateOutcome {
    BypassedRiskReduction,
    Passed,
    Blocked,
}

#[derive(Clone, Copy, Debug)]
pub struct TransactionCostGateDecision {
    pub outcome: TransactionCostGateOutcome,
    pub estimated_notional: f64,
    pub estimated_round_trip_cost: f64,
    pub required_edge_bps: f64,
}

/// Pure transaction-cost edge decision shared by live execution and
/// backtests. The caller remains responsible for currency compatibility.
pub fn evaluate_transaction_cost_gate(
    model: &ExecutionCostModelInput,
    minimum_cost_multiple: f64,
    actual_fee_bps_p90: Option<f64>,
    signal_edge_bps: Option<f64>,
    risk_reducing: bool,
    legs: &[CostGateLegEstimate],
) -> TransactionCostGateDecision {
    if risk_reducing {
        return TransactionCostGateDecision {
            outcome: TransactionCostGateOutcome::BypassedRiskReduction,
            estimated_notional: 0.0,
            estimated_round_trip_cost: 0.0,
            required_edge_bps: 0.0,
        };
    }
    let mut estimated_notional = 0.0;
    let mut estimated_round_trip_cost = 0.0;
    for leg in legs {
        let notional = leg.quantity * leg.price;
        estimated_notional += notional;
        estimated_round_trip_cost +=
            model.estimated_round_trip_cost(notional, leg.quantity, actual_fee_bps_p90);
    }
    let required_edge_bps = if estimated_notional.is_finite() && estimated_notional > 0.0 {
        estimated_round_trip_cost / estimated_notional * 10_000.0 * minimum_cost_multiple
    } else {
        f64::INFINITY
    };
    let passed = required_edge_bps.is_finite()
        && signal_edge_bps.is_some_and(|edge| edge.is_finite() && edge >= required_edge_bps);
    TransactionCostGateDecision {
        outcome: if passed {
            TransactionCostGateOutcome::Passed
        } else {
            TransactionCostGateOutcome::Blocked
        },
        estimated_notional,
        estimated_round_trip_cost,
        required_edge_bps,
    }
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

fn normalized_currency(value: &str) -> Result<String> {
    let currency = value.trim().to_ascii_uppercase();
    if currency.len() == 3
        && currency
            .chars()
            .all(|character| character.is_ascii_alphabetic())
    {
        Ok(currency)
    } else {
        Err(AppError::Storage(
            "currency must be a three-letter alphabetic code".into(),
        ))
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ClaimedStrategyAction {
    pub action_id: uuid::Uuid,
    pub strategy_id: uuid::Uuid,
    pub evaluation_id: uuid::Uuid,
    pub source_evaluation_id: uuid::Uuid,
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

#[derive(Clone, Debug)]
pub struct RevokedStrategyOrderCancellation {
    pub strategy_id: uuid::Uuid,
    pub action_id: uuid::Uuid,
    pub leg_index: i32,
    pub broker_order_id: i32,
}

impl ClaimedStrategyLeg {
    pub fn is_risk_reducing(&self) -> bool {
        position_change_is_risk_reducing(self.current_quantity, self.target_quantity)
    }
}

pub(crate) fn position_change_is_risk_reducing(current: f64, target: f64) -> bool {
    target.abs() + POSITION_QUANTITY_EPSILON < current.abs()
        && (target.abs() <= POSITION_QUANTITY_EPSILON || target.signum() == current.signum())
}

fn historical_target_authorizes_short(
    target_quantity: Option<f64>,
    target_evidence_conflict: bool,
    projected_quantity: f64,
) -> bool {
    !target_evidence_conflict
        && projected_quantity < -POSITION_QUANTITY_EPSILON
        && target_quantity.is_some_and(|target| {
            target < -POSITION_QUANTITY_EPSILON
                && projected_quantity >= target - POSITION_QUANTITY_EPSILON
        })
}

fn missing_historical_short_authorization(
    conid: i32,
    target_quantity: Option<f64>,
    target_evidence_conflict: bool,
    projected_quantity: f64,
) -> String {
    let evidence = if target_evidence_conflict {
        "conflicting historical action-leg targets".to_owned()
    } else if let Some(target) = target_quantity {
        format!("historical action-leg target {target}")
    } else {
        "no historical action-leg target".to_owned()
    };
    format!(
        "sell execution for conid {conid} would create or increase a short position to \
         {projected_quantity}; {evidence} does not provide unambiguous authorization"
    )
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

fn completed_fill_size(status: &str) -> Option<f64> {
    status
        .split_once("Filled Size:")
        .and_then(|(_, value)| value.trim().split_whitespace().next())
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn order_status_is_terminal(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "filled" | "cancelled" | "canceled" | "inactive" | "rejected" | "not_open"
    )
}

fn fx_observation_is_fresh(
    observed_at: DateTime<Utc>,
    as_of: DateTime<Utc>,
    maximum_age_seconds: u64,
) -> bool {
    let age_seconds = (as_of - observed_at).num_seconds();
    let maximum_age_seconds = i64::try_from(maximum_age_seconds).unwrap_or(i64::MAX);
    age_seconds >= 0 && age_seconds <= maximum_age_seconds
}

#[derive(Clone, Debug, Default)]
struct PerformancePosition {
    quantity: f64,
    average_price: f64,
    currency: String,
}

#[derive(Clone, Debug, Default)]
struct RiskCyclePosition {
    quantity: f64,
    average_price: f64,
    gross_pnl: f64,
    commissions: f64,
    /// True when any execution in this still-open position cycle could not be
    /// valued completely. This must follow the cycle across rolling windows.
    tainted: bool,
}

#[derive(Clone, Copy, Debug)]
struct CompletedRiskCycle {
    closed_at: DateTime<Utc>,
    gross_pnl: f64,
    commissions: f64,
    tainted: bool,
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
            pending_market_trades: HashMap::new(),
        };
        storage.migrate()?;
        storage.repair_reconciled_execution_times()?;
        storage.recover_interrupted_jobs()?;
        storage.recover_dst_decoder_backfill_failures()?;
        // A daemon restart is a safe point to collapse overlapping queued
        // downloads left by older versions or repeated Web submissions.
        storage.deduplicate_active_backfill_jobs()?;
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
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // Keep leg rows consistent with their parent action after a crash;
        // otherwise legs of a failed batch linger in 'processing' forever.
        self.connection
            .execute(
                "UPDATE strategy_execution_action_legs AS l SET state = 'failed',
                    detail = 'daemon stopped while leg outcome was unknown; manual review required',
                    updated_at = ?
                 WHERE l.state = 'processing'
                   AND EXISTS (SELECT 1 FROM strategy_execution_actions a
                               WHERE a.action_id = l.action_id
                                 AND a.state IN ('failed', 'rejected'))",
                params![Utc::now()],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // An intent stuck in 'approved' means the daemon stopped between risk
        // approval and the broker acknowledgement. The order may or may not
        // have reached IBKR; per the crash-recovery contract it must be marked
        // 'unknown' and resolved through reconciliation, never resubmitted.
        self.connection
            .execute(
                "UPDATE order_intents SET status = 'unknown',
                    rejection_reason = 'daemon stopped before the broker acknowledgement was \
                                        recorded; resolve through reconciliation',
                    updated_at = ?
                 WHERE status = 'approved'
                   AND NOT EXISTS (SELECT 1 FROM orders o
                                   WHERE o.order_intent_id = order_intents.order_intent_id)",
                params![Utc::now()],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn repair_reconciled_execution_times(&mut self) -> Result<()> {
        let rows: Vec<(String, DateTime<Utc>, DateTime<Utc>, String)> = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT e.broker_execution_id, e.executed_at, o.created_at,
                            json_extract_string(b.payload_json, '$.completed_time')
                     FROM executions e
                     JOIN orders o ON o.order_id = e.order_id
                     JOIN broker_order_events b
                       ON b.connection_session_id = o.connection_session_id
                      AND b.broker_order_id = o.broker_order_id
                     WHERE b.event_type = 'open_order'
                       AND json_extract_string(b.payload_json, '$.completed_time') <> ''
                       AND date_diff('second', o.created_at, e.executed_at) > 300
                       AND b.received_at = (
                         SELECT max(latest.received_at) FROM broker_order_events latest
                         WHERE latest.connection_session_id = o.connection_session_id
                           AND latest.broker_order_id = o.broker_order_id
                           AND latest.event_type = 'open_order')",
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            statement
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .map_err(|error| AppError::Storage(error.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AppError::Storage(error.to_string()))?
        };
        for (execution_id, current, created_at, raw_time) in rows {
            let Ok(executed_at) = crate::ibkr::parse_ibkr_execution_datetime(&raw_time) else {
                continue;
            };
            if (executed_at - created_at).num_minutes().abs() <= 5 && executed_at != current {
                self.connection
                    .execute(
                        "UPDATE executions SET executed_at = ? WHERE broker_execution_id = ?",
                        params![executed_at, execution_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn recover_dst_decoder_backfill_failures(&mut self) -> Result<()> {
        self.connection
            .execute(
                "UPDATE data_jobs
                 SET state = 'retrying', attempts = 0,
                     last_error = 'automatically retrying after DST historical decoder recovery',
                     updated_at = ?
                 WHERE state = 'failed'
                   AND last_error LIKE 'IBKR historical decoder panicked on an ambiguous daylight-saving-time endpoint%'",
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
            "strategy_execution_desired_targets",
            "strategy_execution_configs",
            "strategy_cost_controls",
            "strategy_risk_controls",
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

    #[cfg(test)]
    pub fn configure_strategy_execution(&mut self, config: &StrategyExecutionConfig) -> Result<()> {
        self.configure_strategy_execution_with_capital_currency(config, "USD")
    }

    fn validate_contract_canonical_currency(
        &self,
        contract: &crate::ibkr::ContractCandidate,
    ) -> Result<String> {
        let contract_currency = normalized_currency(&contract.currency)?;
        let canonical = self
            .connection
            .query_row(
                "SELECT currency FROM instruments WHERE conid = ?",
                params![contract.conid],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if let Some(canonical) = canonical {
            let canonical = normalized_currency(&canonical)?;
            if canonical != contract_currency {
                return Err(AppError::Storage(format!(
                    "execution contract currency {contract_currency} for conid {} does not match canonical instrument currency {canonical}",
                    contract.conid
                )));
            }
        }
        Ok(contract_currency)
    }

    fn strategy_execution_contracts(
        &self,
        strategy_id: uuid::Uuid,
    ) -> Result<Vec<crate::ibkr::ContractCandidate>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT contract_json::VARCHAR
                 FROM strategy_execution_portfolio_legs
                 WHERE strategy_id = ? ORDER BY leg_index",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let contract_json = statement
            .query_map(params![strategy_id], |row| row.get::<_, String>(0))
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        if !contract_json.is_empty() {
            return contract_json
                .iter()
                .map(|value| serde_json::from_str(value).map_err(AppError::from))
                .collect();
        }
        let scalar = self
            .connection
            .query_row(
                "SELECT contract_json::VARCHAR FROM strategy_execution_configs
                 WHERE strategy_id = ?",
                params![strategy_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        scalar
            .map(|value| serde_json::from_str(&value).map_err(AppError::from))
            .transpose()
            .map(|contract| contract.into_iter().collect())
    }

    fn validate_execution_contract_currencies(
        &self,
        contracts: &[crate::ibkr::ContractCandidate],
        model_currency: Option<&str>,
    ) -> Result<()> {
        let model_currency = model_currency.map(normalized_currency).transpose()?;
        for contract in contracts {
            let contract_currency = self.validate_contract_canonical_currency(contract)?;
            if let Some(model_currency) = model_currency.as_deref()
                && contract_currency != model_currency
            {
                return Err(AppError::Storage(format!(
                    "execution contract currency {contract_currency} for conid {} does not match assigned cost model currency {model_currency}",
                    contract.conid
                )));
            }
        }
        Ok(())
    }

    fn validate_strategy_execution_currency_assignment(
        &self,
        strategy_id: uuid::Uuid,
    ) -> Result<()> {
        let contracts = self.strategy_execution_contracts(strategy_id)?;
        let model = self.execution_cost_model_for_strategy(strategy_id)?;
        self.validate_execution_contract_currencies(
            &contracts,
            model.as_ref().map(|model| model.currency.as_str()),
        )
    }

    fn validate_strategy_execution_config(
        &self,
        config: &StrategyExecutionConfig,
        portfolio_targets: bool,
    ) -> Result<bool> {
        if config.account.trim().is_empty()
            || !config.target_quantity.is_finite()
            || !config.short_target_quantity.is_finite()
            || (!portfolio_targets && config.target_quantity <= 0.0)
            || (!portfolio_targets && config.short_target_quantity > 0.0)
            || (!portfolio_targets && !config.allow_short && config.short_target_quantity < 0.0)
            || !matches!(config.order_type.as_str(), "market" | "limit")
            || (config.outside_rth && config.order_type != "limit")
            || !config.paper_only
            || config.contract.conid <= 0
        {
            return Err(AppError::Storage(
                "execution requires account, target_quantity > 0, a non-positive short target, \
                 a supported order type, limit orders for outside-RTH execution, \
                 paper_only=true and a valid contract"
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
        let supports_short_targets = strategy_catalog_backend::metadata(&kind)
            .ok_or_else(|| AppError::Storage(format!("strategy metadata not found: {kind}")))?
            .capabilities
            .supports_short_targets;
        if !portfolio_targets
            && (config.allow_short || config.short_target_quantity < 0.0)
            && !supports_short_targets
        {
            return Err(AppError::Storage(format!(
                "strategy kind {kind} does not support short targets; disable allow_short and use a zero sell target"
            )));
        }
        Ok(supports_short_targets)
    }

    fn persist_strategy_execution_config(
        connection: &Connection,
        config: &StrategyExecutionConfig,
        capital_currency: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        connection
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
            .map_err(|error| AppError::Storage(error.to_string()))?;
        connection
            .execute(
                "UPDATE strategy_execution_desired_targets
                 SET state = 'cancelled',
                     detail = 'execution configuration was replaced or disabled',
                     updated_at = ?
                 WHERE strategy_id = ? AND state = 'active'",
                params![now, config.strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // New strategies receive conservative, editable database defaults.
        // Existing controls are never overwritten when execution is edited.
        connection
            .execute(
                "INSERT INTO strategy_risk_controls
                 (strategy_id, enabled, strategy_capital,
                  maximum_position_capital_ratio,
                  maximum_rolling_24h_realized_net_loss_ratio,
                  maximum_consecutive_net_losing_trades,
                  maximum_rolling_24h_completed_trades,
                  maximum_rolling_24h_turnover_capital_ratio,
                  statistics_reset_at, statistics_reset_note, created_at, updated_at,
                  capital_currency)
                 VALUES (?, true, 100000, 1.0, 0.02, 3, 10, 10.0,
                         ?, 'initial execution configuration', ?, ?, ?)
                 ON CONFLICT (strategy_id) DO NOTHING",
                params![config.strategy_id, now, now, now, capital_currency],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    /// Production callers must pass the configured portfolio-risk base
    /// currency so the numeric strategy capital is persisted with an explicit
    /// unit. The compatibility wrapper above uses `RiskConfig::default()`'s
    /// USD unit for direct library callers and tests.
    pub fn configure_strategy_execution_with_capital_currency(
        &mut self,
        config: &StrategyExecutionConfig,
        capital_currency: &str,
    ) -> Result<()> {
        let capital_currency = normalized_currency(capital_currency)?;
        self.validate_strategy_execution_config(config, false)?;
        let assigned_model = self.execution_cost_model_for_strategy(config.strategy_id)?;
        self.validate_execution_contract_currencies(
            std::slice::from_ref(&config.contract),
            assigned_model.as_ref().map(|model| model.currency.as_str()),
        )?;
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Self::persist_strategy_execution_config(&transaction, config, &capital_currency, now)?;
        // A scalar execution configuration is authoritative. Leaving old
        // portfolio legs behind would silently keep trading those contracts.
        transaction
            .execute(
                "DELETE FROM strategy_execution_portfolio_legs WHERE strategy_id = ?",
                params![config.strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn configure_strategy_portfolio_execution_with_capital_currency(
        &mut self,
        config: &StrategyPortfolioExecutionConfig,
        capital_currency: &str,
    ) -> Result<()> {
        if config.legs.is_empty() {
            return Err(AppError::Storage(
                "portfolio execution requires at least one leg".into(),
            ));
        }
        let mut conids = BTreeSet::new();
        for leg in &config.legs {
            if leg.contract.conid <= 0
                || !leg.buy_target_quantity.is_finite()
                || !leg.sell_target_quantity.is_finite()
            {
                return Err(AppError::Storage(
                    "portfolio legs require valid contracts and finite targets".into(),
                ));
            }
            if !conids.insert(leg.contract.conid) {
                return Err(AppError::Storage(format!(
                    "portfolio execution contains duplicate conid {}; each contract may appear only once",
                    leg.contract.conid
                )));
            }
        }
        let first = &config.legs[0];
        let has_negative_target = config
            .legs
            .iter()
            .any(|leg| leg.buy_target_quantity < 0.0 || leg.sell_target_quantity < 0.0);
        let primary = StrategyExecutionConfig {
            strategy_id: config.strategy_id,
            account: config.account.clone(),
            target_quantity: first.buy_target_quantity,
            short_target_quantity: first.sell_target_quantity,
            allow_short: has_negative_target,
            order_type: config.order_type.clone(),
            paper_only: config.paper_only,
            outside_rth: config.outside_rth,
            contract: first.contract.clone(),
        };
        let capital_currency = normalized_currency(capital_currency)?;
        let supports_short_targets = self.validate_strategy_execution_config(&primary, true)?;
        if has_negative_target && !supports_short_targets {
            return Err(AppError::Storage(
                "this strategy kind does not support negative portfolio buy or sell targets".into(),
            ));
        }
        let assigned_model = self.execution_cost_model_for_strategy(config.strategy_id)?;
        let contracts = config
            .legs
            .iter()
            .map(|leg| leg.contract.clone())
            .collect::<Vec<_>>();
        self.validate_execution_contract_currencies(
            &contracts,
            assigned_model.as_ref().map(|model| model.currency.as_str()),
        )?;
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Self::persist_strategy_execution_config(&transaction, &primary, &capital_currency, now)?;
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

    #[cfg(test)]
    pub fn set_strategy_execution_enabled(
        &mut self,
        strategy_id: uuid::Uuid,
        enabled: bool,
    ) -> Result<bool> {
        self.set_strategy_execution_enabled_with_capital_currency(strategy_id, enabled, "USD")
    }

    fn enabled_strategy_execution_ownership_conflict(
        &self,
        strategy_id: uuid::Uuid,
    ) -> Result<Option<(uuid::Uuid, String, String, i64)>> {
        self.connection
            .query_row(
                "WITH execution_conids AS (
                   SELECT c.strategy_id, c.account_id,
                          try_cast(json_extract_string(c.contract_json, '$.conid') AS BIGINT) AS conid
                   FROM strategy_execution_configs c
                   UNION
                   SELECT c.strategy_id, c.account_id,
                          try_cast(json_extract_string(l.contract_json, '$.conid') AS BIGINT) AS conid
                   FROM strategy_execution_configs c
                   JOIN strategy_execution_portfolio_legs l USING (strategy_id)
                 )
                 SELECT occupied.strategy_id, s.name, occupied.account_id, occupied.conid
                 FROM execution_conids requested
                 JOIN execution_conids occupied
                   ON occupied.account_id = requested.account_id
                  AND occupied.conid = requested.conid
                  AND occupied.strategy_id <> requested.strategy_id
                 JOIN strategy_execution_configs occupied_config
                   ON occupied_config.strategy_id = occupied.strategy_id
                  AND occupied_config.enabled = true
                 JOIN strategies s ON s.strategy_id = occupied.strategy_id
                 WHERE requested.strategy_id = ? AND requested.conid IS NOT NULL
                 ORDER BY s.name, occupied.conid LIMIT 1",
                params![strategy_id],
                |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn set_strategy_execution_enabled_with_capital_currency(
        &mut self,
        strategy_id: uuid::Uuid,
        enabled: bool,
        capital_currency: &str,
    ) -> Result<bool> {
        let capital_currency = normalized_currency(capital_currency)?;
        if enabled {
            let risk_currency = self
                .connection
                .query_row(
                    "SELECT enabled, capital_currency FROM strategy_risk_controls
                     WHERE strategy_id = ?",
                    params![strategy_id],
                    |row| Ok((row.get::<_, bool>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            match risk_currency {
                None => {
                    return Err(AppError::Storage(
                        "cannot enable automatic execution: strategy risk control is missing; configure it before enabling execution"
                            .into(),
                    ));
                }
                Some((_enabled, configured_currency)) => match configured_currency {
                    Some(configured)
                        if configured.trim().eq_ignore_ascii_case(&capital_currency) => {}
                    Some(configured) => {
                        return Err(AppError::Storage(format!(
                            "cannot enable automatic execution: strategy capital currency {} does not match current risk.base_currency {}; pause the strategy and save its risk control again",
                            configured.trim().to_ascii_uppercase(),
                            capital_currency
                        )));
                    }
                    None => {
                        return Err(AppError::Storage(format!(
                            "cannot enable automatic execution: legacy strategy risk control has no capital currency; save it again using current risk.base_currency {capital_currency}"
                        )));
                    }
                },
            }
            self.validate_strategy_execution_currency_assignment(strategy_id)
                .map_err(|error| {
                    AppError::Storage(format!("cannot enable automatic execution: {error}"))
                })?;
            if let Some((occupied_strategy_id, occupied_name, account, conid)) =
                self.enabled_strategy_execution_ownership_conflict(strategy_id)?
            {
                return Err(AppError::Storage(format!(
                    "cannot enable automatic execution: account {account} conid {conid} is already controlled by enabled strategy '{occupied_name}' ({occupied_strategy_id}); one account position may have only one automatic strategy owner"
                )));
            }
        }
        let now = Utc::now();
        let changed = self
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET enabled = ?, enabled_at = CASE WHEN ? THEN ? ELSE NULL END,
                     updated_at = ?
                 WHERE strategy_id = ?",
                params![enabled, enabled, now, now, strategy_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if !enabled {
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'cancelled',
                         detail = 'automatic strategy execution was disabled',
                         updated_at = ?
                     WHERE strategy_id = ? AND state = 'active'",
                    params![now, strategy_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        Ok(changed > 0)
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

    /// Backward-compatible helper used by storage tests and local callers.
    #[cfg(test)]
    pub fn claim_strategy_action(&mut self) -> Result<Option<ClaimedStrategyAction>> {
        self.claim_strategy_action_inner("USD", 3_600, 30, Utc::now(), false)
    }

    pub fn claim_strategy_action_with_risk(
        &mut self,
        base_currency: &str,
        maximum_fx_age_seconds: u64,
        maximum_market_data_age_seconds: u64,
        now: DateTime<Utc>,
    ) -> Result<Option<ClaimedStrategyAction>> {
        self.claim_strategy_action_inner(
            base_currency,
            maximum_fx_age_seconds,
            maximum_market_data_age_seconds,
            now,
            true,
        )
    }

    /// Compatibility path for evaluations inserted by older binaries/tests.
    /// Production evaluation writes the desired target in the same transaction
    /// as the signal. Only the latest directional evaluation for each strategy
    /// may be recovered: replaying an older untracked row after a newer action
    /// would resurrect a target that the market regime has already replaced.
    /// An existing terminal action does not exclude recovery; claim-time
    /// position classification permits only a still-protective reduction to
    /// retry and abandons an old risk-increasing target.
    fn synchronize_untracked_desired_targets(&mut self) -> Result<()> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.evaluation_id, e.strategy_id, e.signal, e.created_at,
                        e.output_json::VARCHAR
                 FROM strategy_evaluations e
                 JOIN strategy_execution_configs c ON c.strategy_id = e.strategy_id
                 LEFT JOIN strategy_execution_desired_targets d
                   ON d.source_evaluation_id = e.evaluation_id
                 WHERE c.enabled = true AND c.enabled_at IS NOT NULL
                   AND e.created_at >= c.enabled_at
                   AND e.signal IN ('buy', 'sell')
                   AND d.source_evaluation_id IS NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM strategy_evaluations newer
                     WHERE newer.strategy_id = e.strategy_id
                       AND newer.signal IN ('buy', 'sell')
                       AND (newer.created_at > e.created_at OR
                            (newer.created_at = e.created_at
                             AND newer.evaluation_id > e.evaluation_id))
                   )
                 ORDER BY e.created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let evaluations = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, uuid::Uuid>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, DateTime<Utc>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        for (evaluation_id, strategy_id, signal, created_at, output_json) in evaluations {
            let flatten_only = output_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                .and_then(|output| {
                    output
                        .get("target_intent")
                        .and_then(serde_json::Value::as_str)
                        .map(|intent| intent == "flatten_only")
                })
                .unwrap_or(false);
            let Some(targets) = self.strategy_signal_targets(strategy_id, &signal, flatten_only)?
            else {
                continue;
            };
            let transaction = self
                .connection
                .transaction()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'superseded', superseded_by_evaluation_id = ?,
                         detail = 'superseded by a newer buy/sell signal', updated_at = ?
                     WHERE strategy_id = ? AND state = 'active'",
                    params![evaluation_id, created_at, strategy_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute(
                    "INSERT INTO strategy_execution_desired_targets
                     (desired_target_id, strategy_id, source_evaluation_id, signal,
                      targets_json, state, requires_flatten, flatten_completed_at,
                      superseded_by_evaluation_id, detail, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, 'active', false, NULL, NULL,
                             'recovered desired target for an untracked signal', ?, ?)
                     ON CONFLICT (source_evaluation_id) DO NOTHING",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        evaluation_id,
                        signal,
                        serde_json::to_string(&targets)?,
                        created_at,
                        created_at
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .commit()
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    fn claim_strategy_action_inner(
        &mut self,
        base_currency: &str,
        maximum_fx_age_seconds: u64,
        maximum_market_data_age_seconds: u64,
        now: DateTime<Utc>,
        enforce_strategy_risk: bool,
    ) -> Result<Option<ClaimedStrategyAction>> {
        self.record_disabled_strategy_actions()?;
        self.synchronize_untracked_desired_targets()?;
        // Position targets are computed from positions_current. While an IBKR
        // position snapshot is synchronizing that table is transiently empty;
        // claiming during the window would compute deltas against a zero
        // position (losing close signals forever or doubling entries). Defer
        // claiming until the snapshot is ready; evaluations stay unclaimed and
        // are retried on the next scheduler tick.
        let (position_sync_state, position_snapshot_completed_at) = self
            .connection
            .query_row(
                "SELECT state, snapshot_completed_at
                 FROM position_sync_state WHERE singleton",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<DateTime<Utc>>>(1)?,
                    ))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if position_sync_state != "ready" {
            return Ok(None);
        }
        // Due persistent targets are considered before fresh entries so a
        // continuous stream of new signals from another strategy cannot
        // starve a protective exit. Retry actions use a fresh synthetic
        // carrier UUID; they do not depend on another Hold evaluation and can
        // therefore continue while the strategy is paused or Bars are quiet.
        let retry_candidate = self
            .connection
            .query_row(
                "SELECT e.evaluation_id, e.strategy_id, e.signal,
                    c.account_id, c.target_quantity, c.order_type,
                    c.paper_only, c.contract_json::VARCHAR,
                    c.short_target_quantity, c.allow_short, e.output_json::VARCHAR,
                    e.short_value, e.long_value, s.kind, c.outside_rth, e.created_at,
                    d.desired_target_id, d.source_evaluation_id,
                    d.targets_json::VARCHAR, d.requires_flatten,
                    d.flatten_completed_at, true
             FROM strategy_execution_desired_targets d
             JOIN strategy_evaluations e
               ON e.evaluation_id = d.source_evaluation_id
             JOIN strategy_execution_configs c ON c.strategy_id = d.strategy_id
             JOIN strategies s ON s.strategy_id = d.strategy_id
             WHERE d.state = 'active' AND c.enabled = true
               AND c.enabled_at IS NOT NULL
               AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
               AND e.created_at >= c.enabled_at
               AND EXISTS (
                 SELECT 1 FROM strategy_execution_actions source_action
                 WHERE source_action.source_evaluation_id = d.source_evaluation_id
                    OR source_action.evaluation_id = d.source_evaluation_id
               )
             ORDER BY coalesce(d.next_attempt_at, d.created_at), d.created_at
             LIMIT 1",
                params![now],
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
                        row.get::<_, DateTime<Utc>>(15)?,
                        row.get::<_, uuid::Uuid>(16)?,
                        row.get::<_, uuid::Uuid>(17)?,
                        row.get::<_, String>(18)?,
                        row.get::<_, bool>(19)?,
                        row.get::<_, Option<DateTime<Utc>>>(20)?,
                        row.get::<_, bool>(21)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let candidate = if retry_candidate.is_some() {
            retry_candidate
        } else {
            self.connection
                .query_row(
                    "SELECT e.evaluation_id, e.strategy_id, e.signal,
                        c.account_id, c.target_quantity, c.order_type,
                        c.paper_only, c.contract_json::VARCHAR,
                        c.short_target_quantity, c.allow_short, e.output_json::VARCHAR,
                        e.short_value, e.long_value, s.kind, c.outside_rth, e.created_at,
                        d.desired_target_id, d.source_evaluation_id,
                        d.targets_json::VARCHAR, d.requires_flatten,
                        d.flatten_completed_at, false
                     FROM strategy_evaluations e
                     JOIN strategy_execution_configs c ON c.strategy_id = e.strategy_id
                     JOIN strategies s ON s.strategy_id = e.strategy_id
                     JOIN strategy_execution_desired_targets d
                       ON d.source_evaluation_id = e.evaluation_id
                      AND d.state = 'active'
                     LEFT JOIN strategy_execution_actions a
                       ON a.evaluation_id = e.evaluation_id
                     WHERE c.enabled = true AND c.enabled_at IS NOT NULL
                       AND e.created_at >= c.enabled_at
                       AND e.signal IN ('buy', 'sell')
                       AND a.evaluation_id IS NULL
                       AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
                     ORDER BY coalesce(d.next_attempt_at, d.created_at), e.created_at LIMIT 1",
                    params![now],
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
                            row.get::<_, DateTime<Utc>>(15)?,
                            row.get::<_, uuid::Uuid>(16)?,
                            row.get::<_, uuid::Uuid>(17)?,
                            row.get::<_, String>(18)?,
                            row.get::<_, bool>(19)?,
                            row.get::<_, Option<DateTime<Utc>>>(20)?,
                            row.get::<_, bool>(21)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?
        };
        let Some((
            mut carrier_evaluation_id,
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
            evaluation_created_at,
            desired_target_id,
            source_evaluation_id,
            desired_targets_json,
            desired_requires_flatten,
            _desired_flatten_completed_at,
            is_retry,
        )) = candidate
        else {
            return Ok(None);
        };
        if is_retry {
            carrier_evaluation_id = uuid::Uuid::now_v7();
        }
        let desired_targets: Vec<DesiredTargetLeg> = serde_json::from_str(&desired_targets_json)?;
        let mut desired_context = DesiredActionContext {
            desired_target_id,
            source_evaluation_id,
            carrier_evaluation_id,
            targets: desired_targets,
            requires_flatten: desired_requires_flatten,
            is_retry,
        };
        // Queue age is evaluated only after current positions classify the
        // requested change. An old opening signal must expire, but an old
        // target that is still a strict reduction must never be discarded.
        let signal_age_seconds = (now - evaluation_created_at).num_seconds();
        let signal_edge_bps = output_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .and_then(|output| {
                strategy_signal_edge_bps(&strategy_kind, &signal, indicator_a, indicator_b, &output)
            });
        let mut cost_control = self
            .connection
            .query_row(
                "SELECT m.cost_model_id, m.name, m.currency,
                        m.buy_fixed_fee, m.buy_per_share_fee,
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
                        model: ExecutionCostModelInput {
                            cost_model_id: Some(row.get(0)?),
                            name: row.get(1)?,
                            currency: row.get(2)?,
                            buy_fixed_fee: row.get(3)?,
                            buy_per_share_fee: row.get(4)?,
                            buy_rate_bps: row.get(5)?,
                            buy_min_fee: row.get(6)?,
                            sell_fixed_fee: row.get(7)?,
                            sell_per_share_fee: row.get(8)?,
                            sell_rate_bps: row.get(9)?,
                            sell_min_fee: row.get(10)?,
                            sell_tax_bps: row.get(11)?,
                            estimated_spread_bps: row.get(12)?,
                            estimated_slippage_bps: row.get(13)?,
                        },
                        minimum_cost_multiple: row.get(14)?,
                        maximum_commission_to_gross_profit_ratio: row.get(15)?,
                        minimum_completed_trades: row.get::<_, i64>(16)?.max(0) as usize,
                        actual_fee_bps_p90: None,
                    })
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if let Some(control) = &mut cost_control {
            control.actual_fee_bps_p90 =
                self.actual_fee_bps_p90_for_strategy(strategy_id, &control.model.currency)?;
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
        let idempotency_key = if desired_context.is_retry {
            format!(
                "strategy:{strategy_id}:{}:retry-of:{}",
                desired_context.carrier_evaluation_id, desired_context.source_evaluation_id
            )
        } else {
            format!(
                "strategy:{strategy_id}:{}",
                desired_context.carrier_evaluation_id
            )
        };
        let mut leg_inputs = Vec::new();
        let mut active_order_details = Vec::new();
        for (leg_index, leg_contract_json, _buy_target, _sell_target) in configured_legs {
            let leg_contract: crate::ibkr::ContractCandidate =
                serde_json::from_str(&leg_contract_json)?;
            let Some(final_target) = desired_context.targets.iter().find_map(|target| {
                (target.leg_index == leg_index && target.conid == leg_contract.conid)
                    .then_some(target.target_quantity)
            }) else {
                self.connection
                    .execute(
                        "UPDATE strategy_execution_desired_targets
                         SET state = 'abandoned',
                             detail = 'execution leg configuration no longer matches the desired target snapshot',
                             updated_at = ?
                         WHERE desired_target_id = ? AND state = 'active'",
                        params![now, desired_context.desired_target_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                return Ok(None);
            };
            let current_position = self
                .connection
                .query_row(
                    "SELECT quantity, observed_at FROM positions_current
                     WHERE account_id = ? AND conid = ?",
                    params![account, leg_contract.conid],
                    |row| Ok((row.get::<_, f64>(0)?, row.get::<_, DateTime<Utc>>(1)?)),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let evidence = self.position_evidence_state(&account, leg_contract.conid)?;
            let position_evidence_observed_at = current_position
                .map(|(_, observed_at)| observed_at)
                .or(position_snapshot_completed_at);
            if position_evidence_observed_at.is_none()
                || !evidence.is_caught_up(position_evidence_observed_at)
            {
                // A terminal order/execution can become visible before IBKR's
                // position stream publishes the resulting quantity.  Keep the
                // evaluation unclaimed so it is retried after positions_current
                // catches up instead of computing another target order from a
                // stale quantity. A short defer prevents this one row from
                // starving due exits belonging to other strategies.
                self.connection
                    .execute(
                        "UPDATE strategy_execution_desired_targets
                         SET next_attempt_at = ?,
                             detail = ?,
                             updated_at = ?
                         WHERE desired_target_id = ? AND state = 'active'",
                        params![
                            now + chrono::Duration::seconds(
                                DESIRED_TARGET_POSITION_EVIDENCE_RETRY_SECONDS,
                            ),
                            POSITION_EVIDENCE_WAIT_DETAIL,
                            now,
                            desired_context.desired_target_id
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                return Ok(None);
            }
            let current_position = current_position
                .map(|(quantity, _)| quantity)
                .unwrap_or(0.0);
            leg_inputs.push(DesiredLegInput {
                leg_index,
                contract: leg_contract.clone(),
                current_quantity: current_position,
                final_target_quantity: final_target,
            });
            let mut active_statement = self
                .connection
                .prepare(
                    "SELECT o.order_id, o.broker_order_id, o.status
                     FROM orders o JOIN order_intents i
                       ON i.order_intent_id = o.order_intent_id
                     WHERE i.account_id = ? AND i.conid = ?
                       AND (lower(o.status) IN
                              ('submitted','presubmitted','pending_submit','pendingsubmit',
                               'pending_cancel','pendingcancel','cancel_pending',
                               'apipending','api_pending')
                            OR o.filled_quantity > coalesce((
                                 SELECT sum(e.quantity) FROM executions e
                                 WHERE e.order_id = o.order_id), 0) + 0.000000001)
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
            // Intents whose broker submission is in flight ('approved') or
            // whose outcome could not be confirmed ('unknown') have no orders
            // row yet, but the order may already be live at IBKR. They must
            // block further automatic submissions for the same contract until
            // they resolve (unknown intents resolve only through
            // reconciliation).
            let mut pending_statement = self
                .connection
                .prepare(
                    "SELECT order_intent_id, status FROM order_intents
                     WHERE account_id = ? AND conid = ?
                       AND status IN ('approved', 'unknown')
                     ORDER BY created_at",
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let pending_intents = pending_statement
                .query_map(params![account, leg_contract.conid], |row| {
                    Ok((row.get::<_, uuid::Uuid>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| AppError::Storage(error.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            drop(pending_statement);
            if !active_orders.is_empty() || !pending_intents.is_empty() {
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
                for (intent_id, status) in pending_intents {
                    active_order_details.push(format!(
                        "{}（Conid {}，账户 {}）存在未决订单意图：Intent {}，状态 {}；\
                         'unknown' 状态必须先通过对账确认结果",
                        leg_contract.symbol, leg_contract.conid, account, intent_id, status
                    ));
                }
                continue;
            }
        }
        let reversal_detected = leg_inputs.iter().any(|leg| {
            leg.current_quantity.abs() > POSITION_QUANTITY_EPSILON
                && leg.final_target_quantity.abs() > POSITION_QUANTITY_EPSILON
                && leg.current_quantity.signum() != leg.final_target_quantity.signum()
        });
        if reversal_detected {
            for target in &mut desired_context.targets {
                if let Some(leg) = leg_inputs.iter().find(|leg| {
                    leg.leg_index == target.leg_index && leg.contract.conid == target.conid
                }) {
                    target.requires_flatten |= leg.current_quantity.abs()
                        > POSITION_QUANTITY_EPSILON
                        && leg.final_target_quantity.abs() > POSITION_QUANTITY_EPSILON
                        && leg.current_quantity.signum() != leg.final_target_quantity.signum();
                }
            }
        }
        let requires_flatten = desired_context.requires_flatten || reversal_detected;
        if reversal_detected && !desired_context.requires_flatten {
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET requires_flatten = true, targets_json = ?,
                         detail = 'cross-zero reversal is executing its flatten phase',
                         updated_at = ?
                     WHERE desired_target_id = ? AND state = 'active'",
                    params![
                        serde_json::to_string(&desired_context.targets)?,
                        now,
                        desired_context.desired_target_id
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            desired_context.requires_flatten = true;
        }
        // A cross-zero event authorizes only the protective flatten. Opening
        // exposure on the opposite side needs a later, explicit directional
        // evaluation which will create a new desired target and rerun all
        // entry filters, cost checks and risk gates with current data.
        let flatten_phase = requires_flatten;
        let mut legs = Vec::new();
        let mut target_positions = Vec::new();
        let all_at_final_target = leg_inputs.iter().all(|leg| {
            (leg.current_quantity - leg.final_target_quantity).abs() <= POSITION_QUANTITY_EPSILON
        });
        for leg in leg_inputs {
            let target_requires_flatten = desired_context.targets.iter().any(|target| {
                target.leg_index == leg.leg_index
                    && target.conid == leg.contract.conid
                    && target.requires_flatten
            });
            let target = if flatten_phase && target_requires_flatten {
                0.0
            } else if flatten_phase
                && !position_change_is_risk_reducing(
                    leg.current_quantity,
                    leg.final_target_quantity,
                )
            {
                // A portfolio reversal's first phase may include other strict
                // reductions, but never smuggles a risk-increasing leg into
                // the flatten batch.
                leg.current_quantity
            } else {
                leg.final_target_quantity
            };
            target_positions.push((leg.contract.clone(), target));
            let delta = target - leg.current_quantity;
            let side = if delta > POSITION_QUANTITY_EPSILON {
                Some("buy")
            } else if delta < -POSITION_QUANTITY_EPSILON {
                Some("sell")
            } else {
                None
            };
            if let Some(side) = side {
                legs.push(ClaimedStrategyLeg {
                    leg_index: leg.leg_index,
                    side: side.into(),
                    quantity: delta.abs(),
                    current_quantity: leg.current_quantity,
                    target_quantity: target,
                    contract: leg.contract,
                    idempotency_key: format!("{idempotency_key}:leg:{}", leg.leg_index),
                });
            }
        }
        let risk_reducing =
            !legs.is_empty() && legs.iter().all(ClaimedStrategyLeg::is_risk_reducing);
        if desired_context.is_retry && !active_order_details.is_empty() {
            // Let another strategy's due protective target be considered on
            // the next worker tick instead of repeatedly selecting this
            // blocked row and starving the whole queue.
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET next_attempt_at = ?, updated_at = ?
                     WHERE desired_target_id = ? AND state = 'active'",
                    params![
                        now + chrono::Duration::seconds(DESIRED_TARGET_RETRY_DELAY_SECONDS),
                        now,
                        desired_context.desired_target_id
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Ok(None);
        }
        if all_at_final_target && active_order_details.is_empty() {
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'satisfied', detail = 'desired target is reflected by the position stream',
                         updated_at = ?
                     WHERE desired_target_id = ? AND state = 'active'",
                    params![now, desired_context.desired_target_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            if desired_context.is_retry {
                // Reaching the target during retry is state, not another
                // execution attempt; avoid synthetic skipped-action noise.
                return Ok(None);
            }
        }
        if requires_flatten && legs.is_empty() && active_order_details.is_empty() {
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'satisfied', requires_flatten = true,
                         flatten_completed_at = ?,
                         detail = 'protective flatten is confirmed; opening the opposite side requires a fresh directional signal',
                         updated_at = ?
                     WHERE desired_target_id = ? AND state = 'active'",
                    params![now, now, desired_context.desired_target_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Ok(None);
        }
        if desired_context.is_retry && !risk_reducing {
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'abandoned',
                         detail = 'risk-increasing target is not automatically retried; a fresh directional signal is required',
                         updated_at = ?
                     WHERE desired_target_id = ? AND state = 'active'",
                    params![now, desired_context.desired_target_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Ok(None);
        }
        let source_bar_staleness = if risk_reducing {
            None
        } else {
            let (bar_time, config_json): (DateTime<Utc>, String) = self
                .connection
                .query_row(
                    "SELECT e.bar_time, s.config_json::VARCHAR
                     FROM strategy_evaluations e
                     JOIN strategies s ON s.strategy_id = e.strategy_id
                     WHERE e.evaluation_id = ?",
                    params![source_evaluation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let strategy = crate::strategy::build(
                &strategy_kind,
                serde_json::from_str::<serde_json::Value>(&config_json)?,
            )
            .map_err(AppError::Storage)?;
            let maximum_bar_age = timeframe_duration(strategy.bar_timeframe())?
                .num_seconds()
                .saturating_mul(3)
                .max(30);
            let bar_age = (now - bar_time).num_seconds();
            (bar_age > maximum_bar_age).then_some((
                bar_time,
                bar_age,
                maximum_bar_age,
                strategy.bar_timeframe().to_owned(),
            ))
        };
        if !risk_reducing
            && (signal_age_seconds > MAX_EXECUTABLE_SIGNAL_AGE_SECONDS
                || source_bar_staleness.is_some())
        {
            let idempotency_key = format!("strategy:{strategy_id}:{carrier_evaluation_id}");
            let stale_detail = if let Some((bar_time, bar_age, maximum_bar_age, timeframe)) =
                source_bar_staleness
            {
                format!(
                    "source {timeframe} Bar at {bar_time} is {bar_age} seconds old; maximum is \
                     {maximum_bar_age} seconds"
                )
            } else {
                format!(
                    "evaluation was queued for {signal_age_seconds} seconds; maximum is \
                     {MAX_EXECUTABLE_SIGNAL_AGE_SECONDS} seconds"
                )
            };
            self.connection
                .execute(
                    "INSERT INTO strategy_execution_actions
                     (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                      requested_quantity, state, detail, created_at, updated_at,
                      cost_gate_result, source_evaluation_id)
                     VALUES (?, ?, ?, ?, ?, NULL, 'skipped', ?, ?, ?, 'stale_signal', ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        carrier_evaluation_id,
                        idempotency_key,
                        signal,
                        format!(
                            "risk-increasing signal skipped as stale: {stale_detail}; old market \
                             context is never used to open exposure"
                        ),
                        now,
                        now,
                        source_evaluation_id
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            self.connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'expired',
                         detail = 'risk-increasing source signal expired before execution',
                         updated_at = ?
                     WHERE desired_target_id = ? AND state = 'active'",
                    params![now, desired_target_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Ok(None);
        }
        let risk_control = if enforce_strategy_risk {
            self.strategy_risk_control(strategy_id)?
        } else {
            None
        };
        let requested_capital_currency = base_currency.trim().to_ascii_uppercase();
        let risk_currency_issue = if enforce_strategy_risk && risk_control.is_none() {
            Some("策略风险控制缺失；请暂停策略并重新保存风险控制".to_owned())
        } else {
            risk_control
                .as_ref()
                .and_then(|control| match control.capital_currency.as_deref() {
                Some(currency)
                    if currency
                        .trim()
                        .eq_ignore_ascii_case(&requested_capital_currency) =>
                {
                    None
                }
                Some(currency) => Some(format!(
                    "策略资本币种 {} 与当前 risk.base_currency {} 不一致；请暂停策略并重新保存风险控制",
                    currency.trim().to_ascii_uppercase(), requested_capital_currency
                )),
                None => Some(format!(
                    "旧版风险控制没有保存策略资本币种；当前 risk.base_currency 为 {}，请暂停策略并重新保存风险控制",
                    requested_capital_currency
                )),
            })
        };
        let statistics_reset_at = risk_control
            .as_ref()
            .map(|control| control.statistics_reset_at)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("Unix epoch is valid"));
        let needs_statistics = !risk_reducing
            && (risk_control.as_ref().is_some_and(|control| control.enabled)
                || cost_control.is_some());
        let statistics = needs_statistics
            .then(|| {
                self.strategy_risk_statistics(
                    strategy_id,
                    base_currency,
                    maximum_fx_age_seconds,
                    statistics_reset_at,
                    now,
                )
            })
            .transpose()?;
        let mut opening_block_detail =
            (!risk_reducing && active_order_details.is_empty() && !legs.is_empty())
                .then(|| {
                    risk_currency_issue
                        .as_deref()
                        .map(|issue| format!("策略风险门控仅阻止开仓：{issue}；减仓和平仓仍允许"))
                })
                .flatten();
        if !risk_reducing
            && active_order_details.is_empty()
            && !legs.is_empty()
            && let Some(control) = risk_control.as_ref().filter(|control| control.enabled)
        {
            let statistics = statistics.as_ref().expect("risk statistics requested");
            if let Some(issue) = risk_currency_issue.as_deref() {
                opening_block_detail =
                    Some(format!("策略风险门控仅阻止开仓：{issue}；减仓和平仓仍允许"));
            } else if !statistics.data_complete {
                opening_block_detail = Some(format!(
                    "策略风险门控仅阻止开仓：统计数据不完整（{}）；减仓和平仓仍允许",
                    statistics.warning.as_deref().unwrap_or("原因未知")
                ));
            }
            let mut projected_exposure = 0.0;
            if opening_block_detail.is_none() && control.maximum_position_capital_ratio > 0.0 {
                for (contract, target) in &target_positions {
                    let health = self.market_data_health(
                        contract.conid,
                        maximum_market_data_age_seconds,
                        now,
                    )?;
                    let Some(price) = (health.state == "fresh")
                        .then_some(health.latest_price)
                        .flatten()
                    else {
                        opening_block_detail = Some(format!(
                            "策略风险门控仅阻止开仓：{}（Conid {}）没有新鲜实时行情；减仓和平仓仍允许",
                            contract.symbol, contract.conid
                        ));
                        break;
                    };
                    let Some(fx) = self.currency_conversion_rate(
                        &contract.currency,
                        base_currency,
                        maximum_fx_age_seconds,
                        now,
                    )?
                    else {
                        opening_block_detail = Some(format!(
                            "策略风险门控仅阻止开仓：没有可将 {} 转换为 {} 的新鲜汇率；减仓和平仓仍允许",
                            contract.currency, base_currency
                        ));
                        break;
                    };
                    projected_exposure += target.abs() * price * fx;
                }
                let maximum = control.strategy_capital * control.maximum_position_capital_ratio;
                if opening_block_detail.is_none() && projected_exposure > maximum {
                    opening_block_detail = Some(format!(
                        "策略风险门控仅阻止开仓：目标持仓预计占用 {:.2} {}，超过策略资本上限 {:.2} {}（资本 {:.2} × 比例 {:.4}）；减仓和平仓仍允许",
                        projected_exposure,
                        base_currency.to_ascii_uppercase(),
                        maximum,
                        base_currency.to_ascii_uppercase(),
                        control.strategy_capital,
                        control.maximum_position_capital_ratio
                    ));
                }
            }
            let loss_limit =
                control.strategy_capital * control.maximum_rolling_24h_realized_net_loss_ratio;
            if opening_block_detail.is_none()
                && loss_limit > 0.0
                && statistics.rolling_24h_realized_net_pnl <= -loss_limit
            {
                opening_block_detail = Some(format!(
                    "策略风险门控仅阻止开仓：滚动 24 小时已实现净损益 {:.2} {} 已达到亏损上限 -{:.2} {}；减仓和平仓仍允许",
                    statistics.rolling_24h_realized_net_pnl,
                    base_currency.to_ascii_uppercase(),
                    loss_limit,
                    base_currency.to_ascii_uppercase()
                ));
            }
            if opening_block_detail.is_none()
                && control.maximum_consecutive_net_losing_trades > 0
                && statistics.consecutive_net_losing_trades
                    >= control.maximum_consecutive_net_losing_trades
            {
                opening_block_detail = Some(format!(
                    "策略风险门控仅阻止开仓：连续净亏损交易 {} 笔，达到上限 {} 笔；减仓和平仓仍允许",
                    statistics.consecutive_net_losing_trades,
                    control.maximum_consecutive_net_losing_trades
                ));
            }
            if opening_block_detail.is_none()
                && control.maximum_rolling_24h_completed_trades > 0
                && statistics.rolling_24h_completed_trades
                    >= control.maximum_rolling_24h_completed_trades
            {
                opening_block_detail = Some(format!(
                    "策略风险门控仅阻止开仓：滚动 24 小时已完成 {} 笔交易，达到上限 {} 笔；减仓和平仓仍允许",
                    statistics.rolling_24h_completed_trades,
                    control.maximum_rolling_24h_completed_trades
                ));
            }
            let turnover_limit =
                control.strategy_capital * control.maximum_rolling_24h_turnover_capital_ratio;
            if opening_block_detail.is_none()
                && turnover_limit > 0.0
                && statistics.rolling_24h_turnover >= turnover_limit
            {
                opening_block_detail = Some(format!(
                    "策略风险门控仅阻止开仓：滚动 24 小时换手 {:.2} {}，达到上限 {:.2} {}；减仓和平仓仍允许",
                    statistics.rolling_24h_turnover,
                    base_currency.to_ascii_uppercase(),
                    turnover_limit,
                    base_currency.to_ascii_uppercase()
                ));
            }
        }
        if !risk_reducing
            && opening_block_detail.is_none()
            && active_order_details.is_empty()
            && !legs.is_empty()
            && let (Some(control), Some(statistics)) = (&cost_control, &statistics)
        {
            if !statistics.data_complete {
                opening_block_detail = Some(format!(
                    "成本绩效门控仅阻止开仓：成交或佣金统计不完整（{}）；减仓和平仓仍允许",
                    statistics.warning.as_deref().unwrap_or("原因未知")
                ));
            } else if let Some(ratio) = blocked_commission_performance_ratio(
                statistics.completed_trades_since_reset,
                statistics.gross_pnl_since_reset,
                statistics.commissions_since_reset,
                control.minimum_completed_trades,
                control.maximum_commission_to_gross_profit_ratio,
            ) {
                opening_block_detail = Some(format!(
                    "成本绩效门控仅阻止开仓：完成 {} 笔交易后佣金/毛损益比例 {:.2}% 超过上限 {:.2}%；减仓和平仓仍允许",
                    statistics.completed_trades_since_reset,
                    ratio * 100.0,
                    control.maximum_commission_to_gross_profit_ratio * 100.0
                ));
            }
        }
        let quantity = (active_order_details.is_empty() && !legs.is_empty())
            .then_some(legs.iter().map(|leg| leg.quantity).sum::<f64>());
        let action_id = uuid::Uuid::now_v7();
        let (state, detail, gate_result) = match quantity {
            Some(_) if opening_block_detail.is_some() => {
                ("skipped", opening_block_detail, Some("risk_blocked"))
            }
            Some(_) => ("processing", None, None),
            None if !active_order_details.is_empty() => (
                "skipped",
                Some(format!(
                    "未提交新订单，以避免同一证券重复或冲突下单。{}。请等待订单结束，或在“订单与成交”页面手动取消。",
                    active_order_details.join("；")
                )),
                Some("not_run"),
            ),
            None => (
                "skipped",
                Some("signal requires no position change".to_owned()),
                Some("not_run"),
            ),
        };
        self.connection
            .execute(
                "INSERT INTO strategy_execution_actions
                 (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                  requested_quantity, state, order_intent_id, broker_order_id,
                  detail, created_at, updated_at, cost_gate_result,
                  source_evaluation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)",
                params![
                    action_id,
                    strategy_id,
                    carrier_evaluation_id,
                    idempotency_key,
                    signal,
                    quantity,
                    state,
                    detail,
                    now,
                    now,
                    gate_result,
                    source_evaluation_id
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.connection
            .execute(
                "UPDATE strategy_execution_desired_targets
                 SET last_attempt_at = ?, next_attempt_at = ?, updated_at = ?
                 WHERE desired_target_id = ? AND state = 'active'",
                params![
                    now,
                    now + chrono::Duration::seconds(DESIRED_TARGET_RETRY_DELAY_SECONDS),
                    now,
                    desired_context.desired_target_id
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some(quantity) = quantity else {
            return Ok(None);
        };
        let leg_state = if state == "processing" {
            "processing"
        } else {
            "skipped"
        };
        for leg in &legs {
            self.connection
                .execute(
                    "INSERT INTO strategy_execution_action_legs
                     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)",
                    params![
                        action_id,
                        leg.leg_index,
                        leg.contract.conid,
                        leg.contract.symbol,
                        leg.target_quantity,
                        leg.side,
                        leg.quantity,
                        leg_state,
                        detail,
                        now,
                        now
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        if state != "processing" {
            return Ok(None);
        }
        let first = legs
            .first()
            .cloned()
            .expect("non-empty legs imply claimed quantity");
        Ok(Some(ClaimedStrategyAction {
            action_id,
            strategy_id,
            evaluation_id: carrier_evaluation_id,
            source_evaluation_id: desired_context.source_evaluation_id,
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
                      cost_gate_result, source_evaluation_id)
                     VALUES (?, ?, ?, ?, ?, NULL, 'skipped', ?, ?, ?,
                             'execution_disabled', ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        evaluation_id,
                        format!("strategy:{strategy_id}:{evaluation_id}"),
                        signal,
                        "automatic execution skipped: strategy execution is disabled; enable \
                         Paper execution to process future signals",
                        now,
                        now,
                        evaluation_id
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
                "UPDATE strategy_execution_actions SET state = ?,
                order_intent_id = coalesce(?, order_intent_id),
                broker_order_id = coalesce(?, broker_order_id), detail = ?, updated_at = ?
                 WHERE action_id = ?",
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
                 SET state = ?,
                     order_intent_id = coalesce(?, order_intent_id),
                     broker_order_id = coalesce(?, broker_order_id),
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

    /// Binds an order intent to its automatic strategy leg before the RPC
    /// handler releases the storage mutex or contacts IBKR. If the process then
    /// stops before the worker receives the RPC response, recovery and
    /// reconciliation can still trace the approved/unknown intent back to the
    /// revoked desired target.
    pub fn bind_strategy_action_leg_order_intent(
        &mut self,
        action_id: uuid::Uuid,
        leg_index: i32,
        order_intent_id: uuid::Uuid,
    ) -> Result<()> {
        let now = Utc::now();
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let changed = transaction
            .execute(
                "UPDATE strategy_execution_action_legs
                 SET order_intent_id = ?, updated_at = ?
                 WHERE action_id = ? AND leg_index = ? AND state = 'processing'
                   AND (order_intent_id IS NULL OR order_intent_id = ?)",
                params![order_intent_id, now, action_id, leg_index, order_intent_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(AppError::Storage(format!(
                "cannot bind order intent {order_intent_id}: strategy action {action_id} leg {leg_index} is no longer processing or already references another intent"
            )));
        }
        let parent_changed = transaction
            .execute(
                "UPDATE strategy_execution_actions
                 SET order_intent_id = coalesce(order_intent_id, ?), updated_at = ?
                 WHERE action_id = ? AND state = 'processing'",
                params![order_intent_id, now, action_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if parent_changed != 1 {
            return Err(AppError::Storage(format!(
                "cannot bind order intent {order_intent_id}: strategy action {action_id} is no longer processing"
            )));
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    /// Fail-closed authorization check performed immediately before each
    /// broker submission. Claiming an action does not hold the storage lock
    /// across asynchronous calendar and RPC work, so a newer signal or an
    /// operator reconfiguration may revoke it in the meantime.
    pub fn ensure_strategy_action_leg_submission_authorized(
        &self,
        action_id: uuid::Uuid,
        leg_index: i32,
        expected_account: &str,
        expected_contract: &crate::ibkr::ContractCandidate,
    ) -> Result<()> {
        let authorization = self
            .connection
            .query_row(
                "SELECT a.strategy_id, a.state,
                        coalesce(a.source_evaluation_id, a.evaluation_id),
                        c.enabled, c.account_id, l.state, l.conid
                 FROM strategy_execution_actions a
                 JOIN strategy_execution_configs c ON c.strategy_id = a.strategy_id
                 JOIN strategy_execution_action_legs l ON l.action_id = a.action_id
                 WHERE a.action_id = ? AND l.leg_index = ?",
                params![action_id, leg_index],
                |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, uuid::Uuid>(2)?,
                        row.get::<_, bool>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| {
                AppError::Storage(format!(
                    "strategy submission authorization revoked: action {action_id} leg {leg_index} or its execution configuration no longer exists"
                ))
            })?;
        let (
            strategy_id,
            action_state,
            source_evaluation_id,
            execution_enabled,
            configured_account,
            leg_state,
            action_leg_conid,
        ) = authorization;
        if action_state != "processing" {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: action {action_id} is {action_state}, not processing"
            )));
        }
        if leg_state != "processing" {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: action {action_id} leg {leg_index} is {leg_state}, not processing"
            )));
        }
        if !execution_enabled {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: automatic execution was disabled"
                    .into(),
            ));
        }
        if configured_account != expected_account.trim() {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: configured account changed from {} to {}",
                expected_account.trim(),
                configured_account
            )));
        }
        if action_leg_conid != i64::from(expected_contract.conid) {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: action leg conid {action_leg_conid} does not match claimed conid {}",
                expected_contract.conid
            )));
        }
        let desired_is_active: bool = self
            .connection
            .query_row(
                "SELECT count(*) > 0 FROM strategy_execution_desired_targets
                 WHERE strategy_id = ? AND source_evaluation_id = ? AND state = 'active'",
                params![strategy_id, source_evaluation_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if !desired_is_active {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: the source target was superseded, satisfied, cancelled or expired"
                    .into(),
            ));
        }
        let configured_contract_json = self
            .connection
            .query_row(
                "SELECT contract_json::VARCHAR
                 FROM strategy_execution_portfolio_legs
                 WHERE strategy_id = ? AND leg_index = ?
                 UNION ALL
                 SELECT c.contract_json::VARCHAR
                 FROM strategy_execution_configs c
                 WHERE c.strategy_id = ?
                   AND NOT EXISTS (
                     SELECT 1 FROM strategy_execution_portfolio_legs p
                     WHERE p.strategy_id = c.strategy_id
                   )
                 LIMIT 1",
                params![strategy_id, leg_index, strategy_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| {
                AppError::Storage(format!(
                    "strategy submission authorization revoked: configured leg {leg_index} no longer exists"
                ))
            })?;
        let configured_contract: serde_json::Value =
            serde_json::from_str(&configured_contract_json)?;
        let expected_contract = serde_json::to_value(expected_contract)?;
        if configured_contract != expected_contract {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: configured contract changed after the action was claimed"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Revalidates an automatic strategy order at the final local commit point.
    ///
    /// Callers must keep the storage mutex guard alive through the subsequent
    /// `create_order_intent` call. This closes the gap between the execution
    /// worker's asynchronous preflight and intent persistence: a superseding
    /// signal, configuration change, position update, or competing order cannot
    /// silently turn a target-position delta into an overfill or reverse entry.
    pub fn ensure_strategy_order_submission_authorized(
        &self,
        provenance: &quant_rpc_types::StrategyOrderProvenance,
        idempotency_key: &str,
        expected_account: &str,
        request: &crate::ibkr::BrokerOrderRequest,
    ) -> Result<()> {
        self.ensure_strategy_action_leg_submission_authorized(
            provenance.action_id,
            provenance.leg_index,
            expected_account,
            &request.contract,
        )?;

        let persisted = self
            .connection
            .query_row(
                "SELECT a.strategy_id, coalesce(a.source_evaluation_id, a.evaluation_id),
                        a.idempotency_key, l.target_quantity, l.requested_side,
                        l.requested_quantity, c.order_type, c.outside_rth
                 FROM strategy_execution_actions a
                 JOIN strategy_execution_action_legs l USING (action_id)
                 JOIN strategy_execution_configs c ON c.strategy_id = a.strategy_id
                 WHERE a.action_id = ? AND l.leg_index = ?",
                params![provenance.action_id, provenance.leg_index],
                |row| {
                    Ok((
                        row.get::<_, uuid::Uuid>(0)?,
                        row.get::<_, uuid::Uuid>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<f64>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, bool>(7)?,
                    ))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let (
            persisted_strategy_id,
            persisted_source_evaluation_id,
            action_idempotency_key,
            persisted_target,
            persisted_side,
            persisted_quantity,
            configured_order_type,
            configured_outside_rth,
        ) = persisted;
        let expected_leg_idempotency_key =
            format!("{action_idempotency_key}:leg:{}", provenance.leg_index);
        if persisted_strategy_id != provenance.strategy_id {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: provenance strategy does not match the persisted action"
                    .into(),
            ));
        }
        if persisted_source_evaluation_id != provenance.source_evaluation_id {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: provenance source evaluation does not match the active desired target"
                    .into(),
            ));
        }
        if idempotency_key != expected_leg_idempotency_key {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: leg idempotency key does not match the persisted action"
                    .into(),
            ));
        }
        let persisted_side = persisted_side.ok_or_else(|| {
            AppError::Storage(
                "strategy submission authorization revoked: persisted leg has no side".into(),
            )
        })?;
        let persisted_quantity = persisted_quantity.ok_or_else(|| {
            AppError::Storage(
                "strategy submission authorization revoked: persisted leg has no quantity".into(),
            )
        })?;
        let quantities_match = |left: f64, right: f64| {
            left.is_finite()
                && right.is_finite()
                && (left - right).abs() <= POSITION_QUANTITY_EPSILON
        };
        if !quantities_match(persisted_target, provenance.target_quantity)
            || !persisted_side.eq_ignore_ascii_case(&provenance.side)
            || !persisted_side.eq_ignore_ascii_case(&request.side)
            || !quantities_match(persisted_quantity, provenance.quantity)
            || !quantities_match(persisted_quantity, request.quantity)
        {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: target, side, or quantity provenance does not match the persisted action leg"
                    .into(),
            ));
        }

        let configured_order_type = configured_order_type.trim().to_ascii_lowercase();
        let requested_order_type = request.order_type.trim().to_ascii_lowercase();
        if !matches!(configured_order_type.as_str(), "market" | "limit")
            || requested_order_type != configured_order_type
        {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: order type {} does not match configured order type {}",
                request.order_type, configured_order_type
            )));
        }
        if request.outside_rth != configured_outside_rth {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: outside_rth={} does not match configured outside_rth={configured_outside_rth}",
                request.outside_rth
            )));
        }
        match (configured_order_type.as_str(), request.limit_price) {
            ("market", None) => {}
            ("market", Some(_)) => {
                return Err(AppError::Storage(
                    "strategy submission authorization revoked: a market order must not carry a limit price"
                        .into(),
                ));
            }
            ("limit", Some(price)) if price.is_finite() && price > 0.0 => {}
            ("limit", _) => {
                return Err(AppError::Storage(
                    "strategy submission authorization revoked: a limit order requires a finite positive limit price"
                        .into(),
                ));
            }
            _ => unreachable!("configured order type was validated above"),
        }

        let (position_sync_state, snapshot_completed_at) = self
            .connection
            .query_row(
                "SELECT state, snapshot_completed_at
                 FROM position_sync_state WHERE singleton",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<DateTime<Utc>>>(1)?,
                    ))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if position_sync_state != "ready" {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: position snapshot is not ready".into(),
            ));
        }
        let current_position = self
            .connection
            .query_row(
                "SELECT quantity, observed_at FROM positions_current
                 WHERE account_id = ? AND conid = ?",
                params![expected_account.trim(), request.contract.conid],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, DateTime<Utc>>(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let evidence =
            self.position_evidence_state(expected_account.trim(), request.contract.conid)?;
        let position_evidence_observed_at = current_position
            .map(|(_, observed_at)| observed_at)
            .or(snapshot_completed_at);
        if position_evidence_observed_at.is_none()
            || !evidence.is_caught_up(position_evidence_observed_at)
        {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: position evidence has not caught up with executions"
                    .into(),
            ));
        }
        let current_quantity = current_position
            .map(|(quantity, _)| quantity)
            .unwrap_or(0.0);
        if !quantities_match(current_quantity, provenance.claimed_current_quantity) {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: position changed after claim from {} to {}; the desired target will be recomputed",
                provenance.claimed_current_quantity, current_quantity
            )));
        }
        let delta = persisted_target - current_quantity;
        let (expected_side, expected_quantity) = if delta > POSITION_QUANTITY_EPSILON {
            ("buy", delta)
        } else if delta < -POSITION_QUANTITY_EPSILON {
            ("sell", -delta)
        } else {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: the latest position already reflects the target"
                    .into(),
            ));
        };
        if !persisted_side.eq_ignore_ascii_case(expected_side)
            || !quantities_match(persisted_quantity, expected_quantity)
        {
            return Err(AppError::Storage(format!(
                "strategy submission authorization revoked: latest position {} requires {} {} to reach target {}, not {} {}",
                current_quantity,
                expected_side,
                expected_quantity,
                persisted_target,
                persisted_side,
                persisted_quantity
            )));
        }

        let conflicting_order_count: i64 = self
            .connection
            .query_row(
                "SELECT count(*)
                 FROM order_intents i
                 LEFT JOIN orders o USING (order_intent_id)
                 WHERE i.account_id = ? AND i.conid = ?
                   AND (i.status IN ('approved', 'unknown')
                        OR lower(o.status) IN
                           ('submitted','presubmitted','pending_submit','pendingsubmit',
                            'pending_cancel','pendingcancel','cancel_pending',
                            'apipending','api_pending'))",
                params![expected_account.trim(), request.contract.conid],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if conflicting_order_count > 0 {
            return Err(AppError::Storage(
                "strategy submission authorization revoked: another intent or active order now controls this account position"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Returns every known, still-open broker order whose originating desired
    /// target is no longer active (superseded, cancelled, expired, abandoned,
    /// otherwise completed, or absent after an older-schema migration).
    ///
    /// Unknown intents are deliberately excluded: without a confirmed broker
    /// order identity they must remain blocked until reconciliation/manual
    /// resolution. The caller uses the ordinary reconciliation-first cancel
    /// RPC, so this method never sends a compensation order.
    pub fn revoked_strategy_order_cancellations(
        &self,
    ) -> Result<Vec<RevokedStrategyOrderCancellation>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT a.strategy_id, a.action_id, l.leg_index, o.broker_order_id
                 FROM strategy_execution_actions a
                 JOIN strategy_execution_action_legs l ON l.action_id = a.action_id
                 JOIN order_intents i ON i.order_intent_id = l.order_intent_id
                 JOIN orders o ON o.order_intent_id = i.order_intent_id
                 WHERE i.status = 'submitted'
                   AND o.broker_order_id IS NOT NULL
                   AND lower(o.status) IN
                       ('submitted', 'presubmitted', 'pendingsubmit', 'apipending')
                   AND NOT EXISTS (
                       SELECT 1
                       FROM strategy_execution_desired_targets d
                       WHERE d.strategy_id = a.strategy_id
                         AND d.source_evaluation_id =
                             coalesce(a.source_evaluation_id, a.evaluation_id)
                         AND d.state = 'active'
                   )
                 ORDER BY o.created_at, a.created_at, l.leg_index",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        statement
            .query_map([], |row| {
                Ok(RevokedStrategyOrderCancellation {
                    strategy_id: row.get(0)?,
                    action_id: row.get(1)?,
                    leg_index: row.get(2)?,
                    broker_order_id: row.get(3)?,
                })
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
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
                    signal_edge_bps, cost_gate_result, source_evaluation_id
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
                    "cost_gate_result": row.get::<_, Option<String>>(16)?,
                    "source_evaluation_id": row.get::<_, Option<uuid::Uuid>>(17)?
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
                    i.security_type, i.currency, i.local_symbol,
                    EXISTS (
                        SELECT 1 FROM strategy_execution_portfolio_legs portfolio
                        WHERE portfolio.strategy_id = s.strategy_id
                    ) AS is_portfolio
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
                    "local_symbol": row.get::<_, Option<String>>(16)?,
                    "is_portfolio": row.get::<_, bool>(17)?
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut strategies = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for strategy in &mut strategies {
            let strategy_id = strategy["strategy_id"]
                .as_str()
                .unwrap_or("unknown strategy");
            let kind = strategy["kind"]
                .as_str()
                .ok_or_else(|| AppError::Storage(format!("strategy {strategy_id} has no kind")))?;
            let implementation =
                crate::strategy::build(kind, strategy["config"].clone()).map_err(|error| {
                    AppError::Storage(format!(
                        "failed to build strategy {strategy_id} while listing it: {error}"
                    ))
                })?;
            strategy["bar_timeframe"] = serde_json::json!(implementation.bar_timeframe());
            strategy["minimum_history"] = serde_json::json!(implementation.minimum_history());
        }
        Ok(strategies)
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

    fn strategy_signal_targets(
        &self,
        strategy_id: uuid::Uuid,
        signal: &str,
        flatten_only: bool,
    ) -> Result<Option<Vec<DesiredTargetLeg>>> {
        if !matches!(signal, "buy" | "sell") {
            return Ok(None);
        }
        let primary = self
            .connection
            .query_row(
                "SELECT contract_json::VARCHAR, target_quantity,
                        short_target_quantity, allow_short
                 FROM strategy_execution_configs
                 WHERE strategy_id = ? AND enabled = true",
                params![strategy_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let Some((contract_json, buy_target, short_target, allow_short)) = primary else {
            return Ok(None);
        };
        let mut statement = self
            .connection
            .prepare(
                "SELECT leg_index, contract_json::VARCHAR,
                        buy_target_quantity, sell_target_quantity
                 FROM strategy_execution_portfolio_legs
                 WHERE strategy_id = ? ORDER BY leg_index",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let configured = statement
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
        drop(statement);
        let mut targets = Vec::with_capacity(configured.len().max(1));
        for (leg_index, contract_json, leg_buy_target, leg_sell_target) in configured {
            let contract: crate::ibkr::ContractCandidate = serde_json::from_str(&contract_json)?;
            targets.push(DesiredTargetLeg {
                leg_index,
                conid: contract.conid,
                target_quantity: if flatten_only {
                    0.0
                } else if signal == "buy" {
                    leg_buy_target
                } else {
                    leg_sell_target
                },
                requires_flatten: false,
            });
        }
        if targets.is_empty() {
            let contract: crate::ibkr::ContractCandidate = serde_json::from_str(&contract_json)?;
            targets.push(DesiredTargetLeg {
                leg_index: 0,
                conid: contract.conid,
                target_quantity: if flatten_only {
                    0.0
                } else {
                    scalar_target_for_signal(signal, buy_target, short_target, allow_short)
                        .expect("buy/sell signal validated above")
                },
                requires_flatten: false,
            });
        }
        Ok(Some(targets))
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
        let expected_seconds = match strategy.bar_timeframe() {
            "5s" => 5,
            "1m" => 60,
            _ => unreachable!("timeframe validated above"),
        };
        if bars
            .windows(2)
            .any(|pair| (pair[1].time - pair[0].time).num_seconds() != expected_seconds)
        {
            // A quote-silent period or a recent reconnect is an expected
            // readiness state, not a strategy failure. Do not emit an ERROR on
            // every scheduler pass; the next real tick will carry short gaps
            // forward and evaluation resumes once the window is contiguous.
            self.connection
                .execute(
                    "UPDATE strategies SET last_error = NULL
                     WHERE strategy_id = ?
                       AND last_error LIKE '%contiguous%Bars%waiting%gaps%'",
                    params![strategy_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Ok(false);
        }
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
        let evaluation_id = uuid::Uuid::now_v7();
        let signal = output.signal.as_str();
        let flatten_only = output
            .details
            .get("target_intent")
            .and_then(serde_json::Value::as_str)
            == Some("flatten_only");
        let desired_targets = self.strategy_signal_targets(strategy_id, signal, flatten_only)?;
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
                    evaluation_id,
                    strategy_id,
                    strategy.conid(),
                    bar_time,
                    output.indicator_a,
                    output.indicator_b,
                    output.previous_indicator_a,
                    output.previous_indicator_b,
                    signal,
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
        if let Some(targets) = desired_targets {
            transaction
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = 'superseded', superseded_by_evaluation_id = ?,
                         detail = 'superseded by a newer buy/sell signal', updated_at = ?
                     WHERE strategy_id = ? AND state = 'active'",
                    params![evaluation_id, now, strategy_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute(
                    "INSERT INTO strategy_execution_desired_targets
                     (desired_target_id, strategy_id, source_evaluation_id, signal,
                      targets_json, state, requires_flatten, flatten_completed_at,
                      superseded_by_evaluation_id, detail, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, 'active', false, NULL, NULL, NULL, ?, ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        evaluation_id,
                        signal,
                        serde_json::to_string(&targets)?,
                        now,
                        now
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
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

    fn release_position_evidence_deferrals_for_contract(
        &mut self,
        account: &str,
        conid: i32,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE strategy_execution_desired_targets AS d
                 SET next_attempt_at = NULL, detail = NULL, updated_at = ?
                 WHERE d.state = 'active' AND d.detail = ?
                   AND EXISTS (
                       SELECT 1 FROM strategy_execution_configs c
                       WHERE c.strategy_id = d.strategy_id AND c.account_id = ?
                         AND (
                           (NOT EXISTS (
                              SELECT 1 FROM strategy_execution_portfolio_legs p
                              WHERE p.strategy_id = c.strategy_id
                            ) AND try_cast(json_extract_string(c.contract_json, '$.conid') AS BIGINT) = ?)
                           OR EXISTS (
                              SELECT 1 FROM strategy_execution_portfolio_legs p
                              WHERE p.strategy_id = c.strategy_id
                                AND try_cast(json_extract_string(p.contract_json, '$.conid') AS BIGINT) = ?
                           )
                         )
                   )",
                params![
                    Utc::now(),
                    POSITION_EVIDENCE_WAIT_DETAIL,
                    account.trim(),
                    conid,
                    conid
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn release_all_position_evidence_deferrals(&mut self) -> Result<()> {
        self.connection
            .execute(
                "UPDATE strategy_execution_desired_targets
                 SET next_attempt_at = NULL, detail = NULL, updated_at = ?
                 WHERE state = 'active' AND detail = ?",
                params![Utc::now(), POSITION_EVIDENCE_WAIT_DETAIL],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
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
        maximum_position_age_seconds: u64,
        now: DateTime<Utc>,
    ) -> Result<CloseOnlyDecision> {
        let (sync_state, sync_observed_at, snapshot_completed_at) = self
            .connection
            .query_row(
                "SELECT state, observed_at, snapshot_completed_at
                 FROM position_sync_state WHERE singleton",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<DateTime<Utc>>>(1)?,
                        row.get::<_, Option<DateTime<Utc>>>(2)?,
                    ))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if sync_state != "ready" {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: None,
                maximum_closing_quantity: 0.0,
                reason: "IBKR position subscription is not ready".into(),
                position_observed_at: sync_observed_at,
            });
        }
        let Some(sync_observed_at) = sync_observed_at else {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: None,
                maximum_closing_quantity: 0.0,
                reason: "IBKR position subscription has no freshness lease".into(),
                position_observed_at: None,
            });
        };
        if sync_observed_at < session_connected_at {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: None,
                maximum_closing_quantity: 0.0,
                reason: "position subscription lease predates the active IBKR session".into(),
                position_observed_at: Some(sync_observed_at),
            });
        }
        if (now - sync_observed_at).num_seconds().max(0) > maximum_position_age_seconds as i64 {
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: None,
                maximum_closing_quantity: 0.0,
                reason: "IBKR position subscription lease is stale".into(),
                position_observed_at: Some(sync_observed_at),
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
        let evidence = self.position_evidence_state(account, conid)?;
        let position_evidence_observed_at = position
            .map(|(_, observed_at)| observed_at)
            .or(snapshot_completed_at);
        if !evidence.is_caught_up(position_evidence_observed_at) {
            let reason = if evidence.has_incomplete_fill_evidence {
                "local terminal fill evidence is incomplete; reconciliation must record all executions before close-only bypass is safe"
            } else {
                "position snapshot has not caught up with the latest locally received execution"
            };
            return Ok(CloseOnlyDecision {
                allowed: false,
                current_quantity: position.map(|(quantity, _)| quantity),
                maximum_closing_quantity: 0.0,
                reason: reason.into(),
                position_observed_at: position.map(|(_, observed_at)| observed_at),
            });
        }
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
        let normalized_side = side.to_ascii_lowercase();
        let reserved_closing_quantity =
            self.outstanding_closing_quantity(account, conid, normalized_side.as_str())?;
        let maximum_closing_quantity =
            (current_quantity.abs() - reserved_closing_quantity).max(0.0);
        let closing_direction = match normalized_side.as_str() {
            "sell" => current_quantity > 0.0,
            "buy" => current_quantity < 0.0,
            _ => false,
        };
        let allowed = closing_direction
            && quantity.is_finite()
            && quantity > 0.0
            && quantity <= maximum_closing_quantity;
        let reason = if !closing_direction {
            "order side does not reduce the current position".to_owned()
        } else if quantity > maximum_closing_quantity {
            if reserved_closing_quantity > 0.0 {
                format!(
                    "outstanding same-side intents/orders already reserve {:.8} units; this order would exceed the remaining {:.8} closing units and could cross through flat",
                    reserved_closing_quantity, maximum_closing_quantity
                )
            } else {
                "order quantity would cross through flat and open a reverse position".to_owned()
            }
        } else if !quantity.is_finite() || quantity <= 0.0 {
            "order quantity must be positive and finite".to_owned()
        } else if reserved_closing_quantity > 0.0 {
            format!(
                "order strictly reduces the current position after reserving {:.8} units for outstanding same-side intents/orders",
                reserved_closing_quantity
            )
        } else {
            "order strictly reduces the current position".to_owned()
        };
        Ok(CloseOnlyDecision {
            allowed,
            current_quantity: Some(current_quantity),
            maximum_closing_quantity,
            reason,
            position_observed_at: Some(observed_at),
        })
    }

    /// Returns local fill evidence that must be reflected by the IBKR position
    /// stream before another target-position decision is safe.  Both
    /// `received_at` and `positions_current.observed_at` are generated by this
    /// daemon's clock, avoiding broker timestamp/time-zone drift in the
    /// freshness comparison.
    fn position_evidence_state(&self, account: &str, conid: i32) -> Result<PositionEvidenceState> {
        let latest_execution_received_at = self
            .connection
            .query_row(
                "SELECT max(e.received_at)
                 FROM executions e
                 JOIN orders o ON o.order_id = e.order_id
                 JOIN order_intents i ON i.order_intent_id = o.order_intent_id
                 WHERE i.account_id = ? AND i.conid = ?",
                params![account, conid],
                |row| row.get::<_, Option<DateTime<Utc>>>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let has_incomplete_fill_evidence = self
            .connection
            .query_row(
                "SELECT count(*) > 0
                 FROM orders o
                 JOIN order_intents i ON i.order_intent_id = o.order_intent_id
                 WHERE i.account_id = ? AND i.conid = ?
                   AND greatest(
                         coalesce(o.filled_quantity, 0),
                         CASE WHEN lower(o.status) = 'filled' THEN
                           coalesce(try_cast(json_extract_string(
                             i.payload_json, '$.quantity') AS DOUBLE), 0)
                         ELSE 0 END
                       ) > coalesce((
                         SELECT sum(e.quantity) FROM executions e
                         WHERE e.order_id = o.order_id
                       ), 0) + 0.000000001",
                params![account, conid],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(PositionEvidenceState {
            latest_execution_received_at,
            has_incomplete_fill_evidence,
        })
    }

    /// Quantity already committed in the same direction as a proposed
    /// close-only order. The order RPC holds the Storage mutex across this
    /// check and intent insertion, so concurrent submissions cannot both
    /// consume the same closing headroom.
    fn outstanding_closing_quantity(&self, account: &str, conid: i32, side: &str) -> Result<f64> {
        let pending_intent_quantity = self
            .connection
            .query_row(
                "SELECT coalesce(sum(coalesce(try_cast(json_extract_string(
                           i.payload_json, '$.quantity') AS DOUBLE), 0)), 0)
                 FROM order_intents i
                 WHERE i.account_id = ? AND i.conid = ?
                   AND lower(i.status) IN ('approved', 'unknown')
                   AND lower(json_extract_string(i.payload_json, '$.side')) = ?
                   AND NOT EXISTS (
                     SELECT 1 FROM orders o
                     WHERE o.order_intent_id = i.order_intent_id
                   )",
                params![account, conid, side],
                |row| row.get::<_, f64>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let active_order_quantity = self
            .connection
            .query_row(
                "SELECT coalesce(sum(greatest(
                           coalesce(o.remaining_quantity, 0),
                           coalesce(try_cast(json_extract_string(
                             i.payload_json, '$.quantity') AS DOUBLE), 0)
                             - coalesce(o.filled_quantity, 0),
                           0)), 0)
                 FROM orders o
                 JOIN order_intents i ON i.order_intent_id = o.order_intent_id
                 WHERE i.account_id = ? AND i.conid = ?
                   AND lower(json_extract_string(i.payload_json, '$.side')) = ?
                   AND lower(o.status) IN
                     ('submitted', 'presubmitted', 'pending_submit',
                      'pendingsubmit', 'pending_cancel', 'pendingcancel',
                      'cancel_pending', 'apipending', 'api_pending')",
                params![account, conid, side],
                |row| row.get::<_, f64>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok((pending_intent_quantity + active_order_quantity).max(0.0))
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
        let normalized_model_currency = normalized_currency(&input.currency)?;
        let id = input.cost_model_id.unwrap_or_else(uuid::Uuid::now_v7);
        let mut assigned_statement = self
            .connection
            .prepare("SELECT strategy_id FROM strategy_cost_controls WHERE cost_model_id = ?")
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let assigned_strategies = assigned_statement
            .query_map(params![id], |row| row.get::<_, uuid::Uuid>(0))
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(assigned_statement);
        for strategy_id in assigned_strategies {
            let contracts = self.strategy_execution_contracts(strategy_id)?;
            self.validate_execution_contract_currencies(
                &contracts,
                Some(&normalized_model_currency),
            )?;
        }
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
                    normalized_model_currency,
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
        let minimum_completed_trades =
            i64::try_from(input.minimum_completed_trades).map_err(|_| {
                AppError::Storage(
                    "minimum_completed_trades exceeds the database integer range".into(),
                )
            })?;
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
        let model = self
            .execution_cost_model_by_id(input.cost_model_id)?
            .expect("validated cost model reference");
        let contracts = self.strategy_execution_contracts(input.strategy_id)?;
        self.validate_execution_contract_currencies(&contracts, Some(&model.currency))?;
        let strategy_state: String = self
            .connection
            .query_row(
                "SELECT state FROM strategies WHERE strategy_id = ?",
                params![input.strategy_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if strategy_state == "running" {
            return Err(AppError::Storage(
                "策略正在运行，无法修改成本控制；请先暂停策略，保存后再重新启动".into(),
            ));
        }
        self.ensure_no_processing_strategy_action(input.strategy_id, "成本控制")?;
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
                    minimum_completed_trades,
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

    pub fn configure_strategy_risk_control(
        &mut self,
        input: &StrategyRiskControlInput,
    ) -> Result<()> {
        let capital_currency = normalized_currency(
            input
                .capital_currency
                .as_deref()
                .ok_or_else(|| AppError::Storage("capital_currency is required".into()))?,
        )?;
        let maximum_consecutive_net_losing_trades =
            i64::try_from(input.maximum_consecutive_net_losing_trades).map_err(|_| {
                AppError::Storage(
                    "maximum_consecutive_net_losing_trades exceeds the database integer range"
                        .into(),
                )
            })?;
        let maximum_rolling_24h_completed_trades =
            i64::try_from(input.maximum_rolling_24h_completed_trades).map_err(|_| {
                AppError::Storage(
                    "maximum_rolling_24h_completed_trades exceeds the database integer range"
                        .into(),
                )
            })?;
        let ratios = [
            input.maximum_position_capital_ratio,
            input.maximum_rolling_24h_realized_net_loss_ratio,
            input.maximum_rolling_24h_turnover_capital_ratio,
        ];
        if !input.strategy_capital.is_finite()
            || input.strategy_capital <= 0.0
            || ratios
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(AppError::Storage(
                "strategy risk control requires a three-letter capital currency, positive finite capital and non-negative finite ratios"
                    .into(),
            ));
        }
        let state: String = self
            .connection
            .query_row(
                "SELECT state FROM strategies WHERE strategy_id = ?",
                params![input.strategy_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| AppError::Storage("strategy not found".into()))?;
        if state == "running" {
            return Err(AppError::Storage(
                "策略正在运行，无法修改风险控制；请先暂停策略，保存后再重新启动".into(),
            ));
        }
        self.ensure_no_processing_strategy_action(input.strategy_id, "风险控制")?;
        let now = Utc::now();
        self.connection
            .execute(
                "INSERT INTO strategy_risk_controls
                 (strategy_id, enabled, strategy_capital,
                  maximum_position_capital_ratio,
                  maximum_rolling_24h_realized_net_loss_ratio,
                  maximum_consecutive_net_losing_trades,
                  maximum_rolling_24h_completed_trades,
                 maximum_rolling_24h_turnover_capital_ratio,
                  statistics_reset_at, statistics_reset_note, created_at, updated_at,
                  capital_currency)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'initial risk configuration', ?, ?, ?)
                 ON CONFLICT (strategy_id) DO UPDATE SET
                   enabled = excluded.enabled,
                   strategy_capital = excluded.strategy_capital,
                   maximum_position_capital_ratio =
                       excluded.maximum_position_capital_ratio,
                   maximum_rolling_24h_realized_net_loss_ratio =
                       excluded.maximum_rolling_24h_realized_net_loss_ratio,
                   maximum_consecutive_net_losing_trades =
                       excluded.maximum_consecutive_net_losing_trades,
                   maximum_rolling_24h_completed_trades =
                       excluded.maximum_rolling_24h_completed_trades,
                   maximum_rolling_24h_turnover_capital_ratio =
                       excluded.maximum_rolling_24h_turnover_capital_ratio,
                   capital_currency = excluded.capital_currency,
                   updated_at = excluded.updated_at",
                params![
                    input.strategy_id,
                    input.enabled,
                    input.strategy_capital,
                    input.maximum_position_capital_ratio,
                    input.maximum_rolling_24h_realized_net_loss_ratio,
                    maximum_consecutive_net_losing_trades,
                    maximum_rolling_24h_completed_trades,
                    input.maximum_rolling_24h_turnover_capital_ratio,
                    now,
                    now,
                    now,
                    capital_currency
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn ensure_no_processing_strategy_action(
        &self,
        strategy_id: uuid::Uuid,
        control_name: &str,
    ) -> Result<()> {
        let processing_count: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_actions
                 WHERE strategy_id = ? AND state = 'processing'",
                params![strategy_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if processing_count > 0 {
            return Err(AppError::Storage(format!(
                "策略仍有 {processing_count} 个处理中动作，无法修改{control_name}；请等待动作完成，若状态长期不变请先完成 IBKR 对账"
            )));
        }
        Ok(())
    }

    pub fn reset_strategy_risk_statistics(
        &mut self,
        input: &StrategyRiskResetInput,
    ) -> Result<bool> {
        if !input.confirm || input.note.trim().is_empty() {
            return Err(AppError::Storage(
                "risk statistics reset requires confirm=true and a non-empty note".into(),
            ));
        }
        let open_attributed_position_count: i64 = self
            .connection
            .query_row(
                "WITH attributed AS (
                   SELECT order_intent_id FROM strategy_execution_actions
                   WHERE strategy_id = ? AND order_intent_id IS NOT NULL
                   UNION
                   SELECT l.order_intent_id
                   FROM strategy_execution_action_legs l
                   JOIN strategy_execution_actions a USING (action_id)
                   WHERE a.strategy_id = ? AND l.order_intent_id IS NOT NULL
                 ), net_positions AS (
                   SELECT oi.account_id, e.conid,
                          sum(CASE WHEN lower(e.side) IN ('bought', 'buy')
                                   THEN e.quantity
                                   WHEN lower(e.side) IN ('sold', 'sell')
                                   THEN -e.quantity ELSE 0 END) AS quantity
                   FROM attributed
                   JOIN orders o USING (order_intent_id)
                   JOIN order_intents oi USING (order_intent_id)
                   JOIN executions e ON e.order_id = o.order_id
                   GROUP BY oi.account_id, e.conid
                 )
                 SELECT count(*) FROM net_positions
                 WHERE abs(quantity) > 0.000000001",
                params![input.strategy_id, input.strategy_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let unresolved_order_count: i64 = self
            .connection
            .query_row(
                "WITH attributed AS (
                   SELECT order_intent_id FROM strategy_execution_actions
                   WHERE strategy_id = ? AND order_intent_id IS NOT NULL
                   UNION
                   SELECT l.order_intent_id
                   FROM strategy_execution_action_legs l
                   JOIN strategy_execution_actions a USING (action_id)
                   WHERE a.strategy_id = ? AND l.order_intent_id IS NOT NULL
                 )
                 SELECT count(*)
                 FROM attributed
                 JOIN order_intents i USING (order_intent_id)
                 LEFT JOIN orders o USING (order_intent_id)
                 WHERE i.status IN ('approved', 'unknown')
                    OR lower(o.status) IN
                       ('submitted', 'presubmitted', 'pending_submit',
                        'pendingsubmit', 'pending_cancel', 'pendingcancel',
                        'cancel_pending', 'apipending', 'api_pending')
                    OR o.filled_quantity > coalesce((
                         SELECT sum(e.quantity) FROM executions e
                         WHERE e.order_id = o.order_id), 0) + 0.000000001",
                params![input.strategy_id, input.strategy_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let processing_action_count: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_actions
                 WHERE strategy_id = ? AND state = 'processing'",
                params![input.strategy_id],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if open_attributed_position_count > 0
            || unresolved_order_count > 0
            || processing_action_count > 0
        {
            return Err(AppError::Storage(format!(
                "cannot reset strategy risk statistics while {open_attributed_position_count} attributed position cycle(s), {unresolved_order_count} unresolved order(s), or {processing_action_count} processing action(s) remain; flatten and reconcile them first"
            )));
        }
        self.connection
            .execute(
                "UPDATE strategy_risk_controls
                 SET statistics_reset_at = ?, statistics_reset_note = ?, updated_at = ?
                 WHERE strategy_id = ?",
                params![Utc::now(), input.note.trim(), Utc::now(), input.strategy_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn list_strategy_risk_controls(
        &self,
        base_currency: &str,
        maximum_fx_age_seconds: u64,
        now: DateTime<Utc>,
    ) -> Result<Vec<serde_json::Value>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT r.strategy_id, s.name, r.enabled, r.strategy_capital,
                        r.maximum_position_capital_ratio,
                        r.maximum_rolling_24h_realized_net_loss_ratio,
                        r.maximum_consecutive_net_losing_trades,
                        r.maximum_rolling_24h_completed_trades,
                        r.maximum_rolling_24h_turnover_capital_ratio,
                        r.statistics_reset_at, r.statistics_reset_note, r.updated_at,
                        r.capital_currency
                 FROM strategy_risk_controls r
                 JOIN strategies s USING (strategy_id)
                 ORDER BY s.name",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, f64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, f64>(8)?,
                    row.get::<_, DateTime<Utc>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, DateTime<Utc>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        rows.into_iter()
            .map(
                |(
                    strategy_id,
                    strategy_name,
                    enabled,
                    strategy_capital,
                    maximum_position_capital_ratio,
                    maximum_rolling_24h_realized_net_loss_ratio,
                    maximum_consecutive_net_losing_trades,
                    maximum_rolling_24h_completed_trades,
                    maximum_rolling_24h_turnover_capital_ratio,
                    statistics_reset_at,
                    statistics_reset_note,
                    updated_at,
                    capital_currency,
                )| {
                    let daemon_currency = base_currency.trim().to_ascii_uppercase();
                    let stored_currency = capital_currency
                        .as_deref()
                        .map(str::trim)
                        .filter(|currency| !currency.is_empty())
                        .map(str::to_ascii_uppercase);
                    let currency_matches_daemon = stored_currency
                        .as_deref()
                        .is_some_and(|currency| currency == daemon_currency);
                    let statistics = if currency_matches_daemon {
                        self.strategy_risk_statistics(
                            strategy_id,
                            stored_currency.as_deref().expect("validated currency"),
                            maximum_fx_age_seconds,
                            statistics_reset_at,
                            now,
                        )?
                    } else {
                        StrategyRiskStatistics {
                            data_complete: false,
                            warning: Some(match stored_currency.as_deref() {
                                Some(currency) => format!(
                                    "策略资本币种 {currency} 与当前 risk.base_currency {daemon_currency} 不一致；请暂停策略并重新保存风险控制"
                                ),
                                None => format!(
                                    "旧版风险控制没有保存策略资本币种；当前 risk.base_currency 为 {daemon_currency}，请暂停策略并重新保存风险控制"
                                ),
                            }),
                            ..StrategyRiskStatistics::default()
                        }
                    };
                    Ok(serde_json::json!({
                        "strategy_id": strategy_id,
                        "strategy_name": strategy_name,
                        "enabled": enabled,
                        // Keep the long-standing base_currency response field
                        // a string for RPC v2 clients. capital_currency is the
                        // authoritative persisted unit and remains null for a
                        // legacy row until the operator explicitly resaves it.
                        "base_currency": stored_currency.clone().unwrap_or_else(|| daemon_currency.clone()),
                        "capital_currency": stored_currency,
                        "daemon_base_currency": daemon_currency,
                        "currency_matches_daemon": currency_matches_daemon,
                        "strategy_capital": strategy_capital,
                        "maximum_position_capital_ratio": maximum_position_capital_ratio,
                        "maximum_rolling_24h_realized_net_loss_ratio": maximum_rolling_24h_realized_net_loss_ratio,
                        "maximum_consecutive_net_losing_trades": maximum_consecutive_net_losing_trades,
                        "maximum_rolling_24h_completed_trades": maximum_rolling_24h_completed_trades,
                        "maximum_rolling_24h_turnover_capital_ratio": maximum_rolling_24h_turnover_capital_ratio,
                        "statistics_reset_at": statistics_reset_at,
                        "statistics_reset_note": statistics_reset_note,
                        "updated_at": updated_at,
                        "statistics": statistics,
                    }))
                },
            )
            .collect()
    }

    fn strategy_risk_control(
        &self,
        strategy_id: uuid::Uuid,
    ) -> Result<Option<StrategyRiskControl>> {
        self.connection
            .query_row(
                "SELECT enabled, strategy_capital,
                        capital_currency,
                        maximum_position_capital_ratio,
                        maximum_rolling_24h_realized_net_loss_ratio,
                        maximum_consecutive_net_losing_trades,
                        maximum_rolling_24h_completed_trades,
                        maximum_rolling_24h_turnover_capital_ratio,
                        statistics_reset_at
                 FROM strategy_risk_controls WHERE strategy_id = ?",
                params![strategy_id],
                |row| {
                    Ok(StrategyRiskControl {
                        enabled: row.get(0)?,
                        strategy_capital: row.get(1)?,
                        capital_currency: row.get(2)?,
                        maximum_position_capital_ratio: row.get(3)?,
                        maximum_rolling_24h_realized_net_loss_ratio: row.get(4)?,
                        maximum_consecutive_net_losing_trades: row.get::<_, i64>(5)?.max(0)
                            as usize,
                        maximum_rolling_24h_completed_trades: row.get::<_, i64>(6)?.max(0) as usize,
                        maximum_rolling_24h_turnover_capital_ratio: row.get(7)?,
                        statistics_reset_at: row.get(8)?,
                    })
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    /// Returns attributed orders for which IBKR has reported more filled
    /// quantity than the local execution ledger contains.  Completed-order
    /// snapshots sometimes carry `Filled Size: N` while the ordinary order
    /// status remains `Submitted`, so status alone is not authoritative.
    fn attributed_incomplete_fills(
        &self,
        strategy_id: uuid::Uuid,
    ) -> Result<Vec<(Option<i64>, f64, f64)>> {
        let mut statement = self
            .connection
            .prepare(
                "WITH attributed AS (
                   SELECT order_intent_id FROM strategy_execution_actions
                   WHERE strategy_id = ? AND order_intent_id IS NOT NULL
                   UNION
                   SELECT l.order_intent_id
                   FROM strategy_execution_action_legs l
                   JOIN strategy_execution_actions a USING (action_id)
                   WHERE a.strategy_id = ? AND l.order_intent_id IS NOT NULL
                 ), evidence AS (
                   SELECT o.order_id, o.broker_order_id,
                     greatest(
                       coalesce(o.filled_quantity, 0),
                       CASE WHEN lower(o.status) = 'filled' THEN
                         coalesce(try_cast(json_extract_string(
                           i.payload_json, '$.quantity') AS DOUBLE), 0)
                       ELSE 0 END,
                       coalesce((
                         SELECT max(try_cast(json_extract_string(
                           oe.payload_json, '$.filled') AS DOUBLE))
                         FROM order_events oe
                         WHERE oe.order_id = o.order_id
                           AND oe.event_type = 'ibkr_order_status'
                       ), 0),
                       coalesce((
                         SELECT max(CASE WHEN lower(json_extract_string(
                           oe.payload_json, '$.status')) = 'filled' THEN
                             coalesce(try_cast(json_extract_string(
                               i.payload_json, '$.quantity') AS DOUBLE), 0)
                           ELSE 0 END)
                         FROM order_events oe
                         WHERE oe.order_id = o.order_id
                           AND oe.event_type = 'ibkr_open_order'
                       ), 0),
                       coalesce((
                         SELECT max(try_cast(regexp_extract(
                           json_extract_string(oe.payload_json, '$.completed_status'),
                           'Filled Size:[ ]*([0-9]+([.][0-9]+)?)', 1
                         ) AS DOUBLE))
                         FROM order_events oe
                         WHERE oe.order_id = o.order_id
                           AND oe.event_type = 'ibkr_open_order'
                       ), 0)
                     ) AS broker_filled_quantity,
                     coalesce((SELECT sum(e.quantity) FROM executions e
                               WHERE e.order_id = o.order_id), 0)
                       AS recorded_execution_quantity
                   FROM attributed
                   JOIN orders o USING (order_intent_id)
                   JOIN order_intents i USING (order_intent_id)
                 )
                 SELECT broker_order_id, broker_filled_quantity,
                        recorded_execution_quantity
                 FROM evidence
                 WHERE broker_filled_quantity > recorded_execution_quantity + 0.000000001
                 ORDER BY broker_order_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        statement
            .query_map(params![strategy_id, strategy_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn strategy_risk_statistics(
        &self,
        strategy_id: uuid::Uuid,
        base_currency: &str,
        maximum_fx_age_seconds: u64,
        statistics_reset_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<StrategyRiskStatistics> {
        // Resetting the cumulative/consecutive baseline must never erase the
        // objective rolling-24h loss, turnover, or trade-count window.  The
        // data-integrity check therefore covers whichever window starts
        // earlier: the reset baseline or the rolling window.
        let rolling_cutoff = now - chrono::Duration::hours(24);
        let required_since = statistics_reset_at.min(rolling_cutoff);
        let missing_execution_count = self.attributed_incomplete_fills(strategy_id)?.len();
        let mut statement = self
            .connection
            .prepare(
                "SELECT oi.account_id, e.executed_at, lower(e.side), e.quantity, e.price,
                        e.commission, e.currency,
                        json_extract_string(oi.payload_json, '$.contract.currency'),
                        e.conid,
                        CASE WHEN attributed.target_conid = e.conid
                             THEN attributed.target_quantity ELSE NULL END,
                        attributed.target_evidence_conflict
                 FROM executions e
                 JOIN orders o ON o.order_id = e.order_id
                 JOIN order_intents oi ON oi.order_intent_id = o.order_intent_id
                 JOIN (
                    SELECT strategy_id, order_intent_id,
                           CASE WHEN count(target_quantity) = 0 THEN NULL
                                WHEN count(DISTINCT target_quantity) = 1
                                 AND count(DISTINCT target_conid) = 1
                                THEN min(target_quantity) ELSE NULL END
                             AS target_quantity,
                           CASE WHEN count(target_conid) = 0 THEN NULL
                                WHEN count(DISTINCT target_conid) = 1
                                THEN min(target_conid) ELSE NULL END AS target_conid,
                           count(target_quantity) > 0 AND
                             (count(DISTINCT target_quantity) <> 1 OR
                              count(DISTINCT target_conid) <> 1)
                             AS target_evidence_conflict
                    FROM (
                       SELECT strategy_id, order_intent_id,
                              NULL::DOUBLE AS target_quantity,
                              NULL::INTEGER AS target_conid
                       FROM strategy_execution_actions
                       WHERE order_intent_id IS NOT NULL
                       UNION ALL
                       SELECT a.strategy_id, l.order_intent_id,
                              l.target_quantity, l.conid
                       FROM strategy_execution_action_legs l
                       JOIN strategy_execution_actions a USING (action_id)
                       WHERE l.order_intent_id IS NOT NULL
                    ) evidence
                    GROUP BY strategy_id, order_intent_id
                 ) attributed ON attributed.order_intent_id = o.order_intent_id
                 WHERE attributed.strategy_id = ?
                 ORDER BY e.executed_at, e.broker_execution_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![strategy_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, DateTime<Utc>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, Option<f64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i32>(8)?,
                    row.get::<_, Option<f64>>(9)?,
                    row.get::<_, bool>(10)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);

        let mut positions: HashMap<(String, i32), RiskCyclePosition> = HashMap::new();
        let mut cycles = Vec::<CompletedRiskCycle>::new();
        let mut rolling_turnover = 0.0;
        let mut rolling_24h_realized_net_pnl = 0.0;
        let mut commissions_since_reset = 0.0;
        let mut data_complete = missing_execution_count == 0;
        let mut warnings = Vec::new();
        if missing_execution_count > 0 {
            warnings.push(format!(
                "{missing_execution_count} attributed filled order(s) are missing execution details"
            ));
        }

        for (
            account,
            executed_at,
            side,
            quantity,
            price,
            commission,
            commission_currency,
            trade_currency,
            conid,
            historical_target_quantity,
            target_evidence_conflict,
        ) in rows
        {
            if quantity <= 0.0 || price <= 0.0 {
                data_complete = false;
                warnings.push("an attributed execution has invalid quantity or price".into());
                continue;
            }
            let mut row_tainted = false;
            let trade_currency = trade_currency.filter(|value| !value.trim().is_empty());
            let trade_fx = if let Some(currency) = trade_currency.as_deref() {
                match self.currency_conversion_rate_at(
                    currency,
                    base_currency,
                    maximum_fx_age_seconds,
                    executed_at,
                )? {
                    Some(fx) => fx,
                    None => {
                        row_tainted = true;
                        warnings.push(format!(
                            "no fresh FX rate converts {currency} to {base_currency}"
                        ));
                        0.0
                    }
                }
            } else {
                row_tainted = true;
                warnings.push(format!(
                    "execution for conid {conid} is missing its contract currency"
                ));
                0.0
            };
            let commission_base = match commission {
                Some(value) => {
                    let currency = commission_currency.as_deref().or(trade_currency.as_deref());
                    if let Some(currency) = currency {
                        match self.currency_conversion_rate_at(
                            currency,
                            base_currency,
                            maximum_fx_age_seconds,
                            executed_at,
                        )? {
                            Some(fx) => value * fx,
                            None => {
                                row_tainted = true;
                                warnings.push(format!(
                                    "no fresh FX rate converts commission currency {currency} to {base_currency}"
                                ));
                                0.0
                            }
                        }
                    } else {
                        row_tainted = true;
                        warnings.push(format!(
                            "commission currency is missing for an execution of conid {conid}"
                        ));
                        0.0
                    }
                }
                None => {
                    row_tainted = true;
                    warnings.push(format!(
                        "commission report is missing for an execution of conid {conid}"
                    ));
                    0.0
                }
            };
            if row_tainted && executed_at >= required_since {
                data_complete = false;
            }
            if executed_at >= rolling_cutoff {
                rolling_turnover += quantity * price * trade_fx;
                // Commissions are realized cash expenses when their execution
                // occurs, even if the position cycle remains partially open.
                rolling_24h_realized_net_pnl -= commission_base;
            }
            if executed_at >= statistics_reset_at {
                commissions_since_reset += commission_base;
            }

            let signed_quantity = if side.starts_with("bought") || side == "buy" {
                quantity
            } else if side.starts_with("sold") || side == "sell" {
                -quantity
            } else {
                data_complete = false;
                warnings.push(format!("unsupported execution side {side}"));
                continue;
            };
            let position = positions.entry((account, conid)).or_default();
            if position.quantity <= POSITION_QUANTITY_EPSILON && signed_quantity < 0.0 {
                let projected_quantity = position.quantity + signed_quantity;
                if !historical_target_authorizes_short(
                    historical_target_quantity,
                    target_evidence_conflict,
                    projected_quantity,
                ) {
                    // Current execution settings are mutable and cannot prove
                    // that an old sell was authorized to open short. Without a
                    // matching persisted action-leg target, fail closed rather
                    // than inventing a short and its future PnL.
                    data_complete = false;
                    warnings.push(missing_historical_short_authorization(
                        conid,
                        historical_target_quantity,
                        target_evidence_conflict,
                        projected_quantity,
                    ));
                    continue;
                }
            }
            position.tainted |= row_tainted;
            if position.quantity.abs() <= POSITION_QUANTITY_EPSILON
                || position.quantity.signum() == signed_quantity.signum()
            {
                let previous_abs = position.quantity.abs();
                let next_abs = previous_abs + quantity;
                position.average_price = if next_abs > 0.0 {
                    (position.average_price * previous_abs + price * quantity) / next_abs
                } else {
                    0.0
                };
                position.quantity += signed_quantity;
                position.commissions += commission_base;
                continue;
            }

            let closing_quantity = position.quantity.abs().min(quantity);
            let closing_fraction = closing_quantity / quantity;
            let realized = if position.quantity > 0.0 {
                (price - position.average_price) * closing_quantity * trade_fx
            } else {
                (position.average_price - price) * closing_quantity * trade_fx
            };
            if executed_at >= rolling_cutoff {
                // Count every partial close immediately. Waiting for the final
                // share to close would let a large realized loss evade the
                // rolling loss gate indefinitely.
                rolling_24h_realized_net_pnl += realized;
            }
            position.gross_pnl += realized;
            position.commissions += commission_base * closing_fraction;
            let previous_sign = position.quantity.signum();
            position.quantity += signed_quantity.signum() * closing_quantity;
            let remaining = quantity - closing_quantity;
            if position.quantity.abs() <= POSITION_QUANTITY_EPSILON {
                cycles.push(CompletedRiskCycle {
                    closed_at: executed_at,
                    gross_pnl: position.gross_pnl,
                    commissions: position.commissions,
                    tainted: position.tainted,
                });
                *position = RiskCyclePosition::default();
            }
            if remaining > POSITION_QUANTITY_EPSILON {
                let projected_quantity = -previous_sign * remaining;
                if signed_quantity < 0.0
                    && !historical_target_authorizes_short(
                        historical_target_quantity,
                        target_evidence_conflict,
                        projected_quantity,
                    )
                {
                    data_complete = false;
                    warnings.push(missing_historical_short_authorization(
                        conid,
                        historical_target_quantity,
                        target_evidence_conflict,
                        projected_quantity,
                    ));
                } else {
                    position.quantity = projected_quantity;
                    position.average_price = price;
                    position.commissions = commission_base * (1.0 - closing_fraction);
                    position.tainted = row_tainted;
                }
            }
        }

        // An incomplete opening leg can predate the rolling/reset boundary but
        // still contaminate a position that is open now or a cycle that closes
        // inside the active window. Never let age alone turn that cycle into a
        // complete one.
        if positions
            .values()
            .any(|position| position.quantity.abs() > POSITION_QUANTITY_EPSILON && position.tainted)
        {
            data_complete = false;
            warnings.push("an open attributed position contains incomplete execution data".into());
        }
        if cycles
            .iter()
            .any(|cycle| cycle.closed_at >= required_since && cycle.tainted)
        {
            data_complete = false;
            warnings.push(
                "a completed trade in the active statistics window contains incomplete execution data"
                    .into(),
            );
        }

        let rolling_cycles = cycles
            .iter()
            .filter(|cycle| cycle.closed_at >= rolling_cutoff && !cycle.tainted)
            .collect::<Vec<_>>();
        let completed_trades_since_reset = cycles
            .iter()
            .filter(|cycle| cycle.closed_at >= statistics_reset_at && !cycle.tainted)
            .count();
        let gross_pnl_since_reset = cycles
            .iter()
            .filter(|cycle| cycle.closed_at >= statistics_reset_at && !cycle.tainted)
            .map(|cycle| cycle.gross_pnl)
            .sum();
        let mut consecutive_net_losing_trades = 0;
        for cycle in cycles
            .iter()
            .filter(|cycle| cycle.closed_at >= statistics_reset_at && !cycle.tainted)
        {
            if cycle.gross_pnl - cycle.commissions < 0.0 {
                consecutive_net_losing_trades += 1;
            } else {
                consecutive_net_losing_trades = 0;
            }
        }
        warnings.sort();
        warnings.dedup();
        Ok(StrategyRiskStatistics {
            data_complete,
            warning: (!warnings.is_empty()).then(|| warnings.join("; ")),
            rolling_24h_realized_net_pnl,
            rolling_24h_turnover: rolling_turnover,
            rolling_24h_completed_trades: rolling_cycles.len(),
            consecutive_net_losing_trades,
            gross_pnl_since_reset,
            commissions_since_reset,
            completed_trades_since_reset,
        })
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
        // Intents that are approved but not yet acknowledged by the broker, or
        // whose outcome is unknown, may already be live at IBKR. Project them
        // into the position so concurrent submissions cannot squeeze past the
        // position and exposure limits together.
        let pending_intent_quantity: f64 = self
            .connection
            .query_row(
                "SELECT coalesce(sum(CASE
                     WHEN lower(json_extract_string(payload_json, '$.side')) = 'buy'
                     THEN coalesce(try_cast(
                         json_extract_string(payload_json, '$.quantity') AS DOUBLE), 0)
                     ELSE -coalesce(try_cast(
                         json_extract_string(payload_json, '$.quantity') AS DOUBLE), 0)
                 END), 0)
                 FROM order_intents
                 WHERE account_id = ? AND conid = ? AND status IN ('approved', 'unknown')",
                params![account, request.contract.conid],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let pending_order_quantity: f64 = self
            .connection
            .query_row(
                "SELECT coalesce(sum(
                   CASE lower(json_extract_string(i.payload_json, '$.side'))
                     WHEN 'buy' THEN 1 WHEN 'sell' THEN -1 ELSE 0 END
                   * greatest(coalesce(o.remaining_quantity,
                       coalesce(try_cast(json_extract_string(
                         i.payload_json, '$.quantity') AS DOUBLE), 0)
                       - o.filled_quantity), 0)
                 ), 0)
                 FROM orders o JOIN order_intents i USING (order_intent_id)
                 WHERE i.account_id = ? AND i.conid = ?
                   AND lower(o.status) IN
                     ('submitted', 'presubmitted', 'pending_submit',
                      'pendingsubmit', 'pending_cancel', 'pendingcancel',
                      'cancel_pending', 'apipending', 'api_pending')",
                params![account, request.contract.conid],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let projected_position =
            current_position + pending_intent_quantity + pending_order_quantity + signed_quantity;
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
        // Exposure conversion fails closed: a position whose currency has no
        // fresh FX rate must reject opening orders instead of being silently
        // excluded from the gross/net exposure totals.
        let mut missing_fx_currencies: Vec<String> = Vec::new();
        for (quantity, average_cost, currency) in position_rows {
            let rate = self.currency_conversion_rate(
                &currency,
                &config.base_currency,
                config.max_fx_rate_age_seconds,
                now,
            )?;
            let Some(rate) = rate else {
                if quantity != 0.0 && !missing_fx_currencies.contains(&currency) {
                    missing_fx_currencies.push(currency);
                }
                continue;
            };
            let exposure = quantity * average_cost * rate;
            gross_exposure += exposure.abs();
            net_exposure += exposure;
        }
        let request_fx_rate = match self.currency_conversion_rate(
            &request.contract.currency,
            &config.base_currency,
            config.max_fx_rate_age_seconds,
            now,
        )? {
            Some(rate) => rate,
            None => {
                let currency = request.contract.currency.clone();
                if !missing_fx_currencies.contains(&currency) {
                    missing_fx_currencies.push(currency);
                }
                0.0
            }
        };
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
        // Counts broker-active orders plus intents that are in flight
        // ('approved') or unresolved ('unknown'); both may already occupy an
        // order slot at IBKR even though no orders row exists yet.
        let active_order_count: i64 = self
            .connection
            .query_row(
                "SELECT
                   (SELECT count(*) FROM orders o
                    JOIN order_intents i USING (order_intent_id)
                    WHERE i.account_id = ?
                      AND (lower(o.status) IN
                             ('submitted', 'presubmitted', 'pending_submit',
                              'pendingsubmit', 'pending_cancel', 'pendingcancel',
                              'cancel_pending', 'apipending', 'api_pending')
                           OR o.filled_quantity > coalesce((
                                SELECT sum(e.quantity) FROM executions e
                                WHERE e.order_id = o.order_id), 0) + 0.000000001))
                 + (SELECT count(*) FROM order_intents
                    WHERE account_id = ? AND status IN ('approved', 'unknown'))",
                params![account, account],
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
        if !close_only && !missing_fx_currencies.is_empty() {
            return Ok(portfolio_reject(
                decision,
                "FX_RATE_UNAVAILABLE",
                format!(
                    "portfolio exposure cannot be converted to {}: no fresh FX rate for {}; \
                     opening risk is blocked",
                    config.base_currency,
                    missing_fx_currencies.join(", ")
                ),
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
        if !close_only && recent_order_count as usize >= config.max_orders_per_minute {
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

    /// IBKR sends the last price and its exchange timestamp as separate tick
    /// messages, in either order.  A streaming subscription also replays the
    /// previous close/last trade when it starts.  Pair the two messages by
    /// receipt time and build bars at the exchange timestamp so a reconnect
    /// cannot manufacture a new bar during a weekend or market closure.
    fn observe_timestamped_market_trade(
        &mut self,
        conid: i32,
        tick_type: &str,
        numeric_value: Option<f64>,
        text_value: Option<&str>,
        received_at: DateTime<Utc>,
    ) -> Result<()> {
        let delayed = match tick_type {
            "Last" => false,
            "DelayedLast" => true,
            "LastTimestamp" => false,
            "DelayedLastTimestamp" => true,
            _ => return Ok(()),
        };
        let is_price = matches!(tick_type, "Last" | "DelayedLast");
        let pair = self
            .pending_market_trades
            .entry((conid, delayed))
            .or_default();
        if is_price {
            let Some(price) = numeric_value.filter(|price| price.is_finite() && *price > 0.0)
            else {
                return Ok(());
            };
            pair.price = Some(PendingMarketTradePrice {
                tick_type: tick_type.to_owned(),
                price,
                received_at,
            });
        } else {
            let Some(source_at) = text_value
                .and_then(|value| value.trim().parse::<i64>().ok())
                .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
            else {
                return Ok(());
            };
            pair.timestamp = Some(PendingMarketTradeTimestamp {
                source_at,
                received_at,
            });
        }

        let matched = match (&pair.price, pair.timestamp) {
            (Some(price), Some(timestamp))
                if (price.received_at - timestamp.received_at)
                    .num_seconds()
                    .abs()
                    <= MARKET_TRADE_PAIR_MAX_RECEIPT_GAP_SECONDS
                    && pair.last_emitted_receipts
                        != Some((price.received_at, timestamp.received_at)) =>
            {
                pair.last_emitted_receipts = Some((price.received_at, timestamp.received_at));
                Some((
                    price.tick_type.clone(),
                    price.price,
                    price.received_at.max(timestamp.received_at),
                    timestamp.source_at,
                ))
            }
            _ => None,
        };
        let Some((price_tick_type, price, latest_receipt, source_at)) = matched else {
            return Ok(());
        };

        // Make quote freshness reflect the exchange trade time as well.  An
        // unpaired Last is inserted with the Unix epoch by apply_broker_event,
        // so it can never briefly authorize an order before its timestamp is
        // known.
        self.connection
            .execute(
                "UPDATE market_ticks_current SET observed_at = ?
                 WHERE conid = ? AND tick_type = ?",
                params![source_at, conid, price_tick_type],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;

        let source_age = (latest_receipt - source_at).num_seconds();
        let maximum_source_age = if delayed {
            DELAYED_TRADE_MAX_SOURCE_AGE_SECONDS
        } else {
            LIVE_TRADE_MAX_SOURCE_AGE_SECONDS
        };
        if !(-5..=maximum_source_age).contains(&source_age) {
            tracing::debug!(
                conid,
                %price_tick_type,
                %source_at,
                source_age_seconds = source_age,
                "ignored replayed or invalid IBKR last trade for Bar aggregation"
            );
            return Ok(());
        }
        self.update_market_minute_bar(conid, price, source_at)?;
        self.update_market_five_second_bar(conid, price, source_at)
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
        let previous: Option<(DateTime<Utc>, f64)> = transaction
            .query_row(
                &format!(
                    "SELECT bar_time, close FROM {table}
                     WHERE conid = ? AND bar_time < ? ORDER BY bar_time DESC LIMIT 1"
                ),
                params![conid, bar_time],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if let Some((previous_time, previous_close)) = previous {
            let missing = (bar_time - previous_time).num_seconds() / interval_seconds - 1;
            // Carry forward short quote-silent periods so an N-Bar indicator
            // still represents N fixed time buckets. A long disconnect is not
            // synthesized; strategy evaluation then pauses until enough fresh,
            // contiguous history has accumulated.
            if missing > 0 && missing <= 120 {
                for offset in 1..=missing {
                    let synthetic_time =
                        previous_time + chrono::Duration::seconds(offset * interval_seconds);
                    transaction
                        .execute(
                            &format!(
                                "INSERT INTO {table}
                                 VALUES (?, ?, ?, ?, ?, ?, 0, true, ?)
                                 ON CONFLICT (conid, bar_time) DO NOTHING"
                            ),
                            params![
                                conid,
                                synthetic_time,
                                previous_close,
                                previous_close,
                                previous_close,
                                previous_close,
                                observed_at
                            ],
                        )
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                }
            }
        }
        transaction
            .execute(
                &format!(
                    "UPDATE {table} SET final = true, updated_at = ?
                 WHERE conid = ? AND bar_time < ? AND final = false",
                ),
                params![observed_at, conid, bar_time],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // A finalised bar is immutable: strategy evaluations are idempotent
        // per bar, so a late out-of-order tick must never rewrite the OHLC of
        // a bar that may already have produced a persisted signal.
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
                   updated_at = excluded.updated_at
                 WHERE {table}.final = false"
                ),
                params![conid, bar_time, price, price, price, price, observed_at],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn create_backfill_job(
        &mut self,
        request: &BackfillJobRequest,
    ) -> Result<BackfillJobCreation> {
        if request.end <= request.start {
            return Err(AppError::Storage("backfill end must be after start".into()));
        }
        // DuckDB TIMESTAMPTZ stores microsecond precision. Keep request_json and
        // cursor/end columns at the same precision so a completed job cannot
        // appear to have a sub-microsecond uncovered tail.
        let mut request = request.clone();
        request.start = duckdb_timestamp(request.start);
        request.end = duckdb_timestamp(request.end);
        self.deduplicate_active_backfill_jobs()?;

        if let Some(existing) = self.active_backfill_jobs()?.into_iter().find(|job| {
            same_backfill_scope(&job.request, &request)
                    && backfill_ranges_overlap(&job.request, &request)
                    // Moving a running cursor backwards would race the slice
                    // that is already in flight. End-only expansion is safe;
                    // the worker reads the persisted end on advance.
                    && (job.state != "running" || request.start >= job.request.start)
        }) {
            let merged_start = existing.request.start.min(request.start);
            let merged_end = existing.request.end.max(request.end);
            let range_expanded =
                merged_start != existing.request.start || merged_end != existing.request.end;
            if range_expanded {
                let mut merged = existing.request.clone();
                merged.start = merged_start;
                merged.end = merged_end;
                self.update_backfill_job_range(&existing, &merged)?;
                self.deduplicate_active_backfill_jobs()?;
            }
            return Ok(BackfillJobCreation {
                job_id: existing.job_id,
                reused: true,
                range_expanded,
            });
        }

        let job_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        let request_json = serde_json::to_string(&request)?;
        self.connection
            .execute(
                "INSERT INTO data_jobs VALUES
                 (?, 'historical_backfill', 'pending', ?, ?, ?, 0, 0, NULL, ?, ?)",
                params![job_id, request_json, request.start, request.end, now, now],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(BackfillJobCreation {
            job_id,
            reused: false,
            range_expanded: false,
        })
    }

    /// Creates work only for portions of a requested range that have not
    /// already been traversed successfully by an earlier backfill. A range can
    /// contain several independent holes, so each hole receives its own
    /// durable job instead of restarting one job from the original beginning.
    pub fn create_unverified_backfill_jobs(
        &mut self,
        request: &BackfillJobRequest,
    ) -> Result<Vec<(BackfillJobRequest, BackfillJobCreation)>> {
        if request.end <= request.start {
            return Err(AppError::Storage("backfill end must be after start".into()));
        }
        let mut request = request.clone();
        request.start = duckdb_timestamp(request.start);
        request.end = duckdb_timestamp(request.end);

        let mut statement = self
            .connection
            .prepare(
                "SELECT request_json::VARCHAR, cursor_time
                 FROM data_jobs
                 WHERE job_type = 'historical_backfill' AND cursor_time > ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let jobs = statement
            .query_map(params![request.start], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, DateTime<Utc>>(1)?))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);

        let verified_intervals = jobs
            .into_iter()
            .filter_map(|(request_json, cursor_time)| {
                let existing = serde_json::from_str::<BackfillJobRequest>(&request_json).ok()?;
                same_backfill_coverage_scope(&existing, &request)
                    .then(|| (existing.start, cursor_time.min(existing.end)))
            })
            .filter(|(start, end)| end > start)
            .collect::<Vec<_>>();
        let gaps = interval_gaps(request.start, request.end, &verified_intervals);
        let mut created = Vec::with_capacity(gaps.len());
        for (start, end) in gaps {
            let mut gap_request = request.clone();
            gap_request.start = start;
            gap_request.end = end;
            let creation = self.create_backfill_job(&gap_request)?;
            created.push((gap_request, creation));
        }
        Ok(created)
    }

    fn active_backfill_jobs(&self) -> Result<Vec<ActiveBackfillJob>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT job_id, state, request_json::VARCHAR, cursor_time,
                        completed_slices
                 FROM data_jobs
                 WHERE job_type = 'historical_backfill'
                   AND state IN ('pending', 'retrying', 'running')
                   AND attempts < 3
                 ORDER BY created_at, job_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                let request_json = row.get::<_, String>(2)?;
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, String>(1)?,
                    request_json,
                    row.get::<_, DateTime<Utc>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?;
        rows.map(|row| {
            let (job_id, state, request_json, cursor_time, completed_slices) =
                row.map_err(|error| AppError::Storage(error.to_string()))?;
            let request = serde_json::from_str(&request_json)?;
            Ok(ActiveBackfillJob {
                job_id,
                state,
                request,
                cursor_time,
                completed_slices,
            })
        })
        .collect()
    }

    fn update_backfill_job_range(
        &mut self,
        existing: &ActiveBackfillJob,
        merged: &BackfillJobRequest,
    ) -> Result<()> {
        let reset_cursor = merged.start < existing.request.start;
        let cursor_time = if reset_cursor {
            merged.start
        } else {
            existing.cursor_time
        };
        let completed_slices = if reset_cursor {
            0
        } else {
            existing.completed_slices
        };
        let request_json = serde_json::to_string(merged)?;
        self.connection
            .execute(
                "UPDATE data_jobs
                 SET request_json = ?, cursor_time = ?, end_time = ?,
                     completed_slices = ?, updated_at = ?
                 WHERE job_id = ?",
                params![
                    request_json,
                    cursor_time,
                    merged.end,
                    completed_slices,
                    Utc::now(),
                    existing.job_id
                ],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn deduplicate_active_backfill_jobs(&mut self) -> Result<()> {
        loop {
            let jobs = self.active_backfill_jobs()?;
            let mut duplicate = None;
            'outer: for (index, primary) in jobs.iter().enumerate() {
                for secondary in jobs.iter().skip(index + 1) {
                    if !same_backfill_scope(&primary.request, &secondary.request)
                        || !backfill_ranges_overlap(&primary.request, &secondary.request)
                        || secondary.state == "running"
                    {
                        continue;
                    }
                    let merged_start = primary.request.start.min(secondary.request.start);
                    // A running task may only be extended forward. Startup
                    // recovery turns interrupted jobs into retrying before this
                    // method runs, so this restriction only affects live RPCs.
                    if primary.state == "running" && merged_start < primary.request.start {
                        continue;
                    }
                    duplicate = Some((primary.clone(), secondary.clone()));
                    break 'outer;
                }
            }
            let Some((primary, secondary)) = duplicate else {
                return Ok(());
            };
            let mut merged = primary.request.clone();
            merged.start = merged.start.min(secondary.request.start);
            merged.end = merged.end.max(secondary.request.end);
            self.update_backfill_job_range(&primary, &merged)?;
            self.connection
                .execute(
                    "UPDATE data_jobs
                     SET state = 'cancelled', last_error = ?, updated_at = ?
                     WHERE job_id = ? AND state IN ('pending', 'retrying')",
                    params![
                        format!("merged into historical data job {}", primary.job_id),
                        Utc::now(),
                        secondary.job_id
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
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
        _end_time: DateTime<Utc>,
    ) -> Result<()> {
        // The range can be expanded while a slice is in flight. Always use the
        // persisted end instead of the stale end captured when the worker
        // claimed that slice.
        let end_time = self
            .connection
            .query_row(
                "SELECT end_time FROM data_jobs WHERE job_id = ?",
                params![job_id],
                |row| row.get::<_, DateTime<Utc>>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
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

    #[cfg(test)]
    pub fn list_data_jobs(&self, worker_ready: bool) -> Result<Vec<serde_json::Value>> {
        self.list_data_jobs_page(worker_ready, 1, 200)
            .map(|(jobs, _)| jobs)
    }

    pub fn data_job_queue_status(&self) -> Result<(Option<uuid::Uuid>, usize)> {
        let jobs = self.active_backfill_jobs()?;
        Ok((jobs.first().map(|job| job.job_id), jobs.len()))
    }

    pub fn list_data_jobs_page(
        &self,
        worker_ready: bool,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let queue_positions = self
            .active_backfill_jobs()?
            .into_iter()
            .enumerate()
            .map(|(index, job)| (job.job_id, index + 1))
            .collect::<std::collections::HashMap<_, _>>();
        let total: i64 = self
            .connection
            .query_row("SELECT count(*) FROM data_jobs", [], |row| row.get(0))
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let page_size = page_size.clamp(1, 500);
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        let mut statement = self
            .connection
            .prepare(
                "SELECT job_id, job_type, state, request_json::VARCHAR, cursor_time, end_time, attempts,
                        completed_slices, last_error, created_at, updated_at
                 FROM data_jobs ORDER BY created_at DESC LIMIT ? OFFSET ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![page_size as i64, offset as i64], |row| {
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
        let mut jobs = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        for job in &mut jobs {
            let Some(object) = job.as_object_mut() else {
                continue;
            };
            let job_id = object.get("job_id").and_then(Value::as_str);
            let state = object
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let position = job_id
                .and_then(|value| uuid::Uuid::parse_str(value).ok())
                .and_then(|value| queue_positions.get(&value).copied());
            let runtime_state = match position {
                Some(1) if !worker_ready => "waiting_for_ibkr",
                Some(1) => "running",
                Some(_) => "queued",
                None => state,
            };
            object.insert("runtime_state".into(), serde_json::json!(runtime_state));
            object.insert("queue_position".into(), serde_json::json!(position));
            object.insert(
                "jobs_ahead".into(),
                serde_json::json!(position.map(|value| value.saturating_sub(1))),
            );
        }
        Ok((jobs, total.max(0) as usize))
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

    #[cfg(test)]
    pub fn historical_coverage(
        &self,
        conid: i32,
        timeframe: &str,
        requested_start: DateTime<Utc>,
        requested_end: DateTime<Utc>,
    ) -> Result<serde_json::Value> {
        self.historical_coverage_for_session(
            conid,
            timeframe,
            requested_start,
            requested_end,
            false,
        )
    }

    /// Reports whether a backtest range has been fully inspected by successful
    /// IBKR backfill slices for the requested session scope.
    ///
    /// Manifest timestamps alone cannot prove this: nights, weekends, holidays
    /// and valid no-trade slices create no Parquet rows. Conversely, a single
    /// Parquet fragment inside a long request must not make that entire request
    /// runnable. `data_jobs.cursor_time` advances only after a slice succeeds,
    /// so the union of `[request.start, cursor_time)` is the authoritative
    /// download-verification range.
    pub fn historical_coverage_for_session(
        &self,
        conid: i32,
        timeframe: &str,
        requested_start: DateTime<Utc>,
        requested_end: DateTime<Utc>,
        outside_rth: bool,
    ) -> Result<serde_json::Value> {
        let requested_start = duckdb_timestamp(requested_start);
        let requested_end = duckdb_timestamp(requested_end);
        if requested_end <= requested_start {
            return Err(AppError::Storage("coverage end must be after start".into()));
        }
        let step = timeframe_duration(timeframe)?;
        let session_kind = if outside_rth { "extended" } else { "regular" };
        let mut statement = self
            .connection
            .prepare(
                "SELECT min_time, max_time, row_count, relative_path
                 FROM dataset_files
                 WHERE dataset = 'bars' AND conid = ? AND timeframe = ? AND active = true
                   AND coalesce(session_kind, 'regular') = ?
                   AND max_time >= ? AND min_time < ?
                 ORDER BY min_time",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let files = statement
            .query_map(
                params![
                    conid,
                    timeframe,
                    session_kind,
                    requested_start,
                    requested_end
                ],
                |row| {
                    Ok((
                        row.get::<_, DateTime<Utc>>(0)?,
                        row.get::<_, DateTime<Utc>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;

        let mut rows = 0_i64;
        let mut file_values = Vec::new();
        let mut file_intervals = Vec::new();
        let mut first_bar_time: Option<DateTime<Utc>> = None;
        let mut last_bar_time: Option<DateTime<Utc>> = None;
        for (min_time, max_time, row_count, path) in files {
            rows += row_count;
            file_intervals.push((min_time, max_time + step));
            first_bar_time = Some(first_bar_time.map_or(min_time, |value| value.min(min_time)));
            last_bar_time = Some(last_bar_time.map_or(max_time, |value| value.max(max_time)));
            file_values.push(serde_json::json!({
                "path": path,
                "min_time": min_time,
                "max_time": max_time,
                "row_count": row_count,
                "session_kind": session_kind
            }));
        }

        let raw_gaps = interval_gaps(requested_start, requested_end, &file_intervals);
        let raw_covered = raw_gaps.is_empty();

        let mut statement = self
            .connection
            .prepare(
                "SELECT job_id, state, request_json::VARCHAR, cursor_time
                 FROM data_jobs
                 WHERE job_type = 'historical_backfill' AND cursor_time > ?
                 ORDER BY created_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let jobs = statement
            .query_map(params![requested_start], |row| {
                Ok((
                    row.get::<_, uuid::Uuid>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, DateTime<Utc>>(3)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut verified_intervals = Vec::new();
        let mut verified_jobs = Vec::new();
        for (job_id, state, request_json, cursor_time) in jobs {
            let Ok(mut request) = serde_json::from_str::<BackfillJobRequest>(&request_json) else {
                continue;
            };
            request.start = duckdb_timestamp(request.start);
            request.end = duckdb_timestamp(request.end);
            if request.contract.conid != conid
                || request.timeframe != timeframe
                || request.outside_rth != outside_rth
            {
                continue;
            }
            let verified_end = cursor_time.min(request.end);
            if verified_end <= request.start {
                continue;
            }
            verified_intervals.push((request.start, verified_end));
            verified_jobs.push(serde_json::json!({
                "job_id": job_id,
                "state": state,
                "start": request.start,
                "verified_end": verified_end,
                "requested_end": request.end
            }));
        }

        let verified_ranges =
            merged_time_intervals(requested_start, requested_end, &verified_intervals);
        let verified_gaps = interval_gaps(requested_start, requested_end, &verified_ranges);
        let download_verified = verified_gaps.is_empty();
        let has_data = !file_values.is_empty() && rows > 0;
        // Only successful slice traversal proves that an exchange-data range
        // is complete. Raw Parquet continuity is diagnostic-only: a compact
        // file can span a missing month, while nights/weekends legitimately
        // produce no files at all.
        let backtest_ready = has_data && download_verified;
        let coverage_error = if !download_verified {
            Some(format!(
                "historical download has not successfully fetched {} interval(s) in the requested range",
                verified_gaps.len()
            ))
        } else if !has_data {
            Some(
                "the requested range was fetched but contains no local bars for this session scope"
                    .to_owned(),
            )
        } else {
            None
        };
        let verified_range_values = gaps_to_json(&verified_ranges);
        let verified_gap_values = gaps_to_json(&verified_gaps);
        Ok(serde_json::json!({
            "conid": conid,
            "timeframe": timeframe,
            "requested_start": requested_start,
            "requested_end": requested_end,
            "outside_rth": outside_rth,
            "session_kind": session_kind,
            "covered": download_verified,
            "verified": download_verified,
            "backtest_ready": backtest_ready,
            "coverage_basis": "successful_backfill_ranges",
            "download_verified": download_verified,
            "fetched_ranges": verified_range_values.clone(),
            "verified_ranges": verified_range_values,
            "unfetched_ranges": verified_gap_values.clone(),
            "verified_gaps": verified_gap_values,
            "raw_covered": raw_covered,
            "row_count": rows,
            "row_count_basis": "overlapping_file_manifests",
            "files": file_values,
            "first_bar_time": first_bar_time,
            "last_bar_time": last_bar_time,
            "raw_gaps": gaps_to_json(&raw_gaps),
            "verified_jobs": verified_jobs,
            // The current ContractDetails cache only covers nearby dates and
            // is not a historical-calendar proof. Successful empty backfill
            // slices are what verify nights, weekends and holidays here.
            "calendar_adjusted": false,
            "coverage_error": coverage_error
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
        let database_target = destination.join("state.duckdb");
        fs::copy(&self.database_path, &database_target)?;
        // The manifest carries a checksum of the copied database so a restore
        // can verify the copy is intact. The checksum is computed on the copy
        // (after CHECKPOINT, under the storage lock, so no concurrent writes).
        let database_checksum = file_checksum(&database_target)?;
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
            // Verify each copy against the manifest checksum (or the source
            // checksum when the manifest has none). A mismatch fails the
            // backup instead of silently persisting a corrupt copy.
            let expected = match checksum {
                Some(value) => value,
                None => file_checksum(&source)?,
            };
            let copied = file_checksum(&target)?;
            if copied != expected {
                return Err(AppError::Storage(format!(
                    "backup copy verification failed for {relative_path}: \
                     checksum {copied} does not match expected {expected}"
                )));
            }
            manifest_files.push(serde_json::json!({
                "file_id": file_id,
                "relative_path": relative_path,
                "checksum": expected
            }));
        }
        let manifest = serde_json::json!({
            "backup_id": backup_id,
            "created_at": Utc::now(),
            "schema_version": self.schema_version()?,
            "database": "state.duckdb",
            "database_checksum": database_checksum,
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
        let cost_context = self.resolve_backtest_cost_context(&request)?;
        let cost_model = &cost_context.model;
        let mut persisted_parameters = serde_json::to_value(&request)?;
        persisted_parameters["cost_model"] = serde_json::to_value(cost_model)?;
        persisted_parameters["cost_model_source"] = serde_json::json!(cost_context.model_source);
        persisted_parameters["cost_gate"] = serde_json::to_value(&cost_context.gate)?;
        let coverage = self.historical_coverage_for_session(
            request.conid,
            &request.timeframe,
            request.start,
            request.end,
            request.outside_rth,
        )?;
        if coverage["backtest_ready"] != true {
            let detail = coverage["coverage_error"]
                .as_str()
                .unwrap_or("historical coverage could not be verified");
            return Err(AppError::Storage(format!(
                "backtest data is incomplete for {} {} data: {detail}. \
                 Wait for a matching IBKR backfill job to finish",
                request.timeframe,
                if request.outside_rth {
                    "extended-hours"
                } else {
                    "regular-hours"
                }
            )));
        }
        let session_kind = if request.outside_rth {
            "extended"
        } else {
            "regular"
        };
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id, relative_path FROM dataset_files
                 WHERE dataset = 'bars' AND conid = ? AND timeframe = ? AND active = true
                   AND coalesce(session_kind, 'regular') = ?
                   AND max_time >= ? AND min_time < ?
                 ORDER BY min_time",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let files = statement
            .query_map(
                params![
                    request.conid,
                    request.timeframe,
                    session_kind,
                    request.start,
                    request.end
                ],
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
        let simulation = build_backtest_strategy(&request).and_then(|strategy| {
            simulate_strategy(
                &request,
                strategy.as_ref(),
                cost_model,
                &cost_context.gate,
                &bars,
            )
        });
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
                            serde_json::to_string(&persisted_parameters)?,
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
                    serde_json::to_string(&persisted_parameters)?,
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
                        trade.slippage,
                        trade.spread
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
            "cost_model": cost_model,
            "cost_gate": cost_context.gate,
            "dataset_file_ids": file_ids
        }))
    }

    fn resolve_backtest_cost_context(
        &self,
        request: &BacktestRequest,
    ) -> Result<BacktestCostContext> {
        let mode = request.effective_cost_gate_mode();
        if request.strategy_id.is_none() && mode == BacktestCostGateMode::MatchStrategy {
            return Err(AppError::Storage(
                "match_strategy cost gating requires strategy_id; ad-hoc backtests support fees_only"
                    .into(),
            ));
        }
        let (model, model_source, authoritative_currency, mut gate) = if let Some(strategy_id) =
            request.strategy_id
        {
            let model = self
                .execution_cost_model_for_strategy(strategy_id)?
                .ok_or_else(|| {
                    AppError::Storage(
                        "strategy has no assigned execution cost model; configure one on the 交易成本 page before running a backtest"
                            .into(),
                    )
                })?;
            let (control_enabled, minimum_cost_multiple, maximum_ratio, minimum_trades) = self
                .connection
                .query_row(
                    "SELECT enabled, minimum_cost_multiple,
                            maximum_commission_to_gross_profit_ratio,
                            minimum_completed_trades
                     FROM strategy_cost_controls WHERE strategy_id = ?",
                    params![strategy_id],
                    |row| {
                        Ok((
                            row.get::<_, bool>(0)?,
                            row.get::<_, f64>(1)?,
                            row.get::<_, f64>(2)?,
                            row.get::<_, i64>(3)?.max(0) as usize,
                        ))
                    },
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let execution_contract_json: String = self
                .connection
                .query_row(
                    "SELECT contract_json::VARCHAR FROM strategy_execution_configs
                     WHERE strategy_id = ?",
                    params![strategy_id],
                    |row| row.get(0),
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let execution_contract: crate::ibkr::ContractCandidate =
                serde_json::from_str(&execution_contract_json)?;
            if execution_contract.conid != request.conid {
                return Err(AppError::Storage(
                    "strategy backtest conid does not match its saved execution contract".into(),
                ));
            }
            self.validate_contract_canonical_currency(&execution_contract)?;
            let learned = self.actual_fee_bps_p90_for_strategy(strategy_id, &model.currency)?;
            let gate = BacktestCostGateSnapshot {
                mode,
                strategy_control_enabled: Some(control_enabled),
                applied: mode == BacktestCostGateMode::MatchStrategy && control_enabled,
                minimum_cost_multiple: Some(minimum_cost_multiple),
                maximum_commission_to_gross_profit_ratio: Some(maximum_ratio),
                minimum_completed_trades: Some(minimum_trades),
                actual_fee_bps_p90: learned,
                statistics_baseline: "backtest_start",
                scope: "transaction_cost_and_commission_performance_only; strategy risk, account, market-data freshness, order-conflict and trading-calendar gates are not simulated",
            };
            (
                model,
                "strategy_cost_control",
                Some(execution_contract.currency),
                gate,
            )
        } else {
            let cost_model_id = request.cost_model_id.ok_or_else(|| {
                AppError::Storage(
                    "cost_model_id is required for an ad-hoc backtest; select a database fee model"
                        .into(),
                )
            })?;
            let model = self
                .execution_cost_model_by_id(cost_model_id)?
                .ok_or_else(|| AppError::Storage(format!("unknown cost model {cost_model_id}")))?;
            let instrument_currency = self
                .connection
                .query_row(
                    "SELECT currency FROM instruments WHERE conid = ?",
                    params![request.conid],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            (
                model,
                "explicit_cost_model",
                instrument_currency,
                BacktestCostGateSnapshot::fees_only(mode),
            )
        };
        if let Some(currency) = authoritative_currency
            && !currency.eq_ignore_ascii_case(&model.currency)
        {
            return Err(AppError::Storage(format!(
                "backtest cost model currency {} does not match authoritative execution currency {}",
                model.currency, currency
            )));
        }
        if mode == BacktestCostGateMode::FeesOnly {
            gate.applied = false;
        }
        Ok(BacktestCostContext {
            model,
            model_source,
            gate,
        })
    }

    fn execution_cost_model_for_strategy(
        &self,
        strategy_id: uuid::Uuid,
    ) -> Result<Option<ExecutionCostModelInput>> {
        self.query_execution_cost_model(
            "SELECT m.cost_model_id, m.name, m.currency,
                    m.buy_fixed_fee, m.buy_per_share_fee,
                    m.buy_rate_bps, m.buy_min_fee,
                    m.sell_fixed_fee, m.sell_per_share_fee,
                    m.sell_rate_bps, m.sell_min_fee, m.sell_tax_bps,
                    m.estimated_spread_bps, m.estimated_slippage_bps
             FROM strategy_cost_controls c
             JOIN execution_cost_models m USING (cost_model_id)
             WHERE c.strategy_id = ?",
            strategy_id,
        )
    }

    fn actual_fee_bps_p90_for_strategy(
        &self,
        strategy_id: uuid::Uuid,
        currency: &str,
    ) -> Result<Option<f64>> {
        self.connection
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
                params![strategy_id, currency],
                |row| row.get::<_, Option<f64>>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn execution_cost_model_by_id(
        &self,
        cost_model_id: uuid::Uuid,
    ) -> Result<Option<ExecutionCostModelInput>> {
        self.query_execution_cost_model(
            "SELECT cost_model_id, name, currency,
                    buy_fixed_fee, buy_per_share_fee,
                    buy_rate_bps, buy_min_fee,
                    sell_fixed_fee, sell_per_share_fee,
                    sell_rate_bps, sell_min_fee, sell_tax_bps,
                    estimated_spread_bps, estimated_slippage_bps
             FROM execution_cost_models WHERE cost_model_id = ?",
            cost_model_id,
        )
    }

    fn query_execution_cost_model(
        &self,
        sql: &str,
        id: uuid::Uuid,
    ) -> Result<Option<ExecutionCostModelInput>> {
        self.connection
            .query_row(sql, params![id], |row| {
                Ok(ExecutionCostModelInput {
                    cost_model_id: Some(row.get(0)?),
                    name: row.get(1)?,
                    currency: row.get(2)?,
                    buy_fixed_fee: row.get(3)?,
                    buy_per_share_fee: row.get(4)?,
                    buy_rate_bps: row.get(5)?,
                    buy_min_fee: row.get(6)?,
                    sell_fixed_fee: row.get(7)?,
                    sell_per_share_fee: row.get(8)?,
                    sell_rate_bps: row.get(9)?,
                    sell_min_fee: row.get(10)?,
                    sell_tax_bps: row.get(11)?,
                    estimated_spread_bps: row.get(12)?,
                    estimated_slippage_bps: row.get(13)?,
                })
            })
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn resolve_backtest_request(&self, request: &BacktestRequest) -> Result<BacktestRequest> {
        let Some(strategy_id) = request.strategy_id else {
            return Ok(request.clone());
        };
        let stored = self
            .connection
            .query_row(
                "SELECT s.kind, s.config_json::VARCHAR, c.outside_rth,
                        c.target_quantity, c.short_target_quantity, c.allow_short,
                        EXISTS (
                            SELECT 1 FROM strategy_execution_portfolio_legs portfolio
                            WHERE portfolio.strategy_id = s.strategy_id
                        ) AS is_portfolio
                 FROM strategies s
                 LEFT JOIN strategy_execution_configs c USING (strategy_id)
                 WHERE s.strategy_id = ?",
                params![strategy_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<bool>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                        row.get::<_, Option<bool>>(5)?,
                        row.get::<_, bool>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .ok_or_else(|| AppError::Storage("backtest strategy not found".into()))?;
        if stored.6 {
            return Err(AppError::Storage(
                "strategy-bound backtests currently support only single-leg execution; portfolio execution configurations are not supported"
                    .into(),
            ));
        }
        let (Some(outside_rth), Some(long_target), Some(short_target), Some(allow_short)) =
            (stored.2, stored.3, stored.4, stored.5)
        else {
            return Err(AppError::Storage(
                "strategy has no execution configuration; save its live execution targets before running a strategy-bound backtest"
                    .into(),
            ));
        };
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
        resolved.quantity = long_target;
        resolved.short_target_quantity = short_target;
        resolved.allow_short = allow_short;
        resolved.outside_rth = outside_rth;
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

    #[cfg(test)]
    pub fn backtest_details(&self, backtest_id: uuid::Uuid) -> Result<Option<serde_json::Value>> {
        self.backtest_details_with_options(backtest_id, BacktestDetailOptions::default())
    }

    pub fn backtest_details_with_options(
        &self,
        backtest_id: uuid::Uuid,
        options: BacktestDetailOptions,
    ) -> Result<Option<serde_json::Value>> {
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
        let trade_page = options.trade_page.max(1);
        let trade_page_size = options.trade_page_size.clamp(1, 500);
        let trade_total = self
            .connection
            .query_row(
                "SELECT count(*) FROM backtest_trades WHERE backtest_id = ?",
                params![backtest_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            .max(0) as usize;
        let trade_total_pages = trade_total.div_ceil(trade_page_size).max(1);
        let trade_page = trade_page.min(trade_total_pages);
        let trade_offset = (trade_page - 1).saturating_mul(trade_page_size);
        let mut trade_statement = self
            .connection
            .prepare(
                "SELECT conid, signal_time, fill_time, side, quantity, price,
                        commission, slippage, spread
                 FROM backtest_trades WHERE backtest_id = ?
                 ORDER BY fill_time LIMIT ? OFFSET ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let trades = trade_statement
            .query_map(
                params![backtest_id, trade_page_size as i64, trade_offset as i64],
                |row| {
                    Ok(serde_json::json!({
                        "conid": row.get::<_, i64>(0)?,
                        "signal_time": row.get::<_, DateTime<Utc>>(1)?,
                        "fill_time": row.get::<_, DateTime<Utc>>(2)?,
                        "side": row.get::<_, String>(3)?,
                        "quantity": row.get::<_, f64>(4)?,
                        "price": row.get::<_, f64>(5)?,
                        "commission": row.get::<_, f64>(6)?,
                        "slippage": row.get::<_, f64>(7)?,
                        "spread": row.get::<_, f64>(8)?
                    }))
                },
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let equity_total = self
            .connection
            .query_row(
                "SELECT count(*) FROM backtest_equity WHERE backtest_id = ?",
                params![backtest_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?
            .max(0) as usize;
        let max_equity_points = options.max_equity_points.clamp(100, 5_000);
        let equity_step = equity_total.div_ceil(max_equity_points).max(1);
        let equity_sql = if equity_step == 1 {
            "SELECT observed_at, cash, position, close, equity
             FROM backtest_equity WHERE backtest_id = ? ORDER BY observed_at"
                .to_owned()
        } else {
            format!(
                "WITH ranked AS (
                    SELECT observed_at, cash, position, close, equity,
                           row_number() OVER (ORDER BY observed_at) AS point_number
                    FROM backtest_equity WHERE backtest_id = ?
                 )
                 SELECT observed_at, cash, position, close, equity FROM ranked
                 WHERE point_number = 1 OR point_number = {equity_total}
                    OR (point_number - 1) % {equity_step} = 0
                 ORDER BY observed_at"
            )
        };
        let mut equity_statement = self
            .connection
            .prepare(&equity_sql)
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
        let equity_sampled = equity.len();
        run["trades"] = serde_json::Value::Array(trades);
        run["trades_page"] = serde_json::json!({
            "page": trade_page,
            "page_size": trade_page_size,
            "total_items": trade_total,
            "total_pages": trade_total_pages,
        });
        run["equity"] = serde_json::Value::Array(equity);
        run["equity_sampling"] = serde_json::json!({
            "total_points": equity_total,
            "returned_points": equity_sampled,
            "step": equity_step,
            "downsampled": equity_step > 1,
        });
        Ok(Some(run))
    }

    #[cfg(test)]
    pub fn write_historical_bars(
        &mut self,
        lake_dir: &Path,
        staging_dir: &Path,
        bars: &[crate::ibkr::HistoricalBar],
    ) -> Result<DatasetFile> {
        self.write_historical_bars_for_session(lake_dir, staging_dir, bars, false)
    }

    pub fn write_historical_bars_for_session(
        &mut self,
        lake_dir: &Path,
        staging_dir: &Path,
        bars: &[crate::ibkr::HistoricalBar],
        outside_rth: bool,
    ) -> Result<DatasetFile> {
        let first = bars
            .first()
            .ok_or_else(|| AppError::Storage("IBKR returned no historical bars".into()))?;
        validate_bars(bars)?;
        let session_kind = if outside_rth { "extended" } else { "regular" };
        fs::create_dir_all(staging_dir)?;
        let file_id = uuid::Uuid::now_v7();
        let staging_path = staging_dir.join(format!("{file_id}.parquet.tmp"));
        let final_dir = lake_dir
            .join("bars")
            .join(format!("timeframe={}", first.timeframe))
            .join(format!("conid={}", first.conid))
            .join(format!("session_kind={session_kind}"));
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
        // Verify the staged Parquet file before publishing it: the row count
        // and time range must match the validated in-memory batch, otherwise
        // the temporary file is discarded and the slice fails (and retries).
        let (written_rows, written_min, written_max): (i64, DateTime<Utc>, DateTime<Utc>) = self
            .connection
            .query_row(
                &format!(
                    "SELECT count(*), min(open_time), max(open_time)
                     FROM read_parquet('{}')",
                    sql_path(&staging_path)
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if written_rows != bars.len() as i64 || written_min != min_time || written_max != max_time {
            let _ = fs::remove_file(&staging_path);
            return Err(AppError::Storage(format!(
                "staged parquet verification failed: wrote {written_rows} rows \
                 [{written_min} .. {written_max}], expected {} rows [{min_time} .. {max_time}]",
                bars.len()
            )));
        }
        let checksum = file_checksum(&staging_path)?;
        fs::rename(&staging_path, &final_path)?;
        let metadata = fs::metadata(&final_path)?;
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
                  min_time, max_time, row_count, byte_size, active, created_at, checksum,
                  session_kind)
                 VALUES (?, 'bars', ?, 1, ?, ?, ?, ?, ?, ?, true, ?, ?, ?)",
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
                    session_kind,
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        // Retire older manifest entries whose time range is fully covered by the
        // new file so repeated backfills do not accumulate duplicate active data.
        transaction
            .execute(
                "UPDATE dataset_files SET active = false
                 WHERE dataset = 'bars' AND conid = ? AND timeframe = ?
                   AND coalesce(session_kind, 'regular') = ?
                   AND active = true AND file_id <> ?
                   AND min_time >= ? AND max_time <= ?",
                params![
                    first.conid,
                    first.timeframe,
                    session_kind,
                    file_id,
                    min_time,
                    max_time
                ],
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

    /// Manually resolves an 'unknown' intent after the operator confirmed the
    /// true outcome against IBKR. Unknown intents block automatic execution
    /// for their contract and occupy risk headroom until resolved; resolution
    /// is deliberately manual because the daemon cannot correlate an intent
    /// that never received a broker order id. The resolution is audited as a
    /// risk decision.
    pub fn resolve_order_intent(
        &mut self,
        intent_id: uuid::Uuid,
        note: &str,
    ) -> Result<serde_json::Value> {
        let note = note.trim();
        if note.is_empty() {
            return Err(AppError::Storage(
                "intent resolution note cannot be empty".into(),
            ));
        }
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let now = Utc::now();
        let changed = transaction
            .execute(
                "UPDATE order_intents
                 SET status = 'resolved_manual',
                     rejection_reason = concat(coalesce(rejection_reason, ''),
                                               '; manually resolved: ', ?),
                     updated_at = ?
                 WHERE order_intent_id = ? AND status = 'unknown'",
                params![note, now, intent_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        if changed == 0 {
            let status: Option<String> = transaction
                .query_row(
                    "SELECT status FROM order_intents WHERE order_intent_id = ?",
                    params![intent_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            return Err(AppError::Storage(match status {
                Some(status) => format!(
                    "only intents in status 'unknown' can be manually resolved; \
                     intent {intent_id} has status '{status}'"
                ),
                None => format!("order intent {intent_id} does not exist"),
            }));
        }
        transaction
            .execute(
                "INSERT INTO risk_decisions VALUES (?, ?, 'resolved', 'MANUAL_RESOLUTION', ?, ?)",
                params![uuid::Uuid::now_v7(), intent_id, note, now],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(serde_json::json!({
            "order_intent_id": intent_id,
            "status": "resolved_manual",
            "note": note,
        }))
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
        self.replay_broker_events_for_order(order_id)?;
        self.drain_pending_broker_executions()?;
        Ok(order_id)
    }

    /// Replays broker events which raced ahead of the local `orders` insert.
    /// Terminal completed-order events are applied last because IBKR treats
    /// them as authoritative over an earlier submitted status notification.
    fn replay_broker_events_for_order(&mut self, order_id: uuid::Uuid) -> Result<()> {
        self.connection
            .execute(
                "UPDATE orders AS o SET
                   status = json_extract_string(b.payload_json, '$.status'),
                   filled_quantity = coalesce(
                     try_cast(json_extract_string(b.payload_json, '$.filled') AS DOUBLE),
                     o.filled_quantity),
                   remaining_quantity = try_cast(
                     json_extract_string(b.payload_json, '$.remaining') AS DOUBLE),
                   average_fill_price = try_cast(
                     json_extract_string(b.payload_json, '$.average_fill_price') AS DOUBLE),
                   last_fill_price = try_cast(
                     json_extract_string(b.payload_json, '$.last_fill_price') AS DOUBLE),
                   broker_perm_id = CASE WHEN b.broker_perm_id <> 0 THEN b.broker_perm_id
                                         ELSE o.broker_perm_id END,
                   why_held = json_extract_string(b.payload_json, '$.why_held'),
                   market_cap_price = try_cast(
                     json_extract_string(b.payload_json, '$.market_cap_price') AS DOUBLE),
                   updated_at = greatest(o.updated_at, b.received_at)
                 FROM broker_order_events AS b
                 WHERE o.order_id = ?
                   AND b.connection_session_id = o.connection_session_id
                   AND b.broker_order_id = o.broker_order_id
                   AND b.event_type = 'order_status'
                   AND b.received_at = (
                     SELECT max(latest.received_at)
                     FROM broker_order_events AS latest
                     WHERE latest.connection_session_id = o.connection_session_id
                       AND latest.broker_order_id = o.broker_order_id
                       AND latest.event_type = 'order_status')",
                params![order_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.connection
            .execute(
                "UPDATE orders AS o SET
                   status = json_extract_string(b.payload_json, '$.status'),
                   broker_perm_id = CASE WHEN b.broker_perm_id <> 0 THEN b.broker_perm_id
                                         ELSE o.broker_perm_id END,
                   updated_at = greatest(o.updated_at, b.received_at)
                 FROM broker_order_events AS b
                 WHERE o.order_id = ?
                   AND b.connection_session_id = o.connection_session_id
                   AND b.broker_order_id = o.broker_order_id
                   AND b.event_type = 'open_order'
                   AND lower(json_extract_string(b.payload_json, '$.status')) IN
                     ('filled','cancelled','canceled','inactive','rejected')
                   AND b.received_at = (
                     SELECT max(latest.received_at)
                     FROM broker_order_events AS latest
                     WHERE latest.connection_session_id = o.connection_session_id
                       AND latest.broker_order_id = o.broker_order_id
                       AND latest.event_type = 'open_order')",
                params![order_id],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        self.apply_completed_fill_evidence_for_order(order_id)
    }

    /// IBKR completed-order snapshots can report `Filled Size: N` while their
    /// ordinary status field still says `Submitted`. Treat the explicit fill
    /// quantity as evidence, and promote the order to `Filled` only when it
    /// covers the locally requested quantity.
    fn apply_completed_fill_evidence_for_order(&mut self, order_id: uuid::Uuid) -> Result<()> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT json_extract_string(b.payload_json, '$.completed_status')
                 FROM orders o
                 JOIN broker_order_events b ON b.event_type = 'open_order'
                   AND ((b.connection_session_id = o.connection_session_id
                         AND b.broker_order_id = o.broker_order_id)
                     OR (o.broker_perm_id IS NOT NULL AND o.broker_perm_id <> 0
                         AND b.broker_perm_id = o.broker_perm_id))
                 WHERE o.order_id = ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let statuses = statement
            .query_map(params![order_id], |row| row.get::<_, Option<String>>(0))
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);
        let filled = statuses
            .into_iter()
            .flatten()
            .filter_map(|status| completed_fill_size(&status))
            .fold(0.0_f64, f64::max);
        if filled <= 0.0 {
            return Ok(());
        }
        self.connection
            .execute(
                "UPDATE orders AS o SET
                   filled_quantity = greatest(o.filled_quantity, ?),
                   status = CASE WHEN ? + 0.000000001 >= coalesce(try_cast(
                     json_extract_string(i.payload_json, '$.quantity') AS DOUBLE), ?)
                     THEN 'Filled' ELSE o.status END,
                   updated_at = ?
                 FROM order_intents i
                 WHERE o.order_id = ? AND i.order_intent_id = o.order_intent_id",
                params![filled, filled, filled, Utc::now(), order_id],
            )
            .map(|_| ())
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn drain_pending_broker_executions(&mut self) -> Result<()> {
        type PendingExecution = (
            String,
            Option<uuid::Uuid>,
            i32,
            i64,
            i32,
            String,
            f64,
            f64,
            DateTime<Utc>,
            DateTime<Utc>,
        );
        let pending: Vec<PendingExecution> = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT broker_execution_id, connection_session_id, broker_order_id,
                            broker_perm_id, conid, side, quantity, price, executed_at, received_at
                     FROM pending_broker_executions ORDER BY received_at",
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                    ))
                })
                .map_err(|error| AppError::Storage(error.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AppError::Storage(error.to_string()))?
        };
        for (
            execution_id,
            session_id,
            broker_order_id,
            perm_id,
            conid,
            side,
            quantity,
            price,
            executed_at,
            received_at,
        ) in pending
        {
            let order_id: Option<uuid::Uuid> = self
                .connection
                .query_row(
                    "SELECT order_id FROM orders
                     WHERE (? IS NOT NULL AND connection_session_id = ? AND broker_order_id = ?)
                        OR (? <> 0 AND broker_perm_id = ?)
                     ORDER BY created_at DESC LIMIT 1",
                    params![session_id, session_id, broker_order_id, perm_id, perm_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            let Some(order_id) = order_id else { continue };
            let transaction = self
                .connection
                .transaction()
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute(
                    "INSERT INTO executions
                       (execution_id, broker_execution_id, order_id, conid, side, quantity,
                        price, executed_at, received_at, connection_session_id, broker_perm_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (broker_execution_id) DO NOTHING",
                    params![
                        uuid::Uuid::now_v7(),
                        execution_id,
                        order_id,
                        conid,
                        side,
                        quantity,
                        price,
                        executed_at,
                        received_at,
                        session_id,
                        perm_id
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
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
            transaction
                .execute(
                    "DELETE FROM pending_commissions WHERE broker_execution_id = ?
                     AND EXISTS (SELECT 1 FROM executions WHERE broker_execution_id = ?)",
                    params![execution_id, execution_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .execute(
                    "DELETE FROM pending_broker_executions WHERE broker_execution_id = ?
                     AND EXISTS (SELECT 1 FROM executions WHERE broker_execution_id = ?)",
                    params![execution_id, execution_id],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            transaction
                .commit()
                .map_err(|error| AppError::Storage(error.to_string()))?;
        }
        Ok(())
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
                // A late or replayed non-terminal status must never demote a
                // terminal order back to active, regress its filled quantity,
                // or erase a learned perm id with 0.
                transaction
                    .execute(
                        "UPDATE orders SET status = ?,
                             filled_quantity = greatest(filled_quantity, ?),
                             remaining_quantity = ?, average_fill_price = ?,
                             last_fill_price = ?,
                             broker_perm_id = CASE WHEN ? <> 0 THEN ?
                                                   ELSE broker_perm_id END,
                             why_held = ?,
                             market_cap_price = ?, updated_at = ?
                         WHERE ((connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0))
                           AND NOT (lower(status) IN
                                 ('filled','cancelled','canceled','inactive',
                                  'rejected','not_open')
                             AND lower(?) NOT IN
                                 ('filled','cancelled','canceled','inactive','rejected'))",
                        params![
                            status,
                            filled,
                            remaining,
                            average_fill_price,
                            last_fill_price,
                            perm_id,
                            perm_id,
                            why_held,
                            market_cap_price,
                            now,
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id,
                            status,
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
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let order_id = self
                    .connection
                    .query_row(
                        "SELECT order_id FROM orders
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)
                         ORDER BY created_at DESC LIMIT 1",
                        params![
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id
                        ],
                        |row| row.get::<_, uuid::Uuid>(0),
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if let Some(order_id) = order_id {
                    self.apply_completed_fill_evidence_for_order(order_id)?;
                }
                Ok(())
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
                           broker_perm_id = CASE WHEN ? <> 0 THEN ?
                                                 ELSE broker_perm_id END,
                           updated_at = ?
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)",
                        params![
                            status,
                            perm_id,
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
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let order_id = self
                    .connection
                    .query_row(
                        "SELECT order_id FROM orders
                         WHERE (connection_session_id = ? AND broker_order_id = ?)
                            OR (? IS NULL AND broker_perm_id = ? AND ? <> 0)
                         ORDER BY created_at DESC LIMIT 1",
                        params![
                            connection_session_id,
                            broker_order_id,
                            connection_session_id,
                            perm_id,
                            perm_id
                        ],
                        |row| row.get::<_, uuid::Uuid>(0),
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if let Some(order_id) = order_id {
                    self.apply_completed_fill_evidence_for_order(order_id)?;
                }
                Ok(())
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
                executed_at,
            } => {
                let now = Utc::now();
                // The execution insert, pending-commission application and
                // pending-commission cleanup commit atomically so a crash
                // cannot strand a commission in the pending table.
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let changed = transaction
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
                            executed_at,
                            now,
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
                if changed == 0 {
                    let already_recorded: bool = transaction
                        .query_row(
                            "SELECT count(*) > 0 FROM executions
                             WHERE broker_execution_id = ?",
                            params![execution_id],
                            |row| row.get(0),
                        )
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                    if !already_recorded {
                        transaction
                            .execute(
                                "INSERT INTO pending_broker_executions
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                 ON CONFLICT (broker_execution_id) DO UPDATE SET
                                   connection_session_id = excluded.connection_session_id,
                                   broker_order_id = excluded.broker_order_id,
                                   broker_perm_id = excluded.broker_perm_id,
                                   conid = excluded.conid,
                                   side = excluded.side,
                                   quantity = excluded.quantity,
                                   price = excluded.price,
                                   executed_at = excluded.executed_at,
                                   received_at = excluded.received_at",
                                params![
                                    execution_id,
                                    connection_session_id,
                                    broker_order_id,
                                    perm_id,
                                    conid,
                                    side,
                                    quantity,
                                    price,
                                    executed_at,
                                    now
                                ],
                            )
                            .map_err(|error| AppError::Storage(error.to_string()))?;
                    }
                }
                transaction
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
                transaction
                    .execute(
                        "DELETE FROM pending_commissions WHERE broker_execution_id = ?
                         AND EXISTS (SELECT 1 FROM executions WHERE broker_execution_id = ?)",
                        params![execution_id, execution_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::Commission {
                execution_id,
                commission,
                currency,
                ..
            } => {
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let changed = transaction
                    .execute(
                        "UPDATE executions SET commission = ?, currency = ?
                     WHERE broker_execution_id = ?",
                        params![commission, currency, execution_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if changed == 0 {
                    transaction
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
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
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
            crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at,
            } => {
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let changed = transaction
                    .execute(
                        "UPDATE position_sync_state
                         SET state = 'syncing', observed_at = ?, subscription_id = ?,
                             snapshot_completed_at = NULL
                         WHERE singleton
                           AND (observed_at IS NULL OR observed_at < ?)",
                        params![observed_at, subscription_id, observed_at],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if changed > 0 {
                    transaction
                        .execute(
                            "UPDATE positions_current SET quantity = 0, average_cost = 0",
                            [],
                        )
                        .map_err(|error| AppError::Storage(error.to_string()))?;
                }
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))
            }
            crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position,
            } => {
                let current: bool = self
                    .connection
                    .query_row(
                        "SELECT count(*) > 0 FROM position_sync_state
                         WHERE singleton AND subscription_id = ?
                           AND state IN ('syncing', 'ready')",
                        params![subscription_id],
                        |row| row.get(0),
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if current {
                    self.upsert_position(position)?;
                    // A newly observed position may be the first evidence
                    // newer than a locally recorded fill. Make deferred
                    // targets immediately eligible; targets whose evidence is
                    // still stale will simply be deferred again by the normal
                    // claim check.
                    self.release_position_evidence_deferrals_for_contract(
                        &position.account,
                        position.conid,
                    )
                } else {
                    Ok(())
                }
            }
            crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at,
            } => {
                let current: bool = self
                    .connection
                    .query_row(
                        "SELECT count(*) > 0 FROM position_sync_state
                         WHERE singleton AND subscription_id = ? AND state = 'syncing'",
                        params![subscription_id],
                        |row| row.get(0),
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if !current {
                    return Ok(());
                }
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
                        "UPDATE position_sync_state SET state = 'ready', observed_at = ?,
                             snapshot_completed_at = ?
                         WHERE singleton AND subscription_id = ? AND state = 'syncing'",
                        params![observed_at, observed_at, subscription_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                // A completed full snapshot is authoritative for both present
                // positions and contracts omitted because they are flat.
                self.release_all_position_evidence_deferrals()
            }
            crate::ibkr::BrokerEvent::PositionSubscriptionHeartbeat {
                subscription_id,
                observed_at,
            } => self
                .connection
                .execute(
                    "UPDATE position_sync_state SET observed_at = ?
                     WHERE singleton AND state = 'ready' AND subscription_id = ?",
                    params![observed_at, subscription_id],
                )
                .map(|_| ())
                .map_err(|error| AppError::Storage(error.to_string())),
            crate::ibkr::BrokerEvent::PositionSubscriptionEnded {
                subscription_id,
                observed_at,
                reason,
            } => {
                let changed = self
                    .connection
                    .execute(
                        "UPDATE position_sync_state SET state = 'stale', observed_at = ?
                         WHERE singleton AND subscription_id = ?
                           AND state IN ('syncing', 'ready')",
                        params![observed_at, subscription_id],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                if changed > 0 {
                    tracing::warn!(%reason, %subscription_id, "invalidating the IBKR position snapshot lease");
                }
                Ok(())
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
                let persisted_observed_at =
                    if matches!(tick_type.as_str(), "Last" | "DelayedLast" | "LastRthTrade") {
                        // Last prices are not trustworthy until their separate
                        // exchange timestamp tick has arrived.
                        DateTime::from_timestamp(0, 0).expect("Unix epoch is valid")
                    } else {
                        *observed_at
                    };
                self.connection
                    .execute(
                        "INSERT INTO market_ticks_current VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (conid, tick_type) DO UPDATE SET
                       numeric_value = excluded.numeric_value,
                       text_value = excluded.text_value,
                       observed_at = excluded.observed_at",
                        params![
                            conid,
                            tick_type,
                            numeric_value,
                            text_value,
                            persisted_observed_at
                        ],
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                self.observe_timestamped_market_trade(
                    *conid,
                    tick_type,
                    *numeric_value,
                    text_value.as_deref(),
                    *observed_at,
                )
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

    /// Returns every non-base currency currently needed by configured strategy
    /// contracts, portfolio legs, orders, positions, or recorded executions.
    /// The daemon uses this list to verify that each periodic IBKR account FX
    /// snapshot actually covers all currencies needed by risk and performance
    /// calculations.
    pub fn required_fx_currencies(&self, base_currency: &str) -> Result<Vec<String>> {
        let base_currency = base_currency.trim().to_ascii_uppercase();
        if base_currency.len() != 3 {
            return Err(AppError::Storage(
                "base currency must be a three-letter code".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "WITH currencies(currency) AS (
                   SELECT json_extract_string(contract_json, '$.currency')
                   FROM strategy_execution_configs
                   UNION ALL
                   SELECT json_extract_string(contract_json, '$.currency')
                   FROM strategy_execution_portfolio_legs
                   UNION ALL
                   SELECT json_extract_string(payload_json, '$.contract.currency')
                   FROM order_intents
                   WHERE status IN ('approved', 'unknown')
                   UNION ALL
                   SELECT i.currency
                   FROM positions_current p
                   JOIN instruments i USING (conid)
                   UNION ALL
                   SELECT currency FROM executions
                 )
                 SELECT DISTINCT upper(trim(currency)) AS currency
                 FROM currencies
                 WHERE currency IS NOT NULL
                   AND length(trim(currency)) = 3
                   AND upper(trim(currency)) <> ?
                 ORDER BY currency",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        statement
            .query_map(params![base_currency], |row| row.get::<_, String>(0))
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn upsert_fx_rate(&mut self, input: &FxRateInput) -> Result<()> {
        const MAXIMUM_FUTURE_CLOCK_SKEW_SECONDS: i64 = 300;
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
        if input.observed_at
            > Utc::now() + chrono::Duration::seconds(MAXIMUM_FUTURE_CLOCK_SKEW_SECONDS)
        {
            return Err(AppError::Storage(format!(
                "FX rate observed_at cannot be more than {MAXIMUM_FUTURE_CLOCK_SKEW_SECONDS} seconds in the future"
            )));
        }
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO fx_rates VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (base_currency, quote_currency) DO UPDATE SET
                   rate = CASE WHEN excluded.observed_at >= fx_rates.observed_at
                               THEN excluded.rate ELSE fx_rates.rate END,
                   observed_at = greatest(excluded.observed_at, fx_rates.observed_at),
                   source = CASE WHEN excluded.observed_at >= fx_rates.observed_at
                                 THEN excluded.source ELSE fx_rates.source END",
                params![
                    &base,
                    &quote,
                    input.rate,
                    input.observed_at,
                    input.source.trim()
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO fx_rate_history VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (base_currency, quote_currency, observed_at)
                 DO UPDATE SET rate = excluded.rate, source = excluded.source",
                params![
                    &base,
                    &quote,
                    input.rate,
                    input.observed_at,
                    input.source.trim()
                ],
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        transaction
            .commit()
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

    /// Finds strategy-attributed execution values which cannot be converted
    /// using a quote known at execution time.  The returned range includes a
    /// full freshness lease before the first affected execution so a completed
    /// quote can be selected without using future information.
    pub fn strategy_historical_fx_gaps(
        &self,
        strategy_id: uuid::Uuid,
        quote_currency: &str,
        maximum_age_seconds: u64,
    ) -> Result<Vec<HistoricalFxGap>> {
        let quote_currency = quote_currency.trim().to_ascii_uppercase();
        if quote_currency.len() != 3 {
            return Err(AppError::Storage(
                "historical FX repair requires a three-letter quote currency".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.executed_at,
                        upper(trim(json_extract_string(
                          oi.payload_json, '$.contract.currency'))),
                        upper(trim(coalesce(e.currency,
                          json_extract_string(oi.payload_json, '$.contract.currency'))))
                 FROM executions e
                 JOIN orders o ON o.order_id = e.order_id
                 JOIN order_intents oi ON oi.order_intent_id = o.order_intent_id
                 WHERE EXISTS (
                    SELECT 1 FROM strategy_execution_actions a
                    WHERE a.strategy_id = ?
                      AND a.order_intent_id = o.order_intent_id
                 ) OR EXISTS (
                    SELECT 1 FROM strategy_execution_action_legs l
                    JOIN strategy_execution_actions a USING (action_id)
                    WHERE a.strategy_id = ?
                      AND l.order_intent_id = o.order_intent_id
                 )
                 ORDER BY e.executed_at",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![strategy_id, strategy_id], |row| {
                Ok((
                    row.get::<_, DateTime<Utc>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        drop(statement);

        let mut gaps = BTreeMap::<String, (DateTime<Utc>, DateTime<Utc>, usize)>::new();
        for (executed_at, trade_currency, commission_currency) in rows {
            let mut currencies = Vec::new();
            if let Some(currency) =
                trade_currency.filter(|currency| currency.len() == 3 && currency != &quote_currency)
            {
                currencies.push(currency);
            }
            if let Some(currency) = commission_currency
                .filter(|currency| currency.len() == 3 && currency != &quote_currency)
                && !currencies.contains(&currency)
            {
                currencies.push(currency);
            }
            for currency in currencies {
                if self
                    .currency_conversion_rate_at(
                        &currency,
                        &quote_currency,
                        maximum_age_seconds,
                        executed_at,
                    )?
                    .is_some()
                {
                    continue;
                }
                let entry = gaps
                    .entry(currency)
                    .or_insert((executed_at, executed_at, 0));
                entry.0 = entry.0.min(executed_at);
                entry.1 = entry.1.max(executed_at);
                entry.2 += 1;
            }
        }
        let lease =
            chrono::Duration::seconds(i64::try_from(maximum_age_seconds).unwrap_or(i64::MAX));
        Ok(gaps
            .into_iter()
            .map(
                |(base_currency, (first, last, affected_execution_values))| {
                    HistoricalFxGap {
                        base_currency,
                        quote_currency: quote_currency.clone(),
                        start: first.checked_sub_signed(lease).unwrap_or(first),
                        // Historical bars are timestamped at period open and only
                        // become usable at period close. Include two full minutes
                        // after the last fill so the enclosing bar is returned.
                        end: (last + chrono::Duration::minutes(2)).min(Utc::now()),
                        affected_execution_values,
                    }
                },
            )
            .filter(|gap| gap.end > gap.start)
            .collect())
    }

    pub fn create_strategy_historical_fx_jobs(
        &mut self,
        strategy_id: uuid::Uuid,
        quote_currency: &str,
        maximum_age_seconds: u64,
    ) -> Result<(Vec<HistoricalFxGap>, Vec<BackfillJobCreation>)> {
        let gaps =
            self.strategy_historical_fx_gaps(strategy_id, quote_currency, maximum_age_seconds)?;
        let mut jobs = Vec::with_capacity(gaps.len());
        for gap in &gaps {
            jobs.push(self.create_backfill_job(&BackfillJobRequest {
                contract: crate::ibkr::ContractCandidate {
                    conid: 0,
                    symbol: gap.base_currency.clone(),
                    security_type: "CASH".into(),
                    currency: gap.quote_currency.clone(),
                    exchange: "IDEALPRO".into(),
                    primary_exchange: String::new(),
                    local_symbol: format!("{}.{}", gap.base_currency, gap.quote_currency),
                    description: format!(
                        "{}/{} historical performance FX",
                        gap.base_currency, gap.quote_currency
                    ),
                    derivative_security_types: Vec::new(),
                },
                timeframe: "1m".into(),
                start: gap.start,
                end: gap.end,
                outside_rth: true,
                fx_rate_pair: Some(FxRateBackfillTarget {
                    base_currency: gap.base_currency.clone(),
                    quote_currency: gap.quote_currency.clone(),
                }),
            })?);
        }
        Ok((gaps, jobs))
    }

    /// Persists completed one-minute IBKR MIDPOINT bars as historical FX
    /// observations. The timestamp is the bar close, preventing a fill inside
    /// that minute from seeing a price which was not yet known.
    pub fn write_historical_fx_bars(
        &mut self,
        target: &FxRateBackfillTarget,
        bars: &[crate::ibkr::HistoricalBar],
    ) -> Result<usize> {
        let base = target.base_currency.trim().to_ascii_uppercase();
        let quote = target.quote_currency.trim().to_ascii_uppercase();
        if base.len() != 3 || quote.len() != 3 || base == quote {
            return Err(AppError::Storage(
                "historical FX target requires distinct three-letter currencies".into(),
            ));
        }
        if bars.iter().any(|bar| bar.timeframe != "1m") {
            return Err(AppError::Storage(
                "historical FX repair accepts only one-minute bars".into(),
            ));
        }
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let mut written = 0;
        for bar in bars {
            if !bar.close.is_finite() || bar.close <= 0.0 {
                continue;
            }
            let observed_at = bar.open_time + chrono::Duration::minutes(1);
            transaction
                .execute(
                    "INSERT INTO fx_rate_history VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (base_currency, quote_currency, observed_at)
                     DO UPDATE SET rate = excluded.rate, source = excluded.source",
                    params![
                        &base,
                        &quote,
                        bar.close,
                        observed_at,
                        "ibkr_historical_midpoint_1m"
                    ],
                )
                .map_err(|error| AppError::Storage(error.to_string()))?;
            written += 1;
        }
        transaction
            .commit()
            .map_err(|error| AppError::Storage(error.to_string()))?;
        Ok(written)
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
        // Read the newest non-future observation from history. A tolerated
        // small positive clock skew may be stored, but it must not hide the
        // preceding usable quote or become authoritative before its time.
        let lookup = |base: &str, quote: &str| -> Result<Option<(f64, DateTime<Utc>)>> {
            self.connection
                .query_row(
                    "SELECT rate, observed_at FROM fx_rate_history
                     WHERE base_currency = ? AND quote_currency = ?
                       AND observed_at <= ?
                     ORDER BY observed_at DESC LIMIT 1",
                    params![base, quote, now],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))
        };
        if let Some((rate, observed_at)) = lookup(&from, &to)?
            && fx_observation_is_fresh(observed_at, now, maximum_age_seconds)
            && rate.is_finite()
            && rate > 0.0
        {
            return Ok(Some(rate));
        }
        Ok(lookup(&to, &from)?.and_then(|(rate, observed_at)| {
            (fx_observation_is_fresh(observed_at, now, maximum_age_seconds)
                && rate.is_finite()
                && rate > 0.0)
                .then_some(1.0 / rate)
        }))
    }

    /// Returns the most recent FX quote known at `as_of`, bounded by the same
    /// freshness lease used for live risk. This keeps realized accounting
    /// stable instead of revaluing every historical fill at today's rate.
    fn currency_conversion_rate_at(
        &self,
        from: &str,
        to: &str,
        maximum_age_seconds: u64,
        as_of: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        let from = from.trim().to_ascii_uppercase();
        let to = to.trim().to_ascii_uppercase();
        if from == to {
            return Ok(Some(1.0));
        }
        let lookup = |base: &str, quote: &str| -> Result<Option<(f64, DateTime<Utc>)>> {
            self.connection
                .query_row(
                    "SELECT rate, observed_at FROM fx_rate_history
                     WHERE base_currency = ? AND quote_currency = ?
                       AND observed_at <= ?
                     ORDER BY observed_at DESC LIMIT 1",
                    params![base, quote, as_of],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| AppError::Storage(error.to_string()))
        };
        if let Some((rate, observed_at)) = lookup(&from, &to)?
            && fx_observation_is_fresh(observed_at, as_of, maximum_age_seconds)
            && rate.is_finite()
            && rate > 0.0
        {
            return Ok(Some(rate));
        }
        Ok(lookup(&to, &from)?.and_then(|(rate, observed_at)| {
            (fx_observation_is_fresh(observed_at, as_of, maximum_age_seconds)
                && rate.is_finite()
                && rate > 0.0)
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

    pub fn next_market_session_open_for(
        &self,
        exchange: &str,
        now: DateTime<Utc>,
        outside_rth: bool,
    ) -> Result<Option<DateTime<Utc>>> {
        let exchange = exchange.trim().to_ascii_uppercase();
        let session_kind = if outside_rth { "extended" } else { "regular" };
        self.connection
            .query_row(
                "SELECT min(opens_at) FROM market_session_intervals
                 WHERE exchange = ? AND session_kind = ? AND state = 'open'
                   AND opens_at > ?",
                params![exchange, session_kind, now],
                |row| row.get::<_, Option<DateTime<Utc>>>(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn defer_strategy_action_retry(
        &mut self,
        action_id: uuid::Uuid,
        not_before: DateTime<Utc>,
        detail: &str,
    ) -> Result<bool> {
        let now = Utc::now();
        self.connection
            .execute(
                "UPDATE strategy_execution_desired_targets AS d
                 SET next_attempt_at = CASE
                       WHEN d.next_attempt_at IS NULL OR d.next_attempt_at < ? THEN ?
                       ELSE d.next_attempt_at
                     END,
                     detail = ?, updated_at = ?
                 FROM strategy_execution_actions a
                 WHERE a.action_id = ? AND d.state = 'active'
                   AND d.source_evaluation_id = coalesce(
                         a.source_evaluation_id, a.evaluation_id)",
                params![not_before, not_before, detail, now, action_id],
            )
            .map(|changed| changed > 0)
            .map_err(|error| AppError::Storage(error.to_string()))
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

    fn attributed_account_position(&self, account: &str, conid: i32) -> Result<f64> {
        self.connection
            .query_row(
                "WITH attributed AS (
                   SELECT order_intent_id FROM strategy_execution_actions
                   WHERE order_intent_id IS NOT NULL
                   UNION
                   SELECT order_intent_id FROM strategy_execution_action_legs
                   WHERE order_intent_id IS NOT NULL
                 )
                 SELECT coalesce(sum(CASE
                     WHEN lower(e.side) IN ('buy', 'bought') THEN e.quantity
                     ELSE -e.quantity END), 0)
                 FROM attributed
                 JOIN order_intents oi USING (order_intent_id)
                 JOIN orders o USING (order_intent_id)
                 JOIN executions e ON e.order_id = o.order_id
                 WHERE oi.account_id = ? AND e.conid = ?",
                params![account, conid],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    fn fresh_position_mark(
        &self,
        conid: i32,
        quantity: f64,
        maximum_age_seconds: u64,
        now: DateTime<Utc>,
    ) -> Result<Option<(String, f64, DateTime<Utc>)>> {
        let (preferred, fallback) = if quantity >= 0.0 {
            ("Bid", "Ask")
        } else {
            ("Ask", "Bid")
        };
        self.connection
            .query_row(
                "SELECT tick_type, numeric_value, observed_at
                 FROM market_ticks_current
                 WHERE conid = ? AND observed_at >= ?
                   AND tick_type IN ('Bid', 'Ask', 'Last', 'LastRthTrade')
                   AND numeric_value IS NOT NULL AND numeric_value > 0
                 ORDER BY CASE tick_type
                     WHEN ? THEN 0 WHEN 'Last' THEN 1 WHEN 'LastRthTrade' THEN 2
                     WHEN ? THEN 3 ELSE 4 END, observed_at DESC
                 LIMIT 1",
                params![
                    conid,
                    now - chrono::Duration::seconds(maximum_age_seconds as i64),
                    preferred,
                    fallback
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))
    }

    pub fn strategy_performance_report(
        &self,
        strategy_id: uuid::Uuid,
        initial_capital: f64,
        base_currency: &str,
        maximum_fx_age_seconds: u64,
        maximum_market_data_age_seconds: u64,
        maximum_account_data_age_seconds: u64,
        benchmark_conid: Option<i32>,
        now: DateTime<Utc>,
    ) -> Result<serde_json::Value> {
        if !initial_capital.is_finite() || initial_capital <= 0.0 {
            return Err(AppError::Storage(
                "initial_capital must be finite and greater than zero".into(),
            ));
        }
        let (configured_account, configured_conid): (Option<String>, Option<i32>) = self
            .connection
            .query_row(
                "SELECT account_id,
                        try_cast(json_extract_string(contract_json, '$.conid') AS INTEGER)
                 FROM strategy_execution_configs
                 WHERE strategy_id = ?",
                params![strategy_id],
                |row| Ok((Some(row.get(0)?), row.get(1)?)),
            )
            .optional()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .unwrap_or((None, None));
        let mut data_warnings = Vec::new();
        let mut data_warning_groups = Vec::new();
        let incomplete_fills = self.attributed_incomplete_fills(strategy_id)?;
        if !incomplete_fills.is_empty() {
            data_warning_groups.push(serde_json::json!({
                "code": "missing_execution_details",
                "count": incomplete_fills.len(),
                "title": format!(
                    "{} 笔已成交订单缺少完整成交明细",
                    incomplete_fills.len()
                ),
                "detail": "已先通过 IBKR 完整对账尝试恢复；仍缺失的历史成交必须从 Activity Statement/Flex Report 导入，系统不会按订单状态猜造成交价。"
            }));
        }
        for (broker_order_id, expected, recorded) in incomplete_fills {
            let order_label = broker_order_id
                .map(|value| format!("Broker Order ID {value}"))
                .unwrap_or_else(|| "本地订单".into());
            data_warnings.push(format!(
                "{order_label} 已成交 {expected:.4}，但本地仅有 {recorded:.4} 的成交明细；无法可靠计算该段损益"
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT oi.account_id, e.executed_at, lower(e.side), e.quantity, e.price,
                        e.commission, e.currency,
                        json_extract_string(oi.payload_json, '$.contract.currency'),
                        e.conid,
                        CASE WHEN attributed.target_conid = e.conid
                             THEN attributed.target_quantity ELSE NULL END,
                        attributed.target_evidence_conflict
                 FROM executions e
                 JOIN orders o ON o.order_id = e.order_id
                 JOIN order_intents oi ON oi.order_intent_id = o.order_intent_id
                 JOIN (
                    SELECT strategy_id, order_intent_id,
                           CASE WHEN count(target_quantity) = 0 THEN NULL
                                WHEN count(DISTINCT target_quantity) = 1
                                 AND count(DISTINCT target_conid) = 1
                                THEN min(target_quantity) ELSE NULL END
                             AS target_quantity,
                           CASE WHEN count(target_conid) = 0 THEN NULL
                                WHEN count(DISTINCT target_conid) = 1
                                THEN min(target_conid) ELSE NULL END AS target_conid,
                           count(target_quantity) > 0 AND
                             (count(DISTINCT target_quantity) <> 1 OR
                              count(DISTINCT target_conid) <> 1)
                             AS target_evidence_conflict
                    FROM (
                       SELECT strategy_id, order_intent_id,
                              NULL::DOUBLE AS target_quantity,
                              NULL::INTEGER AS target_conid
                       FROM strategy_execution_actions
                       WHERE order_intent_id IS NOT NULL
                       UNION ALL
                       SELECT a.strategy_id, l.order_intent_id,
                              l.target_quantity, l.conid
                       FROM strategy_execution_action_legs l
                       JOIN strategy_execution_actions a USING (action_id)
                       WHERE l.order_intent_id IS NOT NULL
                    ) evidence
                    GROUP BY strategy_id, order_intent_id
                 ) attributed
                   ON attributed.order_intent_id = o.order_intent_id
                 WHERE attributed.strategy_id = ?
                 ORDER BY e.executed_at, e.broker_execution_id",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![strategy_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, DateTime<Utc>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, Option<f64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i32>(8)?,
                    row.get::<_, Option<f64>>(9)?,
                    row.get::<_, bool>(10)?,
                ))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| AppError::Storage(error.to_string()))?;

        let mut positions: HashMap<(String, i32), PerformancePosition> = HashMap::new();
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
        let mut cycle_realized: HashMap<(String, i32), f64> = HashMap::new();
        let mut cycle_commissions: HashMap<(String, i32), f64> = HashMap::new();
        let mut unmatched_execution_quantity = 0.0_f64;
        let mut historical_fx_warnings =
            BTreeMap::<(String, String), (usize, DateTime<Utc>, DateTime<Utc>)>::new();

        for (
            account,
            executed_at,
            side,
            quantity,
            price,
            commission,
            commission_currency,
            trade_currency,
            conid,
            historical_target_quantity,
            target_evidence_conflict,
        ) in rows
        {
            first_execution_at.get_or_insert(executed_at);
            last_execution_at = Some(executed_at);
            let Some(trade_currency) = trade_currency.filter(|value| !value.trim().is_empty())
            else {
                data_warnings.push(format!(
                    "Conid {conid} 在 {executed_at} 的成交缺少证券币种；该成交已从绩效计算中排除"
                ));
                continue;
            };
            let Some(trade_fx) = self.currency_conversion_rate_at(
                &trade_currency,
                base_currency,
                maximum_fx_age_seconds,
                executed_at,
            )?
            else {
                let key = (
                    trade_currency.to_ascii_uppercase(),
                    base_currency.to_ascii_uppercase(),
                );
                let summary =
                    historical_fx_warnings
                        .entry(key)
                        .or_insert((0, executed_at, executed_at));
                summary.0 += 1;
                summary.1 = summary.1.min(executed_at);
                summary.2 = summary.2.max(executed_at);
                data_warnings.push(format!(
                    "Conid {conid} 在 {executed_at} 的成交没有可将 {trade_currency} 转换为 {base_currency} 的新鲜汇率；该成交已从绩效计算中排除"
                ));
                continue;
            };
            let commission_base = match commission {
                Some(commission) => {
                    let currency = commission_currency.as_deref().unwrap_or(&trade_currency);
                    match self.currency_conversion_rate_at(
                        currency,
                        base_currency,
                        maximum_fx_age_seconds,
                        executed_at,
                    )? {
                        Some(commission_fx) => commission * commission_fx,
                        None => {
                            let key = (
                                currency.to_ascii_uppercase(),
                                base_currency.to_ascii_uppercase(),
                            );
                            let summary = historical_fx_warnings.entry(key).or_insert((
                                0,
                                executed_at,
                                executed_at,
                            ));
                            summary.0 += 1;
                            summary.1 = summary.1.min(executed_at);
                            summary.2 = summary.2.max(executed_at);
                            data_warnings.push(format!(
                                "Conid {conid} 在 {executed_at} 的佣金币种 {currency} 没有到 {base_currency} 的新鲜汇率；净损益暂按零佣金计算，不能视为完整结果"
                            ));
                            0.0
                        }
                    }
                }
                None => {
                    data_warnings.push(format!(
                        "Conid {conid} 在 {executed_at} 的成交尚无 CommissionReport；已实现净损益暂按零佣金计算，不能视为完整结果"
                    ));
                    0.0
                }
            };
            commissions += commission_base;
            turnover += quantity * price * trade_fx;
            let key = (account, conid);
            let position = positions.entry(key.clone()).or_default();
            position.currency = trade_currency;
            *cycle_commissions.entry(key.clone()).or_default() += commission_base;
            let previous_quantity = position.quantity;
            let mut realized = 0.0;
            if side.starts_with("bought") || side == "buy" {
                if position.quantity < 0.0 {
                    let closing = quantity.min(-position.quantity);
                    realized += (position.average_price - price) * closing;
                    position.quantity += closing;
                    let remaining = quantity - closing;
                    if position.quantity.abs() <= POSITION_QUANTITY_EPSILON {
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
                    if position.quantity.abs() <= POSITION_QUANTITY_EPSILON {
                        let projected_quantity = -remaining;
                        if remaining > 0.0
                            && !historical_target_authorizes_short(
                                historical_target_quantity,
                                target_evidence_conflict,
                                projected_quantity,
                            )
                        {
                            unmatched_execution_quantity += remaining;
                            data_warnings.push(missing_historical_short_authorization(
                                conid,
                                historical_target_quantity,
                                target_evidence_conflict,
                                projected_quantity,
                            ));
                            position.quantity = 0.0;
                            position.average_price = 0.0;
                        } else {
                            position.quantity = projected_quantity;
                            position.average_price = if remaining > 0.0 { price } else { 0.0 };
                        }
                    }
                } else {
                    let projected_quantity = position.quantity - quantity;
                    if historical_target_authorizes_short(
                        historical_target_quantity,
                        target_evidence_conflict,
                        projected_quantity,
                    ) {
                        let current_abs = -position.quantity;
                        let next_abs = current_abs + quantity;
                        position.average_price = if next_abs > 0.0 {
                            (position.average_price * current_abs + price * quantity) / next_abs
                        } else {
                            0.0
                        };
                        position.quantity = projected_quantity;
                    } else {
                        unmatched_execution_quantity += quantity;
                        data_warnings.push(missing_historical_short_authorization(
                            conid,
                            historical_target_quantity,
                            target_evidence_conflict,
                            projected_quantity,
                        ));
                    }
                }
            }
            if realized.abs() > f64::EPSILON {
                *cycle_realized.entry(key.clone()).or_default() += realized * trade_fx;
            }
            // A trade cycle ends when an existing position is flattened or
            // reversed.  Gross PnL can be exactly zero; its commissions still
            // make it a completed net-losing cycle and must not leak into the
            // next cycle.
            let closed_cycle = previous_quantity.abs() > POSITION_QUANTITY_EPSILON
                && (position.quantity.abs() <= POSITION_QUANTITY_EPSILON
                    || (position.quantity.abs() > POSITION_QUANTITY_EPSILON
                        && previous_quantity.signum() != position.quantity.signum()));
            if closed_cycle {
                let completed = cycle_realized.remove(&key).unwrap_or_default()
                    - cycle_commissions.remove(&key).unwrap_or_default();
                realized_trade_count += 1;
                if completed > 0.0 {
                    winning_trade_count += 1;
                } else if completed < 0.0 {
                    losing_trade_count += 1;
                }
            }
            gross_pnl += realized * trade_fx;
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
            .filter(|position| position.quantity.abs() > POSITION_QUANTITY_EPSILON)
            .count();
        if unmatched_execution_quantity > POSITION_QUANTITY_EPSILON {
            data_warnings.push(format!(
                "共有 {unmatched_execution_quantity:.4} 股卖出成交找不到可配对的策略买入；已按长期策略安全口径排除，未当作新开空仓"
            ));
        }
        for ((from, to), (count, first_at, last_at)) in historical_fx_warnings {
            data_warning_groups.push(serde_json::json!({
                "code": "historical_fx_unavailable",
                "count": count,
                "title": format!("{count} 笔成交值缺少 {from}/{to} 历史汇率"),
                "detail": format!(
                    "影响范围 {first_at} 至 {last_at}；可使用“修复历史数据”从 IBKR 下载一分钟 MIDPOINT 并重新计算。"
                ),
                "base_currency": from,
                "quote_currency": to,
                "first_at": first_at,
                "last_at": last_at,
            }));
        }
        data_warnings.sort();
        data_warnings.dedup();
        let data_warning_total = data_warnings.len();
        const MAXIMUM_INLINE_PERFORMANCE_WARNINGS: usize = 100;
        let data_warnings_truncated = data_warning_total > MAXIMUM_INLINE_PERFORMANCE_WARNINGS;
        data_warnings.truncate(MAXIMUM_INLINE_PERFORMANCE_WARNINGS);
        let data_complete =
            data_warnings.is_empty() && unmatched_execution_quantity <= POSITION_QUANTITY_EPSILON;
        let mut valuation_warnings = Vec::new();
        let mut open_positions = Vec::new();
        let position_sync = self
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
        let position_snapshot_fresh = position_sync.0 == "ready"
            && position_sync.1.is_some_and(|observed_at| {
                (now - observed_at).num_seconds().max(0) <= maximum_account_data_age_seconds as i64
            });
        if !position_snapshot_fresh {
            valuation_warnings.push(format!(
                "IBKR 持仓快照不可用于估值：状态 {}，时间 {}，允许最大年龄 {} 秒",
                position_sync.0,
                position_sync
                    .1
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "缺失".into()),
                maximum_account_data_age_seconds
            ));
        }
        let mut reconciliation_pairs = positions.keys().cloned().collect::<Vec<_>>();
        if let (Some(account), Some(conid)) = (&configured_account, configured_conid)
            && !reconciliation_pairs
                .iter()
                .any(|(candidate_account, candidate_conid)| {
                    candidate_account == account && *candidate_conid == conid
                })
        {
            reconciliation_pairs.push((account.clone(), conid));
        }
        if configured_account.is_none() {
            valuation_warnings.push("策略没有执行账户，无法核对当前持仓".into());
        }
        if position_snapshot_fresh {
            for (account, conid) in &reconciliation_pairs {
                let broker_quantity = self
                    .connection
                    .query_row(
                        "SELECT quantity FROM positions_current
                         WHERE account_id = ? AND conid = ?",
                        params![account, conid],
                        |row| row.get::<_, f64>(0),
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?
                    .unwrap_or(0.0);
                let attributed_quantity = self.attributed_account_position(account, *conid)?;
                if (broker_quantity - attributed_quantity).abs() > 0.000_000_001 {
                    valuation_warnings.push(format!(
                        "账户 {account} 的 Conid {conid} 券商净仓 {broker_quantity:.4} 与所有策略归因净仓 {attributed_quantity:.4} 不一致；可能存在手工、外部或缺失成交"
                    ));
                }
            }
        }
        let mut unrealized_pnl = 0.0;
        for ((account, conid), position) in positions
            .iter()
            .filter(|(_, position)| position.quantity.abs() > POSITION_QUANTITY_EPSILON)
        {
            let mark = self.fresh_position_mark(
                *conid,
                position.quantity,
                maximum_market_data_age_seconds,
                now,
            )?;
            let Some((price_type, mark_price, observed_at)) = mark else {
                valuation_warnings.push(format!(
                    "账户 {account} 的 Conid {conid} 没有 {} 秒内的新鲜实时估值价格",
                    maximum_market_data_age_seconds
                ));
                open_positions.push(serde_json::json!({
                    "account": account,
                    "conid": conid,
                    "quantity": position.quantity,
                    "average_price": position.average_price,
                    "currency": position.currency,
                    "mark_price": null,
                    "unrealized_pnl": null,
                }));
                continue;
            };
            let fx = self.currency_conversion_rate(
                &position.currency,
                base_currency,
                maximum_fx_age_seconds,
                now,
            )?;
            let Some(fx) = fx else {
                valuation_warnings.push(format!(
                    "Conid {conid} 没有可将 {} 转换为 {} 的新鲜汇率",
                    position.currency, base_currency
                ));
                open_positions.push(serde_json::json!({
                    "account": account,
                    "conid": conid,
                    "quantity": position.quantity,
                    "average_price": position.average_price,
                    "currency": position.currency,
                    "mark_price": mark_price,
                    "mark_price_type": price_type,
                    "mark_observed_at": observed_at,
                    "unrealized_pnl": null,
                }));
                continue;
            };
            let position_unrealized =
                (mark_price - position.average_price) * position.quantity * fx;
            unrealized_pnl += position_unrealized;
            open_positions.push(serde_json::json!({
                "account": account,
                "conid": conid,
                "quantity": position.quantity,
                "average_price": position.average_price,
                "currency": position.currency,
                "mark_price": mark_price,
                "mark_price_type": price_type,
                "mark_observed_at": observed_at,
                "unrealized_pnl": position_unrealized,
            }));
        }
        valuation_warnings.sort();
        valuation_warnings.dedup();
        let valuation_complete = data_complete && valuation_warnings.is_empty();
        let total_net_pnl = valuation_complete.then_some(net_pnl + unrealized_pnl);
        let total_return = total_net_pnl.map(|value| value / initial_capital);
        let benchmark_return = match (benchmark_conid, first_execution_at, last_execution_at) {
            (Some(conid), Some(start), Some(end)) => {
                let first = self
                    .connection
                    .query_row(
                        "SELECT close FROM market_minute_bars
                     WHERE conid = ? AND bar_time >= ? AND bar_time <= ?
                     ORDER BY bar_time LIMIT 1",
                        params![conid, start, end],
                        |row| row.get::<_, f64>(0),
                    )
                    .optional()
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let last = self
                    .connection
                    .query_row(
                        "SELECT close FROM market_minute_bars
                     WHERE conid = ? AND bar_time >= ? AND bar_time <= ?
                     ORDER BY bar_time DESC LIMIT 1",
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
        let realized_return = net_pnl / initial_capital;
        let mut report = serde_json::json!({
            "strategy_id": strategy_id,
            "base_currency": base_currency.to_ascii_uppercase(),
            "initial_capital": initial_capital,
            "gross_pnl": gross_pnl,
            "commissions": commissions,
            "net_pnl": net_pnl,
            "return": realized_return,
            "realized_gross_pnl": gross_pnl,
            "realized_net_pnl": net_pnl,
            "unrealized_pnl": valuation_complete.then_some(unrealized_pnl),
            "total_net_pnl": total_net_pnl,
            "realized_return": realized_return,
            "total_return": total_return,
            "benchmark_conid": benchmark_conid,
            "benchmark_return": benchmark_return,
            "excess_return": benchmark_return.map(|value| realized_return - value),
            "total_excess_return": benchmark_return.zip(total_return)
                .map(|(benchmark, strategy)| strategy - benchmark),
            "turnover": turnover,
            "maximum_drawdown": maximum_drawdown,
            "maximum_drawdown_pct": maximum_drawdown / initial_capital,
            "realized_maximum_drawdown": maximum_drawdown,
            "realized_maximum_drawdown_pct": maximum_drawdown / initial_capital,
            "sharpe": sharpe,
            "sortino": sortino,
            "realized_trade_count": realized_trade_count,
            "winning_trade_count": winning_trade_count,
            "losing_trade_count": losing_trade_count,
            "win_rate": (realized_trade_count > 0)
                .then_some(winning_trade_count as f64 / realized_trade_count as f64),
            "open_position_count": open_position_count,
            "data_complete": data_complete,
            "data_warnings": data_warnings,
            "valuation_complete": valuation_complete,
            "valuation_warnings": valuation_warnings,
            "position_snapshot_state": position_sync.0,
            "position_snapshot_observed_at": position_sync.1,
            "open_positions": open_positions,
            "unmatched_execution_quantity": unmatched_execution_quantity,
            "daily_equity": daily_equity.into_iter().map(|(date, equity)| {
                serde_json::json!({"date": date, "equity": equity})
            }).collect::<Vec<_>>(),
            "generated_at": now,
        });
        report["data_warning_groups"] = serde_json::Value::Array(data_warning_groups);
        report["data_warning_total"] = serde_json::json!(data_warning_total);
        report["data_warnings_truncated"] = serde_json::json!(data_warnings_truncated);
        Ok(report)
    }

    pub fn persist_strategy_performance_snapshot(
        &mut self,
        strategy_id: uuid::Uuid,
        account: &str,
        report: &serde_json::Value,
    ) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO strategy_performance_snapshots
                 (strategy_id, account_id, observed_at, base_currency, gross_pnl,
                  commissions, net_pnl, turnover, realized_trade_count,
                  winning_trade_count, losing_trade_count, open_position_count,
                  unrealized_pnl, total_net_pnl, data_complete,
                  valuation_complete, warnings_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                    report["open_position_count"].as_i64().unwrap_or(0),
                    report["unrealized_pnl"].as_f64(),
                    report["total_net_pnl"].as_f64(),
                    report["data_complete"].as_bool(),
                    report["valuation_complete"].as_bool(),
                    serde_json::to_string(&serde_json::json!({
                        "data_warnings": report["data_warnings"],
                        "valuation_warnings": report["valuation_warnings"]
                    }))?
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
                        winning_trade_count, losing_trade_count, open_position_count,
                        unrealized_pnl, total_net_pnl, data_complete,
                        valuation_complete, warnings_json::VARCHAR
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
                    "unrealized_pnl": row.get::<_, Option<f64>>(11)?,
                    "total_net_pnl": row.get::<_, Option<f64>>(12)?,
                    "data_complete": row.get::<_, Option<bool>>(13)?,
                    "valuation_complete": row.get::<_, Option<bool>>(14)?,
                    "warnings": row.get::<_, Option<String>>(15)?
                        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()),
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

    /// Lists only order intents whose broker outcome is still unknown. These
    /// rows may not have a corresponding `orders` row, so `order.list` cannot
    /// be used to surface them to an operator for manual resolution.
    pub fn list_unknown_order_intents_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let total: i64 = self
            .connection
            .query_row(
                "SELECT count(*) FROM order_intents WHERE status = 'unknown'",
                [],
                |row| row.get(0),
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let page_size = page_size.clamp(1, 500);
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        let mut statement = self
            .connection
            .prepare(
                "SELECT oi.order_intent_id, oi.account_id, oi.conid,
                        json_extract_string(oi.payload_json, '$.side'),
                        try_cast(json_extract_string(oi.payload_json, '$.quantity') AS DOUBLE),
                        oi.rejection_reason, oi.created_at, oi.updated_at,
                        i.symbol, i.description, i.exchange, i.primary_exchange
                 FROM order_intents oi
                 LEFT JOIN instruments i ON i.conid = oi.conid
                 WHERE oi.status = 'unknown'
                 ORDER BY oi.created_at DESC LIMIT ? OFFSET ?",
            )
            .map_err(|error| AppError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![page_size as i64, offset as i64], |row| {
                Ok(serde_json::json!({
                    "order_intent_id": row.get::<_, uuid::Uuid>(0)?,
                    "account_id": row.get::<_, String>(1)?,
                    "conid": row.get::<_, i64>(2)?,
                    "side": row.get::<_, Option<String>>(3)?,
                    "quantity": row.get::<_, Option<f64>>(4)?,
                    "reason": row.get::<_, Option<String>>(5)?,
                    "created_at": row.get::<_, DateTime<Utc>>(6)?,
                    "updated_at": row.get::<_, DateTime<Utc>>(7)?,
                    "symbol": row.get::<_, Option<String>>(8)?,
                    "description": row.get::<_, Option<String>>(9)?,
                    "exchange": row.get::<_, Option<String>>(10)?,
                    "primary_exchange": row.get::<_, Option<String>>(11)?,
                    "status": "unknown",
                }))
            })
            .map_err(|error| AppError::Storage(error.to_string()))?
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
            let completed_filled_quantity = order
                .completed_status
                .as_deref()
                .and_then(completed_fill_size)
                .unwrap_or(0.0);
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
            } else if let Some(local_order_id) = local_order_id {
                let (local_status, requested_quantity, recorded_execution_quantity): (
                    String,
                    Option<f64>,
                    f64,
                ) = transaction
                    .query_row(
                        "SELECT o.status,
                                try_cast(json_extract_string(
                                  i.payload_json, '$.quantity') AS DOUBLE),
                                coalesce((SELECT sum(e.quantity) FROM executions e
                                          WHERE e.order_id = o.order_id), 0)
                         FROM orders o
                         JOIN order_intents i USING (order_intent_id)
                         WHERE o.order_id = ?",
                        params![local_order_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                let executions_cover_request = requested_quantity.is_some_and(|requested| {
                    requested.is_finite()
                        && requested > 0.0
                        && recorded_execution_quantity + 0.000000001 >= requested
                });
                let completed_evidence_covers_request = completed_filled_quantity > 0.0
                    && completed_filled_quantity + 0.000000001 >= order.quantity;
                let effective_status =
                    if executions_cover_request || completed_evidence_covers_request {
                        "Filled"
                    } else if order_status_is_terminal(&order.status) {
                        order.status.as_str()
                    } else if order_status_is_terminal(&local_status) {
                        local_status.as_str()
                    } else {
                        order.status.as_str()
                    };
                let observed_filled_quantity =
                    completed_filled_quantity.max(recorded_execution_quantity);
                transaction
                    .execute(
                        "UPDATE orders SET status = ?, broker_perm_id = ?,
                             filled_quantity = greatest(filled_quantity, ?),
                             connection_session_id = CASE WHEN ? = 'open' THEN ? ELSE connection_session_id END,
                             broker_order_id = CASE WHEN ? = 'open' THEN ? ELSE broker_order_id END,
                             updated_at = ?
                         WHERE order_id = ?",
                        params![
                            effective_status,
                            order.perm_id,
                            observed_filled_quantity,
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
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                        snapshot.completed_at,
                        order.completed_status
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
                   AND lower(status) NOT IN ('filled', 'cancelled', 'canceled', 'inactive',
                                             'rejected', 'not_open')",
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
        // Reconciliation may only now have associated a stable Perm ID with a
        // local order. Executions received earlier in this same snapshot can
        // therefore be attached safely after the order transaction commits.
        self.drain_pending_broker_executions()?;
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

fn duckdb_timestamp(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(value.timestamp_micros())
        .expect("a valid DateTime remains valid at microsecond precision")
}

fn same_backfill_scope(left: &BackfillJobRequest, right: &BackfillJobRequest) -> bool {
    left.contract.conid == right.contract.conid
        && left.contract.symbol == right.contract.symbol
        && left.contract.security_type == right.contract.security_type
        && left.contract.currency == right.contract.currency
        && left.contract.exchange == right.contract.exchange
        && left.timeframe == right.timeframe
        && left.outside_rth == right.outside_rth
        && left.fx_rate_pair == right.fx_rate_pair
}

/// Coverage is tied to the instrument identity and data semantics, not the
/// routing exchange spelling captured in an older contract snapshot. IBKR can
/// return the same conid as SMART or its primary exchange across sessions.
fn same_backfill_coverage_scope(left: &BackfillJobRequest, right: &BackfillJobRequest) -> bool {
    left.contract.conid == right.contract.conid
        && left.timeframe == right.timeframe
        && left.outside_rth == right.outside_rth
        && left.fx_rate_pair == right.fx_rate_pair
}

fn backfill_ranges_overlap(left: &BackfillJobRequest, right: &BackfillJobRequest) -> bool {
    left.start <= right.end && right.start <= left.end
}

fn interval_gaps(
    requested_start: DateTime<Utc>,
    requested_end: DateTime<Utc>,
    intervals: &[(DateTime<Utc>, DateTime<Utc>)],
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let intervals = merged_time_intervals(requested_start, requested_end, intervals);

    let mut gaps = Vec::new();
    let mut cursor = requested_start;
    for (start, end) in intervals {
        if start > cursor {
            gaps.push((cursor, start));
        }
        cursor = cursor.max(end);
        if cursor >= requested_end {
            break;
        }
    }
    if cursor < requested_end {
        gaps.push((cursor, requested_end));
    }
    gaps
}

fn merged_time_intervals(
    requested_start: DateTime<Utc>,
    requested_end: DateTime<Utc>,
    intervals: &[(DateTime<Utc>, DateTime<Utc>)],
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let mut intervals = intervals
        .iter()
        .map(|(start, end)| ((*start).max(requested_start), (*end).min(requested_end)))
        .filter(|(start, end)| end > start)
        .collect::<Vec<_>>();
    intervals.sort_unstable_by_key(|(start, _)| *start);
    let mut merged: Vec<(DateTime<Utc>, DateTime<Utc>)> = Vec::new();
    for (start, end) in intervals {
        if let Some((_, previous_end)) = merged.last_mut()
            && start <= *previous_end
        {
            *previous_end = (*previous_end).max(end);
        } else {
            merged.push((start, end));
        }
    }
    merged
}

fn gaps_to_json(gaps: &[(DateTime<Utc>, DateTime<Utc>)]) -> Vec<serde_json::Value> {
    gaps.iter()
        .map(|(start, end)| serde_json::json!({"start": start, "end": end}))
        .collect()
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

fn commission_to_gross_profit_ratio(gross_pnl: f64, commissions: f64) -> Option<f64> {
    (gross_pnl > 0.0 && commissions.is_finite()).then_some(commissions.max(0.0) / gross_pnl)
}

fn blocked_commission_performance_ratio(
    completed_trades: usize,
    gross_pnl: f64,
    commissions: f64,
    minimum_completed_trades: usize,
    maximum_ratio: f64,
) -> Option<f64> {
    (completed_trades >= minimum_completed_trades)
        .then(|| commission_to_gross_profit_ratio(gross_pnl, commissions))
        .flatten()
        .filter(|ratio| *ratio > maximum_ratio)
}

fn strategy_signal_edge_bps(
    strategy_kind: &str,
    signal: &str,
    indicator_a: f64,
    indicator_b: f64,
    details: &serde_json::Value,
) -> Option<f64> {
    if strategy_kind == "paper_round_trip" || !matches!(signal, "buy" | "sell") {
        return None;
    }
    let price = details
        .pointer("/bar/close")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| details.get("close").and_then(serde_json::Value::as_f64))?;
    let reference = if strategy_kind == "close_threshold" {
        details
            .get(if signal == "buy" {
                "buy_below"
            } else {
                "sell_above"
            })
            .and_then(serde_json::Value::as_f64)?
    } else {
        indicator_b
    };
    (price.is_finite() && price > 0.0 && indicator_a.is_finite() && reference.is_finite())
        .then_some((indicator_a - reference).abs() / price * 10_000.0)
}

fn scalar_target_for_signal(
    signal: &str,
    long_target: f64,
    short_target: f64,
    allow_short: bool,
) -> Option<f64> {
    match signal {
        "buy" => Some(long_target),
        "sell" => Some(if allow_short { short_target } else { 0.0 }),
        _ => None,
    }
}

/// Crossing zero is deliberately a two-order transition.  It lets the
/// position stream confirm that the old exposure is flat before a second,
/// risk-increasing order opens exposure in the opposite direction.
fn next_rebalance_phase_target(current: f64, desired: f64) -> f64 {
    if current.abs() > POSITION_QUANTITY_EPSILON
        && desired.abs() > POSITION_QUANTITY_EPSILON
        && current.signum() != desired.signum()
    {
        0.0
    } else {
        desired
    }
}

fn validate_backtest_request(request: &BacktestRequest) -> Result<()> {
    if request.conid <= 0
        || request.end <= request.start
        || !request.quantity.is_finite()
        || request.quantity <= 0.0
        || !request.short_target_quantity.is_finite()
        || request.short_target_quantity > 0.0
        || (!request.allow_short && request.short_target_quantity.abs() > POSITION_QUANTITY_EPSILON)
        || !request.initial_cash.is_finite()
        || request.initial_cash <= 0.0
    {
        return Err(AppError::Storage("invalid backtest parameters".into()));
    }
    let strategy = build_backtest_strategy(request)?;
    if strategy.conid() != request.conid {
        return Err(AppError::Storage(
            "backtest conid must match strategy config conid".into(),
        ));
    }
    let supports_short_targets = strategy_catalog_backend::metadata(strategy.kind())
        .is_some_and(|metadata| metadata.capabilities.supports_short_targets);
    if (request.allow_short || request.short_target_quantity < -POSITION_QUANTITY_EPSILON)
        && !supports_short_targets
    {
        return Err(AppError::Storage(format!(
            "strategy kind {} does not support short targets",
            strategy.kind()
        )));
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

#[derive(Clone, Debug, Default, Serialize)]
struct BacktestCostGateMetrics {
    transaction_passed: usize,
    transaction_blocked: usize,
    performance_blocked: usize,
    risk_reducing_bypassed: usize,
    missing_signal_edge_blocked: usize,
}

#[derive(Clone, Debug, Default)]
struct BacktestCompletedCycleAccumulator {
    quantity: f64,
    average_price: f64,
    current_cycle_gross_pnl: f64,
    current_cycle_commissions: f64,
    completed_trades: usize,
    completed_gross_pnl: f64,
    commissions_since_start: f64,
}

impl BacktestCompletedCycleAccumulator {
    fn record_fill(&mut self, side: &str, quantity: f64, price: f64, commission: f64) {
        self.commissions_since_start += commission;
        let signed_quantity = if side == "buy" { quantity } else { -quantity };
        if self.quantity.abs() <= POSITION_QUANTITY_EPSILON
            || self.quantity.signum() == signed_quantity.signum()
        {
            let previous_abs = self.quantity.abs();
            let next_abs = previous_abs + quantity;
            self.average_price = if next_abs > POSITION_QUANTITY_EPSILON {
                (self.average_price * previous_abs + price * quantity) / next_abs
            } else {
                0.0
            };
            self.quantity += signed_quantity;
            self.current_cycle_commissions += commission;
            return;
        }

        let closing_quantity = self.quantity.abs().min(quantity);
        let closing_fraction = closing_quantity / quantity;
        self.current_cycle_gross_pnl += if self.quantity > 0.0 {
            (price - self.average_price) * closing_quantity
        } else {
            (self.average_price - price) * closing_quantity
        };
        self.current_cycle_commissions += commission * closing_fraction;
        let previous_sign = self.quantity.signum();
        self.quantity += signed_quantity.signum() * closing_quantity;
        let remaining = quantity - closing_quantity;
        if self.quantity.abs() <= POSITION_QUANTITY_EPSILON {
            self.completed_trades += 1;
            self.completed_gross_pnl += self.current_cycle_gross_pnl;
            self.quantity = 0.0;
            self.average_price = 0.0;
            self.current_cycle_gross_pnl = 0.0;
            self.current_cycle_commissions = 0.0;
        }
        if remaining > POSITION_QUANTITY_EPSILON {
            // This branch is retained for parity with imported/live fills even
            // though normal backtest reversals are flattened first.
            self.quantity = -previous_sign * remaining;
            self.average_price = price;
            self.current_cycle_commissions = commission * (1.0 - closing_fraction);
        }
    }
}

fn simulate_strategy(
    request: &BacktestRequest,
    strategy: &dyn crate::strategy::Strategy,
    cost_model: &ExecutionCostModelInput,
    cost_gate: &BacktestCostGateSnapshot,
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
    // A pending item is a desired absolute position, not an order quantity.
    // A cross-zero request executes only its flatten phase; opening the other
    // side requires a later explicit directional signal, matching live safety
    // semantics and preventing stale Hold state from reopening exposure.
    let mut pending: Option<PendingBacktestTarget> = None;
    let mut cost_gate_metrics = BacktestCostGateMetrics::default();
    let mut completed_cycles = BacktestCompletedCycleAccumulator::default();
    let mut history = Vec::with_capacity(bars.len());
    let mut strategy_state = strategy.initial_state();
    for bar in bars {
        if let Some(pending_target) = pending.take() {
            let phase_target = next_rebalance_phase_target(position, pending_target.desired_target);
            let delta = phase_target - position;
            if delta.abs() > POSITION_QUANTITY_EPSILON {
                let (side, cost_side, direction) = if delta > 0.0 {
                    ("buy", CostSide::Buy, 1.0)
                } else {
                    ("sell", CostSide::Sell, -1.0)
                };
                let quantity = delta.abs();
                let reference_notional = bar.open * quantity;
                let estimated =
                    cost_model.estimated_execution_cost(cost_side, reference_notional, quantity);
                let fill_price =
                    bar.open + direction * (estimated.spread + estimated.slippage) / quantity;
                let cash_change = fill_price * quantity;
                let commission = estimated.commission;
                // Closing risk must never be blocked by a cash pre-check. A
                // risk-increasing long entry still uses a cash-only model;
                // short entries are intentionally unconstrained because this
                // simulator does not model broker-specific margin.
                let risk_reducing = position_change_is_risk_reducing(position, phase_target);
                let mut cost_gate_passed = true;
                if cost_gate.applied {
                    if risk_reducing {
                        cost_gate_metrics.risk_reducing_bypassed += 1;
                    } else if blocked_commission_performance_ratio(
                        completed_cycles.completed_trades,
                        completed_cycles.completed_gross_pnl,
                        completed_cycles.commissions_since_start,
                        cost_gate
                            .minimum_completed_trades
                            .expect("applied strategy gate has a minimum trade count"),
                        cost_gate
                            .maximum_commission_to_gross_profit_ratio
                            .expect("applied strategy gate has a maximum ratio"),
                    )
                    .is_some()
                    {
                        cost_gate_metrics.performance_blocked += 1;
                        cost_gate_passed = false;
                    } else {
                        let decision = evaluate_transaction_cost_gate(
                            cost_model,
                            cost_gate
                                .minimum_cost_multiple
                                .expect("applied strategy gate has a cost multiple"),
                            cost_gate.actual_fee_bps_p90,
                            pending_target.signal_edge_bps,
                            false,
                            &[CostGateLegEstimate {
                                quantity,
                                price: bar.open,
                            }],
                        );
                        match decision.outcome {
                            TransactionCostGateOutcome::Passed => {
                                cost_gate_metrics.transaction_passed += 1;
                            }
                            TransactionCostGateOutcome::Blocked => {
                                cost_gate_metrics.transaction_blocked += 1;
                                if pending_target.signal_edge_bps.is_none() {
                                    cost_gate_metrics.missing_signal_edge_blocked += 1;
                                }
                                cost_gate_passed = false;
                            }
                            TransactionCostGateOutcome::BypassedRiskReduction => {
                                unreachable!("risk-increasing branch cannot bypass")
                            }
                        }
                    }
                }
                let filled = if !cost_gate_passed {
                    false
                } else if side == "buy" && (risk_reducing || cash >= cash_change + commission) {
                    cash -= cash_change + commission;
                    position = phase_target;
                    true
                } else if side == "sell" {
                    cash += cash_change - commission;
                    position = phase_target;
                    true
                } else {
                    false
                };
                if filled {
                    completed_cycles.record_fill(side, quantity, fill_price, commission);
                    trades.push(SimulatedTrade {
                        signal_time: pending_target.signal_time,
                        fill_time: bar.open_time,
                        side,
                        quantity,
                        price: fill_price,
                        commission,
                        spread: estimated.spread,
                        slippage: estimated.slippage,
                    });
                }
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
            let signal = output.signal.as_str();
            let signal_edge_bps = strategy_signal_edge_bps(
                strategy.kind(),
                signal,
                output.indicator_a,
                output.indicator_b,
                &output.details,
            );
            let flatten_only = output
                .details
                .get("target_intent")
                .and_then(serde_json::Value::as_str)
                == Some("flatten_only");
            let target = if flatten_only {
                Some(0.0)
            } else {
                scalar_target_for_signal(
                    signal,
                    request.quantity,
                    request.short_target_quantity,
                    request.allow_short,
                )
            };
            if let Some(target) = target {
                // A newer directional signal supersedes an older pending
                // target. If the position already reflects it there is
                // nothing to submit.
                pending = ((position - target).abs() > POSITION_QUANTITY_EPSILON).then(|| {
                    PendingBacktestTarget {
                        desired_target: target,
                        signal_time: bar.open_time,
                        signal_edge_bps,
                    }
                });
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
    let total_commission = trades.iter().map(|trade| trade.commission).sum::<f64>();
    let total_spread = trades.iter().map(|trade| trade.spread).sum::<f64>();
    let total_slippage = trades.iter().map(|trade| trade.slippage).sum::<f64>();
    let metrics = serde_json::json!({
        "bar_count": bars.len(),
        "trade_count": trades.len(),
        "initial_cash": request.initial_cash,
        "final_equity": final_equity,
        "total_return": total_return,
        "bar_return_volatility": volatility,
        "maximum_drawdown": maximum_drawdown,
        "turnover": traded_notional / request.initial_cash,
        "total_commission": total_commission,
        "total_spread": total_spread,
        "total_slippage": total_slippage,
        "total_execution_cost": total_commission + total_spread + total_slippage,
        "cost_gate": cost_gate,
        "cost_gate_decisions": cost_gate_metrics,
        "cost_gate_completed_trades": completed_cycles.completed_trades,
        "cost_gate_completed_gross_pnl": completed_cycles.completed_gross_pnl,
        "cost_gate_commissions_since_start": completed_cycles.commissions_since_start,
        "open_position": position,
        "pending_signal_discarded_at_end": pending.is_some(),
        "long_target_quantity": request.quantity,
        "short_target_quantity": if request.allow_short {
            request.short_target_quantity
        } else {
            0.0
        },
        "allow_short": request.allow_short,
        "reversal_execution": "flatten_then_require_fresh_directional_signal",
        "margin_model": "cash_only_long; short margin and borrow constraints are not modeled",
        "gate_scope": "transaction cost edge and commission/completed-cycle gross-profit controls only; strategy risk, account, market-data freshness, order-conflict and trading-calendar gates are not simulated"
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
    fn commission_ratio_fuse_requires_positive_gross_profit() {
        assert_eq!(commission_to_gross_profit_ratio(100.0, 25.0), Some(0.25));
        assert_eq!(commission_to_gross_profit_ratio(0.0, 25.0), None);
        assert_eq!(commission_to_gross_profit_ratio(-100.0, 25.0), None);
    }

    #[test]
    fn shared_transaction_cost_gate_uses_every_cost_component_and_passes_at_equality() {
        let model = ExecutionCostModelInput {
            cost_model_id: None,
            name: "all-components".into(),
            currency: "USD".into(),
            buy_fixed_fee: 1.0,
            buy_per_share_fee: 0.1,
            buy_rate_bps: 10.0,
            buy_min_fee: 5.0,
            sell_fixed_fee: 2.0,
            sell_per_share_fee: 0.1,
            sell_rate_bps: 20.0,
            sell_min_fee: 6.0,
            sell_tax_bps: 10.0,
            estimated_spread_bps: 4.0,
            estimated_slippage_bps: 3.0,
        };
        let leg = CostGateLegEstimate {
            quantity: 10.0,
            price: 100.0,
        };
        // Configured commissions are 12, but the learned 100 bps single-leg
        // floor produces 20 for the round trip. Spread + slippage add 1.
        let equal =
            evaluate_transaction_cost_gate(&model, 2.0, Some(100.0), Some(420.0), false, &[leg]);
        assert_eq!(equal.outcome, TransactionCostGateOutcome::Passed);
        assert!((equal.estimated_round_trip_cost - 21.0).abs() < 1e-9);
        assert!((equal.required_edge_bps - 420.0).abs() < 1e-9);

        let below =
            evaluate_transaction_cost_gate(&model, 2.0, Some(100.0), Some(419.999), false, &[leg]);
        assert_eq!(below.outcome, TransactionCostGateOutcome::Blocked);
        let reducing = evaluate_transaction_cost_gate(&model, 2.0, Some(100.0), None, true, &[leg]);
        assert_eq!(
            reducing.outcome,
            TransactionCostGateOutcome::BypassedRiskReduction
        );
    }

    #[test]
    fn commission_performance_gate_matches_live_threshold_semantics() {
        assert_eq!(
            blocked_commission_performance_ratio(4, 100.0, 60.0, 5, 0.5),
            None
        );
        assert_eq!(
            blocked_commission_performance_ratio(5, 100.0, 50.0, 5, 0.5),
            None
        );
        assert_eq!(
            blocked_commission_performance_ratio(5, 100.0, 50.01, 5, 0.5),
            Some(0.5001)
        );
        assert_eq!(
            blocked_commission_performance_ratio(5, 0.0, 50.0, 5, 0.5),
            None
        );
    }

    #[test]
    fn creates_and_migrates_database() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let current_version = MIGRATIONS.last().map(|(version, _)| *version).unwrap_or(0);
        assert_eq!(storage.schema_version().unwrap(), current_version);
    }

    #[test]
    fn v2_state_v3_migration_is_fail_closed_and_leaves_other_kinds_running() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let v2_id = storage
            .create_strategy(
                "v2 migration fixture",
                "moving_average_cross_v2",
                &serde_json::json!({
                    "conid": 756733,
                    "short_window": 2,
                    "long_window": 3,
                    "bar_timeframe": "1m"
                }),
            )
            .unwrap();
        let classic_id = storage
            .create_strategy(
                "classic migration control",
                "moving_average_cross",
                &serde_json::json!({
                    "conid": 756733,
                    "short_window": 2,
                    "long_window": 3
                }),
            )
            .unwrap();
        for strategy_id in [v2_id, classic_id] {
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
                    contract: spy_contract(),
                })
                .unwrap();
        }
        let now = Utc::now();
        storage
            .connection
            .execute(
                "UPDATE strategies SET state = 'running', last_evaluated_bar = ?,
                    last_error = 'old error' WHERE strategy_id IN (?, ?)",
                params![now, v2_id, classic_id],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET enabled = true, enabled_at = ? WHERE strategy_id IN (?, ?)",
                params![now, v2_id, classic_id],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "UPDATE strategy_runtime_states SET state_version = 2,
                    state_json = '{\"legacy\":true}', revision = 7,
                    last_transition_bar = ? WHERE strategy_id IN (?, ?)",
                params![now, v2_id, classic_id],
            )
            .unwrap();
        for strategy_id in [v2_id, classic_id] {
            storage
                .connection
                .execute(
                    "INSERT INTO strategy_execution_desired_targets
                     (desired_target_id, strategy_id, source_evaluation_id, signal,
                      targets_json, state, requires_flatten, flatten_completed_at,
                      superseded_by_evaluation_id, detail, last_attempt_at,
                      next_attempt_at, created_at, updated_at)
                     VALUES (?, ?, ?, 'buy', '[]', 'active', false, NULL,
                             NULL, NULL, NULL, NULL, ?, ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        uuid::Uuid::now_v7(),
                        now,
                        now
                    ],
                )
                .unwrap();
        }

        // Re-run the real migration against a populated pre-v3-shaped
        // fixture. This catches SQL that appears safe on a new empty database
        // but fails to pause or revoke an existing V2 strategy.
        storage
            .connection
            .execute("DELETE FROM schema_migrations WHERE version = 37", [])
            .unwrap();
        storage.migrate().unwrap();

        let v2: (
            String,
            Option<DateTime<Utc>>,
            Option<String>,
            bool,
            Option<DateTime<Utc>>,
        ) = storage
            .connection
            .query_row(
                "SELECT s.state, s.last_evaluated_bar, s.last_error,
                            c.enabled, c.enabled_at
                     FROM strategies s JOIN strategy_execution_configs c USING (strategy_id)
                     WHERE s.strategy_id = ?",
                params![v2_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(v2.0, "paused");
        assert!(v2.1.is_none());
        assert!(v2.2.is_some_and(|detail| detail.contains("explicitly resume/re-enable")));
        assert!(!v2.3);
        assert!(v2.4.is_none());
        let v2_runtime: (i64, String, i64, Option<DateTime<Utc>>) = storage
            .connection
            .query_row(
                "SELECT state_version, state_json::VARCHAR, revision, last_transition_bar
                 FROM strategy_runtime_states WHERE strategy_id = ?",
                params![v2_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(v2_runtime, (3, "{}".into(), 8, None));
        let v2_target: (String, Option<String>) = storage
            .connection
            .query_row(
                "SELECT state, detail FROM strategy_execution_desired_targets
                 WHERE strategy_id = ?",
                params![v2_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(v2_target.0, "cancelled");
        assert!(
            v2_target
                .1
                .is_some_and(|detail| detail.contains("state v3"))
        );

        let classic: (String, bool, i64, String, String) = storage
            .connection
            .query_row(
                "SELECT s.state, c.enabled, r.state_version, r.state_json::VARCHAR, d.state
                 FROM strategies s
                 JOIN strategy_execution_configs c USING (strategy_id)
                 JOIN strategy_runtime_states r USING (strategy_id)
                 JOIN strategy_execution_desired_targets d USING (strategy_id)
                 WHERE s.strategy_id = ?",
                params![classic_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            classic,
            (
                "running".into(),
                true,
                2,
                "{\"legacy\":true}".into(),
                "active".into()
            )
        );
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

    fn test_cost_model(fixed_fee: f64, slippage_bps: f64) -> ExecutionCostModelInput {
        ExecutionCostModelInput {
            cost_model_id: None,
            name: format!("test-cost-{fixed_fee}-{slippage_bps}"),
            currency: "USD".into(),
            buy_fixed_fee: fixed_fee,
            buy_per_share_fee: 0.0,
            buy_rate_bps: 0.0,
            buy_min_fee: 0.0,
            sell_fixed_fee: fixed_fee,
            sell_per_share_fee: 0.0,
            sell_rate_bps: 0.0,
            sell_min_fee: 0.0,
            sell_tax_bps: 0.0,
            estimated_spread_bps: 0.0,
            estimated_slippage_bps: slippage_bps,
        }
    }

    fn fees_only_gate() -> BacktestCostGateSnapshot {
        BacktestCostGateSnapshot::fees_only(BacktestCostGateMode::FeesOnly)
    }

    fn matching_gate(
        minimum_cost_multiple: f64,
        maximum_ratio: f64,
        minimum_completed_trades: usize,
        actual_fee_bps_p90: Option<f64>,
    ) -> BacktestCostGateSnapshot {
        BacktestCostGateSnapshot {
            mode: BacktestCostGateMode::MatchStrategy,
            strategy_control_enabled: Some(true),
            applied: true,
            minimum_cost_multiple: Some(minimum_cost_multiple),
            maximum_commission_to_gross_profit_ratio: Some(maximum_ratio),
            minimum_completed_trades: Some(minimum_completed_trades),
            actual_fee_bps_p90,
            statistics_baseline: "backtest_start",
            scope: "test",
        }
    }

    fn simulated_bar(start: DateTime<Utc>, index: usize, close: f64) -> BacktestBar {
        BacktestBar {
            open_time: start + chrono::Duration::minutes(index as i64),
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
        }
    }

    fn close_threshold_backtest_request(
        start: DateTime<Utc>,
        allow_short: bool,
    ) -> BacktestRequest {
        BacktestRequest {
            strategy_id: None,
            cost_model_id: None,
            cost_gate_mode: None,
            conid: 756733,
            timeframe: "1m".into(),
            start,
            end: start + chrono::Duration::minutes(5),
            short_window: None,
            long_window: None,
            strategy_kind: "close_threshold".into(),
            strategy_config: Some(serde_json::json!({
                "conid": 756733,
                "buy_below": 10.0,
                "sell_above": 20.0
            })),
            quantity: 10.0,
            short_target_quantity: if allow_short { -4.0 } else { 0.0 },
            allow_short,
            initial_cash: 10_000.0,
            outside_rth: false,
            seed: 7,
        }
    }

    fn insert_test_cost_model(
        storage: &mut Storage,
        fixed_fee: f64,
        slippage_bps: f64,
    ) -> uuid::Uuid {
        storage
            .upsert_execution_cost_model(&test_cost_model(fixed_fee, slippage_bps))
            .unwrap()
    }

    fn mark_backfill_range_verified(
        storage: &mut Storage,
        timeframe: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        outside_rth: bool,
    ) {
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
            timeframe: timeframe.into(),
            start,
            end,
            outside_rth,
            fx_rate_pair: None,
        };
        let job_id = storage.create_backfill_job(&request).unwrap().job_id;
        storage.advance_backfill_job(job_id, end, end).unwrap();
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
    fn completed_backfill_verifies_a_range_despite_nontrading_file_gaps() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let lake = directory.path().join("lake");
        let staging = directory.path().join("staging");
        let start = "2026-07-31T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let end = "2026-08-04T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        // Friday and Monday live in separate fragments. The wall-clock gap is
        // expected (weekend), and a completed IBKR request proves it was not a
        // silently omitted data range.
        storage
            .write_historical_bars(&lake, &staging, &[test_bar(start, 100.0)])
            .unwrap();
        storage
            .write_historical_bars(
                &lake,
                &staging,
                &[test_bar(start + chrono::Duration::days(3), 101.0)],
            )
            .unwrap();
        mark_backfill_range_verified(&mut storage, "1d", start, end, false);

        let coverage = storage
            .historical_coverage_for_session(756733, "1d", start, end, false)
            .unwrap();
        assert_eq!(coverage["raw_covered"], false);
        assert_eq!(coverage["covered"], true);
        assert_eq!(coverage["verified"], true);
        assert_eq!(coverage["backtest_ready"], true);
        assert_eq!(coverage["coverage_basis"], "successful_backfill_ranges");
        assert!(coverage["verified_gaps"].as_array().unwrap().is_empty());
        assert_eq!(coverage["first_bar_time"], serde_json::json!(start));
        assert_eq!(
            coverage["last_bar_time"],
            serde_json::json!(start + chrono::Duration::days(3))
        );
    }

    #[test]
    fn partial_backfill_never_makes_a_longer_range_runnable() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let lake = directory.path().join("lake");
        let staging = directory.path().join("staging");
        let start = "2025-07-26T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let end = "2026-08-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let downloaded_end = start + chrono::Duration::days(5);
        let bars = (0..5)
            .map(|day| test_bar(start + chrono::Duration::days(day), 100.0 + day as f64))
            .collect::<Vec<_>>();
        storage
            .write_historical_bars(&lake, &staging, &bars)
            .unwrap();
        let job = BackfillJobRequest {
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
            timeframe: "1d".into(),
            start,
            end,
            outside_rth: false,
            fx_rate_pair: None,
        };
        let job_id = storage.create_backfill_job(&job).unwrap().job_id;
        storage
            .advance_backfill_job(job_id, downloaded_end, end)
            .unwrap();

        let coverage = storage
            .historical_coverage_for_session(756733, "1d", start, end, false)
            .unwrap();
        assert_eq!(coverage["covered"], false);
        assert_eq!(coverage["verified"], false);
        assert_eq!(coverage["backtest_ready"], false);
        assert_eq!(
            coverage["fetched_ranges"][0]["end"],
            serde_json::json!(downloaded_end)
        );
        assert_eq!(
            coverage["verified_gaps"][0]["start"],
            serde_json::json!(downloaded_end)
        );

        let cost_model_id = insert_test_cost_model(&mut storage, 0.0, 0.0);
        let request = BacktestRequest {
            strategy_id: None,
            cost_model_id: Some(cost_model_id),
            cost_gate_mode: None,
            conid: 756733,
            timeframe: "1d".into(),
            start,
            end,
            short_window: Some(2),
            long_window: Some(3),
            strategy_kind: "moving_average_cross".into(),
            strategy_config: None,
            quantity: 1.0,
            short_target_quantity: 0.0,
            allow_short: false,
            initial_cash: 100_000.0,
            outside_rth: false,
            seed: 1,
        };
        let error = storage
            .run_moving_average_backtest(&lake, &request)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("historical download has not successfully fetched")
        );
        assert!(storage.list_backtests().unwrap().is_empty());
    }

    #[test]
    fn adjacent_backfills_merge_without_crossing_session_scopes() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let lake = directory.path().join("lake");
        let staging = directory.path().join("staging");
        let start = "2026-07-31T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let middle = start + chrono::Duration::days(1);
        let end = middle + chrono::Duration::days(1);
        storage
            .write_historical_bars(&lake, &staging, &[test_bar(start, 100.0)])
            .unwrap();
        mark_backfill_range_verified(&mut storage, "1d", start, middle, false);
        mark_backfill_range_verified(&mut storage, "1d", middle, end, false);

        let regular = storage
            .historical_coverage_for_session(756733, "1d", start, end, false)
            .unwrap();
        assert_eq!(regular["verified"], true);
        assert_eq!(regular["backtest_ready"], true);
        assert_eq!(regular["verified_ranges"].as_array().unwrap().len(), 1);

        let extended = storage
            .historical_coverage_for_session(756733, "1d", start, end, true)
            .unwrap();
        assert_eq!(extended["verified"], false);
        assert_eq!(extended["backtest_ready"], false);
        assert_eq!(extended["session_kind"], "extended");
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
        mark_backfill_range_verified(
            &mut storage,
            "1d",
            start,
            start + chrono::Duration::days(3),
            false,
        );
        let cost_model_id = insert_test_cost_model(&mut storage, 0.0, 0.0);
        let request = BacktestRequest {
            strategy_id: None,
            cost_model_id: Some(cost_model_id),
            cost_gate_mode: None,
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
            short_target_quantity: 0.0,
            allow_short: false,
            initial_cash: 100.0,
            outside_rth: false,
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
        mark_backfill_range_verified(
            &mut storage,
            "1d",
            start,
            start + chrono::Duration::days(5),
            false,
        );
        let cost_model_id = insert_test_cost_model(&mut storage, 1.0, 0.0);
        let request = BacktestRequest {
            strategy_id: None,
            cost_model_id: Some(cost_model_id),
            cost_gate_mode: None,
            conid: 756733,
            timeframe: "1d".into(),
            start,
            end: start + chrono::Duration::days(5),
            short_window: Some(2),
            long_window: Some(3),
            strategy_kind: "moving_average_cross".into(),
            strategy_config: None,
            quantity: 1.0,
            short_target_quantity: 0.0,
            allow_short: false,
            initial_cash: 100.0,
            outside_rth: false,
            seed: 1,
        };

        let result = storage
            .run_moving_average_backtest(&lake, &request)
            .unwrap();
        let backtest_id = uuid::Uuid::parse_str(result["backtest_id"].as_str().unwrap()).unwrap();
        let details = storage.backtest_details(backtest_id).unwrap().unwrap();
        assert_eq!(details["state"], "completed");
        assert_eq!(details["metrics"]["bar_count"], 5);
        assert_eq!(details["metrics"]["total_commission"], 1.0);
        assert_eq!(details["metrics"]["total_spread"], 0.0);
        assert_eq!(
            details["parameters"]["cost_model_source"],
            "explicit_cost_model"
        );
        assert_eq!(details["parameters"]["cost_model"]["name"], "test-cost-1-0");
        assert_eq!(details["equity"].as_array().unwrap().len(), 5);
        assert_eq!(details["trades"].as_array().unwrap().len(), 1);
        assert_eq!(details["trades"][0]["spread"], 0.0);

        for index in 0..200 {
            storage
                .connection
                .execute(
                    "INSERT INTO backtest_equity VALUES (?, ?, ?, ?, ?, ?)",
                    params![
                        backtest_id,
                        start + chrono::Duration::days(10) + chrono::Duration::seconds(index),
                        100.0,
                        0.0,
                        1.0,
                        100.0 + index as f64,
                    ],
                )
                .unwrap();
        }
        let compact = storage
            .backtest_details_with_options(
                backtest_id,
                BacktestDetailOptions {
                    trade_page: 1,
                    trade_page_size: 1,
                    max_equity_points: 100,
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(compact["trades_page"]["total_items"], 1);
        assert_eq!(compact["equity_sampling"]["total_points"], 205);
        assert_eq!(compact["equity_sampling"]["downsampled"], true);
        assert!(compact["equity"].as_array().unwrap().len() <= 101);
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
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 25.0,
                short_target_quantity: -10.0,
                allow_short: true,
                order_type: "limit".into(),
                paper_only: true,
                outside_rth: true,
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
            })
            .unwrap();
        let start = Utc::now();
        let request = BacktestRequest {
            strategy_id: Some(strategy_id),
            cost_model_id: None,
            cost_gate_mode: None,
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
            short_target_quantity: 0.0,
            allow_short: false,
            initial_cash: 100_000.0,
            outside_rth: false,
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
        assert_eq!(resolved.quantity, 25.0);
        assert_eq!(resolved.short_target_quantity, -10.0);
        assert!(resolved.allow_short);
        assert!(resolved.outside_rth);
    }

    #[test]
    fn strategy_list_exposes_engine_metadata_and_portfolio_backtests_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let config = serde_json::json!({
            "conid": 756733,
            "short_window": 5,
            "long_window": 20,
            "bar_timeframe": "5s"
        });
        let implementation =
            crate::strategy::build("moving_average_cross_v2", config.clone()).unwrap();
        let expected_timeframe = implementation.bar_timeframe().to_owned();
        let expected_minimum_history = implementation.minimum_history() as u64;
        let strategy_id = storage
            .create_strategy("portfolio metadata", "moving_average_cross_v2", &config)
            .unwrap();

        let listed = storage
            .list_strategies()
            .unwrap()
            .into_iter()
            .find(|strategy| strategy["strategy_id"] == strategy_id.to_string())
            .unwrap();
        assert_eq!(listed["bar_timeframe"], expected_timeframe);
        assert_eq!(
            listed["minimum_history"].as_u64(),
            Some(expected_minimum_history)
        );
        assert_eq!(listed["is_portfolio"], false);

        storage
            .configure_strategy_portfolio_execution_with_capital_currency(
                &StrategyPortfolioExecutionConfig {
                    strategy_id,
                    account: "DU123".into(),
                    order_type: "limit".into(),
                    paper_only: true,
                    outside_rth: true,
                    legs: vec![StrategyExecutionLegConfig {
                        contract: spy_contract(),
                        buy_target_quantity: 25.0,
                        sell_target_quantity: -10.0,
                    }],
                },
                "USD",
            )
            .unwrap();

        let listed = storage
            .list_strategies()
            .unwrap()
            .into_iter()
            .find(|strategy| strategy["strategy_id"] == strategy_id.to_string())
            .unwrap();
        assert_eq!(listed["is_portfolio"], true);

        let start = Utc::now();
        let request = BacktestRequest {
            strategy_id: Some(strategy_id),
            cost_model_id: None,
            cost_gate_mode: None,
            conid: 756733,
            timeframe: "5s".into(),
            start,
            end: start + chrono::Duration::minutes(1),
            short_window: None,
            long_window: None,
            strategy_kind: "moving_average_cross_v2".into(),
            strategy_config: Some(config),
            quantity: 25.0,
            short_target_quantity: -10.0,
            allow_short: true,
            initial_cash: 100_000.0,
            outside_rth: true,
            seed: 42,
        };
        let error = storage.resolve_backtest_request(&request).unwrap_err();
        assert!(error.to_string().contains("only single-leg execution"));
        assert!(error.to_string().contains("portfolio"));
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
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU-HKD".into(),
                target_quantity: 100.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: crate::ibkr::ContractCandidate {
                    conid: 272093,
                    symbol: "TEST".into(),
                    security_type: "STK".into(),
                    currency: "HKD".into(),
                    exchange: "SEHK".into(),
                    primary_exchange: "SEHK".into(),
                    local_symbol: "TEST".into(),
                    description: String::new(),
                    derivative_security_types: Vec::new(),
                },
            })
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
        let start = Utc::now();
        let context = storage
            .resolve_backtest_cost_context(&BacktestRequest {
                strategy_id: Some(strategy_id),
                cost_model_id: None,
                cost_gate_mode: None,
                conid: 272093,
                timeframe: "1m".into(),
                start,
                end: start + chrono::Duration::days(1),
                short_window: None,
                long_window: None,
                strategy_kind: "moving_average_cross".into(),
                strategy_config: None,
                quantity: 100.0,
                short_target_quantity: 0.0,
                allow_short: false,
                initial_cash: 100_000.0,
                outside_rth: false,
                seed: 1,
            })
            .unwrap();
        assert_eq!(context.model_source, "strategy_cost_control");
        assert_eq!(context.model.cost_model_id, Some(model_id));
        assert_eq!(context.model.estimated_slippage_bps, 3.0);
        assert_eq!(context.gate.mode, BacktestCostGateMode::MatchStrategy);
        assert!(context.gate.applied);
        assert_eq!(context.gate.minimum_cost_multiple, Some(2.0));
        assert_eq!(
            context.gate.maximum_commission_to_gross_profit_ratio,
            Some(0.5)
        );
        assert_eq!(context.gate.minimum_completed_trades, Some(5));
        assert!(storage.delete_execution_cost_model(model_id).is_err());
    }

    #[test]
    fn execution_and_cost_configuration_reject_currency_drift() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let contract = spy_contract();
        storage.upsert_instrument(&contract).unwrap();
        let strategy_id = storage
            .create_strategy(
                "currency-safe",
                "moving_average_cross",
                &serde_json::json!({"conid": contract.conid, "short_window": 2, "long_window": 3}),
            )
            .unwrap();
        let mut wrong_contract = contract.clone();
        wrong_contract.currency = "EUR".into();
        let config = |contract| StrategyExecutionConfig {
            strategy_id,
            account: "DU123".into(),
            target_quantity: 10.0,
            short_target_quantity: 0.0,
            allow_short: false,
            order_type: "market".into(),
            paper_only: true,
            outside_rth: false,
            contract,
        };
        assert!(
            storage
                .configure_strategy_execution(&config(wrong_contract))
                .unwrap_err()
                .to_string()
                .contains("canonical instrument currency")
        );
        storage
            .configure_strategy_execution(&config(contract))
            .unwrap();
        let mut model = test_cost_model(1.0, 0.0);
        model.name = "wrong-currency".into();
        model.currency = "EUR".into();
        let model_id = storage.upsert_execution_cost_model(&model).unwrap();
        let error = storage
            .configure_strategy_cost_control(&StrategyCostControlInput {
                strategy_id,
                enabled: true,
                cost_model_id: model_id,
                minimum_cost_multiple: 1.0,
                maximum_commission_to_gross_profit_ratio: 0.5,
                minimum_completed_trades: 1,
            })
            .unwrap_err();
        assert!(error.to_string().contains("cost model currency"));
    }

    #[test]
    fn portfolio_short_capability_checks_negative_buy_targets_on_every_leg() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let first = spy_contract();
        let strategy_id = storage
            .create_strategy(
                "long-only-portfolio",
                "paper_round_trip",
                &serde_json::json!({"conid": first.conid, "phase_bars": 1}),
            )
            .unwrap();
        let mut second = first.clone();
        second.conid += 1;
        second.symbol = "SECOND".into();
        let error = storage
            .configure_strategy_portfolio_execution_with_capital_currency(
                &StrategyPortfolioExecutionConfig {
                    strategy_id,
                    account: "DU123".into(),
                    order_type: "market".into(),
                    paper_only: true,
                    outside_rth: false,
                    legs: vec![
                        StrategyExecutionLegConfig {
                            contract: first,
                            buy_target_quantity: 1.0,
                            sell_target_quantity: 0.0,
                        },
                        StrategyExecutionLegConfig {
                            contract: second,
                            buy_target_quantity: -1.0,
                            sell_target_quantity: 0.0,
                        },
                    ],
                },
                "USD",
            )
            .unwrap_err();
        assert!(error.to_string().contains("negative portfolio buy or sell"));
    }

    #[test]
    fn running_strategy_cost_control_requires_pausing_before_save() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let model_id = storage
            .upsert_execution_cost_model(&ExecutionCostModelInput {
                cost_model_id: None,
                name: "test-fees".into(),
                currency: "USD".into(),
                buy_fixed_fee: 0.0,
                buy_per_share_fee: 0.0,
                buy_rate_bps: 0.0,
                buy_min_fee: 0.0,
                sell_fixed_fee: 0.0,
                sell_per_share_fee: 0.0,
                sell_rate_bps: 0.0,
                sell_min_fee: 0.0,
                sell_tax_bps: 0.0,
                estimated_spread_bps: 0.0,
                estimated_slippage_bps: 0.0,
            })
            .unwrap();
        let strategy_id = storage
            .create_strategy(
                "running-cost-control",
                "moving_average_cross",
                &serde_json::json!({
                    "conid": 272093,
                    "short_window": 5,
                    "long_window": 20
                }),
            )
            .unwrap();
        let input = StrategyCostControlInput {
            strategy_id,
            enabled: true,
            cost_model_id: model_id,
            minimum_cost_multiple: 2.0,
            maximum_commission_to_gross_profit_ratio: 0.5,
            minimum_completed_trades: 5,
        };

        storage.set_strategy_state(strategy_id, "running").unwrap();
        let error = storage
            .configure_strategy_cost_control(&input)
            .unwrap_err()
            .to_string();
        assert!(error.contains("请先暂停策略"));
        storage.set_strategy_state(strategy_id, "paused").unwrap();
        storage.configure_strategy_cost_control(&input).unwrap();

        let now = Utc::now();
        let action_id = uuid::Uuid::now_v7();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_execution_actions
                 (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                  state, created_at, updated_at)
                 VALUES (?, ?, ?, ?, 'buy', 'processing', ?, ?)",
                params![
                    action_id,
                    strategy_id,
                    uuid::Uuid::now_v7(),
                    format!("processing-control-edit-{action_id}"),
                    now,
                    now
                ],
            )
            .unwrap();
        let cost_error = storage
            .configure_strategy_cost_control(&input)
            .unwrap_err()
            .to_string();
        assert!(cost_error.contains("处理中动作"));

        let risk_input = StrategyRiskControlInput {
            strategy_id,
            enabled: true,
            strategy_capital: 100_000.0,
            capital_currency: Some("USD".into()),
            maximum_position_capital_ratio: 1.0,
            maximum_rolling_24h_realized_net_loss_ratio: 0.02,
            maximum_consecutive_net_losing_trades: 3,
            maximum_rolling_24h_completed_trades: 10,
            maximum_rolling_24h_turnover_capital_ratio: 10.0,
        };
        let risk_error = storage
            .configure_strategy_risk_control(&risk_input)
            .unwrap_err()
            .to_string();
        assert!(risk_error.contains("处理中动作"));

        storage
            .connection
            .execute(
                "UPDATE strategy_execution_actions SET state = 'failed' WHERE action_id = ?",
                params![action_id],
            )
            .unwrap();
        storage.configure_strategy_cost_control(&input).unwrap();
        storage
            .configure_strategy_risk_control(&risk_input)
            .unwrap();
    }

    #[test]
    fn completed_position_snapshot_refreshes_absent_positions_to_zero() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let old = Utc::now() - chrono::Duration::hours(1);
        storage
            .upsert_position(&crate::ibkr::PositionSnapshot {
                account: "DU123".into(),
                conid: 756733,
                symbol: "SPY".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "ARCA".into(),
                quantity: 2.0,
                average_cost: 500.0,
                observed_at: old,
            })
            .unwrap();
        let subscription_id = uuid::Uuid::now_v7();
        let started = Utc::now();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: started,
            })
            .unwrap();
        assert_eq!(storage.list_positions().unwrap()[0]["quantity"], 0.0);
        let completed = started + chrono::Duration::milliseconds(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
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
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
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

        let refreshed_at =
            now + chrono::Duration::seconds(config.max_account_data_age_seconds as i64 + 1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSubscriptionHeartbeat {
                subscription_id,
                observed_at: refreshed_at,
            })
            .unwrap();
        // PnL has an independent freshness requirement. Refresh it here so the
        // assertion isolates the position-subscription lease.
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Pnl {
                account: "DU123".into(),
                daily_pnl: 0.0,
                unrealized_pnl: Some(0.0),
                realized_pnl: Some(0.0),
                observed_at: refreshed_at,
            })
            .unwrap();
        let refreshed = storage
            .evaluate_portfolio_risk(
                &config,
                "DU123",
                &request,
                Some(100.0),
                Some(100.0),
                false,
                refreshed_at,
            )
            .unwrap();
        assert!(refreshed.allowed, "{refreshed:?}");
        assert_eq!(
            refreshed
                .positions_observed_at
                .map(|time| time.timestamp_micros()),
            Some(refreshed_at.timestamp_micros())
        );
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
    fn incomplete_live_bar_window_is_a_quiet_wait_not_a_strategy_error() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "gapped five second crossover",
                "moving_average_cross_5s",
                &serde_json::json!({"conid": 756733, "short_window": 2, "long_window": 3}),
            )
            .unwrap();
        storage.set_strategy_state(strategy_id, "running").unwrap();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for (offset, close) in [(0, 3.0), (5, 2.0), (15, 1.0), (20, 4.0)] {
            let time = start + chrono::Duration::seconds(offset);
            storage
                .connection
                .execute(
                    "INSERT INTO market_five_second_bars
                     VALUES (756733, ?, ?, ?, ?, ?, 1, true, ?)",
                    params![time, close, close, close, close, time],
                )
                .unwrap();
        }
        storage
            .connection
            .execute(
                "UPDATE strategies SET last_error =
                 'moving_average_cross_v2 requires 25 contiguous 5s Bars; waiting for gaps in live market data to refill'
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();

        assert_eq!(storage.evaluate_running_strategies().unwrap(), 0);
        let strategy = storage
            .list_strategies()
            .unwrap()
            .into_iter()
            .find(|strategy| strategy["strategy_id"] == strategy_id.to_string())
            .unwrap();
        assert!(strategy["last_error"].is_null());
        assert!(
            storage
                .list_strategy_evaluations(strategy_id, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn strategy_execution_claims_each_signal_once_and_targets_position() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
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
    fn scalar_execution_replaces_portfolio_legs_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "portfolio to scalar",
                "close_threshold",
                &serde_json::json!({
                    "conid": 756733,
                    "buy_below": 100.0,
                    "sell_above": 200.0
                }),
            )
            .unwrap();
        let mut second = spy_contract();
        second.conid = 272093;
        second.symbol = "MSFT".into();
        second.local_symbol = "MSFT".into();
        second.primary_exchange = "NASDAQ".into();
        storage
            .configure_strategy_portfolio_execution_with_capital_currency(
                &StrategyPortfolioExecutionConfig {
                    strategy_id,
                    account: "DU123".into(),
                    order_type: "market".into(),
                    paper_only: true,
                    outside_rth: false,
                    legs: vec![
                        StrategyExecutionLegConfig {
                            contract: spy_contract(),
                            buy_target_quantity: 3.0,
                            sell_target_quantity: 0.0,
                        },
                        StrategyExecutionLegConfig {
                            contract: second,
                            buy_target_quantity: 2.0,
                            sell_target_quantity: 0.0,
                        },
                    ],
                },
                "USD",
            )
            .unwrap();
        let portfolio_leg_count: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_portfolio_legs WHERE strategy_id = ?",
                params![strategy_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(portfolio_leg_count, 2);

        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 5.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: spy_contract(),
            })
            .unwrap();
        let portfolio_leg_count: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_portfolio_legs WHERE strategy_id = ?",
                params![strategy_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(portfolio_leg_count, 0);
        let target: f64 = storage
            .connection
            .query_row(
                "SELECT target_quantity FROM strategy_execution_configs WHERE strategy_id = ?",
                params![strategy_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(target, 5.0);
    }

    #[test]
    fn portfolio_execution_rejects_duplicate_contracts_without_mutating_config() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "duplicate portfolio contract",
                "close_threshold",
                &serde_json::json!({
                    "conid": 756733,
                    "buy_below": 100.0,
                    "sell_above": 200.0
                }),
            )
            .unwrap();
        let error = storage
            .configure_strategy_portfolio_execution_with_capital_currency(
                &StrategyPortfolioExecutionConfig {
                    strategy_id,
                    account: "DU123".into(),
                    order_type: "market".into(),
                    paper_only: true,
                    outside_rth: false,
                    legs: vec![
                        StrategyExecutionLegConfig {
                            contract: spy_contract(),
                            buy_target_quantity: 3.0,
                            sell_target_quantity: 0.0,
                        },
                        StrategyExecutionLegConfig {
                            contract: spy_contract(),
                            buy_target_quantity: 2.0,
                            sell_target_quantity: 0.0,
                        },
                    ],
                },
                "USD",
            )
            .unwrap_err();
        assert!(error.to_string().contains("duplicate conid 756733"));
        let config_count: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_configs WHERE strategy_id = ?",
                params![strategy_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(config_count, 0);
    }

    #[test]
    fn execution_configuration_enforces_strategy_short_target_capability() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "long only bollinger",
                "bollinger_rsi_mean_reversion",
                &serde_json::json!({"conid": 756733}),
            )
            .unwrap();
        let scalar_error = storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 3.0,
                short_target_quantity: -2.0,
                allow_short: true,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: spy_contract(),
            })
            .unwrap_err();
        assert!(
            scalar_error
                .to_string()
                .contains("does not support short targets")
        );

        let mut second = spy_contract();
        second.conid = 272093;
        second.symbol = "MSFT".into();
        second.local_symbol = "MSFT".into();
        let portfolio_error = storage
            .configure_strategy_portfolio_execution_with_capital_currency(
                &StrategyPortfolioExecutionConfig {
                    strategy_id,
                    account: "DU123".into(),
                    order_type: "market".into(),
                    paper_only: true,
                    outside_rth: false,
                    legs: vec![
                        StrategyExecutionLegConfig {
                            contract: spy_contract(),
                            buy_target_quantity: 3.0,
                            sell_target_quantity: 0.0,
                        },
                        StrategyExecutionLegConfig {
                            contract: second,
                            buy_target_quantity: 2.0,
                            sell_target_quantity: -1.0,
                        },
                    ],
                },
                "USD",
            )
            .unwrap_err();
        assert!(
            portfolio_error
                .to_string()
                .contains("does not support negative portfolio buy or sell targets")
        );
    }

    #[test]
    fn enabled_strategies_cannot_share_an_account_position() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let mut strategy_ids = Vec::new();
        for name in ["owner one", "owner two"] {
            let strategy_id = storage
                .create_strategy(
                    name,
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
                    contract: spy_contract(),
                })
                .unwrap();
            strategy_ids.push(strategy_id);
        }
        assert!(
            storage
                .set_strategy_execution_enabled(strategy_ids[0], true)
                .unwrap()
        );
        let error = storage
            .set_strategy_execution_enabled(strategy_ids[1], true)
            .unwrap_err();
        assert!(error.to_string().contains("already controlled"));
        assert!(error.to_string().contains("owner one"));
        assert_eq!(
            storage
                .list_strategy_execution_configs()
                .unwrap()
                .into_iter()
                .filter(|config| config["enabled"] == true)
                .count(),
            1
        );
    }

    #[test]
    fn pre_submission_authorization_is_revoked_when_execution_is_disabled() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let action = storage.claim_strategy_action().unwrap().unwrap();
        storage
            .ensure_strategy_action_leg_submission_authorized(
                action.action_id,
                action.legs[0].leg_index,
                &action.account,
                &action.legs[0].contract,
            )
            .unwrap();

        assert!(
            storage
                .set_strategy_execution_enabled(strategy_id, false)
                .unwrap()
        );
        let error = storage
            .ensure_strategy_action_leg_submission_authorized(
                action.action_id,
                action.legs[0].leg_index,
                &action.account,
                &action.legs[0].contract,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("automatic execution was disabled")
        );
    }

    #[test]
    fn pre_submission_authorization_rejects_superseded_targets_and_changed_contracts() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let action = storage.claim_strategy_action().unwrap().unwrap();
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_desired_targets
                 SET state = 'superseded' WHERE strategy_id = ? AND state = 'active'",
                params![strategy_id],
            )
            .unwrap();
        let superseded = storage
            .ensure_strategy_action_leg_submission_authorized(
                action.action_id,
                action.legs[0].leg_index,
                &action.account,
                &action.legs[0].contract,
            )
            .unwrap_err();
        assert!(
            superseded
                .to_string()
                .contains("source target was superseded")
        );

        storage
            .connection
            .execute(
                "UPDATE strategy_execution_desired_targets
                 SET state = 'active' WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let mut changed_contract = spy_contract();
        changed_contract.exchange = "ARCA".into();
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs SET contract_json = ? WHERE strategy_id = ?",
                params![
                    serde_json::to_string(&changed_contract).unwrap(),
                    strategy_id
                ],
            )
            .unwrap();
        let changed = storage
            .ensure_strategy_action_leg_submission_authorized(
                action.action_id,
                action.legs[0].leg_index,
                &action.account,
                &action.legs[0].contract,
            )
            .unwrap_err();
        assert!(changed.to_string().contains("configured contract changed"));
    }

    #[test]
    fn atomic_strategy_submission_authorization_rejects_a_changed_position_delta() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let action = storage.claim_strategy_action().unwrap().unwrap();
        let leg = &action.legs[0];
        let provenance = quant_rpc_types::StrategyOrderProvenance {
            strategy_id,
            action_id: action.action_id,
            leg_index: leg.leg_index,
            source_evaluation_id: action.source_evaluation_id,
            target_quantity: leg.target_quantity,
            claimed_current_quantity: leg.current_quantity,
            side: leg.side.clone(),
            quantity: leg.quantity,
        };
        let request = crate::ibkr::BrokerOrderRequest {
            contract: leg.contract.clone(),
            side: leg.side.clone(),
            quantity: leg.quantity,
            order_type: action.order_type.clone(),
            limit_price: None,
            outside_rth: action.outside_rth,
        };
        storage
            .ensure_strategy_order_submission_authorized(
                &provenance,
                &leg.idempotency_key,
                &action.account,
                &request,
            )
            .unwrap();

        // A manual/external fill reaches the target during asynchronous
        // preflight. Submitting the claimed Buy 3 now would overshoot to 6.
        complete_spy_position_snapshot(&mut storage, 3.0, Utc::now());
        let error = storage
            .ensure_strategy_order_submission_authorized(
                &provenance,
                &leg.idempotency_key,
                &action.account,
                &request,
            )
            .unwrap_err();
        assert!(error.to_string().contains("position changed after claim"));
        let intent_count: i64 = storage
            .connection
            .query_row("SELECT count(*) FROM order_intents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(intent_count, 0);
    }

    #[test]
    fn atomic_strategy_submission_authorization_rejects_tampered_order_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let action = storage.claim_strategy_action().unwrap().unwrap();
        let leg = &action.legs[0];
        let provenance = quant_rpc_types::StrategyOrderProvenance {
            strategy_id,
            action_id: action.action_id,
            leg_index: leg.leg_index,
            source_evaluation_id: action.source_evaluation_id,
            target_quantity: leg.target_quantity,
            claimed_current_quantity: leg.current_quantity,
            side: leg.side.clone(),
            quantity: leg.quantity,
        };
        let valid = crate::ibkr::BrokerOrderRequest {
            contract: leg.contract.clone(),
            side: leg.side.clone(),
            quantity: leg.quantity,
            order_type: "market".into(),
            limit_price: None,
            outside_rth: false,
        };
        storage
            .ensure_strategy_order_submission_authorized(
                &provenance,
                &leg.idempotency_key,
                &action.account,
                &valid,
            )
            .unwrap();

        let mut changed_type = valid.clone();
        changed_type.order_type = "limit".into();
        changed_type.limit_price = Some(100.0);
        assert!(
            storage
                .ensure_strategy_order_submission_authorized(
                    &provenance,
                    &leg.idempotency_key,
                    &action.account,
                    &changed_type,
                )
                .unwrap_err()
                .to_string()
                .contains("order type")
        );

        let mut injected_limit_price = valid.clone();
        injected_limit_price.limit_price = Some(100.0);
        assert!(
            storage
                .ensure_strategy_order_submission_authorized(
                    &provenance,
                    &leg.idempotency_key,
                    &action.account,
                    &injected_limit_price,
                )
                .unwrap_err()
                .to_string()
                .contains("must not carry a limit price")
        );

        let mut changed_session = valid;
        changed_session.outside_rth = true;
        assert!(
            storage
                .ensure_strategy_order_submission_authorized(
                    &provenance,
                    &leg.idempotency_key,
                    &action.account,
                    &changed_session,
                )
                .unwrap_err()
                .to_string()
                .contains("outside_rth")
        );

        // Exercise the inverse shape as well. Direct SQL deliberately models
        // a corrupted/stale caller independently from the public config API,
        // which normally cancels this action while replacing its config.
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET order_type = 'limit', outside_rth = true
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let mut missing_limit_price = changed_session;
        missing_limit_price.order_type = "limit".into();
        missing_limit_price.limit_price = None;
        assert!(
            storage
                .ensure_strategy_order_submission_authorized(
                    &provenance,
                    &leg.idempotency_key,
                    &action.account,
                    &missing_limit_price,
                )
                .unwrap_err()
                .to_string()
                .contains("finite positive limit price")
        );
        missing_limit_price.limit_price = Some(-1.0);
        assert!(
            storage
                .ensure_strategy_order_submission_authorized(
                    &provenance,
                    &leg.idempotency_key,
                    &action.account,
                    &missing_limit_price,
                )
                .unwrap_err()
                .to_string()
                .contains("finite positive limit price")
        );
    }

    #[test]
    fn strategy_leg_keeps_the_rpc_bound_intent_when_worker_finishes_without_ids() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let action = storage.claim_strategy_action().unwrap().unwrap();
        let leg = &action.legs[0];
        let request = crate::ibkr::BrokerOrderRequest {
            contract: leg.contract.clone(),
            side: leg.side.clone(),
            quantity: leg.quantity,
            order_type: action.order_type.clone(),
            limit_price: None,
            outside_rth: action.outside_rth,
        };
        let intent_id = storage
            .create_order_intent(
                &leg.idempotency_key,
                &action.account,
                &request,
                "approved",
                None,
            )
            .unwrap();
        storage
            .bind_strategy_action_leg_order_intent(action.action_id, leg.leg_index, intent_id)
            .unwrap();

        // This is the worker's error path after the RPC handler has already
        // persisted the intent. None must not erase the crash-recovery link.
        storage
            .finish_strategy_action_leg(
                action.action_id,
                leg.leg_index,
                "failed",
                None,
                None,
                Some("simulated worker interruption"),
            )
            .unwrap();
        storage
            .finish_strategy_action(
                action.action_id,
                "failed",
                None,
                None,
                Some("simulated worker interruption"),
            )
            .unwrap();
        let persisted_intent: Option<uuid::Uuid> = storage
            .connection
            .query_row(
                "SELECT order_intent_id FROM strategy_execution_action_legs
                 WHERE action_id = ? AND leg_index = ?",
                params![action.action_id, leg.leg_index],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted_intent, Some(intent_id));
        let parent_intent: Option<uuid::Uuid> = storage
            .connection
            .query_row(
                "SELECT order_intent_id FROM strategy_execution_actions
                 WHERE action_id = ?",
                params![action.action_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parent_intent, Some(intent_id));
    }

    #[test]
    fn deferred_fresh_candidate_does_not_starve_another_target() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let blocked_strategy_id = configure_spy_execution(&mut storage);

        let second_contract = crate::ibkr::ContractCandidate {
            conid: 756734,
            symbol: "QQQ".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            local_symbol: "QQQ".into(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };
        let second_strategy_id = storage
            .create_strategy(
                "fair target",
                "close_threshold",
                &serde_json::json!({
                    "conid": second_contract.conid,
                    "buy_below": 100.0,
                    "sell_above": 200.0
                }),
            )
            .unwrap();
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id: second_strategy_id,
                account: "DU123".into(),
                target_quantity: 2.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: second_contract,
            })
            .unwrap();
        storage
            .set_strategy_execution_enabled(second_strategy_id, true)
            .unwrap();
        let base = Utc::now();

        // A locally recorded SPY fill newer than the completed position
        // snapshot makes only SPY's evidence incomplete.
        record_attributed_spy_fill(
            &mut storage,
            blocked_strategy_id,
            70_001,
            "buy",
            1.0,
            100.0,
            0.0,
            base,
        );
        insert_signal_evaluation(&mut storage, blocked_strategy_id, "buy", base);
        let second_evaluation_id = uuid::Uuid::now_v7();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756734, ?, 90, 100, 90, 200, 'buy', ?, '{}')",
                params![
                    second_evaluation_id,
                    second_strategy_id,
                    base + chrono::Duration::milliseconds(1),
                    base + chrono::Duration::milliseconds(1)
                ],
            )
            .unwrap();

        let claim_at = base + chrono::Duration::seconds(1);
        assert!(
            storage
                .claim_strategy_action_inner("USD", 3_600, 30, claim_at, false)
                .unwrap()
                .is_none()
        );
        let deferred_until: Option<DateTime<Utc>> = storage
            .connection
            .query_row(
                "SELECT next_attempt_at FROM strategy_execution_desired_targets
                 WHERE strategy_id = ? AND state = 'active'",
                params![blocked_strategy_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(deferred_until.is_some_and(|at| at > claim_at));

        let second = storage
            .claim_strategy_action_inner(
                "USD",
                3_600,
                30,
                claim_at + chrono::Duration::seconds(1),
                false,
            )
            .unwrap()
            .unwrap();
        assert_eq!(second.strategy_id, second_strategy_id);
        assert_eq!(second.source_evaluation_id, second_evaluation_id);
    }

    #[test]
    fn inactive_or_missing_target_submitted_leg_is_scheduled_for_cancel_but_unknown_is_not() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let action = storage.claim_strategy_action().unwrap().unwrap();
        let leg = &action.legs[0];
        let request = crate::ibkr::BrokerOrderRequest {
            contract: leg.contract.clone(),
            side: leg.side.clone(),
            quantity: leg.quantity,
            order_type: action.order_type.clone(),
            limit_price: None,
            outside_rth: action.outside_rth,
        };
        let intent_id = storage
            .create_order_intent(
                &leg.idempotency_key,
                &action.account,
                &request,
                "approved",
                None,
            )
            .unwrap();
        storage
            .record_submitted_order(intent_id, 70_002, uuid::Uuid::now_v7())
            .unwrap();
        storage
            .finish_strategy_action_leg(
                action.action_id,
                leg.leg_index,
                "submitted",
                Some(intent_id),
                Some(70_002),
                None,
            )
            .unwrap();
        storage
            .finish_strategy_action(
                action.action_id,
                "submitted",
                Some(intent_id),
                Some(70_002),
                None,
            )
            .unwrap();
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_desired_targets
                 SET state = 'superseded', updated_at = ?
                 WHERE strategy_id = ? AND state = 'active'",
                params![Utc::now(), strategy_id],
            )
            .unwrap();

        let cancellations = storage.revoked_strategy_order_cancellations().unwrap();
        assert_eq!(cancellations.len(), 1);
        let cancellation = &cancellations[0];
        assert_eq!(cancellation.action_id, action.action_id);
        assert_eq!(cancellation.leg_index, leg.leg_index);
        assert_eq!(cancellation.broker_order_id, 70_002);

        for state in ["cancelled", "expired", "abandoned"] {
            storage
                .connection
                .execute(
                    "UPDATE strategy_execution_desired_targets
                     SET state = ?, updated_at = ? WHERE strategy_id = ?",
                    params![state, Utc::now(), strategy_id],
                )
                .unwrap();
            assert_eq!(
                storage
                    .revoked_strategy_order_cancellations()
                    .unwrap()
                    .first()
                    .unwrap()
                    .broker_order_id,
                70_002
            );
        }

        // Databases upgraded from schemas predating desired targets can have
        // a submitted strategy order with no matching desired row at all.
        // Absence must fail closed exactly like an explicitly revoked target.
        storage
            .connection
            .execute(
                "DELETE FROM strategy_execution_desired_targets WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        assert_eq!(
            storage.revoked_strategy_order_cancellations().unwrap()[0].broker_order_id,
            70_002
        );

        storage
            .mark_order_intent_unknown(intent_id, "test uncertain outcome")
            .unwrap();
        assert!(
            storage
                .revoked_strategy_order_cancellations()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rejected_protective_exit_retries_without_a_new_strategy_evaluation() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let base = Utc::now();
        complete_spy_position_snapshot(&mut storage, 3.0, base);
        let source_evaluation_id =
            insert_signal_evaluation(&mut storage, strategy_id, "sell", base);
        let first_at = base + chrono::Duration::seconds(1);
        let first = storage
            .claim_strategy_action_inner("USD", 3_600, 30, first_at, false)
            .unwrap()
            .unwrap();
        assert_eq!(first.side, "sell");
        assert_eq!(first.quantity, 3.0);
        storage
            .finish_strategy_action(first.action_id, "rejected", None, None, Some("closed"))
            .unwrap();
        storage
            .finish_strategy_action_leg(first.action_id, 0, "rejected", None, None, Some("closed"))
            .unwrap();

        let retry_at = first_at + chrono::Duration::seconds(DESIRED_TARGET_RETRY_DELAY_SECONDS + 1);
        let retry = storage
            .claim_strategy_action_inner("USD", 3_600, 30, retry_at, false)
            .unwrap()
            .unwrap();
        assert_ne!(retry.evaluation_id, source_evaluation_id);
        assert_eq!(retry.side, "sell");
        assert_eq!(retry.quantity, 3.0);
        assert!(retry.idempotency_key.contains("retry-of"));
        assert_eq!(
            storage
                .list_strategy_evaluations(strategy_id, 10)
                .unwrap()
                .len(),
            1
        );
        let retry_row = storage
            .list_strategy_execution_actions(10)
            .unwrap()
            .into_iter()
            .find(|action| action["action_id"] == serde_json::json!(retry.action_id))
            .unwrap();
        assert_eq!(
            retry_row["source_evaluation_id"],
            serde_json::json!(source_evaluation_id)
        );
    }

    #[test]
    fn active_exit_order_defers_retry_without_writing_noise() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let base = Utc::now();
        complete_spy_position_snapshot(&mut storage, 3.0, base);
        insert_signal_evaluation(&mut storage, strategy_id, "sell", base);
        let first_at = base + chrono::Duration::seconds(1);
        let first = storage
            .claim_strategy_action_inner("USD", 3_600, 30, first_at, false)
            .unwrap()
            .unwrap();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "SELL".into(),
            quantity: 3.0,
            order_type: "MKT".into(),
            limit_price: None,
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent(
                "persistent-exit-active",
                "DU123",
                &request,
                "approved",
                None,
            )
            .unwrap();
        storage
            .record_submitted_order(intent_id, 8_001, uuid::Uuid::now_v7())
            .unwrap();
        storage
            .finish_strategy_action(
                first.action_id,
                "submitted",
                Some(intent_id),
                Some(8_001),
                None,
            )
            .unwrap();
        storage
            .finish_strategy_action_leg(
                first.action_id,
                0,
                "submitted",
                Some(intent_id),
                Some(8_001),
                None,
            )
            .unwrap();

        let retry_at = first_at + chrono::Duration::seconds(DESIRED_TARGET_RETRY_DELAY_SECONDS + 1);
        let action_count = storage.list_strategy_execution_actions(20).unwrap().len();
        assert!(
            storage
                .claim_strategy_action_inner("USD", 3_600, 30, retry_at, false)
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .claim_strategy_action_inner(
                    "USD",
                    3_600,
                    30,
                    retry_at + chrono::Duration::minutes(1),
                    false,
                )
                .unwrap()
                .is_none()
        );
        let actions = storage.list_strategy_execution_actions(20).unwrap();
        assert_eq!(actions.len(), action_count);
        assert_eq!(
            storage
                .list_strategy_evaluations(strategy_id, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn long_to_short_reversal_requires_a_fresh_sell_after_flattening() {
        fn configure_short_target(storage: &mut Storage) -> uuid::Uuid {
            let strategy_id = configure_spy_execution(storage);
            storage
                .configure_strategy_execution(&StrategyExecutionConfig {
                    strategy_id,
                    account: "DU123".into(),
                    target_quantity: 3.0,
                    short_target_quantity: -2.0,
                    allow_short: true,
                    order_type: "market".into(),
                    paper_only: true,
                    outside_rth: false,
                    contract: spy_contract(),
                })
                .unwrap();
            storage
                .set_strategy_execution_enabled(strategy_id, true)
                .unwrap();
            strategy_id
        }

        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_short_target(&mut storage);
        let base = Utc::now();
        let subscription_id = complete_spy_position_snapshot(&mut storage, 3.0, base);
        let source_evaluation_id =
            insert_signal_evaluation(&mut storage, strategy_id, "sell", base);
        let first_at = base + chrono::Duration::seconds(1);
        let flatten = storage
            .claim_strategy_action_inner("USD", 3_600, 30, first_at, false)
            .unwrap()
            .unwrap();
        assert_eq!(flatten.side, "sell");
        assert_eq!(flatten.quantity, 3.0);
        assert_eq!(flatten.legs[0].target_quantity, 0.0);
        storage
            .finish_strategy_action(flatten.action_id, "rejected", None, None, Some("test fill"))
            .unwrap();
        let flat_at = first_at + chrono::Duration::seconds(DESIRED_TARGET_RETRY_DELAY_SECONDS + 1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 0.0,
                    average_cost: 0.0,
                    observed_at: flat_at,
                },
            })
            .unwrap();
        // The retry observes flat and completes the protective desired target.
        // It must not reuse the old Sell to open a short position.
        assert!(
            storage
                .claim_strategy_action_inner("USD", 3_600, 30, flat_at, false)
                .unwrap()
                .is_none()
        );
        let desired_state: String = storage
            .connection
            .query_row(
                "SELECT state FROM strategy_execution_desired_targets
                 WHERE source_evaluation_id = ?",
                params![source_evaluation_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(desired_state, "satisfied");
        assert_eq!(
            storage.list_strategy_execution_actions(20).unwrap().len(),
            1
        );

        let fresh_sell_at = flat_at + chrono::Duration::seconds(1);
        let fresh_sell = insert_signal_evaluation(&mut storage, strategy_id, "sell", fresh_sell_at);
        let reverse = storage
            .claim_strategy_action_inner("USD", 3_600, 30, fresh_sell_at, false)
            .unwrap()
            .unwrap();
        assert_eq!(reverse.evaluation_id, fresh_sell);
        assert_eq!(reverse.side, "sell");
        assert_eq!(reverse.quantity, 2.0);
        assert_eq!(reverse.legs[0].target_quantity, -2.0);
    }

    #[test]
    fn opposite_signal_supersedes_a_persistent_exit_target() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let base = Utc::now();
        complete_spy_position_snapshot(&mut storage, 3.0, base);
        let sell_source = insert_signal_evaluation(&mut storage, strategy_id, "sell", base);
        let first = storage
            .claim_strategy_action_inner(
                "USD",
                3_600,
                30,
                base + chrono::Duration::seconds(1),
                false,
            )
            .unwrap()
            .unwrap();
        storage
            .finish_strategy_action(first.action_id, "rejected", None, None, Some("closed"))
            .unwrap();

        let buy_at = base + chrono::Duration::seconds(2);
        let buy_source = insert_signal_evaluation(&mut storage, strategy_id, "buy", buy_at);
        assert!(
            storage
                .claim_strategy_action_inner(
                    "USD",
                    3_600,
                    30,
                    buy_at + chrono::Duration::seconds(1),
                    false,
                )
                .unwrap()
                .is_none()
        );
        let states = {
            let mut statement = storage
                .connection
                .prepare(
                    "SELECT source_evaluation_id, state
                     FROM strategy_execution_desired_targets
                     WHERE strategy_id = ? ORDER BY created_at",
                )
                .unwrap();
            statement
                .query_map(params![strategy_id], |row| {
                    Ok((row.get::<_, uuid::Uuid>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(
            states,
            vec![
                (sell_source, "superseded".into()),
                (buy_source, "satisfied".into())
            ]
        );
        insert_signal_evaluation(
            &mut storage,
            strategy_id,
            "hold",
            buy_at + chrono::Duration::minutes(1),
        );
        assert!(
            storage
                .claim_strategy_action_inner(
                    "USD",
                    3_600,
                    30,
                    buy_at + chrono::Duration::minutes(1),
                    false,
                )
                .unwrap()
                .is_none()
        );
        let sell_attempts: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM strategy_execution_actions
                 WHERE source_evaluation_id = ?",
                params![sell_source],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sell_attempts, 1);
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
        complete_empty_position_snapshot(&mut storage);
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
                    completed_status: None,
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
                    completed_status: None,
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
    fn reconciliation_does_not_demote_a_local_terminal_order() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session = uuid::Uuid::now_v7();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "BUY".into(),
            quantity: 10.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("terminal-reconcile", "DU123", &request, "accepted", None)
            .unwrap();
        let order_id = storage
            .record_submitted_order(intent_id, 52, session)
            .unwrap();
        storage
            .connection
            .execute(
                "UPDATE orders SET status = 'Filled' WHERE order_id = ?",
                params![order_id],
            )
            .unwrap();

        storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: session,
                open_orders: vec![crate::ibkr::OpenOrderSnapshot {
                    broker_order_id: 52,
                    perm_id: 5200,
                    client_id: 17,
                    account: "DU123".into(),
                    conid: request.contract.conid,
                    symbol: request.contract.symbol.clone(),
                    side: "BUY".into(),
                    quantity: 10.0,
                    order_type: "LMT".into(),
                    limit_price: Some(500.0),
                    status: "Submitted".into(),
                    completed_time: None,
                    completed_status: None,
                }],
                completed_orders: Vec::new(),
                events: Vec::new(),
                completed_at: Utc::now(),
            })
            .unwrap();

        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
    }

    #[test]
    fn reconciliation_promotes_an_order_fully_covered_by_local_executions() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session = uuid::Uuid::now_v7();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "BUY".into(),
            quantity: 10.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent(
                "execution-covered-reconcile",
                "DU123",
                &request,
                "accepted",
                None,
            )
            .unwrap();
        storage
            .record_submitted_order(intent_id, 53, session)
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Execution {
                connection_session_id: Some(session),
                broker_order_id: 53,
                perm_id: 5300,
                execution_id: "reconcile.execution.covered".into(),
                conid: request.contract.conid,
                side: "Bought".into(),
                quantity: 10.0,
                price: 499.0,
                executed_at: Utc::now(),
            })
            .unwrap();

        storage
            .reconcile(&crate::ibkr::ReconciliationSnapshot {
                connection_session_id: session,
                open_orders: vec![crate::ibkr::OpenOrderSnapshot {
                    broker_order_id: 53,
                    perm_id: 5300,
                    client_id: 17,
                    account: "DU123".into(),
                    conid: request.contract.conid,
                    symbol: request.contract.symbol.clone(),
                    side: "BUY".into(),
                    quantity: 10.0,
                    order_type: "LMT".into(),
                    limit_price: Some(500.0),
                    status: "Submitted".into(),
                    completed_time: None,
                    completed_status: None,
                }],
                completed_orders: Vec::new(),
                events: Vec::new(),
                completed_at: Utc::now(),
            })
            .unwrap();

        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["filled_quantity"], 10.0);
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
    fn broker_completion_and_execution_are_replayed_when_they_arrive_before_order_insert() {
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
            .create_order_intent("fast-fill", "DU123", &request, "accepted", None)
            .unwrap();

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Execution {
                connection_session_id: Some(session),
                broker_order_id: 33,
                perm_id: 1608303481,
                execution_id: "fast.execution.1".into(),
                conid: 272093,
                side: "SLD".into(),
                quantity: 100.0,
                price: 463.6,
                executed_at: Utc::now(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Commission {
                execution_id: "fast.execution.1".into(),
                commission: 1.25,
                currency: "USD".into(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id: Some(session),
                broker_order_id: 33,
                perm_id: 1608303481,
                status: "Filled".into(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: "20260731 12:34:01 US/Eastern".into(),
                completed_status: "Filled Size: 100".into(),
            })
            .unwrap();
        assert_eq!(storage.list_executions_page(1, 10).unwrap().1, 0);

        let order_id = storage
            .record_submitted_order(intent_id, 33, session)
            .unwrap();
        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["broker_perm_id"], 1608303481_i64);
        let (executions, count) = storage.list_executions_page(1, 10).unwrap();
        assert_eq!(count, 1);
        assert_eq!(executions[0]["order_id"], order_id.to_string());
        assert_eq!(executions[0]["broker_execution_id"], "fast.execution.1");
        assert_eq!(executions[0]["commission"], 1.25);
        assert_eq!(executions[0]["currency"], "USD");
        let pending: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM pending_broker_executions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 0);
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
                    status: "Submitted".into(),
                    completed_time: Some("20260729 14:38:37 US/Eastern".into()),
                    completed_status: Some("Filled Size: 100".into()),
                }],
                events: Vec::new(),
                completed_at: now,
            })
            .unwrap();

        assert!(report.healthy);
        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["broker_perm_id"], 549849917);
        assert_eq!(order["filled_quantity"], 100.0);
        let completed_status: String = storage
            .connection
            .query_row(
                "SELECT completed_status FROM broker_order_snapshots
                 WHERE local_order_id = ?",
                params![order_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(completed_status, "Filled Size: 100");

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
    fn backtest_sell_signal_flattens_when_shorting_is_disabled() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 15.0, 25.0, 15.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, false);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, equity, metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(1.0, 0.0),
            &fees_only_gate(),
            &bars,
        )
        .unwrap();

        assert_eq!(trades.len(), 2);
        assert_eq!((trades[0].side, trades[0].quantity), ("buy", 10.0));
        assert_eq!((trades[1].side, trades[1].quantity), ("sell", 10.0));
        assert_eq!(equity.last().unwrap().position, 0.0);
        assert_eq!(metrics["total_commission"], 2.0);
        assert_eq!(metrics["allow_short"], false);
    }

    #[test]
    fn backtest_long_to_short_reversal_flattens_on_a_separate_bar() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 15.0, 25.0, 25.0, 15.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, true);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, equity, metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(1.0, 0.0),
            &fees_only_gate(),
            &bars,
        )
        .unwrap();

        assert_eq!(trades.len(), 3);
        assert_eq!((trades[0].side, trades[0].quantity), ("buy", 10.0));
        assert_eq!((trades[1].side, trades[1].quantity), ("sell", 10.0));
        assert_eq!((trades[2].side, trades[2].quantity), ("sell", 4.0));
        assert_eq!(trades[1].fill_time, bars[3].open_time);
        assert_eq!(trades[2].fill_time, bars[4].open_time);
        assert_eq!(equity.last().unwrap().position, -4.0);
        assert_eq!(metrics["total_commission"], 3.0);
        assert_eq!(
            metrics["reversal_execution"],
            "flatten_then_require_fresh_directional_signal"
        );
    }

    #[test]
    fn backtest_hold_after_flatten_never_reuses_the_old_signal_to_open_short() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 15.0, 25.0, 15.0, 15.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, true);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, equity, _) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(0.0, 0.0),
            &fees_only_gate(),
            &bars,
        )
        .unwrap();

        assert_eq!(trades.len(), 2);
        assert_eq!((trades[1].side, trades[1].quantity), ("sell", 10.0));
        assert_eq!(equity.last().unwrap().position, 0.0);
    }

    #[test]
    fn backtest_short_to_long_reversal_flattens_on_a_separate_bar() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [25.0, 15.0, 5.0, 5.0, 15.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, true);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, equity, _) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(0.0, 0.0),
            &fees_only_gate(),
            &bars,
        )
        .unwrap();

        assert_eq!(trades.len(), 3);
        assert_eq!((trades[0].side, trades[0].quantity), ("sell", 4.0));
        assert_eq!((trades[1].side, trades[1].quantity), ("buy", 4.0));
        assert_eq!((trades[2].side, trades[2].quantity), ("buy", 10.0));
        assert_eq!(trades[1].fill_time, bars[3].open_time);
        assert_eq!(trades[2].fill_time, bars[4].open_time);
        assert_eq!(equity.last().unwrap().position, 10.0);
    }

    #[test]
    fn backtest_rejects_a_short_target_when_shorting_is_disabled() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let mut request = close_threshold_backtest_request(start, false);
        request.short_target_quantity = -4.0;
        assert!(validate_backtest_request(&request).is_err());
    }

    #[test]
    fn strategy_cost_gate_blocks_an_entry_whose_edge_cannot_cover_round_trip_cost() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 15.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, false);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, _, metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(100.0, 0.0),
            &matching_gate(1.0, 0.5, 5, None),
            &bars,
        )
        .unwrap();

        assert!(trades.is_empty());
        assert_eq!(metrics["cost_gate_decisions"]["transaction_blocked"], 1);
        assert_eq!(metrics["cost_gate"]["mode"], "match_strategy");
        assert_eq!(
            metrics["cost_gate"]["statistics_baseline"],
            "backtest_start"
        );
    }

    #[test]
    fn strategy_cost_gate_always_bypasses_a_strict_reduction() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 15.0, 25.0, 15.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, false);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, equity, metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(5.0, 0.0),
            &matching_gate(5.0, 0.0001, 0, None),
            &bars,
        )
        .unwrap();

        assert_eq!(trades.len(), 2);
        assert_eq!(equity.last().unwrap().position, 0.0);
        assert_eq!(metrics["cost_gate_decisions"]["transaction_passed"], 1);
        assert_eq!(metrics["cost_gate_decisions"]["risk_reducing_bypassed"], 1);
    }

    #[test]
    fn strategy_backtest_commission_performance_gate_is_path_dependent() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 5.0, 25.0, 25.0, 5.0, 5.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, false);
        let strategy = build_backtest_strategy(&request).unwrap();
        let model = test_cost_model(1.0, 0.0);
        let (gated_trades, _, gated_metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &model,
            &matching_gate(1.0, 0.005, 1, None),
            &bars,
        )
        .unwrap();
        let (fees_only_trades, _, _) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &model,
            &fees_only_gate(),
            &bars,
        )
        .unwrap();

        assert_eq!(gated_trades.len(), 2);
        assert_eq!(fees_only_trades.len(), 3);
        assert_eq!(
            gated_metrics["cost_gate_decisions"]["performance_blocked"],
            1
        );
        assert_eq!(gated_metrics["cost_gate_completed_trades"], 1);
        assert_eq!(gated_metrics["cost_gate_completed_gross_pnl"], 200.0);
        assert_eq!(gated_metrics["cost_gate_commissions_since_start"], 2.0);
    }

    #[test]
    fn a_performance_fuse_that_trips_while_open_never_blocks_the_exit() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars = [5.0, 5.0, 25.0, 25.0, 5.0, 5.0, 25.0, 25.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| simulated_bar(start, index, close))
            .collect::<Vec<_>>();
        let request = close_threshold_backtest_request(start, false);
        let strategy = build_backtest_strategy(&request).unwrap();
        let (trades, equity, metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &test_cost_model(1.0, 0.0),
            &matching_gate(1.0, 0.012, 1, None),
            &bars,
        )
        .unwrap();

        assert_eq!(trades.len(), 4);
        assert_eq!(equity.last().unwrap().position, 0.0);
        assert_eq!(metrics["cost_gate_decisions"]["risk_reducing_bypassed"], 2);
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
            cost_model_id: None,
            cost_gate_mode: None,
            conid: 756733,
            timeframe: "1m".into(),
            start,
            end: start + chrono::Duration::minutes(5),
            short_window: Some(2),
            long_window: Some(3),
            strategy_kind: "moving_average_cross".into(),
            strategy_config: None,
            quantity: 1.0,
            short_target_quantity: 0.0,
            allow_short: false,
            initial_cash: 100.0,
            outside_rth: false,
            seed: 7,
        };
        let strategy = build_backtest_strategy(&request).unwrap();
        let cost_model = test_cost_model(1.0, 100.0);
        let (trades, _, metrics) = simulate_strategy(
            &request,
            strategy.as_ref(),
            &cost_model,
            &fees_only_gate(),
            &bars,
        )
        .unwrap();
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
                completed_status: None,
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
        let subscription_id = uuid::Uuid::now_v7();
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
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
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
    fn live_bar_builder_carries_forward_short_quote_silence() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = "2026-08-01T13:00:00Z".parse::<DateTime<Utc>>().unwrap();
        storage
            .update_market_five_second_bar(272093, 100.0, start)
            .unwrap();
        storage
            .update_market_five_second_bar(272093, 101.0, start + chrono::Duration::seconds(20))
            .unwrap();

        let mut bars = storage.list_market_bars(272093, "5s", 10).unwrap();
        bars.reverse();
        assert_eq!(bars.len(), 5);
        for pair in bars.windows(2) {
            let left = pair[0]["bar_time"]
                .as_str()
                .unwrap()
                .parse::<DateTime<Utc>>()
                .unwrap();
            let right = pair[1]["bar_time"]
                .as_str()
                .unwrap()
                .parse::<DateTime<Utc>>()
                .unwrap();
            assert_eq!((right - left).num_seconds(), 5);
        }
        assert_eq!(bars[1]["tick_count"], 0);
        assert_eq!(bars[1]["close"], 100.0);
        assert_eq!(bars[4]["close"], 101.0);
    }

    #[test]
    fn performance_excludes_unmatched_long_only_sells_and_counts_round_trips() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "performance-integrity",
                "close_threshold",
                &serde_json::json!({"conid": 272093, "buy_below": 90.0, "sell_above": 110.0}),
            )
            .unwrap();
        let contract = crate::ibkr::ContractCandidate {
            conid: 272093,
            symbol: "MSFT".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            local_symbol: "MSFT".into(),
            description: "MICROSOFT CORP".into(),
            derivative_security_types: Vec::new(),
        };
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 100.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: contract.clone(),
            })
            .unwrap();
        let session = uuid::Uuid::now_v7();
        let start = "2026-08-01T14:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let mut add_order = |broker_id: i32, side: &str, price: Option<f64>, parts: &[f64]| {
            let request = crate::ibkr::BrokerOrderRequest {
                contract: contract.clone(),
                side: side.into(),
                quantity: 100.0,
                order_type: "MKT".into(),
                limit_price: None,
                outside_rth: false,
            };
            let intent_id = storage
                .create_order_intent(
                    &format!("performance-{broker_id}"),
                    "DU123",
                    &request,
                    "accepted",
                    None,
                )
                .unwrap();
            let now = start + chrono::Duration::minutes(broker_id as i64);
            storage
                .connection
                .execute(
                    "INSERT INTO strategy_execution_actions
                     (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                      requested_quantity, state, order_intent_id, broker_order_id,
                      created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, 100, 'submitted', ?, ?, ?, ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        strategy_id,
                        uuid::Uuid::now_v7(),
                        format!("performance-{broker_id}"),
                        side.to_ascii_lowercase(),
                        intent_id,
                        broker_id,
                        now,
                        now
                    ],
                )
                .unwrap();
            storage
                .record_submitted_order(intent_id, broker_id, session)
                .unwrap();
            if let Some(price) = price {
                for (index, quantity) in parts.iter().enumerate() {
                    storage
                        .apply_broker_event(&crate::ibkr::BrokerEvent::Execution {
                            connection_session_id: Some(session),
                            broker_order_id: broker_id,
                            perm_id: 10_000 + broker_id as i64,
                            execution_id: format!("performance.{broker_id}.{index}"),
                            conid: 272093,
                            side: if side == "BUY" { "Bought" } else { "Sold" }.into(),
                            quantity: *quantity,
                            price,
                            executed_at: now,
                        })
                        .unwrap();
                }
            }
            storage
                .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                    connection_session_id: Some(session),
                    broker_order_id: broker_id,
                    perm_id: 10_000 + broker_id as i64,
                    status: "Filled".into(),
                    reject_reason: String::new(),
                    warning_text: String::new(),
                    completed_time: String::new(),
                    completed_status: "Filled Size: 100".into(),
                })
                .unwrap();
        };
        add_order(1, "BUY", None, &[]); // Historical execution is missing.
        add_order(2, "SELL", Some(99.0), &[100.0]); // Must not create a synthetic short.
        add_order(3, "BUY", Some(100.0), &[100.0]);
        add_order(4, "SELL", Some(101.0), &[40.0, 60.0]);
        drop(add_order);
        // `allow_short` is mutable configuration, not historical evidence.
        // Enabling it now must not reinterpret the first unmatched sell as an
        // authorized short position.
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET allow_short = true, short_target_quantity = -100
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO market_minute_bars VALUES
                 (272093, ?, 100, 100, 100, 100, 1, true, ?),
                 (272093, ?, 101, 101, 101, 101, 1, true, ?)",
                params![
                    start + chrono::Duration::minutes(2),
                    start,
                    start + chrono::Duration::minutes(4),
                    start
                ],
            )
            .unwrap();

        let report = storage
            .strategy_performance_report(
                strategy_id,
                100_000.0,
                "USD",
                300,
                300,
                300,
                Some(272093),
                start + chrono::Duration::minutes(5),
            )
            .unwrap();
        assert_eq!(report["gross_pnl"], 100.0);
        assert_eq!(report["realized_trade_count"], 1);
        assert_eq!(report["winning_trade_count"], 1);
        assert_eq!(report["losing_trade_count"], 0);
        assert_eq!(report["open_position_count"], 0);
        assert_eq!(report["data_complete"], false);
        assert!(
            report["data_warning_groups"]
                .as_array()
                .unwrap()
                .iter()
                .any(|group| group["code"] == "missing_execution_details")
        );
        assert_eq!(report["unmatched_execution_quantity"], 100.0);
        assert!((report["benchmark_return"].as_f64().unwrap() - 0.01).abs() < 0.000000001);
    }

    #[test]
    fn performance_separates_realized_and_fresh_mark_to_market_pnl() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = storage
            .create_strategy(
                "mark-to-market",
                "close_threshold",
                &serde_json::json!({"conid": 272093, "buy_below": 90.0, "sell_above": 110.0}),
            )
            .unwrap();
        let contract = crate::ibkr::ContractCandidate {
            conid: 272093,
            symbol: "MSFT".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            local_symbol: "MSFT".into(),
            description: "MICROSOFT CORP".into(),
            derivative_security_types: Vec::new(),
        };
        storage
            .configure_strategy_execution(&StrategyExecutionConfig {
                strategy_id,
                account: "DU123".into(),
                target_quantity: 10.0,
                short_target_quantity: 0.0,
                allow_short: false,
                order_type: "market".into(),
                paper_only: true,
                outside_rth: false,
                contract: contract.clone(),
            })
            .unwrap();
        let now = Utc::now();
        let request = crate::ibkr::BrokerOrderRequest {
            contract,
            side: "BUY".into(),
            quantity: 10.0,
            order_type: "MKT".into(),
            limit_price: None,
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("mark-to-market-buy", "DU123", &request, "accepted", None)
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_execution_actions
                 (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                  requested_quantity, state, order_intent_id, broker_order_id,
                  created_at, updated_at)
                 VALUES (?, ?, ?, 'mark-to-market-buy', 'buy', 10, 'submitted', ?, 1, ?, ?)",
                params![
                    uuid::Uuid::now_v7(),
                    strategy_id,
                    uuid::Uuid::now_v7(),
                    intent_id,
                    now,
                    now
                ],
            )
            .unwrap();
        let session = uuid::Uuid::now_v7();
        storage
            .record_submitted_order(intent_id, 1, session)
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Execution {
                connection_session_id: Some(session),
                broker_order_id: 1,
                perm_id: 1,
                execution_id: "mark-to-market.execution".into(),
                conid: 272093,
                side: "Bought".into(),
                quantity: 10.0,
                price: 100.0,
                executed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Commission {
                execution_id: "mark-to-market.execution".into(),
                commission: 1.0,
                currency: "USD".into(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id: Some(session),
                broker_order_id: 1,
                perm_id: 1,
                status: "Filled".into(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: String::new(),
                completed_status: "Filled Size: 10".into(),
            })
            .unwrap();
        let position_subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: position_subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: position_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 272093,
                    symbol: "MSFT".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "NASDAQ".into(),
                    quantity: 10.0,
                    average_cost: 100.0,
                    observed_at: now,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: position_subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid: 272093,
                tick_type: "Bid".into(),
                numeric_value: Some(110.0),
                text_value: None,
                observed_at: now,
            })
            .unwrap();

        let report = storage
            .strategy_performance_report(
                strategy_id,
                100_000.0,
                "USD",
                300,
                300,
                300,
                None,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(report["data_complete"], true);
        assert_eq!(report["valuation_complete"], true);
        assert_eq!(report["realized_gross_pnl"], 0.0);
        assert_eq!(report["realized_net_pnl"], -1.0);
        assert_eq!(report["unrealized_pnl"], 100.0);
        assert_eq!(report["total_net_pnl"], 99.0);
        assert_eq!(report["open_position_count"], 1);
    }

    #[test]
    fn degraded_close_only_never_crosses_through_flat() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let connected_at = Utc::now();
        let snapshot_started_at = connected_at + chrono::Duration::milliseconds(1);
        let observed_at = connected_at + chrono::Duration::milliseconds(2);
        let snapshot_completed_at = connected_at + chrono::Duration::milliseconds(3);
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: snapshot_started_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
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
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: snapshot_completed_at,
            })
            .unwrap();

        assert!(
            storage
                .evaluate_close_only(
                    "DU123",
                    756733,
                    "sell",
                    5.0,
                    connected_at,
                    120,
                    snapshot_completed_at,
                )
                .unwrap()
                .allowed
        );
        assert!(
            !storage
                .evaluate_close_only(
                    "DU123",
                    756733,
                    "sell",
                    6.0,
                    connected_at,
                    120,
                    snapshot_completed_at,
                )
                .unwrap()
                .allowed
        );
        assert!(
            !storage
                .evaluate_close_only(
                    "DU123",
                    756733,
                    "buy",
                    1.0,
                    connected_at,
                    120,
                    snapshot_completed_at,
                )
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
                    observed_at + chrono::Duration::seconds(1),
                    120,
                    observed_at + chrono::Duration::seconds(1),
                )
                .unwrap()
                .allowed
        );

        let stale_at = snapshot_completed_at + chrono::Duration::seconds(121);
        let stale = storage
            .evaluate_close_only("DU123", 756733, "sell", 1.0, connected_at, 120, stale_at)
            .unwrap();
        assert!(!stale.allowed);
        assert!(stale.reason.contains("lease is stale"));
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSubscriptionHeartbeat {
                subscription_id,
                observed_at: stale_at,
            })
            .unwrap();
        assert!(
            storage
                .evaluate_close_only("DU123", 756733, "sell", 1.0, connected_at, 120, stale_at,)
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn close_only_reserves_pending_orders_and_waits_for_position_after_fill() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let connected_at = Utc::now();
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 100.0,
                    average_cost: 100.0,
                    observed_at: Utc::now(),
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();

        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "SELL".into(),
            quantity: 60.0,
            order_type: "MKT".into(),
            limit_price: None,
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("reserved-close", "DU123", &request, "approved", None)
            .unwrap();
        let pending = storage
            .evaluate_close_only("DU123", 756733, "sell", 41.0, connected_at, 120, Utc::now())
            .unwrap();
        assert!(!pending.allowed);
        assert_eq!(pending.maximum_closing_quantity, 40.0);
        assert!(pending.reason.contains("already reserve"));
        assert!(
            storage
                .evaluate_close_only("DU123", 756733, "sell", 40.0, connected_at, 120, Utc::now(),)
                .unwrap()
                .allowed
        );

        let session_id = uuid::Uuid::now_v7();
        storage
            .record_submitted_order(intent_id, 7_002, session_id)
            .unwrap();
        let active = storage
            .evaluate_close_only("DU123", 756733, "sell", 41.0, connected_at, 120, Utc::now())
            .unwrap();
        assert!(!active.allowed);
        assert_eq!(active.maximum_closing_quantity, 40.0);

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Execution {
                connection_session_id: Some(session_id),
                broker_order_id: 7_002,
                perm_id: 7_002,
                execution_id: "reserved-close.execution".into(),
                conid: 756733,
                side: "Sold".into(),
                quantity: 60.0,
                price: 100.0,
                executed_at: Utc::now(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id: Some(session_id),
                broker_order_id: 7_002,
                perm_id: 7_002,
                status: "Filled".into(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: String::new(),
                completed_status: "Filled Size: 60".into(),
            })
            .unwrap();

        let stale = storage
            .evaluate_close_only("DU123", 756733, "sell", 1.0, connected_at, 120, Utc::now())
            .unwrap();
        assert!(!stale.allowed);
        assert_eq!(stale.maximum_closing_quantity, 0.0);
        assert!(stale.reason.contains("has not caught up"));

        let execution_received_at = storage
            .position_evidence_state("DU123", 756733)
            .unwrap()
            .latest_execution_received_at
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 40.0,
                    average_cost: 100.0,
                    observed_at: execution_received_at + chrono::Duration::milliseconds(1),
                },
            })
            .unwrap();
        assert!(
            storage
                .evaluate_close_only("DU123", 756733, "sell", 40.0, connected_at, 120, Utc::now(),)
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn ended_position_subscription_revokes_close_only_until_a_new_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let connected_at = Utc::now();
        let first_started_at = connected_at + chrono::Duration::milliseconds(1);
        let first_completed_at = connected_at + chrono::Duration::milliseconds(2);
        let first_subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: first_subscription_id,
                observed_at: first_started_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: first_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 5.0,
                    average_cost: 700.0,
                    observed_at: first_started_at,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: first_subscription_id,
                observed_at: first_completed_at,
            })
            .unwrap();

        let ended_at = first_completed_at + chrono::Duration::milliseconds(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSubscriptionEnded {
                subscription_id: first_subscription_id,
                observed_at: ended_at,
                reason: "test stream failure".into(),
            })
            .unwrap();
        let ended = storage
            .evaluate_close_only("DU123", 756733, "sell", 1.0, connected_at, 120, ended_at)
            .unwrap();
        assert!(!ended.allowed);
        assert!(ended.reason.contains("not ready"));

        let second_started_at = ended_at + chrono::Duration::milliseconds(1);
        let second_completed_at = ended_at + chrono::Duration::milliseconds(2);
        let second_subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: second_subscription_id,
                observed_at: second_started_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: second_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 5.0,
                    average_cost: 700.0,
                    observed_at: second_started_at,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: second_subscription_id,
                observed_at: second_completed_at,
            })
            .unwrap();
        assert!(
            storage
                .evaluate_close_only(
                    "DU123",
                    756733,
                    "sell",
                    1.0,
                    connected_at,
                    120,
                    second_completed_at,
                )
                .unwrap()
                .allowed
        );

        // A delayed terminal event from the superseded attempt must not revoke
        // the newer authoritative snapshot.
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSubscriptionEnded {
                subscription_id: first_subscription_id,
                observed_at: second_completed_at + chrono::Duration::milliseconds(1),
                reason: "late old-stream event".into(),
            })
            .unwrap();
        assert!(
            storage
                .evaluate_close_only(
                    "DU123",
                    756733,
                    "sell",
                    1.0,
                    connected_at,
                    120,
                    second_completed_at + chrono::Duration::milliseconds(1),
                )
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn late_events_from_old_position_subscription_do_not_overwrite_new_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let first_subscription_id = uuid::Uuid::now_v7();
        let second_subscription_id = uuid::Uuid::now_v7();
        let first_started_at = Utc::now();
        let second_started_at = first_started_at + chrono::Duration::milliseconds(1);
        let second_position_at = second_started_at + chrono::Duration::milliseconds(1);
        let late_at = second_position_at + chrono::Duration::milliseconds(1);

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: first_subscription_id,
                observed_at: first_started_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: first_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 1.0,
                    average_cost: 100.0,
                    observed_at: first_started_at,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: second_subscription_id,
                observed_at: second_started_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: second_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 7.0,
                    average_cost: 101.0,
                    observed_at: second_position_at,
                },
            })
            .unwrap();

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: first_subscription_id,
                observed_at: first_started_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: first_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 99.0,
                    average_cost: 999.0,
                    observed_at: late_at,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: first_subscription_id,
                observed_at: late_at,
            })
            .unwrap();

        let (state, current_subscription_id, sync_observed_at): (
            String,
            uuid::Uuid,
            DateTime<Utc>,
        ) = storage
            .connection
            .query_row(
                "SELECT state, subscription_id, observed_at FROM position_sync_state
                 WHERE singleton",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, "syncing");
        assert_eq!(current_subscription_id, second_subscription_id);
        assert_eq!(
            sync_observed_at.timestamp_micros(),
            second_started_at.timestamp_micros()
        );
        let position = &storage.list_positions().unwrap()[0];
        assert_eq!(position["quantity"], 7.0);
        assert_eq!(position["average_cost"], 101.0);
        assert_eq!(
            serde_json::from_value::<DateTime<Utc>>(position["observed_at"].clone())
                .unwrap()
                .timestamp_micros(),
            second_position_at.timestamp_micros()
        );

        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: second_subscription_id,
                observed_at: late_at + chrono::Duration::milliseconds(1),
            })
            .unwrap();
        let state: String = storage
            .connection
            .query_row(
                "SELECT state FROM position_sync_state WHERE singleton",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "ready");
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
                    completed_status: None,
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
            storage
                .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                    conid: contract.conid,
                    tick_type: "LastTimestamp".into(),
                    numeric_value: None,
                    text_value: Some(observed_at.timestamp().to_string()),
                    observed_at: observed_at + chrono::Duration::milliseconds(1),
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
        assert_eq!(five_second_bars.len(), 13);
        assert_eq!(
            five_second_bars
                .iter()
                .filter(|bar| bar["final"] == true)
                .count(),
            12
        );
        assert_eq!(five_second_bars[12]["close"], 700.0);
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
    fn replayed_last_trade_uses_source_time_and_does_not_create_a_new_bar() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let conid = 272093;
        let received_at = DateTime::from_timestamp(1_786_200_000, 0).unwrap();
        let stale_source_at = received_at - chrono::Duration::days(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataStatus {
                conid,
                state: "active".into(),
                error: None,
                observed_at: received_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid,
                tick_type: "LastTimestamp".into(),
                numeric_value: None,
                text_value: Some(stale_source_at.timestamp().to_string()),
                observed_at: received_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid,
                tick_type: "Last".into(),
                numeric_value: Some(499.38),
                text_value: None,
                observed_at: received_at + chrono::Duration::milliseconds(1),
            })
            .unwrap();

        assert!(
            storage
                .list_market_bars(conid, "5s", 10)
                .unwrap()
                .is_empty()
        );
        let health = storage
            .market_data_health(conid, 30, received_at + chrono::Duration::seconds(1))
            .unwrap();
        assert_eq!(health.state, "stale");
        assert_eq!(health.observed_at, Some(stale_source_at));

        let fresh_source_at = received_at + chrono::Duration::seconds(10);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid,
                tick_type: "Last".into(),
                numeric_value: Some(500.0),
                text_value: None,
                observed_at: fresh_source_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid,
                tick_type: "LastTimestamp".into(),
                numeric_value: None,
                text_value: Some(fresh_source_at.timestamp().to_string()),
                observed_at: fresh_source_at + chrono::Duration::milliseconds(1),
            })
            .unwrap();
        let bars = storage.list_market_bars(conid, "5s", 10).unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0]["bar_time"], serde_json::json!(fresh_source_at));
        assert_eq!(bars[0]["close"], 500.0);
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
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
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
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: now,
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

    fn spy_contract() -> crate::ibkr::ContractCandidate {
        crate::ibkr::ContractCandidate {
            conid: 756733,
            symbol: "SPY".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "ARCA".into(),
            local_symbol: "SPY".into(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        }
    }

    fn configure_spy_execution(storage: &mut Storage) -> uuid::Uuid {
        let strategy_id = storage
            .create_strategy(
                &format!("pending intent test {}", uuid::Uuid::now_v7()),
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
                contract: spy_contract(),
            })
            .unwrap();
        storage
            .set_strategy_execution_enabled(strategy_id, true)
            .unwrap();
        strategy_id
    }

    fn complete_empty_position_snapshot(storage: &mut Storage) {
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
    }

    fn complete_spy_position_snapshot(
        storage: &mut Storage,
        quantity: f64,
        observed_at: DateTime<Utc>,
    ) -> uuid::Uuid {
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity,
                    average_cost: 100.0,
                    observed_at,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at,
            })
            .unwrap();
        subscription_id
    }

    #[test]
    fn strategy_capital_currency_must_be_explicit_and_match_before_execution_is_enabled() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        storage
            .set_strategy_execution_enabled(strategy_id, false)
            .unwrap();

        // Simulate a pre-schema-34 risk row. Its numeric capital cannot safely
        // be guessed as either USD or HKD.
        storage
            .connection
            .execute(
                "UPDATE strategy_risk_controls SET enabled = false, capital_currency = NULL
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let error = storage
            .set_strategy_execution_enabled_with_capital_currency(strategy_id, true, "HKD")
            .unwrap_err()
            .to_string();
        assert!(error.contains("no capital currency"));

        storage
            .connection
            .execute(
                "DELETE FROM strategy_risk_controls WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let error = storage
            .set_strategy_execution_enabled_with_capital_currency(strategy_id, true, "HKD")
            .unwrap_err()
            .to_string();
        assert!(error.contains("risk control is missing"));

        storage
            .configure_strategy_risk_control(&StrategyRiskControlInput {
                strategy_id,
                enabled: true,
                strategy_capital: 100_000.0,
                capital_currency: Some("HKD".into()),
                maximum_position_capital_ratio: 1.0,
                maximum_rolling_24h_realized_net_loss_ratio: 0.02,
                maximum_consecutive_net_losing_trades: 3,
                maximum_rolling_24h_completed_trades: 10,
                maximum_rolling_24h_turnover_capital_ratio: 10.0,
            })
            .unwrap();
        assert!(
            storage
                .set_strategy_execution_enabled_with_capital_currency(strategy_id, true, "HKD",)
                .unwrap()
        );
        let control = storage
            .list_strategy_risk_controls("HKD", 3_600, Utc::now())
            .unwrap()
            .remove(0);
        assert_eq!(control["base_currency"], "HKD");
        assert_eq!(control["currency_matches_daemon"], true);

        // A row disappearing after execution was enabled must also fail
        // closed in the claim path, not only in the enable RPC.
        storage
            .connection
            .execute(
                "DELETE FROM strategy_risk_controls WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        complete_empty_position_snapshot(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        assert!(
            storage
                .claim_strategy_action_with_risk("HKD", 3_600, 30, Utc::now())
                .unwrap()
                .is_none()
        );
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["state"], "skipped");
        assert!(
            actions[0]["detail"]
                .as_str()
                .unwrap()
                .contains("风险控制缺失")
        );
    }

    fn record_attributed_spy_fill(
        storage: &mut Storage,
        strategy_id: uuid::Uuid,
        broker_order_id: i32,
        side: &str,
        quantity: f64,
        price: f64,
        commission: f64,
        executed_at: DateTime<Utc>,
    ) {
        record_attributed_spy_fill_with_target(
            storage,
            strategy_id,
            broker_order_id,
            side,
            quantity,
            price,
            commission,
            executed_at,
            None,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_attributed_spy_fill_with_target(
        storage: &mut Storage,
        strategy_id: uuid::Uuid,
        broker_order_id: i32,
        side: &str,
        quantity: f64,
        price: f64,
        commission: f64,
        executed_at: DateTime<Utc>,
        target_quantity: Option<f64>,
    ) {
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: side.into(),
            quantity,
            order_type: "MKT".into(),
            limit_price: None,
            outside_rth: false,
        };
        let key = format!("risk-statistics-{broker_order_id}");
        let intent_id = storage
            .create_order_intent(&key, "DU123", &request, "accepted", None)
            .unwrap();
        let action_id = uuid::Uuid::now_v7();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_execution_actions
                 (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                  requested_quantity, state, order_intent_id, broker_order_id,
                  created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, 'submitted', ?, ?, ?, ?)",
                params![
                    action_id,
                    strategy_id,
                    uuid::Uuid::now_v7(),
                    key,
                    side.to_ascii_lowercase(),
                    quantity,
                    intent_id,
                    broker_order_id,
                    executed_at,
                    executed_at
                ],
            )
            .unwrap();
        if let Some(target_quantity) = target_quantity {
            storage
                .connection
                .execute(
                    "INSERT INTO strategy_execution_action_legs
                     (action_id, leg_index, conid, symbol, target_quantity,
                      requested_side, requested_quantity, order_intent_id,
                      broker_order_id, state, detail, created_at, updated_at)
                     VALUES (?, 0, 756733, 'SPY', ?, ?, ?, ?, ?, 'submitted',
                             NULL, ?, ?)",
                    params![
                        action_id,
                        target_quantity,
                        side.to_ascii_lowercase(),
                        quantity,
                        intent_id,
                        broker_order_id,
                        executed_at,
                        executed_at
                    ],
                )
                .unwrap();
        }
        let session = uuid::Uuid::now_v7();
        storage
            .record_submitted_order(intent_id, broker_order_id, session)
            .unwrap();
        let execution_id = format!("risk-statistics.execution.{broker_order_id}");
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Execution {
                connection_session_id: Some(session),
                broker_order_id,
                perm_id: i64::from(broker_order_id),
                execution_id: execution_id.clone(),
                conid: 756733,
                side: if side.eq_ignore_ascii_case("buy") {
                    "Bought".into()
                } else {
                    "Sold".into()
                },
                quantity,
                price,
                executed_at,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Commission {
                execution_id,
                commission,
                currency: "USD".into(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id: Some(session),
                broker_order_id,
                perm_id: i64::from(broker_order_id),
                status: "Filled".into(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: String::new(),
                completed_status: format!("Filled Size: {quantity}"),
            })
            .unwrap();
    }

    fn insert_buy_evaluation(
        storage: &mut Storage,
        strategy_id: uuid::Uuid,
        created_at: DateTime<Utc>,
    ) -> uuid::Uuid {
        insert_signal_evaluation(storage, strategy_id, "buy", created_at)
    }

    fn insert_signal_evaluation(
        storage: &mut Storage,
        strategy_id: uuid::Uuid,
        signal: &str,
        created_at: DateTime<Utc>,
    ) -> uuid::Uuid {
        let evaluation_id = uuid::Uuid::now_v7();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 90, 100, 90, 200, ?, ?, '{}')",
                params![evaluation_id, strategy_id, created_at, signal, created_at],
            )
            .unwrap();
        evaluation_id
    }

    #[test]
    fn in_flight_and_unknown_intents_block_claims_and_occupy_risk_headroom() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
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
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
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
            contract: spy_contract(),
            side: "buy".into(),
            quantity: 2.0,
            order_type: "limit".into(),
            limit_price: Some(100.0),
            outside_rth: false,
        };
        // An approved intent whose broker call is still in flight: no orders
        // row exists yet.
        let pending_intent_id = storage
            .create_order_intent("in-flight", "DU123", &request, "approved", None)
            .unwrap();
        let mut config = crate::config::RiskConfig::default();
        config.max_open_orders = 1;
        let decision = storage
            .evaluate_portfolio_risk(&config, "DU123", &request, None, Some(100.0), false, now)
            .unwrap();
        assert_eq!(decision.reason_code, "MAX_OPEN_ORDERS");
        assert_eq!(decision.active_order_count, 1);
        // 5 held + 2 pending intent + 2 this request.
        assert_eq!(decision.projected_position, 9.0);
        // Once IBKR acknowledges the order, its unfilled remainder moves from
        // the intent bucket into the active-order bucket without disappearing
        // from projected position risk.
        storage
            .record_submitted_order(pending_intent_id, 9001, uuid::Uuid::now_v7())
            .unwrap();
        let submitted = storage
            .evaluate_portfolio_risk(&config, "DU123", &request, None, Some(100.0), false, now)
            .unwrap();
        assert_eq!(submitted.reason_code, "MAX_OPEN_ORDERS");
        assert_eq!(submitted.active_order_count, 1);
        assert_eq!(submitted.projected_position, 9.0);
        // The claim path must also treat the acknowledged active order as a
        // blocker for the same contract.
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        assert!(storage.claim_strategy_action().unwrap().is_none());
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["state"], "skipped");
        assert!(actions[0]["detail"].as_str().unwrap().contains("活动订单"));
    }

    #[test]
    fn stale_signals_are_recorded_as_skipped_not_executed() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs SET enabled_at = ? WHERE strategy_id = ?",
                params![Utc::now() - chrono::Duration::hours(3), strategy_id],
            )
            .unwrap();
        insert_buy_evaluation(
            &mut storage,
            strategy_id,
            Utc::now() - chrono::Duration::seconds(MAX_EXECUTABLE_SIGNAL_AGE_SECONDS + 60),
        );
        assert!(storage.claim_strategy_action().unwrap().is_none());
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["state"], "skipped");
        assert!(actions[0]["detail"].as_str().unwrap().contains("stale"));
        // The stale signal is consumed and never retried.
        assert!(storage.claim_strategy_action().unwrap().is_none());
        assert_eq!(
            storage.list_strategy_execution_actions(10).unwrap().len(),
            1
        );
    }

    #[test]
    fn stale_protective_exit_is_still_claimed() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        complete_spy_position_snapshot(&mut storage, 3.0, now);
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs SET enabled_at = ? WHERE strategy_id = ?",
                params![now - chrono::Duration::hours(3), strategy_id],
            )
            .unwrap();
        insert_signal_evaluation(
            &mut storage,
            strategy_id,
            "sell",
            now - chrono::Duration::hours(1),
        );

        let action = storage
            .claim_strategy_action_inner("USD", 3_600, 30, now, false)
            .unwrap()
            .unwrap();
        assert_eq!(action.side, "sell");
        assert_eq!(action.quantity, 3.0);
        assert!(action.legs[0].is_risk_reducing());
    }

    #[test]
    fn freshly_written_evaluation_from_an_old_bar_cannot_open_risk() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        complete_empty_position_snapshot(&mut storage);
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs SET enabled_at = ? WHERE strategy_id = ?",
                params![now - chrono::Duration::hours(3), strategy_id],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 90, 100, 90, 200, 'buy', ?, '{}')",
                params![
                    uuid::Uuid::now_v7(),
                    strategy_id,
                    now - chrono::Duration::hours(1),
                    now
                ],
            )
            .unwrap();

        assert!(
            storage
                .claim_strategy_action_inner("USD", 3_600, 30, now, false)
                .unwrap()
                .is_none()
        );
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["cost_gate_result"], "stale_signal");
        assert!(actions[0]["detail"].as_str().unwrap().contains("old"));
    }

    #[test]
    fn claims_are_deferred_while_position_snapshot_is_syncing() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        insert_buy_evaluation(&mut storage, strategy_id, Utc::now());
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
        // While syncing, the evaluation must stay unclaimed (deferred, not
        // consumed) because position deltas would be computed against a
        // transiently empty positions table.
        assert!(storage.claim_strategy_action().unwrap().is_none());
        assert!(
            storage
                .list_strategy_execution_actions(10)
                .unwrap()
                .is_empty()
        );
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
        let action = storage.claim_strategy_action().unwrap().unwrap();
        assert_eq!(action.quantity, 3.0);
    }

    #[test]
    fn strategy_claim_waits_for_position_stream_to_reflect_a_recorded_fill() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();

        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            7_001,
            "buy",
            3.0,
            100.0,
            1.0,
            Utc::now(),
        );
        let evaluation_id = insert_buy_evaluation(&mut storage, strategy_id, Utc::now());

        // The order is terminal and its execution is recorded, but the empty
        // snapshot still says flat.  The evaluation must remain unclaimed so
        // another target-size buy cannot be submitted from stale position data.
        assert!(storage.claim_strategy_action().unwrap().is_none());
        let action_exists: bool = storage
            .connection
            .query_row(
                "SELECT count(*) > 0 FROM strategy_execution_actions
                 WHERE evaluation_id = ?",
                params![evaluation_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!action_exists);

        let execution_received_at = storage
            .position_evidence_state("DU123", 756733)
            .unwrap()
            .latest_execution_received_at
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 3.0,
                    average_cost: 100.0,
                    observed_at: execution_received_at + chrono::Duration::milliseconds(1),
                },
            })
            .unwrap();

        // Once the position stream catches up, the same evaluation is
        // consumed as a no-op because the target position is already held.
        assert!(storage.claim_strategy_action().unwrap().is_none());
        let action = storage
            .list_strategy_execution_actions(20)
            .unwrap()
            .into_iter()
            .find(|action| action["evaluation_id"] == serde_json::json!(evaluation_id))
            .unwrap();
        assert_eq!(action["state"], "skipped");
        assert_eq!(action["detail"], "signal requires no position change");
    }

    #[test]
    fn completed_empty_position_snapshot_releases_flat_target_after_sell_fill() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let initial_subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: initial_subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id: initial_subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 3.0,
                    average_cost: 100.0,
                    observed_at: Utc::now(),
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: initial_subscription_id,
                observed_at: Utc::now(),
            })
            .unwrap();

        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            7_003,
            "sell",
            3.0,
            100.0,
            1.0,
            Utc::now(),
        );
        let evaluation_id = uuid::Uuid::now_v7();
        let evaluation_at = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 210, 100, 210, 200, 'sell', ?, '{}')",
                params![evaluation_id, strategy_id, evaluation_at, evaluation_at],
            )
            .unwrap();

        assert!(storage.claim_strategy_action().unwrap().is_none());
        let execution_received_at = storage
            .position_evidence_state("DU123", 756733)
            .unwrap()
            .latest_execution_received_at
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSubscriptionHeartbeat {
                subscription_id: initial_subscription_id,
                observed_at: execution_received_at + chrono::Duration::milliseconds(1),
            })
            .unwrap();
        // A lease heartbeat does not prove that the old quantity reflects the
        // sell fill, so it must not release the evaluation.
        assert!(storage.claim_strategy_action().unwrap().is_none());

        let fresh_subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id: fresh_subscription_id,
                observed_at: execution_received_at + chrono::Duration::milliseconds(2),
            })
            .unwrap();
        // Model IBKR's empty snapshot representation where a flat contract has
        // no positions_current row at all.
        storage
            .connection
            .execute(
                "DELETE FROM positions_current WHERE account_id = 'DU123' AND conid = 756733",
                [],
            )
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id: fresh_subscription_id,
                observed_at: execution_received_at + chrono::Duration::milliseconds(3),
            })
            .unwrap();

        assert!(storage.claim_strategy_action().unwrap().is_none());
        let action = storage
            .list_strategy_execution_actions(20)
            .unwrap()
            .into_iter()
            .find(|action| action["evaluation_id"] == serde_json::json!(evaluation_id))
            .unwrap();
        assert_eq!(action["state"], "skipped");
        assert_eq!(action["detail"], "signal requires no position change");
    }

    #[test]
    fn strategy_risk_capital_gate_blocks_opening_but_never_traps_an_exit() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        storage
            .configure_strategy_risk_control(&StrategyRiskControlInput {
                strategy_id,
                enabled: true,
                strategy_capital: 100.0,
                capital_currency: Some("USD".into()),
                maximum_position_capital_ratio: 1.0,
                maximum_rolling_24h_realized_net_loss_ratio: 0.02,
                maximum_consecutive_net_losing_trades: 3,
                maximum_rolling_24h_completed_trades: 10,
                maximum_rolling_24h_turnover_capital_ratio: 10.0,
            })
            .unwrap();
        let now = Utc::now();
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .add_market_data_subscription(&spy_contract())
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataStatus {
                conid: 756733,
                state: "active".into(),
                error: None,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::MarketDataTick {
                conid: 756733,
                tick_type: "Bid".into(),
                numeric_value: Some(100.0),
                text_value: None,
                observed_at: now,
            })
            .unwrap();

        insert_buy_evaluation(&mut storage, strategy_id, now);
        assert!(
            storage
                .claim_strategy_action_with_risk(
                    "USD",
                    3_600,
                    30,
                    now + chrono::Duration::milliseconds(1),
                )
                .unwrap()
                .is_none()
        );
        let actions = storage.list_strategy_execution_actions(10).unwrap();
        assert_eq!(actions[0]["state"], "skipped");
        assert_eq!(actions[0]["cost_gate_result"], "risk_blocked");
        assert!(
            actions[0]["detail"]
                .as_str()
                .unwrap()
                .contains("超过策略资本上限")
        );
        assert!(storage.list_strategy_execution_configs().unwrap()[0]["enabled"] == true);

        // Once a position exists, a target-to-flat signal remains claimable
        // even if all opening-risk market data has become stale.
        let exit_time = now + chrono::Duration::hours(1);
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 756733,
                    symbol: "SPY".into(),
                    security_type: "STK".into(),
                    currency: "USD".into(),
                    exchange: "ARCA".into(),
                    quantity: 3.0,
                    average_cost: 100.0,
                    observed_at: exit_time,
                },
            })
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_evaluations
                 VALUES (?, ?, 756733, ?, 210, 100, 90, 200, 'sell', ?, '{}')",
                params![uuid::Uuid::now_v7(), strategy_id, exit_time, exit_time],
            )
            .unwrap();
        let exit = storage
            .claim_strategy_action_with_risk(
                "USD",
                3_600,
                30,
                exit_time + chrono::Duration::milliseconds(1),
            )
            .unwrap()
            .expect("a strict reduction must not be trapped by opening-risk gates");
        assert_eq!(exit.side, "sell");
        assert_eq!(exit.quantity, 3.0);
        assert!(exit.legs[0].is_risk_reducing());
    }

    #[test]
    fn resetting_risk_baseline_does_not_erase_rolling_24h_losses() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            101,
            "buy",
            10.0,
            100.0,
            1.0,
            now - chrono::Duration::hours(2),
        );
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            102,
            "sell",
            10.0,
            90.0,
            1.0,
            now - chrono::Duration::hours(1),
        );

        assert!(
            storage
                .reset_strategy_risk_statistics(&StrategyRiskResetInput {
                    strategy_id,
                    confirm: true,
                    note: "reviewed rolling loss before reset".into(),
                })
                .unwrap()
        );
        let controls = storage
            .list_strategy_risk_controls("USD", 3_600, now + chrono::Duration::seconds(1))
            .unwrap();
        assert_eq!(
            controls[0]["statistics_reset_note"],
            "reviewed rolling loss before reset"
        );
        let statistics = &controls[0]["statistics"];
        assert_eq!(statistics["data_complete"], true);
        assert_eq!(statistics["rolling_24h_completed_trades"], 1);
        assert_eq!(statistics["rolling_24h_realized_net_pnl"], -102.0);
        assert_eq!(statistics["rolling_24h_turnover"], 1_900.0);
        assert_eq!(statistics["consecutive_net_losing_trades"], 0);
        assert_eq!(statistics["completed_trades_since_reset"], 0);
    }

    #[test]
    fn old_incomplete_opening_leg_taints_a_cycle_closed_in_the_active_window() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        storage
            .connection
            .execute(
                "UPDATE strategy_risk_controls SET statistics_reset_at = ?
                 WHERE strategy_id = ?",
                params![now - chrono::Duration::hours(72), strategy_id],
            )
            .unwrap();
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            201,
            "buy",
            10.0,
            100.0,
            1.0,
            now - chrono::Duration::hours(48),
        );
        storage
            .connection
            .execute(
                "UPDATE executions SET commission = NULL
                 WHERE broker_execution_id = 'risk-statistics.execution.201'",
                [],
            )
            .unwrap();
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            202,
            "sell",
            10.0,
            90.0,
            1.0,
            now - chrono::Duration::hours(1),
        );

        let controls = storage
            .list_strategy_risk_controls("USD", 3_600, now)
            .unwrap();
        let statistics = &controls[0]["statistics"];
        assert_eq!(statistics["data_complete"], false);
        assert!(
            statistics["warning"]
                .as_str()
                .unwrap()
                .contains("active statistics window")
        );
    }

    #[test]
    fn rolling_realized_loss_includes_partial_closes_before_the_cycle_is_flat() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            211,
            "buy",
            10.0,
            100.0,
            1.0,
            now - chrono::Duration::hours(2),
        );
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            212,
            "sell",
            5.0,
            90.0,
            1.0,
            now - chrono::Duration::hours(1),
        );

        let controls = storage
            .list_strategy_risk_controls("USD", 3_600, now)
            .unwrap();
        let statistics = &controls[0]["statistics"];
        assert_eq!(statistics["data_complete"], true);
        assert_eq!(statistics["rolling_24h_realized_net_pnl"], -52.0);
        assert_eq!(statistics["rolling_24h_completed_trades"], 0);
    }

    #[test]
    fn long_only_risk_statistics_do_not_invent_a_short_from_unmatched_history() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            221,
            "sell",
            3.0,
            100.0,
            1.0,
            now - chrono::Duration::minutes(1),
        );
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET allow_short = true, short_target_quantity = -3
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();

        let controls = storage
            .list_strategy_risk_controls("USD", 3_600, now)
            .unwrap();
        let statistics = &controls[0]["statistics"];
        assert_eq!(statistics["data_complete"], false);
        assert!(
            statistics["warning"]
                .as_str()
                .unwrap()
                .contains("historical action-leg target")
        );
        assert_eq!(statistics["rolling_24h_completed_trades"], 0);
    }

    #[test]
    fn historical_short_replay_uses_action_leg_targets_not_current_allow_short() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET allow_short = true, short_target_quantity = -3
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let now = Utc::now() + chrono::Duration::seconds(1);
        record_attributed_spy_fill_with_target(
            &mut storage,
            strategy_id,
            231,
            "sell",
            3.0,
            100.0,
            1.0,
            now - chrono::Duration::minutes(2),
            Some(-3.0),
        );
        record_attributed_spy_fill_with_target(
            &mut storage,
            strategy_id,
            232,
            "buy",
            3.0,
            90.0,
            1.0,
            now - chrono::Duration::minutes(1),
            Some(0.0),
        );

        let risk_before = storage
            .list_strategy_risk_controls("USD", 3_600, now)
            .unwrap()[0]["statistics"]
            .clone();
        let performance_before = storage
            .strategy_performance_report(strategy_id, 100_000.0, "USD", 3_600, 30, 120, None, now)
            .unwrap();
        assert_eq!(risk_before["data_complete"], true);
        assert_eq!(risk_before["rolling_24h_completed_trades"], 1);
        assert_eq!(risk_before["rolling_24h_realized_net_pnl"], 28.0);
        assert_eq!(performance_before["data_complete"], true);
        assert_eq!(performance_before["realized_trade_count"], 1);
        assert_eq!(performance_before["gross_pnl"], 30.0);
        assert_eq!(performance_before["net_pnl"], 28.0);

        // Disabling shorting later changes future order generation only. The
        // completed historical short remains authorized by its persisted -3
        // action-leg target and must produce byte-for-byte equivalent metrics.
        storage
            .connection
            .execute(
                "UPDATE strategy_execution_configs
                 SET allow_short = false, short_target_quantity = 0
                 WHERE strategy_id = ?",
                params![strategy_id],
            )
            .unwrap();
        let risk_after = storage
            .list_strategy_risk_controls("USD", 3_600, now)
            .unwrap()[0]["statistics"]
            .clone();
        let performance_after = storage
            .strategy_performance_report(strategy_id, 100_000.0, "USD", 3_600, 30, 120, None, now)
            .unwrap();
        assert_eq!(risk_after, risk_before);
        for key in [
            "data_complete",
            "gross_pnl",
            "commissions",
            "net_pnl",
            "realized_trade_count",
            "winning_trade_count",
            "losing_trade_count",
            "unmatched_execution_quantity",
        ] {
            assert_eq!(performance_after[key], performance_before[key], "{key}");
        }
    }

    #[test]
    fn completed_order_fill_evidence_detects_missing_executions_even_when_status_is_submitted() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "BUY".into(),
            quantity: 10.0,
            order_type: "MKT".into(),
            limit_price: None,
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent(
                "submitted-with-completed-fill",
                "DU123",
                &request,
                "accepted",
                None,
            )
            .unwrap();
        let now = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO strategy_execution_actions
                 (action_id, strategy_id, evaluation_id, idempotency_key, signal,
                  requested_quantity, state, order_intent_id, broker_order_id,
                  created_at, updated_at)
                 VALUES (?, ?, ?, 'submitted-with-completed-fill', 'buy', 10,
                         'submitted', ?, 301, ?, ?)",
                params![
                    uuid::Uuid::now_v7(),
                    strategy_id,
                    uuid::Uuid::now_v7(),
                    intent_id,
                    now,
                    now
                ],
            )
            .unwrap();
        let session = uuid::Uuid::now_v7();
        storage
            .record_submitted_order(intent_id, 301, session)
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OpenOrder {
                connection_session_id: Some(session),
                broker_order_id: 301,
                perm_id: 301,
                status: "Submitted".into(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: "20260808 12:00:00 Asia/Shanghai".into(),
                completed_status: "Filled Size: 10".into(),
            })
            .unwrap();

        let order = &storage.list_orders_page(1, 10).unwrap().0[0];
        assert_eq!(order["status"], "Filled");
        assert_eq!(order["filled_quantity"], 10.0);

        let controls = storage
            .list_strategy_risk_controls("USD", 3_600, now)
            .unwrap();
        assert_eq!(controls[0]["statistics"]["data_complete"], false);
        assert!(
            controls[0]["statistics"]["warning"]
                .as_str()
                .unwrap()
                .contains("missing execution details")
        );
    }

    #[test]
    fn risk_statistics_reset_rejects_an_open_attributed_position_cycle() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            401,
            "buy",
            10.0,
            100.0,
            1.0,
            Utc::now(),
        );

        let error = storage
            .reset_strategy_risk_statistics(&StrategyRiskResetInput {
                strategy_id,
                confirm: true,
                note: "must not split an open cycle".into(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("attributed position cycle"));
    }

    #[test]
    fn performance_counts_a_flat_price_round_trip_as_a_net_commission_loss() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let now = Utc::now();
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            501,
            "buy",
            10.0,
            100.0,
            1.0,
            now - chrono::Duration::minutes(2),
        );
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            502,
            "sell",
            10.0,
            100.0,
            1.0,
            now - chrono::Duration::minutes(1),
        );

        let report = storage
            .strategy_performance_report(strategy_id, 100_000.0, "USD", 3_600, 30, 120, None, now)
            .unwrap();
        assert_eq!(report["gross_pnl"], 0.0);
        assert_eq!(report["net_pnl"], -2.0);
        assert_eq!(report["realized_trade_count"], 1);
        assert_eq!(report["winning_trade_count"], 0);
        assert_eq!(report["losing_trade_count"], 1);
    }

    #[test]
    fn missing_fx_rate_for_a_position_blocks_opening_risk() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        let eur_contract = crate::ibkr::ContractCandidate {
            conid: 999001,
            symbol: "SAP".into(),
            security_type: "STK".into(),
            currency: "EUR".into(),
            exchange: "IBIS".into(),
            primary_exchange: "IBIS".into(),
            local_symbol: "SAP".into(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };
        storage.upsert_instrument(&eur_contract).unwrap();
        let subscription_id = uuid::Uuid::now_v7();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotStarted {
                subscription_id,
                observed_at: now,
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::Position {
                subscription_id,
                position: crate::ibkr::PositionSnapshot {
                    account: "DU123".into(),
                    conid: 999001,
                    symbol: "SAP".into(),
                    security_type: "STK".into(),
                    currency: "EUR".into(),
                    exchange: "IBIS".into(),
                    quantity: 10.0,
                    average_cost: 50.0,
                    observed_at: now,
                },
            })
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::PositionSnapshotCompleted {
                subscription_id,
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
        let opening = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "buy".into(),
            quantity: 1.0,
            order_type: "limit".into(),
            limit_price: Some(100.0),
            outside_rth: false,
        };
        // No EUR->USD FX rate exists: the EUR position cannot be converted,
        // so opening risk fails closed instead of understating exposure.
        let decision = storage
            .evaluate_portfolio_risk(
                &crate::config::RiskConfig::default(),
                "DU123",
                &opening,
                None,
                Some(100.0),
                false,
                now,
            )
            .unwrap();
        assert_eq!(decision.reason_code, "FX_RATE_UNAVAILABLE");
        // A strictly risk-reducing close still bypasses the FX gate.
        let closing = crate::ibkr::BrokerOrderRequest {
            contract: eur_contract,
            side: "sell".into(),
            quantity: 1.0,
            order_type: "limit".into(),
            limit_price: Some(50.0),
            outside_rth: false,
        };
        assert!(
            storage
                .evaluate_portfolio_risk(
                    &crate::config::RiskConfig::default(),
                    "DU123",
                    &closing,
                    None,
                    None,
                    true,
                    now,
                )
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn late_out_of_order_ticks_never_rewrite_final_bars() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        storage
            .update_market_minute_bar(756733, 100.0, start + chrono::Duration::seconds(5))
            .unwrap();
        // First tick of the next minute finalises the previous bar.
        storage
            .update_market_minute_bar(756733, 101.0, start + chrono::Duration::seconds(65))
            .unwrap();
        // A late tick for the already-final bucket must be discarded.
        storage
            .update_market_minute_bar(756733, 999.0, start + chrono::Duration::seconds(30))
            .unwrap();
        let (close, high, tick_count, is_final): (f64, f64, i64, bool) = storage
            .connection
            .query_row(
                "SELECT close, high, tick_count, final FROM market_minute_bars
                 WHERE conid = 756733 ORDER BY bar_time LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert!(is_final);
        assert_eq!(close, 100.0);
        assert_eq!(high, 100.0);
        assert_eq!(tick_count, 1);
    }

    #[test]
    fn late_order_status_never_demotes_a_terminal_order_or_erases_perm_id() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let session_id = uuid::Uuid::now_v7();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "BUY".into(),
            quantity: 5.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("terminal-guard", "DU123", &request, "approved", None)
            .unwrap();
        storage
            .record_submitted_order(intent_id, 42, session_id)
            .unwrap();
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OrderStatus {
                connection_session_id: Some(session_id),
                broker_order_id: 42,
                status: "Filled".into(),
                filled: 5.0,
                remaining: 0.0,
                average_fill_price: Some(500.0),
                last_fill_price: Some(500.0),
                perm_id: 777,
                why_held: String::new(),
                market_cap_price: None,
            })
            .unwrap();
        // A replayed pre-fill status arriving late must not resurrect the
        // order, regress its filled quantity or zero out the perm id.
        storage
            .apply_broker_event(&crate::ibkr::BrokerEvent::OrderStatus {
                connection_session_id: Some(session_id),
                broker_order_id: 42,
                status: "Submitted".into(),
                filled: 0.0,
                remaining: 5.0,
                average_fill_price: None,
                last_fill_price: None,
                perm_id: 0,
                why_held: String::new(),
                market_cap_price: None,
            })
            .unwrap();
        let (status, filled_quantity, perm_id): (String, f64, i64) = storage
            .connection
            .query_row(
                "SELECT status, filled_quantity, broker_perm_id FROM orders
                 WHERE broker_order_id = 42",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "Filled");
        assert_eq!(filled_quantity, 5.0);
        assert_eq!(perm_id, 777);
    }

    #[test]
    fn unknown_intents_require_manual_resolution_with_a_note() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "BUY".into(),
            quantity: 1.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        let intent_id = storage
            .create_order_intent("resolve-me", "DU123", &request, "approved", None)
            .unwrap();
        // Only 'unknown' intents can be resolved.
        assert!(storage.resolve_order_intent(intent_id, "note").is_err());
        storage
            .mark_order_intent_unknown(intent_id, "ack timeout")
            .unwrap();
        let (unknown, total) = storage.list_unknown_order_intents_page(1, 25).unwrap();
        assert_eq!(total, 1);
        assert_eq!(unknown[0]["order_intent_id"], intent_id.to_string());
        assert_eq!(unknown[0]["account_id"], "DU123");
        assert_eq!(unknown[0]["side"], "BUY");
        assert_eq!(unknown[0]["quantity"], 1.0);
        assert_eq!(unknown[0]["reason"], "ack timeout");
        assert!(storage.resolve_order_intent(intent_id, "  ").is_err());
        let resolved = storage
            .resolve_order_intent(intent_id, "verified against IBKR: not open, no fills")
            .unwrap();
        assert_eq!(resolved["status"], "resolved_manual");
        assert_eq!(storage.list_unknown_order_intents_page(1, 25).unwrap().1, 0);
        // Resolution is final and audited.
        assert!(storage.resolve_order_intent(intent_id, "again").is_err());
        let audit: i64 = storage
            .connection
            .query_row(
                "SELECT count(*) FROM risk_decisions
                 WHERE order_intent_id = ? AND reason_code = 'MANUAL_RESOLUTION'",
                params![intent_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit, 1);
    }

    #[test]
    fn stale_approved_intents_become_unknown_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.duckdb");
        let request = crate::ibkr::BrokerOrderRequest {
            contract: spy_contract(),
            side: "BUY".into(),
            quantity: 1.0,
            order_type: "LMT".into(),
            limit_price: Some(500.0),
            outside_rth: false,
        };
        {
            let mut storage = Storage::open(&path).unwrap();
            storage
                .create_order_intent("crashed-in-flight", "DU123", &request, "approved", None)
                .unwrap();
        }
        let storage = Storage::open(&path).unwrap();
        let status: String = storage
            .connection
            .query_row(
                "SELECT status FROM order_intents WHERE idempotency_key = 'crashed-in-flight'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "unknown");
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
            fx_rate_pair: None,
        };
        let job_id = storage.create_backfill_job(&request).unwrap().job_id;
        let claimed = storage.claim_backfill_job().unwrap().unwrap();
        assert_eq!(claimed.job_id, job_id);
        storage
            .advance_backfill_job(job_id, start + chrono::Duration::days(1), end)
            .unwrap();
        assert_eq!(storage.list_data_jobs(true).unwrap()[0]["state"], "pending");
        let claimed = storage.claim_backfill_job().unwrap().unwrap();
        storage
            .advance_backfill_job(job_id, end, claimed.request.end)
            .unwrap();
        assert_eq!(
            storage.list_data_jobs(true).unwrap()[0]["state"],
            "completed"
        );
        let coverage = storage
            .historical_coverage(756733, "1m", start, end)
            .unwrap();
        assert_eq!(coverage["covered"], true);
        assert_eq!(coverage["verified"], true);
        assert_eq!(coverage["backtest_ready"], false);
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
    fn overlapping_active_backfill_requests_reuse_and_expand_the_oldest_job() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut request = BackfillJobRequest {
            contract: spy_contract(),
            timeframe: "5s".into(),
            start,
            end: start + chrono::Duration::days(7),
            outside_rth: false,
            fx_rate_pair: None,
        };
        let first = storage.create_backfill_job(&request).unwrap();
        assert!(!first.reused);
        let claimed = storage.claim_backfill_job().unwrap().unwrap();

        request.start += chrono::Duration::days(6);
        request.end += chrono::Duration::days(2);
        let second = storage.create_backfill_job(&request).unwrap();
        assert_eq!(second.job_id, first.job_id);
        assert!(second.reused);
        assert!(second.range_expanded);
        storage
            .advance_backfill_job(first.job_id, claimed.request.end, claimed.request.end)
            .unwrap();

        let jobs = storage.list_data_jobs(true).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["state"], "pending");
        assert_eq!(jobs[0]["runtime_state"], "running");
        assert_eq!(jobs[0]["queue_position"], 1);
        assert_eq!(
            jobs[0].pointer("/request/end").and_then(Value::as_str),
            Some("2026-07-10T00:00:00Z")
        );
    }

    #[test]
    fn missing_backfill_creation_skips_verified_ranges_and_splits_gaps() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let day = chrono::Duration::days(1);
        mark_backfill_range_verified(&mut storage, "5s", start, start + day * 2, false);
        mark_backfill_range_verified(&mut storage, "5s", start + day * 3, start + day * 4, false);

        let mut contract = spy_contract();
        // Contract routing can change between IBKR sessions without changing
        // the historical data identity represented by the conid.
        contract.exchange = "ARCA".into();
        let request = BackfillJobRequest {
            contract,
            timeframe: "5s".into(),
            start,
            end: start + day * 5,
            outside_rth: false,
            fx_rate_pair: None,
        };
        let created = storage.create_unverified_backfill_jobs(&request).unwrap();
        assert_eq!(created.len(), 2);
        assert_eq!(created[0].0.start, start + day * 2);
        assert_eq!(created[0].0.end, start + day * 3);
        assert_eq!(created[1].0.start, start + day * 4);
        assert_eq!(created[1].0.end, start + day * 5);

        for (gap, creation) in created {
            storage
                .advance_backfill_job(creation.job_id, gap.end, gap.end)
                .unwrap();
        }
        assert!(
            storage
                .create_unverified_backfill_jobs(&request)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn historical_fx_jobs_for_different_pairs_never_merge() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = "2026-07-29T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let mut request = BackfillJobRequest {
            contract: crate::ibkr::ContractCandidate {
                conid: 0,
                symbol: "USD".into(),
                security_type: "CASH".into(),
                currency: "HKD".into(),
                exchange: "IDEALPRO".into(),
                primary_exchange: String::new(),
                local_symbol: "USD.HKD".into(),
                description: String::new(),
                derivative_security_types: Vec::new(),
            },
            timeframe: "1m".into(),
            start,
            end: start + chrono::Duration::days(1),
            outside_rth: true,
            fx_rate_pair: Some(FxRateBackfillTarget {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
            }),
        };
        let usd = storage.create_backfill_job(&request).unwrap();
        request.contract.symbol = "EUR".into();
        request.contract.local_symbol = "EUR.HKD".into();
        request.fx_rate_pair = Some(FxRateBackfillTarget {
            base_currency: "EUR".into(),
            quote_currency: "HKD".into(),
        });
        let eur = storage.create_backfill_job(&request).unwrap();

        assert_ne!(usd.job_id, eur.job_id);
        assert_eq!(storage.data_job_queue_status().unwrap().1, 2);
    }

    #[test]
    fn data_jobs_report_effective_runtime_state_and_queue_position() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let first_request = BackfillJobRequest {
            contract: spy_contract(),
            timeframe: "5s".into(),
            start,
            end: start + chrono::Duration::days(1),
            outside_rth: false,
            fx_rate_pair: None,
        };
        let mut second_request = first_request.clone();
        second_request.start = first_request.end + chrono::Duration::hours(1);
        second_request.end = second_request.start + chrono::Duration::days(1);
        let first = storage.create_backfill_job(&first_request).unwrap();
        let second = storage.create_backfill_job(&second_request).unwrap();

        let jobs = storage.list_data_jobs(true).unwrap();
        let first_job = jobs
            .iter()
            .find(|job| job["job_id"] == serde_json::json!(first.job_id))
            .unwrap();
        let second_job = jobs
            .iter()
            .find(|job| job["job_id"] == serde_json::json!(second.job_id))
            .unwrap();
        assert_eq!(first_job["runtime_state"], "running");
        assert_eq!(first_job["queue_position"], 1);
        assert_eq!(first_job["jobs_ahead"], 0);
        assert_eq!(second_job["runtime_state"], "queued");
        assert_eq!(second_job["queue_position"], 2);
        assert_eq!(second_job["jobs_ahead"], 1);

        let disconnected = storage.list_data_jobs(false).unwrap();
        let first_job = disconnected
            .iter()
            .find(|job| job["job_id"] == serde_json::json!(first.job_id))
            .unwrap();
        assert_eq!(first_job["runtime_state"], "waiting_for_ibkr");
    }

    #[test]
    fn data_jobs_can_be_paginated_without_losing_queue_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let start = DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let first_request = BackfillJobRequest {
            contract: spy_contract(),
            timeframe: "5s".into(),
            start,
            end: start + chrono::Duration::hours(1),
            outside_rth: false,
            fx_rate_pair: None,
        };
        let mut second_request = first_request.clone();
        second_request.start += chrono::Duration::hours(2);
        second_request.end += chrono::Duration::hours(2);
        let first = storage.create_backfill_job(&first_request).unwrap();
        let second = storage.create_backfill_job(&second_request).unwrap();

        let (first_page, total) = storage.list_data_jobs_page(true, 1, 1).unwrap();
        let (second_page, second_total) = storage.list_data_jobs_page(true, 2, 1).unwrap();
        assert_eq!(total, 2);
        assert_eq!(second_total, 2);
        assert_eq!(first_page.len(), 1);
        assert_eq!(second_page.len(), 1);
        assert_ne!(first_page[0]["job_id"], second_page[0]["job_id"]);
        assert!(matches!(
            first_page[0]["queue_position"].as_u64(),
            Some(1 | 2)
        ));
        assert!(matches!(
            second_page[0]["queue_position"].as_u64(),
            Some(1 | 2)
        ));
        assert_eq!(storage.data_job_queue_status().unwrap().1, 2);
        assert!(matches!(
            storage.data_job_queue_status().unwrap().0,
            Some(id) if id == first.job_id || id == second.job_id
        ));
        assert_ne!(first.job_id, second.job_id);
    }

    #[test]
    fn opening_storage_collapses_overlapping_jobs_from_older_versions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.duckdb");
        let start = DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let request = BackfillJobRequest {
            contract: spy_contract(),
            timeframe: "5s".into(),
            start,
            end: start + chrono::Duration::days(7),
            outside_rth: false,
            fx_rate_pair: None,
        };
        let primary_id;
        {
            let mut storage = Storage::open(&path).unwrap();
            primary_id = storage.create_backfill_job(&request).unwrap().job_id;
            let mut duplicate = request.clone();
            duplicate.start += chrono::Duration::days(6);
            duplicate.end += chrono::Duration::days(2);
            let now = Utc::now() + chrono::Duration::milliseconds(1);
            storage
                .connection
                .execute(
                    "INSERT INTO data_jobs VALUES
                     (?, 'historical_backfill', 'pending', ?, ?, ?, 0, 0, NULL, ?, ?)",
                    params![
                        uuid::Uuid::now_v7(),
                        serde_json::to_string(&duplicate).unwrap(),
                        duplicate.start,
                        duplicate.end,
                        now,
                        now
                    ],
                )
                .unwrap();
        }

        let storage = Storage::open(&path).unwrap();
        let jobs = storage.list_data_jobs(true).unwrap();
        assert_eq!(
            jobs.iter()
                .filter(|job| {
                    matches!(
                        job.get("state").and_then(Value::as_str),
                        Some("pending" | "retrying" | "running")
                    )
                })
                .count(),
            1
        );
        let primary = jobs
            .iter()
            .find(|job| job["job_id"] == serde_json::json!(primary_id))
            .unwrap();
        assert_eq!(
            primary.pointer("/request/end").and_then(Value::as_str),
            Some("2026-07-10T00:00:00Z")
        );
    }

    #[test]
    fn required_fx_currencies_are_discovered_from_current_positions() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        storage
            .connection
            .execute(
                "INSERT INTO instruments
                 (instrument_id, conid, symbol, security_type, currency, exchange,
                  created_at, updated_at, primary_exchange, local_symbol, description)
                 VALUES (?, 272093, 'MSFT', 'STK', 'usd', 'NASDAQ', ?, ?,
                         'NASDAQ', 'MSFT', 'MICROSOFT CORP')",
                params![uuid::Uuid::now_v7(), now, now],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO positions_current VALUES ('DU123', 272093, 10, 450, ?)",
                params![now],
            )
            .unwrap();

        assert_eq!(storage.required_fx_currencies("HKD").unwrap(), ["USD"]);
        assert!(storage.required_fx_currencies("US").is_err());
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
    fn historical_fx_lookup_uses_the_quote_known_at_execution_time() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let first = "2026-08-08T01:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let second = first + chrono::Duration::minutes(1);
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.70,
                source: "test-first".into(),
                observed_at: first,
            })
            .unwrap();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.80,
                source: "test-second".into(),
                observed_at: second,
            })
            .unwrap();

        assert_eq!(
            storage
                .currency_conversion_rate_at(
                    "USD",
                    "HKD",
                    300,
                    first + chrono::Duration::seconds(30),
                )
                .unwrap(),
            Some(7.70)
        );
        assert_eq!(
            storage
                .currency_conversion_rate("USD", "HKD", 300, second)
                .unwrap(),
            Some(7.80)
        );
    }

    #[test]
    fn historical_fx_midpoint_becomes_visible_only_after_its_bar_closes() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let open_time = "2026-07-29T15:36:00Z".parse::<DateTime<Utc>>().unwrap();
        let bars = [crate::ibkr::HistoricalBar {
            conid: 0,
            timeframe: "1m".into(),
            open_time,
            open: 7.80,
            high: 7.81,
            low: 7.79,
            close: 7.805,
            volume: 0.0,
            wap: 0.0,
            trade_count: 0,
        }];
        assert_eq!(
            storage
                .write_historical_fx_bars(
                    &FxRateBackfillTarget {
                        base_currency: "USD".into(),
                        quote_currency: "HKD".into(),
                    },
                    &bars,
                )
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .currency_conversion_rate_at(
                    "USD",
                    "HKD",
                    3_600,
                    open_time + chrono::Duration::seconds(30),
                )
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .currency_conversion_rate_at(
                    "USD",
                    "HKD",
                    3_600,
                    open_time + chrono::Duration::minutes(1),
                )
                .unwrap(),
            Some(7.805)
        );
        assert!(storage.list_fx_rates().unwrap().is_empty());
    }

    #[test]
    fn strategy_fx_repair_queues_only_the_missing_execution_time_range() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let strategy_id = configure_spy_execution(&mut storage);
        let executed_at = duckdb_timestamp(Utc::now() - chrono::Duration::minutes(10));
        record_attributed_spy_fill(
            &mut storage,
            strategy_id,
            501,
            "buy",
            10.0,
            100.0,
            1.0,
            executed_at,
        );

        let (gaps, jobs) = storage
            .create_strategy_historical_fx_jobs(strategy_id, "HKD", 3_600)
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].base_currency, "USD");
        assert_eq!(gaps[0].quote_currency, "HKD");
        assert_eq!(gaps[0].start, executed_at - chrono::Duration::hours(1));
        assert_eq!(gaps[0].end, executed_at + chrono::Duration::minutes(2));
        assert_eq!(gaps[0].affected_execution_values, 1);
        assert_eq!(jobs.len(), 1);
        let claimed = storage.claim_backfill_job().unwrap().unwrap();
        assert_eq!(claimed.request.contract.security_type, "CASH");
        assert_eq!(claimed.request.contract.exchange, "IDEALPRO");
        assert_eq!(
            claimed.request.fx_rate_pair,
            Some(FxRateBackfillTarget {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
            })
        );
    }

    #[test]
    fn stale_fx_upsert_is_kept_in_history_without_regressing_the_latest_quote() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        let older = now - chrono::Duration::minutes(2);
        let newer = now - chrono::Duration::minutes(1);
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.80,
                source: "newer".into(),
                observed_at: newer,
            })
            .unwrap();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.70,
                source: "late-old-sample".into(),
                observed_at: older,
            })
            .unwrap();

        let latest = storage.list_fx_rates().unwrap();
        assert_eq!(latest[0]["rate"], 7.80);
        assert_eq!(latest[0]["source"], "newer");
        assert_eq!(
            storage
                .currency_conversion_rate_at(
                    "USD",
                    "HKD",
                    300,
                    older + chrono::Duration::seconds(30),
                )
                .unwrap(),
            Some(7.70)
        );
    }

    #[test]
    fn expired_direct_fx_quote_falls_back_to_a_fresh_inverse_quote() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.70,
                source: "expired-direct".into(),
                observed_at: now - chrono::Duration::minutes(10),
            })
            .unwrap();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "HKD".into(),
                quote_currency: "USD".into(),
                rate: 0.128,
                source: "fresh-inverse".into(),
                observed_at: now - chrono::Duration::seconds(30),
            })
            .unwrap();

        assert_eq!(
            storage
                .currency_conversion_rate("USD", "HKD", 60, now)
                .unwrap(),
            Some(1.0 / 0.128)
        );
        assert_eq!(
            storage
                .currency_conversion_rate_at("USD", "HKD", 60, now)
                .unwrap(),
            Some(1.0 / 0.128)
        );
    }

    #[test]
    fn future_fx_quotes_are_never_fresh_and_excessive_future_skew_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(&directory.path().join("state.duckdb")).unwrap();
        let now = Utc::now();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 7.80,
                source: "current".into(),
                observed_at: now - chrono::Duration::seconds(30),
            })
            .unwrap();
        storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 99.0,
                source: "slightly-future".into(),
                observed_at: now + chrono::Duration::seconds(30),
            })
            .unwrap();

        // The tolerated future sample may be retained for audit, but cannot
        // replace the most recent quote that was actually known at `now`.
        assert_eq!(
            storage
                .currency_conversion_rate("USD", "HKD", 300, now)
                .unwrap(),
            Some(7.80)
        );
        let error = storage
            .upsert_fx_rate(&FxRateInput {
                base_currency: "USD".into(),
                quote_currency: "HKD".into(),
                rate: 100.0,
                source: "far-future".into(),
                observed_at: now + chrono::Duration::minutes(10),
            })
            .unwrap_err();
        assert!(error.to_string().contains("future"));
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
        complete_empty_position_snapshot(&mut storage);
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
