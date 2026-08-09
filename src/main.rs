mod config;
mod error;
mod ibkr;
mod process_lock;
mod risk;
mod rpc;
mod storage;
mod strategy;
mod telemetry;
mod web_server;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    error::Result,
    process_lock::ProcessLock,
    rpc::{RpcServer, SystemState, SystemStatus},
    storage::{Storage, StorageMutexExt},
};

#[derive(Debug, Parser)]
#[command(
    name = "quant",
    version,
    about = "Personal quantitative trading platform"
)]
struct Cli {
    #[arg(long, global = true, env = "QUANT_CONFIG")]
    config: Option<PathBuf>,

    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the long-lived background process.
    Daemon,
    /// Show daemon health and component status.
    Status,
    /// Show operational health, queue counts, and storage sizes.
    Health,
    /// Gracefully stop the daemon.
    Shutdown,
    /// Show daemon and RPC protocol versions.
    Version,
    /// Manage the IBKR connection.
    Ibkr {
        #[command(subcommand)]
        command: IbkrCommand,
    },
    /// Query account information from the active IBKR session.
    Account {
        #[command(subcommand)]
        command: AccountCommand,
    },
    /// Search and inspect IBKR contracts.
    Instrument {
        #[command(subcommand)]
        command: InstrumentCommand,
    },
    /// Query the current portfolio.
    Positions,
    /// List locally persisted executions.
    Executions,
    /// Reconcile local order state with IBKR.
    Reconcile {
        #[command(subcommand)]
        command: Option<ReconcileCommand>,
    },
    /// Historical market-data operations.
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
    /// Manage streaming market-data subscriptions and inspect cached quotes.
    MarketData {
        #[command(subcommand)]
        command: MarketDataCommand,
    },
    /// Create and operate durable strategy instances.
    Strategy {
        #[command(subcommand)]
        command: StrategyCommand,
    },
    /// Run and inspect deterministic simulations over local Parquet bars.
    Backtest {
        #[command(subcommand)]
        command: BacktestCommand,
    },
    /// Create and inspect recoverable local backups.
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// Inspect and operate persistent trading safety controls.
    Safety {
        #[command(subcommand)]
        command: SafetyCommand,
    },
    /// Preview or submit a risk-checked order.
    Order {
        #[command(subcommand)]
        command: OrderCommand,
    },
    /// Strategy-level net performance and equity snapshots.
    Performance {
        #[command(subcommand)]
        command: PerformanceCommand,
    },
    /// Manage currency conversion rates used by risk and reporting.
    Fx {
        #[command(subcommand)]
        command: FxCommand,
    },
    /// Manage explicit UTC market sessions and holidays.
    Calendar {
        #[command(subcommand)]
        command: CalendarCommand,
    },
    /// Inspect metrics and persistent operational alerts.
    Monitor {
        #[command(subcommand)]
        command: MonitorCommand,
    },
}

#[derive(Debug, Subcommand)]
enum IbkrCommand {
    /// Show the current IBKR connection state.
    Status,
    /// Request a connection to TWS or IB Gateway.
    Connect,
    /// Disconnect and disable automatic reconnect.
    Disconnect,
}

#[derive(Debug, Subcommand)]
enum AccountCommand {
    /// List accounts managed by the connected TWS or IB Gateway session.
    Managed,
    /// Show the latest continuously synchronized account summary.
    Summary,
    /// Show the latest continuously synchronized account PnL.
    Pnl,
}

#[derive(Debug, Subcommand)]
enum InstrumentCommand {
    /// Search matching IBKR contracts.
    Search { pattern: String },
    /// List locally persisted instruments and internal IDs.
    List,
}

#[derive(Debug, Subcommand)]
enum ReconcileCommand {
    /// Show readiness for the active IBKR connection session.
    Status,
    /// List persisted reconciliation differences.
    Differences,
    /// Acknowledge a difference after operator review; does not unblock trading.
    Acknowledge {
        #[arg(long)]
        difference_id: uuid::Uuid,
        #[arg(long)]
        note: String,
    },
}

#[derive(Debug, Subcommand)]
enum DataCommand {
    /// Download historical bars and atomically persist them as Parquet.
    Backfill {
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = "STK")]
        security_type: String,
        #[arg(long, default_value = "USD")]
        currency: String,
        #[arg(long, default_value = "SMART")]
        exchange: String,
        #[arg(long, default_value = "")]
        primary_exchange: String,
        #[arg(long, default_value = "")]
        local_symbol: String,
        #[arg(long)]
        timeframe: String,
        #[arg(long)]
        start: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        end: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        outside_rth: bool,
    },
    /// List persisted historical data jobs and progress.
    Jobs,
    /// Cancel a queued or retrying historical-data job.
    Cancel { job_id: uuid::Uuid },
    /// Inspect verified historical-download coverage and raw file gaps.
    Coverage {
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        timeframe: String,
        #[arg(long)]
        start: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        end: chrono::DateTime<chrono::Utc>,
        /// Inspect data requested with extended-hours trading enabled.
        #[arg(long)]
        outside_rth: bool,
    },
    /// Verify size and checksum of every active dataset file.
    Verify,
    /// Create or list immutable active-file snapshots.
    Snapshot {
        #[command(subcommand)]
        command: DatasetSnapshotCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DatasetSnapshotCommand {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "bars")]
        dataset: String,
    },
    List,
}

#[derive(Debug, Subcommand)]
enum MarketDataCommand {
    /// Persist and start a streaming quote subscription.
    Subscribe {
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = "STK")]
        security_type: String,
        #[arg(long, default_value = "USD")]
        currency: String,
        #[arg(long, default_value = "SMART")]
        exchange: String,
        #[arg(long, default_value = "")]
        primary_exchange: String,
        #[arg(long, default_value = "")]
        local_symbol: String,
    },
    /// Stop and remove a streaming quote subscription.
    Unsubscribe {
        #[arg(long)]
        conid: i32,
    },
    /// List persistent streaming subscriptions.
    Subscriptions,
    /// Show the latest locally cached ticks for a contract.
    Quote {
        #[arg(long)]
        conid: i32,
    },
    /// Show quote freshness and subscription health.
    Health {
        #[arg(long)]
        conid: i32,
    },
    /// Show locally aggregated live trade bars.
    Bars {
        #[arg(long)]
        conid: i32,
        #[arg(long, default_value = "1m")]
        timeframe: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
}

#[derive(Debug, Subcommand)]
enum StrategyCommand {
    /// Create any strategy registered in Rust from a JSON config.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        kind: String,
        #[arg(long)]
        config_json: String,
    },
    /// Create a stopped moving-average crossover strategy.
    CreateMa {
        #[arg(long)]
        name: String,
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        short_window: usize,
        #[arg(long)]
        long_window: usize,
    },
    /// List all strategy definitions and runtime state.
    List,
    /// List strategy kinds compiled into this binary.
    Kinds,
    /// Rename a strategy without changing its stable UUID or history.
    Rename {
        strategy_id: uuid::Uuid,
        #[arg(long)]
        name: String,
    },
    /// Start evaluating a strategy.
    Start { strategy_id: uuid::Uuid },
    /// Pause a strategy without losing its cursor.
    Pause { strategy_id: uuid::Uuid },
    /// Stop a strategy.
    Stop { strategy_id: uuid::Uuid },
    /// Permanently delete a stopped strategy and its strategy-owned records.
    Delete {
        strategy_id: uuid::Uuid,
        #[arg(long)]
        confirm: bool,
    },
    /// List durable strategy evaluations and signals.
    Signals {
        strategy_id: uuid::Uuid,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Configure and audit signal-to-order execution.
    Execution {
        #[command(subcommand)]
        command: StrategyExecutionCommand,
    },
}

#[derive(Debug, Subcommand)]
enum StrategyExecutionCommand {
    Configure {
        #[arg(long)]
        strategy_id: uuid::Uuid,
        #[arg(long)]
        account: String,
        #[arg(long)]
        target_quantity: f64,
        /// Target position for a sell signal. Set below zero to permit shorting.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        short_target_quantity: f64,
        #[arg(long)]
        allow_short: bool,
        /// Permit execution outside regular trading hours using limit orders.
        #[arg(long)]
        outside_rth: bool,
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = "STK")]
        security_type: String,
        #[arg(long, default_value = "USD")]
        currency: String,
        #[arg(long, default_value = "SMART")]
        exchange: String,
        #[arg(long, default_value = "")]
        primary_exchange: String,
        #[arg(long, default_value = "")]
        local_symbol: String,
    },
    /// Configure multiple target-position legs from a JSON array.
    ConfigurePortfolio {
        #[arg(long)]
        strategy_id: uuid::Uuid,
        #[arg(long)]
        account: String,
        /// Permit execution outside regular trading hours using limit orders.
        #[arg(long)]
        outside_rth: bool,
        /// JSON array of {contract,buy_target_quantity,sell_target_quantity}.
        #[arg(long)]
        legs_json: String,
    },
    Enable {
        strategy_id: uuid::Uuid,
        #[arg(long)]
        confirm: bool,
    },
    Disable {
        strategy_id: uuid::Uuid,
    },
    List,
    Actions {
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
}

#[derive(Debug, Subcommand)]
enum BacktestCommand {
    /// Run the moving-average crossover simulation.
    Run {
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        timeframe: String,
        #[arg(long)]
        start: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        end: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        short_window: usize,
        #[arg(long)]
        long_window: usize,
        /// Target long position for a buy signal. The legacy option name is
        /// retained for RPC/CLI compatibility.
        #[arg(long, default_value_t = 1.0)]
        quantity: f64,
        /// Target position for a sell signal. Use a negative value with
        /// --allow-short to model a short target; zero means flatten.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        short_target_quantity: f64,
        /// Permit sell signals to target a negative position.
        #[arg(long)]
        allow_short: bool,
        #[arg(long, default_value_t = 100_000.0)]
        initial_cash: f64,
        /// Database fee model used by this ad-hoc backtest.
        #[arg(long)]
        cost_model_id: uuid::Uuid,
        /// Use historical data downloaded with extended-hours trading enabled.
        #[arg(long)]
        outside_rth: bool,
        #[arg(long, default_value_t = 0)]
        seed: i64,
    },
    /// Run any strategy registered in Rust from a JSON config.
    RunStrategy {
        #[arg(long)]
        conid: i32,
        #[arg(long)]
        timeframe: String,
        #[arg(long)]
        start: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        end: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        kind: String,
        #[arg(long)]
        config_json: String,
        /// Target long position for a buy signal. The legacy option name is
        /// retained for RPC/CLI compatibility.
        #[arg(long, default_value_t = 1.0)]
        quantity: f64,
        /// Target position for a sell signal. Use a negative value with
        /// --allow-short to model a short target; zero means flatten.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        short_target_quantity: f64,
        /// Permit sell signals to target a negative position.
        #[arg(long)]
        allow_short: bool,
        #[arg(long, default_value_t = 100_000.0)]
        initial_cash: f64,
        /// Database fee model used by this ad-hoc backtest.
        #[arg(long)]
        cost_model_id: uuid::Uuid,
        /// Use historical data downloaded with extended-hours trading enabled.
        #[arg(long)]
        outside_rth: bool,
        #[arg(long, default_value_t = 0)]
        seed: i64,
    },
    /// List persisted backtest runs and metrics.
    List,
}

#[derive(Debug, Subcommand)]
enum BackupCommand {
    Create,
    List,
}

#[derive(Debug, Subcommand)]
enum SafetyCommand {
    Status,
    Set {
        #[arg(long)]
        mode: String,
        #[arg(long)]
        note: String,
        #[arg(long)]
        confirm: bool,
    },
    LiveApprove {
        #[arg(long, value_delimiter = ',')]
        conids: Vec<i32>,
        #[arg(long)]
        note: String,
        #[arg(long)]
        confirm_live_risk: bool,
    },
    LiveRevoke {
        #[arg(long)]
        note: String,
        #[arg(long)]
        confirm: bool,
    },
}

#[derive(Debug, Subcommand)]
enum PerformanceCommand {
    Report {
        strategy_id: uuid::Uuid,
        #[arg(long, default_value_t = 100_000.0)]
        initial_capital: f64,
        #[arg(long)]
        benchmark_conid: Option<i32>,
    },
    Snapshots {
        strategy_id: uuid::Uuid,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
}

#[derive(Debug, Subcommand)]
enum FxCommand {
    Set {
        #[arg(long)]
        base: String,
        #[arg(long)]
        quote: String,
        #[arg(long)]
        rate: f64,
        #[arg(long, default_value = "manual")]
        source: String,
    },
    List,
}

#[derive(Debug, Subcommand)]
enum CalendarCommand {
    Add {
        #[arg(long)]
        exchange: String,
        #[arg(long)]
        date: chrono::NaiveDate,
        #[arg(long)]
        opens_at: chrono::DateTime<chrono::Utc>,
        #[arg(long)]
        closes_at: chrono::DateTime<chrono::Utc>,
        #[arg(long, default_value = "open")]
        state: String,
        #[arg(long, default_value = "manual")]
        source: String,
    },
    List {
        #[arg(long)]
        exchange: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    Status {
        #[arg(long)]
        exchange: String,
        #[arg(long)]
        outside_rth: bool,
    },
}

#[derive(Debug, Subcommand)]
enum MonitorCommand {
    Metrics,
    Alerts {
        #[arg(long)]
        all: bool,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    Acknowledge {
        alert_id: uuid::Uuid,
        #[arg(long)]
        note: String,
    },
}

#[derive(Debug, Subcommand)]
enum OrderCommand {
    /// Run pre-trade risk checks without placing an order.
    Preview(OrderArgs),
    /// Persist and submit an order. Requires --confirm.
    Submit {
        #[command(flatten)]
        order: OrderArgs,
        #[arg(long)]
        confirm: bool,
    },
    /// Request cancellation of a previously submitted broker order.
    Cancel {
        #[arg(long)]
        broker_order_id: i32,
        #[arg(long)]
        confirm: bool,
    },
    /// List locally persisted orders.
    List,
}

#[derive(Debug, Args)]
struct OrderArgs {
    #[arg(long)]
    idempotency_key: String,
    #[arg(long)]
    account: String,
    #[arg(long)]
    conid: i32,
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "STK")]
    security_type: String,
    #[arg(long, default_value = "USD")]
    currency: String,
    #[arg(long, default_value = "SMART")]
    exchange: String,
    #[arg(long, default_value = "")]
    primary_exchange: String,
    #[arg(long, default_value = "")]
    local_symbol: String,
    #[arg(long)]
    side: String,
    #[arg(long)]
    quantity: f64,
    #[arg(long)]
    order_type: String,
    #[arg(long)]
    limit_price: Option<f64>,
    #[arg(long)]
    estimated_price: Option<f64>,
    #[arg(long)]
    outside_rth: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(cli.config.as_deref())?;

    match cli.command {
        Command::Daemon => run_daemon(config).await,
        Command::Status => {
            let value = rpc_call(&config, "system.status").await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Health => {
            let value = rpc_call(&config, "system.health").await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Shutdown => {
            let value = rpc_call(&config, "system.shutdown").await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Version => {
            let value = rpc_call(&config, "system.version").await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Ibkr { command } => {
            let method = match command {
                IbkrCommand::Status => "ibkr.status",
                IbkrCommand::Connect => "ibkr.connect",
                IbkrCommand::Disconnect => "ibkr.disconnect",
            };
            let value = rpc_call(&config, method).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Account { command } => {
            let method = match command {
                AccountCommand::Managed => "account.managed",
                AccountCommand::Summary => "account.summary",
                AccountCommand::Pnl => "account.pnl",
            };
            let value = rpc_call(&config, method).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Instrument { command } => {
            let (method, params) = match command {
                InstrumentCommand::Search { pattern } => {
                    ("instrument.search", json!({"pattern": pattern}))
                }
                InstrumentCommand::List => ("instrument.list", json!({})),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Positions => {
            let value = rpc_call(&config, "portfolio.positions").await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Executions => {
            let value = rpc_call(&config, "execution.list").await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Reconcile { command } => {
            let value = match command {
                None => rpc_call(&config, "reconcile.run").await?,
                Some(ReconcileCommand::Status) => rpc_call(&config, "reconcile.status").await?,
                Some(ReconcileCommand::Differences) => {
                    rpc_call(&config, "reconcile.differences").await?
                }
                Some(ReconcileCommand::Acknowledge {
                    difference_id,
                    note,
                }) => {
                    rpc_call_with_params(
                        &config,
                        "reconcile.acknowledge",
                        json!({"difference_id": difference_id, "note": note}),
                    )
                    .await?
                }
            };
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Data { command } => {
            let params = match command {
                DataCommand::Backfill {
                    conid,
                    symbol,
                    security_type,
                    currency,
                    exchange,
                    primary_exchange,
                    local_symbol,
                    timeframe,
                    start,
                    end,
                    outside_rth,
                } => json!({
                    "contract": {
                        "conid": conid,
                        "symbol": symbol,
                        "security_type": security_type,
                        "currency": currency,
                        "exchange": exchange,
                        "primary_exchange": primary_exchange,
                        "local_symbol": local_symbol,
                        "description": "",
                        "derivative_security_types": []
                    },
                    "timeframe": timeframe,
                    "start": start,
                    "end": end,
                    "outside_rth": outside_rth
                }),
                DataCommand::Jobs => {
                    let value = rpc_call_with_params(
                        &config,
                        "data.jobs",
                        json!({"page": 1, "page_size": 200}),
                    )
                    .await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
                DataCommand::Cancel { job_id } => {
                    let value =
                        rpc_call_with_params(&config, "data.job.cancel", json!({"job_id": job_id}))
                            .await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
                DataCommand::Coverage {
                    conid,
                    timeframe,
                    start,
                    end,
                    outside_rth,
                } => {
                    let value = rpc_call_with_params(
                        &config,
                        "data.coverage",
                        json!({
                            "conid": conid,
                            "timeframe": timeframe,
                            "start": start,
                            "end": end,
                            "outside_rth": outside_rth
                        }),
                    )
                    .await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
                DataCommand::Verify => {
                    let value = rpc_call(&config, "data.verify").await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
                DataCommand::Snapshot { command } => {
                    let (method, params) = match command {
                        DatasetSnapshotCommand::Create { name, dataset } => (
                            "data.snapshot.create",
                            json!({"name": name, "dataset": dataset}),
                        ),
                        DatasetSnapshotCommand::List => ("data.snapshot.list", json!({})),
                    };
                    let value = rpc_call_with_params(&config, method, params).await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
            };
            let value = rpc_call_with_params(&config, "data.backfill", params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::MarketData { command } => {
            let (method, params) = match command {
                MarketDataCommand::Subscribe {
                    conid,
                    symbol,
                    security_type,
                    currency,
                    exchange,
                    primary_exchange,
                    local_symbol,
                } => (
                    "market_data.subscribe",
                    json!({
                        "conid": conid,
                        "symbol": symbol,
                        "security_type": security_type,
                        "currency": currency,
                        "exchange": exchange,
                        "primary_exchange": primary_exchange,
                        "local_symbol": local_symbol,
                        "description": "",
                        "derivative_security_types": []
                    }),
                ),
                MarketDataCommand::Unsubscribe { conid } => {
                    ("market_data.unsubscribe", json!({"conid": conid}))
                }
                MarketDataCommand::Subscriptions => ("market_data.subscriptions", json!({})),
                MarketDataCommand::Quote { conid } => {
                    ("market_data.quote", json!({"conid": conid}))
                }
                MarketDataCommand::Health { conid } => {
                    ("market_data.health", json!({"conid": conid}))
                }
                MarketDataCommand::Bars {
                    conid,
                    timeframe,
                    limit,
                } => (
                    "market_data.bars",
                    json!({"conid": conid, "timeframe": timeframe, "limit": limit}),
                ),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Strategy { command } => {
            let (method, params) = match command {
                StrategyCommand::Create {
                    name,
                    kind,
                    config_json,
                } => (
                    "strategy.create",
                    json!({
                        "name": name,
                        "kind": kind,
                        "config": serde_json::from_str::<Value>(&config_json)?
                    }),
                ),
                StrategyCommand::CreateMa {
                    name,
                    conid,
                    short_window,
                    long_window,
                } => (
                    "strategy.create",
                    json!({
                        "name": name,
                        "kind": "moving_average_cross",
                        "config": {
                            "conid": conid,
                            "short_window": short_window,
                            "long_window": long_window
                        }
                    }),
                ),
                StrategyCommand::List => ("strategy.list", json!({})),
                StrategyCommand::Kinds => ("strategy.kinds", json!({})),
                StrategyCommand::Rename { strategy_id, name } => (
                    "strategy.rename",
                    json!({"strategy_id": strategy_id, "name": name}),
                ),
                StrategyCommand::Start { strategy_id } => {
                    ("strategy.start", json!({"strategy_id": strategy_id}))
                }
                StrategyCommand::Pause { strategy_id } => {
                    ("strategy.pause", json!({"strategy_id": strategy_id}))
                }
                StrategyCommand::Stop { strategy_id } => {
                    ("strategy.stop", json!({"strategy_id": strategy_id}))
                }
                StrategyCommand::Delete {
                    strategy_id,
                    confirm,
                } => (
                    "strategy.delete",
                    json!({"strategy_id": strategy_id, "confirm": confirm}),
                ),
                StrategyCommand::Signals { strategy_id, limit } => (
                    "strategy.signals",
                    json!({"strategy_id": strategy_id, "limit": limit}),
                ),
                StrategyCommand::Execution { command } => match command {
                    StrategyExecutionCommand::Configure {
                        strategy_id,
                        account,
                        target_quantity,
                        short_target_quantity,
                        allow_short,
                        outside_rth,
                        conid,
                        symbol,
                        security_type,
                        currency,
                        exchange,
                        primary_exchange,
                        local_symbol,
                    } => (
                        "strategy.execution.configure",
                        json!({
                            "strategy_id": strategy_id,
                            "account": account,
                            "target_quantity": target_quantity,
                            "short_target_quantity": short_target_quantity,
                            "allow_short": allow_short,
                            "outside_rth": outside_rth,
                            "order_type": if outside_rth { "limit" } else { "market" },
                            "paper_only": true,
                            "contract": {
                                "conid": conid,
                                "symbol": symbol,
                                "security_type": security_type,
                                "currency": currency,
                                "exchange": exchange,
                                "primary_exchange": primary_exchange,
                                "local_symbol": local_symbol,
                                "description": "",
                                "derivative_security_types": []
                            }
                        }),
                    ),
                    StrategyExecutionCommand::ConfigurePortfolio {
                        strategy_id,
                        account,
                        outside_rth,
                        legs_json,
                    } => (
                        "strategy.execution.configure_portfolio",
                        json!({
                            "strategy_id": strategy_id,
                            "account": account,
                            "order_type": if outside_rth { "limit" } else { "market" },
                            "paper_only": true,
                            "outside_rth": outside_rth,
                            "legs": serde_json::from_str::<Value>(&legs_json)?
                        }),
                    ),
                    StrategyExecutionCommand::Enable {
                        strategy_id,
                        confirm,
                    } => (
                        "strategy.execution.enable",
                        json!({"strategy_id": strategy_id, "confirm": confirm}),
                    ),
                    StrategyExecutionCommand::Disable { strategy_id } => (
                        "strategy.execution.disable",
                        json!({"strategy_id": strategy_id, "confirm": false}),
                    ),
                    StrategyExecutionCommand::List => ("strategy.execution.list", json!({})),
                    StrategyExecutionCommand::Actions { limit } => {
                        ("strategy.execution.actions", json!({"limit": limit}))
                    }
                },
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Backtest { command } => {
            let (method, params) = match command {
                BacktestCommand::Run {
                    conid,
                    timeframe,
                    start,
                    end,
                    short_window,
                    long_window,
                    quantity,
                    short_target_quantity,
                    allow_short,
                    initial_cash,
                    cost_model_id,
                    outside_rth,
                    seed,
                } => (
                    "backtest.run",
                    json!({
                        "conid": conid,
                        "timeframe": timeframe,
                        "start": start,
                        "end": end,
                        "short_window": short_window,
                        "long_window": long_window,
                        "strategy_kind": "moving_average_cross",
                        "quantity": quantity,
                        "short_target_quantity": short_target_quantity,
                        "allow_short": allow_short,
                        "initial_cash": initial_cash,
                        "cost_model_id": cost_model_id,
                        "outside_rth": outside_rth,
                        "seed": seed
                    }),
                ),
                BacktestCommand::RunStrategy {
                    conid,
                    timeframe,
                    start,
                    end,
                    kind,
                    config_json,
                    quantity,
                    short_target_quantity,
                    allow_short,
                    initial_cash,
                    cost_model_id,
                    outside_rth,
                    seed,
                } => (
                    "backtest.run",
                    json!({
                        "conid": conid,
                        "timeframe": timeframe,
                        "start": start,
                        "end": end,
                        "strategy_kind": kind,
                        "strategy_config": serde_json::from_str::<Value>(&config_json)?,
                        "quantity": quantity,
                        "short_target_quantity": short_target_quantity,
                        "allow_short": allow_short,
                        "initial_cash": initial_cash,
                        "cost_model_id": cost_model_id,
                        "outside_rth": outside_rth,
                        "seed": seed
                    }),
                ),
                BacktestCommand::List => ("backtest.list", json!({})),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Backup { command } => {
            let method = match command {
                BackupCommand::Create => "backup.create",
                BackupCommand::List => "backup.list",
            };
            let value = rpc_call(&config, method).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Safety { command } => {
            let (method, params) = match command {
                SafetyCommand::Status => ("safety.status", json!({})),
                SafetyCommand::Set {
                    mode,
                    note,
                    confirm,
                } => {
                    if !confirm {
                        return Err(crate::error::AppError::Config(
                            "changing safety mode requires --confirm".into(),
                        ));
                    }
                    ("safety.set", json!({"mode": mode, "note": note}))
                }
                SafetyCommand::LiveApprove {
                    conids,
                    note,
                    confirm_live_risk,
                } => (
                    "safety.live_approve",
                    json!({
                        "conids": conids,
                        "note": note,
                        "confirm_live_risk": confirm_live_risk
                    }),
                ),
                SafetyCommand::LiveRevoke { note, confirm } => {
                    if !confirm {
                        return Err(crate::error::AppError::Config(
                            "revoking live approval requires --confirm".into(),
                        ));
                    }
                    ("safety.live_revoke", json!({"note": note}))
                }
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Order { command } => {
            let (method, order) = match command {
                OrderCommand::Preview(order) => ("order.preview", order),
                OrderCommand::Submit { order, confirm } => {
                    if !confirm {
                        return Err(crate::error::AppError::Config(
                            "order submit requires --confirm".into(),
                        ));
                    }
                    ("order.submit", order)
                }
                OrderCommand::Cancel {
                    broker_order_id,
                    confirm,
                } => {
                    if !confirm {
                        return Err(crate::error::AppError::Config(
                            "order cancel requires --confirm".into(),
                        ));
                    }
                    let value = rpc_call_with_params(
                        &config,
                        "order.cancel",
                        json!({"broker_order_id": broker_order_id}),
                    )
                    .await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
                OrderCommand::List => {
                    let value = rpc_call(&config, "order.list").await?;
                    print_value(&value, cli.json);
                    return Ok(());
                }
            };
            let params = order_params(order);
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Performance { command } => {
            let (method, params) = match command {
                PerformanceCommand::Report {
                    strategy_id,
                    initial_capital,
                    benchmark_conid,
                } => (
                    "performance.report",
                    json!({
                        "strategy_id": strategy_id,
                        "initial_capital": initial_capital
                        ,"benchmark_conid": benchmark_conid
                    }),
                ),
                PerformanceCommand::Snapshots { strategy_id, limit } => (
                    "performance.snapshots",
                    json!({"strategy_id": strategy_id, "limit": limit}),
                ),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Fx { command } => {
            let (method, params) = match command {
                FxCommand::Set {
                    base,
                    quote,
                    rate,
                    source,
                } => (
                    "fx.set",
                    json!({
                        "base_currency": base,
                        "quote_currency": quote,
                        "rate": rate,
                        "source": source
                    }),
                ),
                FxCommand::List => ("fx.list", json!({})),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Calendar { command } => {
            let (method, params) = match command {
                CalendarCommand::Add {
                    exchange,
                    date,
                    opens_at,
                    closes_at,
                    state,
                    source,
                } => (
                    "calendar.add",
                    json!({
                        "exchange": exchange,
                        "trading_date": date,
                        "opens_at": opens_at,
                        "closes_at": closes_at,
                        "state": state,
                        "source": source
                    }),
                ),
                CalendarCommand::List { exchange, limit } => (
                    "calendar.list",
                    json!({"exchange": exchange, "limit": limit}),
                ),
                CalendarCommand::Status {
                    exchange,
                    outside_rth,
                } => (
                    "calendar.status",
                    json!({"exchange": exchange, "outside_rth": outside_rth}),
                ),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
        Command::Monitor { command } => {
            let (method, params) = match command {
                MonitorCommand::Metrics => ("monitor.metrics", json!({})),
                MonitorCommand::Alerts { all, limit } => (
                    "monitor.alerts",
                    json!({"active_only": !all, "limit": limit}),
                ),
                MonitorCommand::Acknowledge { alert_id, note } => (
                    "monitor.acknowledge",
                    json!({"alert_id": alert_id, "note": note}),
                ),
            };
            let value = rpc_call_with_params(&config, method, params).await?;
            print_value(&value, cli.json);
            Ok(())
        }
    }
}

/// Runs a critical background task under supervision.
///
/// The daemon is expected to run without an external supervisor (for example
/// inside a plain `screen` session), so a dead persistence or trading task must
/// not be silent: if the task panics or exits while the daemon is still
/// supposed to be running, the failure is recorded and a graceful shutdown is
/// triggered so the operator sees the daemon stop instead of it continuing in
/// a degraded, partially-functional state.
fn spawn_supervised<F>(
    name: &'static str,
    cancellation: CancellationToken,
    critical_task_failed: Arc<std::sync::atomic::AtomicBool>,
    future: F,
) where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let result = tokio::spawn(future).await;
        if cancellation.is_cancelled() {
            return;
        }
        match result {
            Ok(()) => tracing::error!(
                task = name,
                "critical background task exited unexpectedly; shutting down"
            ),
            Err(error) => tracing::error!(
                task = name,
                %error,
                "critical background task panicked; shutting down"
            ),
        }
        critical_task_failed.store(true, std::sync::atomic::Ordering::SeqCst);
        cancellation.cancel();
    });
}

async fn run_daemon(config: Config) -> Result<()> {
    telemetry::init(&config.logging)
        .map_err(|error| crate::error::AppError::Config(error.to_string()))?;
    for warning in &config.warnings {
        tracing::warn!("{warning}");
    }
    let _lock = ProcessLock::acquire(&config.lock_path())?;
    std::fs::create_dir_all(&config.storage.lake_dir)?;
    std::fs::create_dir_all(&config.storage.staging_dir)?;

    let storage = Arc::new(Mutex::new(Storage::open(&config.storage.duckdb_path)?));
    // Serializes strategy target/configuration mutations with the final
    // authorization-to-broker-acknowledgement interval.  Without this gate a
    // new Bar could supersede a target after local authorization while the old
    // order was already being handed to IBKR.
    let strategy_order_coordination = Arc::new(tokio::sync::Mutex::new(()));
    let schema_version = storage.lock_safe().schema_version()?;
    let started_at = Utc::now();
    let cancellation = CancellationToken::new();
    let critical_task_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ibkr = ibkr::spawn(
        config.ibkr.clone(),
        config.risk.max_account_data_age_seconds,
        cancellation.clone(),
    );
    let persisted_market_data = storage.lock_safe().market_data_subscriptions()?;
    for mut contract in persisted_market_data {
        contract.normalize_streaming_subscription();
        if let Err(error) = contract.validate_streaming_subscription() {
            tracing::warn!(
                conid = contract.conid,
                symbol = %contract.symbol,
                %error,
                "removing invalid persisted market-data subscription"
            );
            storage
                .lock_safe()
                .remove_market_data_subscription(contract.conid)?;
            continue;
        }
        ibkr.subscribe_market_data(contract)
            .await
            .map_err(crate::error::AppError::Config)?;
    }
    let mut ibkr_status = ibkr.subscribe_status();
    let mut broker_events = ibkr
        .take_events()
        .await
        .expect("IBKR event receiver is available exactly once");
    let event_storage = storage.clone();
    let event_cancellation = cancellation.clone();
    spawn_supervised(
        "broker-event-persister",
        cancellation.clone(),
        critical_task_failed.clone(),
        async move {
            loop {
                tokio::select! {
                    _ = event_cancellation.cancelled() => break,
                    event = broker_events.recv() => {
                        let Some(event) = event else { break };
                        let mut guard = event_storage.lock_safe();
                        if let Err(error) = guard.apply_broker_event(&event) {
                            tracing::error!(%error, ?event, "failed to persist IBKR broker event");
                        }
                    }
                }
            }
        },
    );
    let fx_storage = storage.clone();
    let fx_ibkr = ibkr.clone();
    let fx_cancellation = cancellation.clone();
    let fx_base_currency = config.risk.base_currency.trim().to_ascii_uppercase();
    let fx_refresh_seconds = config.risk.fx_rate_refresh_seconds;
    let mut fx_ibkr_status = ibkr.subscribe_status();
    spawn_supervised(
        "fx-rate-refresher",
        cancellation.clone(),
        critical_task_failed.clone(),
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(fx_refresh_seconds));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let refresh = tokio::select! {
                    _ = fx_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        fx_ibkr_status.borrow().state == ibkr::ConnectionState::Ready
                    }
                    changed = fx_ibkr_status.changed() => {
                        if changed.is_err() { break; }
                        fx_ibkr_status.borrow().state == ibkr::ConnectionState::Ready
                    }
                };
                if !refresh {
                    continue;
                }

                let required = match fx_storage
                    .lock_safe()
                    .required_fx_currencies(&fx_base_currency)
                {
                    Ok(required) => required,
                    Err(error) => {
                        tracing::error!(%error, "failed to discover required FX currencies");
                        continue;
                    }
                };
                if required.is_empty() {
                    let _ = fx_storage.lock_safe().upsert_monitoring_alert(
                        "fx_rate_refresh_failed",
                        "warning",
                        "no non-base FX currencies are currently required",
                        false,
                    );
                    continue;
                }

                match fx_ibkr.fx_rate_snapshot(fx_base_currency.clone()).await {
                    Ok(snapshots) => {
                        let available = snapshots
                            .iter()
                            .map(|snapshot| snapshot.base_currency.clone())
                            .collect::<std::collections::HashSet<_>>();
                        let missing = required
                            .iter()
                            .filter(|currency| !available.contains(*currency))
                            .cloned()
                            .collect::<Vec<_>>();
                        let mut guard = fx_storage.lock_safe();
                        let mut persist_error = None;
                        for snapshot in &snapshots {
                            if let Err(error) = guard.upsert_fx_rate(&crate::storage::FxRateInput {
                                base_currency: snapshot.base_currency.clone(),
                                quote_currency: snapshot.quote_currency.clone(),
                                rate: snapshot.rate,
                                source: "ibkr_account_updates".into(),
                                observed_at: snapshot.observed_at,
                            }) {
                                persist_error = Some(error.to_string());
                                break;
                            }
                        }
                        let failure = persist_error.or_else(|| {
                            (!missing.is_empty()).then(|| {
                                format!(
                                    "IBKR account updates did not return required currency pair(s): {} -> {}",
                                    missing.join(", "),
                                    fx_base_currency
                                )
                            })
                        });
                        let _ = guard.upsert_monitoring_alert(
                            "fx_rate_refresh_failed",
                            "warning",
                            failure
                                .as_deref()
                                .unwrap_or("IBKR FX rates are refreshing normally"),
                            failure.is_some(),
                        );
                        if let Some(error) = failure {
                            tracing::warn!(%error, "IBKR FX-rate refresh was incomplete");
                        } else {
                            tracing::info!(
                                rate_count = snapshots.len(),
                                quote_currency = %fx_base_currency,
                                "refreshed FX rates from IBKR account updates"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to refresh FX rates from IBKR");
                        let _ = fx_storage.lock_safe().upsert_monitoring_alert(
                            "fx_rate_refresh_failed",
                            "warning",
                            &error,
                            true,
                        );
                    }
                }
            }
        },
    );
    let strategy_storage = storage.clone();
    let strategy_evaluation_coordination = strategy_order_coordination.clone();
    let strategy_cancellation = cancellation.clone();
    spawn_supervised(
        "strategy-evaluator",
        cancellation.clone(),
        critical_task_failed.clone(),
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = strategy_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let _submission_guard = strategy_evaluation_coordination.lock().await;
                        match strategy_storage
                            .lock_safe()
                            .evaluate_running_strategies()
                        {
                            Ok(count) if count > 0 => {
                                tracing::info!(count, "strategy evaluations persisted");
                            }
                            Ok(_) => {}
                            Err(error) => tracing::error!(%error, "strategy evaluation failed"),
                        }
                    }
                }
            }
        },
    );
    let execution_storage = storage.clone();
    let execution_ibkr = ibkr.clone();
    let execution_cancellation = cancellation.clone();
    let execution_rpc_address = config.rpc.http_listen;
    let execution_timeout = Duration::from_secs(config.rpc.request_timeout_seconds);
    let execution_environment = config.app.environment;
    let execution_trading_enabled = config.risk.trading_enabled;
    let execution_base_currency = config.risk.base_currency.clone();
    let execution_max_fx_rate_age_seconds = config.risk.max_fx_rate_age_seconds;
    let execution_max_market_data_age_seconds = config.risk.max_market_data_age_seconds;
    spawn_supervised(
        "strategy-execution-worker",
        cancellation.clone(),
        critical_task_failed.clone(),
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = execution_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if execution_environment != crate::config::Environment::Paper {
                            continue;
                        }
                        // A newer strategy signal revokes the old target, but a
                        // previously acknowledged limit order remains live until
                        // IBKR cancels it. Process every reconciliation-first
                        // cancellation candidate before any new strategy action.
                        // Trying the whole snapshot prevents one broker-side
                        // cancellation failure from starving unrelated orders.
                        // Unknown outcomes are not included here and continue to
                        // block on reconciliation.
                        let obsolete_orders = execution_storage
                            .lock_safe()
                            .revoked_strategy_order_cancellations();
                        match obsolete_orders {
                            Ok(candidates) if !candidates.is_empty() => {
                                for candidate in candidates {
                                    let result = rpc::call(
                                        execution_rpc_address,
                                        "order.cancel",
                                        json!({"broker_order_id": candidate.broker_order_id}),
                                        execution_timeout,
                                    )
                                    .await;
                                    match result {
                                        Ok(_) => tracing::info!(
                                            strategy_id = %candidate.strategy_id,
                                            action_id = %candidate.action_id,
                                            leg_index = candidate.leg_index,
                                            broker_order_id = candidate.broker_order_id,
                                            "requested cancellation of an order from an inactive strategy target"
                                        ),
                                        Err(error) => tracing::warn!(
                                            %error,
                                            strategy_id = %candidate.strategy_id,
                                            action_id = %candidate.action_id,
                                            leg_index = candidate.leg_index,
                                            broker_order_id = candidate.broker_order_id,
                                            "failed to cancel an order from an inactive strategy target; continuing with the remaining cancellation candidates"
                                        ),
                                    }
                                }
                                continue;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::error!(
                                    %error,
                                    "failed to inspect orders from inactive strategy targets; refusing new automatic execution"
                                );
                                continue;
                            }
                        }
                        if !execution_trading_enabled {
                            continue;
                        }
                        let action = match execution_storage
                            .lock_safe()
                            .claim_strategy_action_with_risk(
                                &execution_base_currency,
                                execution_max_fx_rate_age_seconds,
                                execution_max_market_data_age_seconds,
                                Utc::now(),
                            )
                        {
                            Ok(Some(action)) => action,
                            Ok(None) => continue,
                            Err(error) => {
                                tracing::error!(%error, "failed to claim strategy execution action");
                                continue;
                            }
                        };
                        if !action.paper_only {
                            let _ = execution_storage
                                .lock_safe()
                                .finish_strategy_action(
                                    action.action_id,
                                    "rejected",
                                    None,
                                    None,
                                    Some("only paper-only strategy execution is supported"),
                                );
                            continue;
                        }
                        let mut prepared_legs = Vec::new();
                        let mut preflight_error = None;
                        for leg in &action.legs {
                            let quote_now = Utc::now();
                            let market_data = match execution_storage
                                .lock_safe()
                                .market_data_health(
                                    leg.contract.conid,
                                    execution_max_market_data_age_seconds,
                                    quote_now,
                                )
                            {
                                Ok(health) => health,
                                Err(error) => {
                                    preflight_error = Some(format!(
                                        "failed to inspect market-data health for {}: {error}",
                                        leg.contract.symbol
                                    ));
                                    break;
                                }
                            };
                            let risk_reducing = leg.is_risk_reducing();
                            if let Some(detail) = strategy_market_data_rejection(
                                &leg.contract.symbol,
                                &market_data,
                                risk_reducing,
                            ) {
                                preflight_error = Some(detail);
                                break;
                            }
                            let estimated_price = execution_storage
                                .lock_safe()
                                .latest_quote(leg.contract.conid)
                                .ok()
                                .and_then(|quote| {
                                    strategy_execution_price(
                                        &quote,
                                        &leg.side,
                                        quote_now,
                                        execution_max_market_data_age_seconds,
                                    )
                                });
                            if estimated_price.is_none() && !risk_reducing {
                                preflight_error = Some(format!(
                                    "{} risk-increasing order requires a fresh, side-specific \
                                     live Bid/Ask; stale or delayed ticks are never used for \
                                     automatic execution",
                                    leg.contract.symbol
                                ));
                                break;
                            }
                            let calendar_exchange =
                                strategy_calendar_exchange(&leg.contract).to_owned();
                            let calendar_now = Utc::now();
                            let refresh_calendar = execution_storage
                                .lock_safe()
                                .market_calendar_needs_refresh(
                                    &calendar_exchange,
                                    calendar_now,
                                );
                            match refresh_calendar {
                                Ok(true) => {
                                    match execution_ibkr
                                        .contract_schedule(leg.contract.clone())
                                        .await
                                    {
                                        Ok(schedule) => {
                                            match execution_storage
                                                .lock_safe()
                                                .replace_ibkr_market_sessions(&schedule)
                                            {
                                                Ok(intervals) => tracing::info!(
                                                    conid = schedule.conid,
                                                    exchange = %schedule.exchange,
                                                    intervals,
                                                    timezone = %schedule.time_zone_id,
                                                    "refreshed IBKR trading calendar"
                                                ),
                                                Err(error) => {
                                                    preflight_error = Some(format!(
                                                        "regular-hours order skipped locally: \
                                                         failed to persist IBKR trading calendar \
                                                         for {calendar_exchange}: {error}"
                                                    ));
                                                    break;
                                                }
                                            }
                                        }
                                        Err(error) => {
                                            preflight_error = Some(format!(
                                                "regular-hours order skipped locally: failed to \
                                                 refresh IBKR trading calendar for \
                                                 {calendar_exchange}: {error}"
                                            ));
                                            break;
                                        }
                                    }
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    preflight_error = Some(format!(
                                        "regular-hours order skipped locally: failed to inspect \
                                         trading calendar for {calendar_exchange}: {error}"
                                    ));
                                    break;
                                }
                            }
                            let session_open = execution_storage
                                .lock_safe()
                                .market_session_is_open_for(
                                    &calendar_exchange,
                                    calendar_now,
                                    action.outside_rth,
                                );
                            match session_open {
                                Ok(status) => {
                                    if let Some(detail) = strategy_session_rejection(
                                        action.outside_rth,
                                        &calendar_exchange,
                                        status,
                                    ) {
                                        if status == Some(false) {
                                            let not_before = execution_storage
                                                .lock_safe()
                                                .next_market_session_open_for(
                                                    &calendar_exchange,
                                                    calendar_now,
                                                    action.outside_rth,
                                                )
                                                .ok()
                                                .flatten()
                                                .unwrap_or_else(|| {
                                                    calendar_now + chrono::Duration::minutes(5)
                                                });
                                            let _ = execution_storage
                                                .lock_safe()
                                                .defer_strategy_action_retry(
                                                    action.action_id,
                                                    not_before,
                                                    &format!(
                                                        "market is closed; protective retry deferred until {not_before}"
                                                    ),
                                                );
                                        }
                                        preflight_error = Some(detail);
                                        break;
                                    }
                                }
                                Err(error) => {
                                    preflight_error = Some(format!(
                                        "regular-hours order skipped locally: failed to check \
                                         trading calendar for {calendar_exchange}: {error}"
                                    ));
                                    break;
                                }
                            }
                            prepared_legs.push((leg.clone(), estimated_price));
                        }
                        if let Some(detail) = preflight_error {
                            let mut guard = execution_storage.lock_safe();
                            for leg in &action.legs {
                                let _ = guard.finish_strategy_action_leg(
                                    action.action_id,
                                    leg.leg_index,
                                    "rejected",
                                    None,
                                    None,
                                    Some(&detail),
                                );
                            }
                            let _ = guard.finish_strategy_action(
                                action.action_id,
                                "rejected",
                                None,
                                None,
                                Some(&detail),
                            );
                            continue;
                        }

                        if let Some(cost) = &action.cost_control {
                            let risk_reducing =
                                action.legs.iter().all(|leg| leg.is_risk_reducing());
                            if !risk_reducing && action.legs.iter().any(|leg| {
                                !leg.contract.currency.eq_ignore_ascii_case(&cost.model.currency)
                            }) {
                                let detail = format!(
                                    "cost gate blocked: model currency {} does not match every \
                                     execution leg currency",
                                    cost.model.currency
                                );
                                let mut guard = execution_storage.lock_safe();
                                let _ = guard.record_strategy_cost_gate(
                                    action.action_id,
                                    "skipped",
                                    0.0,
                                    0.0,
                                    0.0,
                                    action.signal_edge_bps,
                                    &detail,
                                );
                                for leg in &action.legs {
                                    let _ = guard.finish_strategy_action_leg(
                                        action.action_id,
                                        leg.leg_index,
                                        "skipped",
                                        None,
                                        None,
                                        Some(&detail),
                                    );
                                }
                                continue;
                            } else {
                                let estimates = prepared_legs
                                    .iter()
                                    .map(|(leg, price)| crate::storage::CostGateLegEstimate {
                                        quantity: leg.quantity,
                                        price: price.unwrap_or(0.0),
                                    })
                                    .collect::<Vec<_>>();
                                let decision = crate::storage::evaluate_transaction_cost_gate(
                                    &cost.model,
                                    cost.minimum_cost_multiple,
                                    cost.actual_fee_bps_p90,
                                    action.signal_edge_bps,
                                    risk_reducing,
                                    &estimates,
                                );
                                let passed = !matches!(
                                    decision.outcome,
                                    crate::storage::TransactionCostGateOutcome::Blocked
                                );
                                let detail = if matches!(
                                    decision.outcome,
                                    crate::storage::TransactionCostGateOutcome::BypassedRiskReduction
                                ) {
                                    "cost gate bypassed: every execution leg reduces or closes an existing position"
                                        .to_owned()
                                } else {
                                    format!(
                                        "cost gate {}: signal edge {} bps, required {:.4} bps, \
                                         estimated round-trip cost {:.4} on notional {:.4}",
                                        if passed { "passed" } else { "blocked" },
                                        action
                                            .signal_edge_bps
                                            .map(|value| format!("{value:.4}"))
                                            .unwrap_or_else(|| "unavailable".into()),
                                        decision.required_edge_bps,
                                        decision.estimated_round_trip_cost,
                                        decision.estimated_notional
                                    )
                                };
                                let state = if passed { "processing" } else { "skipped" };
                                let mut guard = execution_storage.lock_safe();
                                let _ = guard.record_strategy_cost_gate(
                                    action.action_id,
                                    state,
                                    decision.estimated_notional,
                                    decision.estimated_round_trip_cost,
                                    decision.required_edge_bps,
                                    action.signal_edge_bps,
                                    &detail,
                                );
                                if !passed {
                                    for leg in &action.legs {
                                        let _ = guard.finish_strategy_action_leg(
                                            action.action_id,
                                            leg.leg_index,
                                            "skipped",
                                            None,
                                            None,
                                            Some(&detail),
                                        );
                                    }
                                    continue;
                                }
                            }
                        }

                        let mut first_order_intent_id = None;
                        let mut first_broker_order_id = None;
                        let mut batch_error = None;
                        let mut submitted_leg_count = 0usize;
                        for prepared_index in 0..prepared_legs.len() {
                            let (leg, estimated_price) = prepared_legs[prepared_index].clone();
                            if let Err(error) = execution_storage
                                .lock_safe()
                                .ensure_strategy_action_leg_submission_authorized(
                                    action.action_id,
                                    leg.leg_index,
                                    &action.account,
                                    &leg.contract,
                                )
                            {
                                let detail = format!(
                                    "order was not submitted because its persisted authorization \
                                     changed after claim: {error}"
                                );
                                let mut guard = execution_storage.lock_safe();
                                for (remaining_leg, _) in
                                    prepared_legs.iter().skip(prepared_index)
                                {
                                    let _ = guard.finish_strategy_action_leg(
                                        action.action_id,
                                        remaining_leg.leg_index,
                                        "skipped",
                                        None,
                                        None,
                                        Some(&detail),
                                    );
                                }
                                // If an earlier portfolio leg already reached
                                // order.submit, retain the parent as submitted
                                // and its first intent for reconciliation. With
                                // no submitted leg this is a clean local skip.
                                batch_error = Some((
                                    detail,
                                    if submitted_leg_count > 0 {
                                        "submitted"
                                    } else {
                                        "skipped"
                                    },
                                ));
                                break;
                            }
                            let params = json!({
                                "idempotency_key": leg.idempotency_key,
                                "account": action.account.clone(),
                                "contract": leg.contract,
                                "side": leg.side.clone(),
                                "quantity": leg.quantity,
                                "order_type": action.order_type.clone(),
                                "limit_price": if action.order_type == "limit" {
                                    estimated_price
                                } else {
                                    None
                                },
                                "outside_rth": action.outside_rth,
                                "estimated_price": estimated_price,
                                "strategy_provenance": {
                                    "strategy_id": action.strategy_id,
                                    "action_id": action.action_id,
                                    "leg_index": leg.leg_index,
                                    "source_evaluation_id": action.source_evaluation_id,
                                    "target_quantity": leg.target_quantity,
                                    "claimed_current_quantity": leg.current_quantity,
                                    "side": leg.side.clone(),
                                    "quantity": leg.quantity
                                }
                            });
                            match rpc::call(
                                execution_rpc_address,
                                "order.submit",
                                params,
                                execution_timeout,
                            ).await {
                                Ok(result) => {
                                    let order_intent_id = result["order_intent_id"]
                                        .as_str()
                                        .and_then(|value| uuid::Uuid::parse_str(value).ok());
                                    let broker_order_id = result["broker_order_id"]
                                        .as_i64()
                                        .and_then(|value| i32::try_from(value).ok());
                                    first_order_intent_id =
                                        first_order_intent_id.or(order_intent_id);
                                    first_broker_order_id =
                                        first_broker_order_id.or(broker_order_id);
                                    submitted_leg_count += 1;
                                    let _ = execution_storage
                                        .lock_safe()
                                        .finish_strategy_action_leg(
                                            action.action_id,
                                            leg.leg_index,
                                            "submitted",
                                            order_intent_id,
                                            broker_order_id,
                                            None,
                                        );
                                }
                                Err(error) => {
                                    let uncertain = matches!(
                                        &error,
                                        crate::error::AppError::Rpc { code, .. }
                                            if *code == -32001
                                                || *code == -32002
                                                || *code == -32026
                                    ) || matches!(
                                        &error,
                                        crate::error::AppError::DaemonUnavailable { .. }
                                    );
                                    let state = strategy_submission_error_state(uncertain);
                                    let detail = if uncertain {
                                        format!(
                                            "portfolio leg outcome uncertain: {error}; manual \
                                             reconciliation is required and no compensation \
                                             order was sent"
                                        )
                                    } else {
                                        error.to_string()
                                    };
                                    let _ = execution_storage
                                        .lock_safe()
                                        .finish_strategy_action_leg(
                                            action.action_id,
                                            leg.leg_index,
                                            state,
                                            None,
                                            None,
                                            Some(&detail),
                                        );
                                    if prepared_index + 1 < prepared_legs.len() {
                                        let remaining_detail = format!(
                                            "not submitted because an earlier portfolio leg ended \
                                             in state {state}: {detail}"
                                        );
                                        let mut guard = execution_storage.lock_safe();
                                        for (remaining_leg, _) in
                                            prepared_legs.iter().skip(prepared_index + 1)
                                        {
                                            let _ = guard.finish_strategy_action_leg(
                                                action.action_id,
                                                remaining_leg.leg_index,
                                                "skipped",
                                                None,
                                                None,
                                                Some(&remaining_detail),
                                            );
                                        }
                                    }
                                    batch_error = Some((detail, state));
                                    break;
                                }
                            }
                        }
                        let (state, detail) = match batch_error {
                            Some((detail, state)) => (state, Some(detail)),
                            None => ("submitted", None),
                        };
                        let _ = execution_storage
                            .lock_safe()
                            .finish_strategy_action(
                                action.action_id,
                                state,
                                first_order_intent_id,
                                first_broker_order_id,
                                detail.as_deref(),
                            );
                    }
                }
            }
        },
    );
    if config.monitoring.enabled {
        let monitor_storage = storage.clone();
        let monitor_ibkr = ibkr.clone();
        let monitor_cancellation = cancellation.clone();
        let monitor_config = config.monitoring.clone();
        let monitor_risk = config.risk.clone();
        spawn_supervised(
            "monitoring-worker",
            cancellation.clone(),
            critical_task_failed.clone(),
            async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(monitor_config.interval_seconds));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut last_performance_snapshot = tokio::time::Instant::now()
                    - Duration::from_secs(monitor_config.performance_snapshot_seconds);
                loop {
                    tokio::select! {
                        _ = monitor_cancellation.cancelled() => break,
                        _ = interval.tick() => {
                            let ibkr_status = monitor_ibkr.status();
                            let mut guard = monitor_storage.lock_safe();
                            let ibkr_ready =
                                ibkr_status.state == crate::ibkr::ConnectionState::Ready;
                            let _ = guard.upsert_monitoring_alert(
                                "ibkr_not_ready",
                                "critical",
                                "IBKR connection is not ready",
                                !ibkr_ready,
                            );
                            let reconciliation = guard
                                .reconciliation_health(ibkr_status.connection_session_id);
                            let reconciliation_healthy = reconciliation
                                .as_ref()
                                .is_ok_and(|health| health.state == "healthy");
                            let _ = guard.upsert_monitoring_alert(
                                "reconciliation_unhealthy",
                                "critical",
                                "the active IBKR session has not completed a healthy reconciliation",
                                !reconciliation_healthy,
                            );
                            if let Ok(facts) = guard.monitoring_facts(Utc::now()) {
                                let failed_market_data =
                                    facts["failed_market_data"].as_i64().unwrap_or(0);
                                let competing_live_session_count = facts
                                    ["competing_live_session_count"]
                                    .as_u64()
                                    .unwrap_or(0);
                                let competing_live_session_conids = facts
                                    ["competing_live_session_conids"]
                                    .as_array()
                                    .map(|items| {
                                        items
                                            .iter()
                                            .filter_map(serde_json::Value::as_i64)
                                            .map(|conid| conid.to_string())
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    })
                                    .unwrap_or_default();
                                let delayed_market_data =
                                    facts["delayed_market_data"].as_i64().unwrap_or(0);
                                let uncertain_orders =
                                    facts["uncertain_orders"].as_i64().unwrap_or(0);
                                let failed_actions =
                                    facts["failed_strategy_actions"].as_i64().unwrap_or(0);
                                let _ = guard.upsert_monitoring_alert(
                                    "market_data_failed",
                                    "critical",
                                    &format!("{failed_market_data} market-data subscriptions failed"),
                                    failed_market_data > 0,
                                );
                                let _ = guard.upsert_monitoring_alert(
                                    "market_data_competing_live_session",
                                    "critical",
                                    &format!(
                                        "IBKR error 10197: another live session owns market data \
                                         for conid(s) {competing_live_session_conids}. Close or \
                                         disable market data in the competing TWS/IB Gateway \
                                         session; this daemon will keep retrying automatically."
                                    ),
                                    competing_live_session_count > 0,
                                );
                                let _ = guard.upsert_monitoring_alert(
                                    "market_data_delayed",
                                    "warning",
                                    &format!("{delayed_market_data} subscriptions are using delayed data"),
                                    monitor_config.alert_on_delayed_market_data
                                        && delayed_market_data > 0,
                                );
                                let _ = guard.upsert_monitoring_alert(
                                    "uncertain_orders",
                                    "critical",
                                    &format!("{uncertain_orders} orders have unknown outcome"),
                                    uncertain_orders > 0,
                                );
                                let _ = guard.upsert_monitoring_alert(
                                    "failed_strategy_actions",
                                    "critical",
                                    &format!("{failed_actions} strategy actions require review"),
                                    failed_actions > 0,
                                );
                            }
                            if last_performance_snapshot.elapsed()
                                >= Duration::from_secs(
                                    monitor_config.performance_snapshot_seconds,
                                )
                            {
                                let strategies = guard.enabled_strategy_accounts();
                                match strategies {
                                    Ok(strategies) => {
                                        let mut snapshot_failed = None;
                                        for (strategy_id, account) in strategies {
                                            match guard.strategy_performance_report(
                                                strategy_id,
                                                monitor_config.performance_initial_capital,
                                                &monitor_risk.base_currency,
                                                monitor_risk.max_fx_rate_age_seconds,
                                                monitor_risk.max_market_data_age_seconds,
                                                monitor_risk.max_account_data_age_seconds,
                                                None,
                                                Utc::now(),
                                            ) {
                                                Ok(report) => {
                                                    if let Err(error) = guard
                                                        .persist_strategy_performance_snapshot(
                                                            strategy_id,
                                                            &account,
                                                            &report,
                                                        )
                                                    {
                                                        snapshot_failed = Some(error.to_string());
                                                    }
                                                }
                                                Err(error) => {
                                                    snapshot_failed = Some(error.to_string());
                                                }
                                            }
                                        }
                                        let _ = guard.upsert_monitoring_alert(
                                            "performance_snapshot_failed",
                                            "warning",
                                            snapshot_failed.as_deref().unwrap_or(
                                                "strategy performance snapshots are healthy",
                                            ),
                                            snapshot_failed.is_some(),
                                        );
                                    }
                                    Err(error) => tracing::error!(
                                        %error,
                                        "failed to list strategies for performance snapshots"
                                    ),
                                }
                                last_performance_snapshot = tokio::time::Instant::now();
                            }
                        }
                    }
                }
            },
        );
    }
    let job_ibkr = ibkr.clone();
    let job_storage = storage.clone();
    let job_storage_config = config.storage.clone();
    let job_cancellation = cancellation.clone();
    spawn_supervised(
        "backfill-worker",
        cancellation.clone(),
        critical_task_failed.clone(),
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = job_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if job_ibkr.status().state != ibkr::ConnectionState::Ready {
                            continue;
                        }
                        let job = match job_storage
                            .lock_safe()
                            .claim_backfill_job()
                        {
                            Ok(Some(job)) => job,
                            Ok(None) => continue,
                            Err(error) => {
                                tracing::error!(%error, "failed to claim historical data job");
                                continue;
                            }
                        };
                        let slice_end = historical_slice_end(
                            &job.request.timeframe,
                            job.cursor_time,
                            job.request.end,
                        );
                        let request = ibkr::HistoricalBarsRequest {
                            contract: job.request.contract.clone(),
                            timeframe: job.request.timeframe.clone(),
                            start: job.cursor_time,
                            end: slice_end,
                            outside_rth: job.request.outside_rth,
                        };
                        // A slice fetch that hangs must not freeze the job in
                        // 'running' forever; time it out and let the retry
                        // machinery take over.
                        let fetch = tokio::time::timeout(
                            Duration::from_secs(120),
                            job_ibkr.historical_bars(request),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            Err("timed out waiting for IBKR historical bars".into())
                        });
                        match fetch {
                            Ok(bars) => {
                                let result = if bars.is_empty() {
                                    Ok(())
                                } else if let Some(target) = &job.request.fx_rate_pair {
                                    job_storage
                                        .lock_safe()
                                        .write_historical_fx_bars(target, &bars)
                                        .map(|written| {
                                            tracing::info!(
                                                job_id = %job.job_id,
                                                base_currency = %target.base_currency,
                                                quote_currency = %target.quote_currency,
                                                written,
                                                "persisted historical IBKR FX rates"
                                            );
                                        })
                                } else {
                                    job_storage
                                        .lock_safe()
                                        .write_historical_bars_for_session(
                                            &job_storage_config.lake_dir,
                                            &job_storage_config.staging_dir,
                                            &bars,
                                            job.request.outside_rth,
                                        )
                                        .map(|_| ())
                                };
                                match result {
                                    Ok(()) => {
                                        if let Err(error) = job_storage
                                            .lock_safe()
                                            .advance_backfill_job(job.job_id, slice_end, job.request.end)
                                        {
                                            // Leaving the job in 'running' would freeze it until
                                            // the next daemon restart because claiming only
                                            // considers pending/retrying jobs. Rewriting the
                                            // slice on retry is idempotent: fully covered
                                            // Parquet files are superseded transactionally.
                                            tracing::error!(%error, job_id = %job.job_id, "failed to advance data job");
                                            let _ = job_storage
                                                .lock_safe()
                                                .fail_backfill_job(
                                                    job.job_id,
                                                    job.attempts,
                                                    &format!("failed to advance cursor: {error}"),
                                                );
                                        }
                                    }
                                    Err(error) => {
                                        let _ = job_storage
                                            .lock_safe()
                                            .fail_backfill_job(job.job_id, job.attempts, &error.to_string());
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = job_storage
                                    .lock_safe()
                                    .fail_backfill_job(job.job_id, job.attempts, &error);
                            }
                        }
                    }
                }
            }
        },
    );
    let reconcile_ibkr = ibkr.clone();
    let reconcile_storage = storage.clone();
    let reconcile_cancellation = cancellation.clone();
    spawn_supervised(
        "auto-reconciler",
        cancellation.clone(),
        critical_task_failed.clone(),
        async move {
            // Reconciliation runs on every transition to Ready and
            // periodically in between: unknown intents and external orders
            // that appear mid-session must become visible to the readiness
            // gate without waiting for the next reconnect.
            let mut interval = tokio::time::interval(Duration::from_secs(600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let run = tokio::select! {
                    _ = reconcile_cancellation.cancelled() => break,
                    changed = ibkr_status.changed() => {
                        if changed.is_err() { break; }
                        ibkr_status.borrow().state == ibkr::ConnectionState::Ready
                    }
                    _ = interval.tick() => {
                        ibkr_status.borrow().state == ibkr::ConnectionState::Ready
                    }
                };
                if run {
                    match reconcile_ibkr.reconcile().await {
                        Ok(snapshot) => match reconcile_storage.lock_safe().reconcile(&snapshot) {
                            Ok(report) => {
                                tracing::info!(?report, "automatic IBKR reconciliation completed")
                            }
                            Err(error) => {
                                tracing::error!(%error, "automatic reconciliation persistence failed")
                            }
                        },
                        Err(error) => {
                            tracing::error!(%error, "automatic IBKR reconciliation failed")
                        }
                    }
                }
            }
        },
    );
    let initial_status = SystemStatus {
        version: env!("CARGO_PKG_VERSION"),
        pid: std::process::id(),
        started_at,
        uptime_seconds: 0,
        state: SystemState::Starting,
        environment: config.app.environment,
        storage_schema_version: schema_version,
        trading_enabled: config.risk.trading_enabled,
        ibkr: ibkr.status(),
        reconciliation: storage.lock_safe().reconciliation_health(None)?,
    };
    let (status_sender, status_receiver) = watch::channel(initial_status.clone());
    let mut ready_status = initial_status;
    ready_status.state = SystemState::Ready;
    status_sender.send_replace(ready_status);

    if config.web.enabled {
        let web_config = config.web.clone();
        let web_cancellation = cancellation.clone();
        let rpc_address = config.rpc.http_listen;
        spawn_supervised(
            "web-ui-server",
            cancellation.clone(),
            critical_task_failed.clone(),
            async move {
                if let Err(error) = web_server::run(
                    web_config.listen,
                    web_config.static_dir,
                    rpc_address,
                    web_cancellation,
                )
                .await
                {
                    tracing::error!(%error, "Web UI server failed");
                }
            },
        );
    }

    let server = RpcServer::new(
        config.rpc.clone(),
        status_receiver,
        ibkr,
        storage.clone(),
        strategy_order_coordination,
        config.risk.clone(),
        config.storage.lake_dir.clone(),
        config.storage.staging_dir.clone(),
        config.storage.backup_dir.clone(),
        cancellation.clone(),
    );
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for shutdown signal");
        }
        signal_cancellation.cancel();
    });

    tracing::info!(
        environment = ?config.app.environment,
        schema_version,
        trading_enabled = config.risk.trading_enabled,
        "daemon ready"
    );
    server.run().await?;

    let mut draining = status_sender.borrow().clone();
    draining.state = SystemState::Draining;
    draining.uptime_seconds = (Utc::now() - started_at).num_seconds().max(0) as u64;
    status_sender.send_replace(draining);
    drop(storage);
    if critical_task_failed.load(std::sync::atomic::Ordering::SeqCst) {
        tracing::error!("daemon stopped because a critical background task failed");
        return Err(crate::error::AppError::TaskFailed(
            "a critical background task panicked or exited unexpectedly; \
             check the logs above, resolve the cause, and restart the daemon"
                .into(),
        ));
    }
    tracing::info!("daemon stopped");
    Ok(())
}

async fn rpc_call(config: &Config, method: &str) -> Result<Value> {
    rpc_call_with_params(config, method, json!({})).await
}

async fn rpc_call_with_params(config: &Config, method: &str, params: Value) -> Result<Value> {
    rpc::call(
        config.rpc.http_listen,
        method,
        params,
        Duration::from_secs(config.rpc.request_timeout_seconds),
    )
    .await
}

fn print_value(value: &Value, force_json: bool) {
    if force_json {
        println!(
            "{}",
            serde_json::to_string(value).expect("JSON value serializes")
        );
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).expect("JSON value serializes")
        );
    }
}

fn strategy_execution_price(
    quote: &Value,
    side: &str,
    now: DateTime<Utc>,
    maximum_age_seconds: u64,
) -> Option<f64> {
    let tick_type = if side.eq_ignore_ascii_case("buy") {
        "Ask"
    } else {
        "Bid"
    };
    let tick = &quote["ticks"][tick_type];
    let observed_at = tick["observed_at"]
        .as_str()
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())?;
    if (now - observed_at).num_seconds().max(0) > maximum_age_seconds as i64 {
        return None;
    }
    tick["numeric_value"]
        .as_f64()
        .filter(|price| price.is_finite() && *price > 0.0)
}

fn strategy_market_data_rejection(
    symbol: &str,
    health: &crate::storage::MarketDataHealth,
    risk_reducing: bool,
) -> Option<String> {
    if health.state == "fresh" || risk_reducing {
        return None;
    }
    let age = health
        .age_seconds
        .map(|seconds| format!("{seconds}s"))
        .unwrap_or_else(|| "unavailable".into());
    let subscription = health.subscription_state.as_deref().unwrap_or("missing");
    Some(format!(
        "{symbol} risk-increasing order skipped locally: market data is {} \
         (subscription {subscription}, age {age}, maximum {}s); only a strictly \
         position-reducing order may bypass non-fresh market data",
        health.state, health.maximum_age_seconds
    ))
}

fn strategy_submission_error_state(uncertain: bool) -> &'static str {
    if uncertain { "failed" } else { "rejected" }
}

fn strategy_calendar_exchange(contract: &crate::ibkr::ContractCandidate) -> &str {
    let primary = contract.primary_exchange.trim();
    if !primary.is_empty() && !primary.eq_ignore_ascii_case("SMART") {
        primary
    } else {
        contract.exchange.trim()
    }
}

fn strategy_session_rejection(
    outside_rth: bool,
    exchange: &str,
    session_open: Option<bool>,
) -> Option<String> {
    let session_kind = if outside_rth {
        "IBKR extended-hours"
    } else {
        "IBKR regular-hours"
    };
    match session_open {
        Some(true) => None,
        Some(false) => Some(format!(
            "order skipped locally: configured {session_kind} calendar reports {exchange} closed"
        )),
        None => Some(format!(
            "order skipped locally: no {session_kind} calendar is configured for {exchange}; \
             automatic execution fails closed"
        )),
    }
}

fn order_params(order: OrderArgs) -> Value {
    json!({
        "idempotency_key": order.idempotency_key,
        "account": order.account,
        "contract": {
            "conid": order.conid,
            "symbol": order.symbol,
            "security_type": order.security_type,
            "currency": order.currency,
            "exchange": order.exchange,
            "primary_exchange": order.primary_exchange,
            "local_symbol": order.local_symbol,
            "description": "",
            "derivative_security_types": []
        },
        "side": order.side.to_ascii_lowercase(),
        "quantity": order.quantity,
        "order_type": order.order_type.to_ascii_lowercase(),
        "limit_price": order.limit_price,
        "estimated_price": order.estimated_price,
        "outside_rth": order.outside_rth
    })
}

fn historical_slice_end(
    timeframe: &str,
    start: chrono::DateTime<chrono::Utc>,
    requested_end: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    if timeframe == "5s" {
        return (start + chrono::Duration::hours(1)).min(requested_end);
    }
    let days = match timeframe {
        "1m" => 1,
        "5m" => 7,
        "15m" => 14,
        "30m" => 30,
        "1h" => 90,
        "1d" => 365,
        _ => 1,
    };
    (start + chrono::Duration::days(days)).min(requested_end)
}

#[cfg(test)]
mod execution_tests {
    use super::*;
    use crate::storage::position_change_is_risk_reducing;

    #[test]
    fn automatic_execution_uses_only_fresh_live_ticks() {
        let now = "2026-08-07T08:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let delayed = json!({
            "ticks": {
                "DelayedBid": {"numeric_value": 100.0, "observed_at": now},
                "DelayedAsk": {"numeric_value": 101.0, "observed_at": now}
            }
        });
        assert_eq!(strategy_execution_price(&delayed, "buy", now, 30), None);
        let live = json!({
            "ticks": {
                "Bid": {"numeric_value": 100.0, "observed_at": now - chrono::Duration::seconds(1)},
                "Ask": {"numeric_value": 101.0, "observed_at": now - chrono::Duration::seconds(1)}
            }
        });
        assert_eq!(strategy_execution_price(&live, "buy", now, 30), Some(101.0));
        assert_eq!(
            strategy_execution_price(&live, "sell", now, 30),
            Some(100.0)
        );

        let stale_ask = json!({
            "ticks": {
                "Bid": {"numeric_value": 100.0, "observed_at": now - chrono::Duration::seconds(1)},
                "Ask": {"numeric_value": 101.0, "observed_at": now - chrono::Duration::seconds(31)}
            }
        });
        assert_eq!(
            strategy_execution_price(&stale_ask, "buy", now, 30),
            None,
            "a buy must never substitute Bid for a missing/stale Ask"
        );
        let stale = json!({
            "ticks": {
                "Bid": {"numeric_value": 100.0, "observed_at": now - chrono::Duration::seconds(31)},
                "Ask": {"numeric_value": 101.0, "observed_at": now - chrono::Duration::seconds(31)}
            }
        });
        assert_eq!(strategy_execution_price(&stale, "buy", now, 30), None);
    }

    #[test]
    fn non_fresh_market_data_only_bypasses_preflight_for_strict_reductions() {
        let health = crate::storage::MarketDataHealth {
            state: "stale",
            conid: 272093,
            subscription_state: Some("retrying".into()),
            latest_price: Some(100.0),
            latest_price_type: Some("Ask".into()),
            observed_at: None,
            age_seconds: Some(31),
            maximum_age_seconds: 30,
        };

        let rejection = strategy_market_data_rejection("MSFT", &health, false).unwrap();
        assert!(rejection.contains("MSFT risk-increasing order skipped locally"));
        assert!(rejection.contains("market data is stale"));
        assert!(rejection.contains("strictly position-reducing"));
        assert_eq!(strategy_market_data_rejection("MSFT", &health, true), None);

        let fresh = crate::storage::MarketDataHealth {
            state: "fresh",
            ..health
        };
        assert_eq!(strategy_market_data_rejection("MSFT", &fresh, false), None);
    }

    #[test]
    fn five_second_backfills_are_split_into_hourly_requests() {
        let start = Utc::now();
        let end = start + chrono::Duration::hours(3);
        assert_eq!(
            historical_slice_end("5s", start, end),
            start + chrono::Duration::hours(1)
        );
        assert_eq!(
            historical_slice_end("5s", start, start + chrono::Duration::minutes(30)),
            start + chrono::Duration::minutes(30)
        );
    }

    #[test]
    fn cost_estimate_supports_fixed_and_proportional_fees() {
        let mut cost = crate::storage::ClaimedCostControl {
            model: crate::storage::ExecutionCostModelInput {
                cost_model_id: None,
                name: "test".into(),
                currency: "USD".into(),
                buy_fixed_fee: 1.0,
                buy_per_share_fee: 0.0,
                buy_rate_bps: 0.0,
                buy_min_fee: 1.0,
                sell_fixed_fee: 1.0,
                sell_per_share_fee: 0.0,
                sell_rate_bps: 0.0,
                sell_min_fee: 1.0,
                sell_tax_bps: 0.0,
                estimated_spread_bps: 0.0,
                estimated_slippage_bps: 0.0,
            },
            minimum_cost_multiple: 2.0,
            maximum_commission_to_gross_profit_ratio: 0.5,
            minimum_completed_trades: 5,
            actual_fee_bps_p90: None,
        };
        assert_eq!(
            cost.model.estimated_round_trip_cost(1_000.0, 10.0, None),
            2.0
        );
        cost.model.buy_fixed_fee = 0.0;
        cost.model.sell_fixed_fee = 0.0;
        cost.model.buy_min_fee = 0.0;
        cost.model.sell_min_fee = 0.0;
        cost.model.buy_rate_bps = 5.0;
        cost.model.sell_rate_bps = 5.0;
        assert_eq!(
            cost.model.estimated_round_trip_cost(10_000.0, 10.0, None),
            10.0
        );
        cost.model.buy_min_fee = 15.0;
        cost.model.sell_min_fee = 15.0;
        assert_eq!(
            cost.model.estimated_round_trip_cost(1_000.0, 10.0, None),
            30.0
        );
        cost.model.buy_min_fee = 0.0;
        cost.model.sell_min_fee = 0.0;
        cost.model.buy_rate_bps = 0.0;
        cost.model.sell_rate_bps = 0.0;
        cost.model.buy_per_share_fee = 0.005;
        cost.model.sell_per_share_fee = 0.005;
        assert_eq!(
            cost.model.estimated_round_trip_cost(1_000.0, 100.0, None),
            1.0
        );
        cost.model.buy_per_share_fee = 0.0;
        cost.model.sell_per_share_fee = 0.0;
        cost.model.buy_min_fee = 1.0;
        cost.model.sell_min_fee = 1.0;
        cost.model.sell_tax_bps = 10.0;
        cost.model.estimated_spread_bps = 4.0;
        cost.model.estimated_slippage_bps = 3.0;
        // Buy/sell minimum commissions (2), sell tax (1), one full spread
        // crossing (0.4), and two one-way slippage estimates (0.6).
        assert_eq!(
            cost.model.estimated_round_trip_cost(1_000.0, 100.0, None),
            4.0
        );
    }

    #[test]
    fn cost_gate_only_bypasses_position_reducing_changes() {
        assert!(position_change_is_risk_reducing(10.0, 0.0));
        assert!(position_change_is_risk_reducing(10.0, 4.0));
        assert!(position_change_is_risk_reducing(-10.0, -4.0));
        assert!(!position_change_is_risk_reducing(0.0, 10.0));
        assert!(!position_change_is_risk_reducing(4.0, 10.0));
        assert!(!position_change_is_risk_reducing(10.0, -4.0));
    }

    #[test]
    fn deterministic_strategy_submission_errors_are_rejected_not_failed() {
        assert_eq!(strategy_submission_error_state(false), "rejected");
        assert_eq!(strategy_submission_error_state(true), "failed");
    }

    #[test]
    fn automatic_execution_checks_the_primary_exchange_calendar() {
        let contract = crate::ibkr::ContractCandidate {
            conid: 272093,
            symbol: "MSFT".into(),
            security_type: "STK".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            local_symbol: "MSFT".into(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };
        assert_eq!(strategy_calendar_exchange(&contract), "NASDAQ");

        let mut routing_only = contract;
        routing_only.primary_exchange.clear();
        assert_eq!(strategy_calendar_exchange(&routing_only), "SMART");

        assert!(strategy_session_rejection(false, "NASDAQ", Some(true)).is_none());
        assert!(strategy_session_rejection(false, "NASDAQ", Some(false)).is_some());
        assert!(strategy_session_rejection(false, "NASDAQ", None).is_some());
        assert!(strategy_session_rejection(true, "NASDAQ", None).is_some());
    }
}
