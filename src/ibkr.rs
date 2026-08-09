use std::collections::HashMap;
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use futures::{FutureExt, StreamExt};
use ibapi::Client;
use ibapi::{
    accounts::{AccountMultiValue, AccountSummaryResult, AccountUpdateMulti, PositionUpdate},
    contracts::Contract,
    market_data::historical::{BarSize, BarTimestamp, Duration as HistoricalDuration, WhatToShow},
    subscriptions::{SubscriptionItem, SubscriptionItemStreamExt},
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::config::IbkrConfig;

const COMMAND_CAPACITY: usize = 32;

/// Prefix marking broker errors where the request was transmitted but its
/// outcome could not be confirmed. Callers must treat these as "unknown", never
/// as a definitive rejection.
pub const UNKNOWN_OUTCOME_PREFIX: &str = "UNKNOWN_OUTCOME: ";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Ready,
    Reconnecting,
    Stopping,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    pub connection_session_id: Option<uuid::Uuid>,
    pub desired: bool,
    pub endpoint: String,
    pub client_id: i32,
    pub server_version: Option<i32>,
    pub connected_at: Option<DateTime<Utc>>,
    pub managed_accounts: Vec<String>,
    pub last_error: Option<String>,
    pub reconnect_attempt: u32,
}

impl ConnectionStatus {
    pub(crate) fn new(config: &IbkrConfig) -> Self {
        Self {
            state: ConnectionState::Disconnected,
            connection_session_id: None,
            desired: config.connect_on_start,
            endpoint: endpoint(config),
            client_id: config.client_id,
            server_version: None,
            connected_at: None,
            managed_accounts: Vec::new(),
            last_error: None,
            reconnect_attempt: 0,
        }
    }
}

enum Command {
    Connect {
        response: oneshot::Sender<CommandResult>,
    },
    Disconnect {
        response: oneshot::Sender<CommandResult>,
    },
    SearchContracts {
        pattern: String,
        response: oneshot::Sender<CommandResult<Vec<ContractCandidate>>>,
    },
    FxRateSnapshot {
        quote_currency: String,
        response: oneshot::Sender<CommandResult<Vec<FxRateSnapshot>>>,
    },
    ContractSchedule {
        contract: ContractCandidate,
        response: oneshot::Sender<CommandResult<ContractSchedule>>,
    },
    HistoricalBars {
        request: HistoricalBarsRequest,
        response: oneshot::Sender<CommandResult<Vec<HistoricalBar>>>,
    },
    PlaceOrder {
        request: BrokerOrderRequest,
        response: oneshot::Sender<CommandResult<i32>>,
    },
    CancelOrder {
        broker_order_id: i32,
        response: oneshot::Sender<CommandResult>,
    },
    Reconcile {
        response: oneshot::Sender<CommandResult<ReconciliationSnapshot>>,
    },
    SubscribeMarketData {
        contract: ContractCandidate,
        response: oneshot::Sender<CommandResult>,
    },
    UnsubscribeMarketData {
        conid: i32,
        response: oneshot::Sender<CommandResult>,
    },
}

type CommandResult<T = ()> = std::result::Result<T, String>;

#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Command>,
    status: watch::Receiver<ConnectionStatus>,
    events: std::sync::Arc<Mutex<Option<mpsc::Receiver<BrokerEvent>>>>,
}

#[derive(Clone, Debug)]
pub enum BrokerEvent {
    OrderStatus {
        connection_session_id: Option<uuid::Uuid>,
        broker_order_id: i32,
        status: String,
        filled: f64,
        remaining: f64,
        average_fill_price: Option<f64>,
        last_fill_price: Option<f64>,
        perm_id: i64,
        why_held: String,
        market_cap_price: Option<f64>,
    },
    OpenOrder {
        connection_session_id: Option<uuid::Uuid>,
        broker_order_id: i32,
        perm_id: i64,
        status: String,
        reject_reason: String,
        warning_text: String,
        completed_time: String,
        completed_status: String,
    },
    Execution {
        connection_session_id: Option<uuid::Uuid>,
        broker_order_id: i32,
        perm_id: i64,
        execution_id: String,
        conid: i32,
        side: String,
        quantity: f64,
        price: f64,
        executed_at: DateTime<Utc>,
    },
    Commission {
        execution_id: String,
        commission: f64,
        currency: String,
    },
    AccountSummary {
        account: String,
        tag: String,
        value: String,
        currency: String,
        observed_at: DateTime<Utc>,
    },
    Position {
        subscription_id: uuid::Uuid,
        position: PositionSnapshot,
    },
    PositionSnapshotStarted {
        subscription_id: uuid::Uuid,
        observed_at: DateTime<Utc>,
    },
    PositionSnapshotCompleted {
        subscription_id: uuid::Uuid,
        observed_at: DateTime<Utc>,
    },
    /// Confirms that the long-lived IBKR position subscription is still being
    /// consumed. IBKR only sends position records when they change, so the
    /// absence of updates after the initial snapshot must not make an
    /// unchanged portfolio appear stale.
    PositionSubscriptionHeartbeat {
        subscription_id: uuid::Uuid,
        observed_at: DateTime<Utc>,
    },
    /// Invalidates the authoritative position lease when IBKR closes the
    /// stream or the stream fails. The subscription identifier ensures a
    /// delayed terminal event cannot invalidate a newer snapshot.
    PositionSubscriptionEnded {
        subscription_id: uuid::Uuid,
        observed_at: DateTime<Utc>,
        reason: String,
    },
    Pnl {
        account: String,
        daily_pnl: f64,
        unrealized_pnl: Option<f64>,
        realized_pnl: Option<f64>,
        observed_at: DateTime<Utc>,
    },
    MarketDataTick {
        conid: i32,
        tick_type: String,
        numeric_value: Option<f64>,
        text_value: Option<String>,
        observed_at: DateTime<Utc>,
    },
    MarketDataStatus {
        conid: i32,
        state: String,
        error: Option<String>,
        observed_at: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct OpenOrderSnapshot {
    pub broker_order_id: i32,
    pub perm_id: i64,
    pub client_id: i32,
    pub account: String,
    pub conid: i32,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub order_type: String,
    pub limit_price: Option<f64>,
    pub status: String,
    pub completed_time: Option<String>,
    pub completed_status: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ReconciliationSnapshot {
    pub connection_session_id: uuid::Uuid,
    pub open_orders: Vec<OpenOrderSnapshot>,
    pub completed_orders: Vec<OpenOrderSnapshot>,
    pub events: Vec<BrokerEvent>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContractCandidate {
    pub conid: i32,
    pub symbol: String,
    pub security_type: String,
    pub currency: String,
    pub exchange: String,
    pub primary_exchange: String,
    pub local_symbol: String,
    pub description: String,
    pub derivative_security_types: Vec<String>,
}

/// An IBKR account exchange rate, expressed as one unit of `base_currency`
/// converted into the account's `quote_currency` (its configured base
/// currency).
#[derive(Clone, Debug, PartialEq)]
pub struct FxRateSnapshot {
    pub account: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub rate: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ContractSchedule {
    pub conid: i32,
    pub exchange: String,
    pub time_zone_id: String,
    pub regular_sessions: Vec<ContractSession>,
    pub extended_sessions: Vec<ContractSession>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ContractSession {
    pub trading_date: NaiveDate,
    pub opens_at: DateTime<Utc>,
    pub closes_at: DateTime<Utc>,
}

impl ContractCandidate {
    pub fn normalize_streaming_subscription(&mut self) {
        self.symbol = self.symbol.trim().to_string();
        self.security_type = self.security_type.trim().to_ascii_uppercase();
        self.currency = self.currency.trim().to_ascii_uppercase();
        self.exchange = self.exchange.trim().to_ascii_uppercase();
        self.primary_exchange = self.primary_exchange.trim().to_string();
        self.local_symbol = self.local_symbol.trim().to_string();

        if self.security_type == "STK" {
            if self.exchange.is_empty() {
                self.exchange = "SMART".into();
            }
            if self.local_symbol.is_empty() {
                self.local_symbol = self.symbol.clone();
            }
        }
    }

    pub fn validate_streaming_subscription(&self) -> CommandResult {
        if self.conid <= 0 {
            return Err("market-data contract requires a positive conid".into());
        }
        if self.security_type != "STK" {
            return Err("market-data service currently supports only STK contracts".into());
        }
        if self.symbol.trim().is_empty() {
            return Err("market-data contract requires a symbol".into());
        }
        if self.currency.trim().is_empty() {
            return Err("market-data contract requires a currency".into());
        }
        if self.exchange.trim().is_empty() {
            return Err(
                "market-data contract requires an exchange; use SMART for STK routing".into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PositionSnapshot {
    pub account: String,
    pub conid: i32,
    pub symbol: String,
    pub security_type: String,
    pub currency: String,
    pub exchange: String,
    pub quantity: f64,
    pub average_cost: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HistoricalBarsRequest {
    pub contract: ContractCandidate,
    pub timeframe: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(default)]
    pub outside_rth: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct HistoricalBar {
    pub conid: i32,
    pub timeframe: String,
    pub open_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub wap: f64,
    pub trade_count: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BrokerOrderRequest {
    pub contract: ContractCandidate,
    pub side: String,
    pub quantity: f64,
    pub order_type: String,
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub outside_rth: bool,
}

impl Handle {
    pub fn status(&self) -> ConnectionStatus {
        self.status.borrow().clone()
    }

    pub async fn connect(&self) -> CommandResult {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Connect { response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn disconnect(&self) -> CommandResult {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Disconnect { response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub fn managed_accounts(&self) -> CommandResult<Vec<String>> {
        let status = self.status();
        if status.state != ConnectionState::Ready {
            return Err("IBKR is not ready".into());
        }
        Ok(status.managed_accounts)
    }

    pub async fn search_contracts(&self, pattern: String) -> CommandResult<Vec<ContractCandidate>> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::SearchContracts { pattern, response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    /// Fetches a fresh account-value snapshot from IBKR and extracts every
    /// `ExchangeRate` whose quote currency is the configured account base
    /// currency. This uses the account API rather than a market-data
    /// subscription, so it does not consume an FX quote subscription or create
    /// another competing market-data session.
    pub async fn fx_rate_snapshot(
        &self,
        quote_currency: String,
    ) -> CommandResult<Vec<FxRateSnapshot>> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::FxRateSnapshot {
                quote_currency,
                response,
            })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn contract_schedule(
        &self,
        contract: ContractCandidate,
    ) -> CommandResult<ContractSchedule> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::ContractSchedule { contract, response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn historical_bars(
        &self,
        request: HistoricalBarsRequest,
    ) -> CommandResult<Vec<HistoricalBar>> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::HistoricalBars { request, response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn place_order(&self, request: BrokerOrderRequest) -> CommandResult<i32> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::PlaceOrder { request, response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn cancel_order(&self, broker_order_id: i32) -> CommandResult {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::CancelOrder {
                broker_order_id,
                response,
            })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn take_events(&self) -> Option<mpsc::Receiver<BrokerEvent>> {
        self.events.lock().await.take()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.clone()
    }

    pub async fn reconcile(&self) -> CommandResult<ReconciliationSnapshot> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Reconcile { response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn subscribe_market_data(&self, contract: ContractCandidate) -> CommandResult {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::SubscribeMarketData { contract, response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }

    pub async fn unsubscribe_market_data(&self, conid: i32) -> CommandResult {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::UnsubscribeMarketData { conid, response })
            .await
            .map_err(|_| "IBKR actor is not running".to_string())?;
        receiver
            .await
            .map_err(|_| "IBKR actor stopped before responding".to_string())?
    }
}

pub fn spawn(
    config: IbkrConfig,
    max_account_data_age_seconds: u64,
    cancellation: CancellationToken,
) -> Handle {
    let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (event_sender, event_receiver) = mpsc::channel(1024);
    let events = std::sync::Arc::new(Mutex::new(Some(event_receiver)));
    let (status_sender, status) = watch::channel(ConnectionStatus::new(&config));
    tokio::spawn(
        Actor {
            desired: config.connect_on_start,
            config,
            position_heartbeat_interval: position_heartbeat_interval(max_account_data_age_seconds),
            pnl_idle_timeout: pnl_idle_timeout(max_account_data_age_seconds),
            commands: receiver,
            status: status_sender,
            cancellation,
            client: None,
            subscription_cancellation: None,
            market_data_contracts: HashMap::new(),
            market_data_cancellations: HashMap::new(),
            next_attempt_at: Instant::now(),
            reconnect_attempt: 0,
            events: event_sender,
        }
        .run(),
    );
    Handle {
        commands,
        status,
        events,
    }
}

struct Actor {
    config: IbkrConfig,
    position_heartbeat_interval: Duration,
    pnl_idle_timeout: Duration,
    commands: mpsc::Receiver<Command>,
    status: watch::Sender<ConnectionStatus>,
    cancellation: CancellationToken,
    client: Option<Arc<Client>>,
    subscription_cancellation: Option<CancellationToken>,
    market_data_contracts: HashMap<i32, ContractCandidate>,
    market_data_cancellations: HashMap<i32, CancellationToken>,
    desired: bool,
    next_attempt_at: Instant,
    reconnect_attempt: u32,
    events: mpsc::Sender<BrokerEvent>,
}

impl Actor {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => break,
                command = self.commands.recv() => {
                    match command {
                        Some(command) => self.handle_command(command).await,
                        None => break,
                    }
                }
                _ = interval.tick() => self.maintain_connection().await,
            }
        }

        self.publish(ConnectionState::Stopping, None, None, None);
        self.stop_account_subscriptions();
        if let Some(client) = self.client.take() {
            client.disconnect().await;
        }
        self.desired = false;
        self.publish(ConnectionState::Disconnected, None, None, None);
        tracing::info!("IBKR actor stopped");
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Connect { response } => {
                self.desired = true;
                self.next_attempt_at = Instant::now();
                self.reconnect_attempt = 0;
                self.publish(self.current_state(), None, None, None);
                let _ = response.send(Ok(()));
            }
            Command::Disconnect { response } => {
                self.desired = false;
                self.stop_account_subscriptions();
                if let Some(client) = self.client.take() {
                    client.disconnect().await;
                }
                self.reconnect_attempt = 0;
                self.publish(ConnectionState::Disconnected, None, None, None);
                let _ = response.send(Ok(()));
            }
            Command::SearchContracts { pattern, response } => {
                let result = match self.ready_client() {
                    Ok(client) => match client.matching_symbols(&pattern).await {
                        Ok(items) => {
                            let mut candidates = Vec::new();
                            for item in items.into_iter().take(20) {
                                let derivative_security_types = item.derivative_security_types;
                                let hinted_contract = item.contract;
                                if hinted_contract.contract_id <= 0 {
                                    continue;
                                }
                                let hinted_conid = hinted_contract.contract_id;
                                let resolved = client
                                    .contract_details(&hinted_contract)
                                    .await
                                    .ok()
                                    .and_then(|details| {
                                        details.into_iter().find(|detail| {
                                            detail.contract.contract_id == hinted_conid
                                        })
                                    });
                                let mut candidate = if let Some(details) = resolved {
                                    let mut candidate = contract_candidate(details.contract);
                                    if candidate.description.is_empty() {
                                        candidate.description = details.long_name;
                                    }
                                    candidate
                                } else {
                                    contract_candidate(hinted_contract)
                                };
                                candidate.derivative_security_types = derivative_security_types;
                                normalize_search_candidate(&mut candidate);
                                candidates.push(candidate);
                            }
                            Ok(candidates)
                        }
                        Err(error) => Err(error.to_string()),
                    },
                    Err(error) => Err(error),
                };
                let _ = response.send(result);
            }
            Command::FxRateSnapshot {
                quote_currency,
                response,
            } => {
                let client = self
                    .client
                    .as_ref()
                    .filter(|client| client.is_connected())
                    .cloned();
                let accounts = self.config.account.clone().map_or_else(
                    || self.status.borrow().managed_accounts.clone(),
                    |account| vec![account],
                );
                let timeout = Duration::from_secs(self.config.request_timeout_seconds);
                match client {
                    Some(client) if !accounts.is_empty() => {
                        tokio::spawn(async move {
                            let result = fetch_fx_rate_snapshots(
                                client,
                                &accounts,
                                &quote_currency,
                                timeout,
                            )
                            .await;
                            let _ = response.send(result);
                        });
                    }
                    Some(_) => {
                        let _ = response.send(Err(
                            "IBKR returned no managed account for the FX-rate snapshot".into(),
                        ));
                    }
                    None => {
                        let _ = response.send(Err("IBKR is not ready".into()));
                    }
                }
            }
            Command::ContractSchedule { contract, response } => {
                let result = match self.ready_client() {
                    Ok(client) => {
                        let requested_conid = contract.conid;
                        match client.contract_details(&candidate_contract(&contract)).await {
                            Ok(details) => details
                                .into_iter()
                                .find(|detail| detail.contract.contract_id == requested_conid)
                                .ok_or_else(|| {
                                    format!(
                                        "IBKR returned no contract details for conid {requested_conid}"
                                    )
                                })
                                .and_then(|details| contract_schedule(&details)),
                            Err(error) => Err(error.to_string()),
                        }
                    }
                    Err(error) => Err(error),
                };
                let _ = response.send(result);
            }
            Command::HistoricalBars { request, response } => {
                let result = self.fetch_historical_bars(request).await;
                let _ = response.send(result);
            }
            Command::PlaceOrder { request, response } => {
                let result = self.submit_order(request).await;
                let _ = response.send(result);
            }
            Command::CancelOrder {
                broker_order_id,
                response,
            } => {
                let timeout = Duration::from_secs(self.config.request_timeout_seconds);
                let result = match self.ready_client() {
                    Ok(client) => match tokio::time::timeout(
                        timeout,
                        client.cancel_order(broker_order_id, ""),
                    )
                    .await
                    {
                        Ok(result) => result
                            .map(|_subscription| ())
                            .map_err(|error| error.to_string()),
                        Err(_) => Err(format!(
                            "IBKR cancel-order request timed out after {} seconds",
                            timeout.as_secs()
                        )),
                    },
                    Err(error) => Err(error),
                };
                let _ = response.send(result);
            }
            Command::Reconcile { response } => {
                let timeout = Duration::from_secs(self.config.request_timeout_seconds);
                let result =
                    match tokio::time::timeout(timeout, self.fetch_reconciliation_snapshot()).await
                    {
                        Ok(result) => result,
                        Err(_) => Err(format!(
                            "IBKR reconciliation timed out after {} seconds; the in-flight \
                             broker requests were cancelled and the IBKR actor remains available",
                            timeout.as_secs()
                        )),
                    };
                let _ = response.send(result);
            }
            Command::SubscribeMarketData {
                mut contract,
                response,
            } => {
                contract.normalize_streaming_subscription();
                let result = if let Err(error) = contract.validate_streaming_subscription() {
                    Err(error)
                } else {
                    let conid = contract.conid;
                    self.market_data_contracts.insert(conid, contract.clone());
                    if let (Some(client), Some(session_cancellation)) = (
                        self.client.as_ref().filter(|client| client.is_connected()),
                        self.subscription_cancellation.as_ref(),
                    ) {
                        if let Some(previous) = self.market_data_cancellations.remove(&conid) {
                            previous.cancel();
                        }
                        let cancellation = session_cancellation.child_token();
                        self.market_data_cancellations
                            .insert(conid, cancellation.clone());
                        spawn_market_data_subscription(
                            client.clone(),
                            self.events.clone(),
                            cancellation,
                            contract,
                        );
                    }
                    Ok(())
                };
                let _ = response.send(result);
            }
            Command::UnsubscribeMarketData { conid, response } => {
                self.market_data_contracts.remove(&conid);
                if let Some(cancellation) = self.market_data_cancellations.remove(&conid) {
                    cancellation.cancel();
                }
                let _ = response.send(Ok(()));
            }
        }
    }

    fn ready_client(&self) -> CommandResult<&Client> {
        self.client
            .as_ref()
            .filter(|client| client.is_connected())
            .map(Arc::as_ref)
            .ok_or_else(|| "IBKR is not ready".into())
    }

    async fn fetch_historical_bars(
        &self,
        request: HistoricalBarsRequest,
    ) -> CommandResult<Vec<HistoricalBar>> {
        let (broker_start, broker_end) = historical_broker_window(&request);
        let first =
            AssertUnwindSafe(self.fetch_historical_bars_once(&request, broker_start, broker_end))
                .catch_unwind()
                .await;
        match first {
            Ok(result) => result,
            Err(_) => {
                // rust-ibapi 3.3 decodes HistoricalDataEnd's local timestamps
                // with OffsetResult::unwrap(). During a daylight-saving fall
                // back, an endpoint inside the repeated hour panics even though
                // every returned Bar carries an unambiguous epoch timestamp.
                // Widening the broker request moves both metadata endpoints out
                // of that repeated hour; results are still filtered to the
                // original requested range below.
                let retry_start = broker_start - chrono::Duration::hours(2);
                let retry_end = broker_end + chrono::Duration::hours(2);
                tracing::warn!(
                    conid = request.contract.conid,
                    retry_start = %retry_start,
                    original_end = %request.end,
                    retry_end = %retry_end,
                    "IBKR historical response used an ambiguous DST endpoint; retrying with a widened broker window"
                );
                match AssertUnwindSafe(self.fetch_historical_bars_once(
                    &request,
                    retry_start,
                    retry_end,
                ))
                .catch_unwind()
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(
                        "IBKR historical decoder panicked on an ambiguous daylight-saving-time endpoint even after widening the request"
                            .into(),
                    ),
                }
            }
        }
    }

    async fn fetch_historical_bars_once(
        &self,
        request: &HistoricalBarsRequest,
        broker_start: DateTime<Utc>,
        broker_end: DateTime<Utc>,
    ) -> CommandResult<Vec<HistoricalBar>> {
        if request.end <= request.start {
            return Err("historical end must be after start".into());
        }
        let contract = candidate_contract(&request.contract);
        let client = self.ready_client()?;
        let end = time::OffsetDateTime::from_unix_timestamp(broker_end.timestamp())
            .map_err(|error| error.to_string())?;
        let trading_hours = if request.outside_rth {
            ibapi::market_data::TradingHours::Extended
        } else {
            ibapi::market_data::TradingHours::Regular
        };
        let duration = historical_duration(&request.timeframe, broker_start, broker_end)?;
        let mut builder = client
            .historical_data(&contract, parse_bar_size(&request.timeframe)?)
            .duration(duration)
            .ending(end)
            .trading_hours(trading_hours);
        // CASH contracts do not provide a stock-like TRADES series. MIDPOINT
        // is the auditable IBKR historical source used to convert executions
        // into the configured performance currency.
        if request.contract.security_type.eq_ignore_ascii_case("CASH") {
            builder = builder.what_to_show(WhatToShow::MidPoint);
        }
        let data = builder.fetch().await.map_err(|error| error.to_string())?;
        data.bars
            .into_iter()
            .map(|bar| {
                Ok(HistoricalBar {
                    conid: request.contract.conid,
                    timeframe: request.timeframe.clone(),
                    open_time: bar_timestamp_utc(bar.date)?,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    volume: bar.volume,
                    wap: bar.wap,
                    trade_count: bar.count,
                })
            })
            .filter(|result| match result {
                Ok(bar) => bar.open_time >= request.start && bar.open_time < request.end,
                Err(_) => true,
            })
            .collect()
    }

    async fn submit_order(&self, request: BrokerOrderRequest) -> CommandResult<i32> {
        let client = self.ready_client()?;
        let connection_session_id = self
            .status
            .borrow()
            .connection_session_id
            .ok_or_else(|| "IBKR connection has no active session id".to_string())?;
        let contract = candidate_contract(&request.contract);
        let builder = client.order(&contract);
        let side = request.side.to_ascii_lowercase();
        let builder = match side.as_str() {
            "buy" => builder.buy(request.quantity),
            "sell" => builder.sell(request.quantity),
            _ => return Err("side must be buy or sell".into()),
        };
        let order_type = request.order_type.to_ascii_lowercase();
        let builder = match order_type.as_str() {
            "market" => builder.market(),
            "limit" => builder.limit(
                request
                    .limit_price
                    .ok_or_else(|| "limit_price is required for limit orders".to_string())?,
            ),
            _ => return Err("order_type must be market or limit".into()),
        };
        let builder = if request.outside_rth {
            builder.outside_rth()
        } else {
            builder
        };
        let order = builder.build().map_err(|error| error.to_string())?;
        let order_id = client
            .next_valid_order_id()
            .await
            .map_err(|error| error.to_string())?;
        let subscription = client
            .place_order(order_id, &contract, &order)
            .await
            .map_err(|error| error.to_string())?;
        let mut updates = subscription.filter_data();
        match tokio::time::timeout(Duration::from_secs(5), updates.next()).await {
            Ok(Some(Ok(update))) => {
                send_place_order_event(&self.events, connection_session_id, update).await;
                let events = self.events.clone();
                tokio::spawn(async move {
                    while let Some(update) = updates.next().await {
                        match update {
                            Ok(update) => {
                                send_place_order_event(&events, connection_session_id, update).await
                            }
                            Err(error) => {
                                tracing::error!(%error, order_id, "IBKR order event stream failed");
                                break;
                            }
                        }
                    }
                });
                Ok(order_id)
            }
            Ok(Some(Err(error))) => {
                let detail = error.to_string();
                if order_error_may_leave_order_active(&detail) {
                    Err(format!(
                        "{UNKNOWN_OUTCOME_PREFIX}{detail}; IBKR may be holding the order for the \
                         next regular trading session"
                    ))
                } else {
                    Err(detail)
                }
            }
            // The order was already transmitted; without an acknowledgement its
            // true state is unknown and must be resolved by reconciliation, not
            // by treating it as rejected.
            Ok(None) => Err(format!(
                "{UNKNOWN_OUTCOME_PREFIX}IBKR closed the order response stream before \
                 acknowledgement; the order may still be live"
            )),
            Err(_) => Err(format!(
                "{UNKNOWN_OUTCOME_PREFIX}timed out waiting for IBKR order acknowledgement; \
                 the order may still be live"
            )),
        }
    }

    async fn fetch_reconciliation_snapshot(&self) -> CommandResult<ReconciliationSnapshot> {
        let client = self.ready_client()?;
        let connection_session_id = self
            .status
            .borrow()
            .connection_session_id
            .ok_or_else(|| "IBKR connection has no active session id".to_string())?;
        // Each subscription terminates when IBKR sends the corresponding End
        // message, so the streams are drained to completion. A whole-phase
        // timeout guards against a hung Gateway; it fails the snapshot rather
        // than silently truncating it, because reconciliation treats the
        // snapshot as authoritative and a truncated one could mark live
        // orders as missing or stale orders as not open.
        let timeout = Duration::from_secs(self.config.request_timeout_seconds);
        let mut open_stream = client
            .all_open_orders()
            .await
            .map_err(|error| error.to_string())?
            .filter_data();
        let mut open_orders = Vec::new();
        tokio::time::timeout(timeout, async {
            while let Some(item) = open_stream.next().await {
                match item.map_err(|error| error.to_string())? {
                    ibapi::orders::Orders::OrderData(data) => {
                        open_orders.push(order_snapshot(data, false));
                    }
                    ibapi::orders::Orders::OrderStatus(status) => {
                        if let Some(order) = open_orders
                            .iter_mut()
                            .find(|order| order.broker_order_id == status.order_id)
                        {
                            order.status = status.status.to_string();
                            order.perm_id = status.perm_id;
                        }
                    }
                }
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|_| {
            "timed out collecting IBKR open orders; refusing a possibly truncated \
             reconciliation snapshot"
                .to_string()
        })??;

        let mut completed_stream = client
            .completed_orders(false)
            .await
            .map_err(|error| error.to_string())?
            .filter_data();
        let mut completed_orders = Vec::new();
        tokio::time::timeout(timeout, async {
            while let Some(item) = completed_stream.next().await {
                match item.map_err(|error| error.to_string())? {
                    ibapi::orders::Orders::OrderData(data) => {
                        completed_orders.push(order_snapshot(data, true));
                    }
                    ibapi::orders::Orders::OrderStatus(_) => {}
                }
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|_| {
            "timed out collecting IBKR completed orders; refusing a possibly truncated \
             reconciliation snapshot"
                .to_string()
        })??;

        let mut execution_stream = client
            .executions(ibapi::orders::ExecutionFilter::default())
            .await
            .map_err(|error| error.to_string())?
            .filter_data();
        let mut events = Vec::new();
        tokio::time::timeout(timeout, async {
            while let Some(item) = execution_stream.next().await {
                let item = item.map_err(|error| error.to_string())?;
                events.push(execution_event(item));
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|_| {
            "timed out collecting IBKR executions; refusing a possibly truncated \
             reconciliation snapshot"
                .to_string()
        })??;
        Ok(ReconciliationSnapshot {
            connection_session_id,
            open_orders,
            completed_orders,
            events,
            completed_at: Utc::now(),
        })
    }

    async fn maintain_connection(&mut self) {
        if !self.desired {
            return;
        }

        if self
            .client
            .as_ref()
            .is_some_and(|client| client.is_connected())
        {
            return;
        }

        if self.client.take().is_some() {
            self.stop_account_subscriptions();
            tracing::warn!("IBKR connection was lost");
            // Publish immediately so status readers do not keep seeing Ready
            // until the next reconnect attempt begins.
            self.publish(
                ConnectionState::Reconnecting,
                None,
                None,
                Some("IBKR connection was lost".into()),
            );
        }
        if Instant::now() < self.next_attempt_at {
            return;
        }

        let state = if self.reconnect_attempt == 0 {
            ConnectionState::Connecting
        } else {
            ConnectionState::Reconnecting
        };
        self.publish(state, None, None, None);

        let address = endpoint(&self.config);
        let timeout = Duration::from_secs(self.config.request_timeout_seconds);
        tracing::info!(
            %address,
            client_id = self.config.client_id,
            attempt = self.reconnect_attempt + 1,
            "connecting to IBKR"
        );
        match tokio::time::timeout(timeout, Client::connect(&address, self.config.client_id)).await
        {
            Ok(Ok(client)) => {
                let server_version = client.server_version();
                match client.managed_accounts().await {
                    Ok(accounts) if self.account_is_allowed(&accounts) => {
                        if let Err(error) = client
                            .switch_market_data_type(ibapi::market_data::MarketDataType::Delayed)
                            .await
                        {
                            tracing::warn!(%error, "failed to enable delayed market-data fallback");
                        }
                        let client = Arc::new(client);
                        self.client = Some(client.clone());
                        self.reconnect_attempt = 0;
                        self.publish(
                            ConnectionState::Ready,
                            Some(server_version),
                            Some(accounts.clone()),
                            None,
                        );
                        tracing::info!(
                            server_version,
                            managed_account_count = accounts.len(),
                            "IBKR connection ready"
                        );
                        self.start_account_subscriptions(client, &accounts);
                    }
                    Ok(accounts) => {
                        client.disconnect().await;
                        self.connection_failed(format!(
                            "configured account is not managed by this IBKR session; available account count: {}",
                            accounts.len()
                        ));
                    }
                    Err(error) => {
                        client.disconnect().await;
                        self.connection_failed(format!(
                            "failed to retrieve managed accounts: {error}"
                        ));
                    }
                }
            }
            Ok(Err(error)) => self.connection_failed(error.to_string()),
            Err(_) => self.connection_failed(format!(
                "connection attempt timed out after {} seconds",
                self.config.request_timeout_seconds
            )),
        }
    }

    fn connection_failed(&mut self, error: String) {
        self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        let delay = reconnect_delay(self.reconnect_attempt, self.config.reconnect_max_seconds)
            + reconnect_jitter();
        self.next_attempt_at = Instant::now() + delay;
        tracing::warn!(
            %error,
            retry_in_seconds = delay.as_secs(),
            attempt = self.reconnect_attempt,
            "IBKR connection failed"
        );
        self.publish(ConnectionState::Reconnecting, None, None, Some(error));
    }

    fn current_state(&self) -> ConnectionState {
        self.status.borrow().state
    }

    fn publish(
        &self,
        state: ConnectionState,
        server_version: Option<i32>,
        managed_accounts: Option<Vec<String>>,
        error: Option<String>,
    ) {
        let previous = self.status.borrow().clone();
        let connection_session_id =
            if state == ConnectionState::Ready && previous.state != ConnectionState::Ready {
                Some(uuid::Uuid::now_v7())
            } else {
                previous.connection_session_id
            };
        self.status.send_replace(ConnectionStatus {
            state,
            connection_session_id,
            desired: self.desired,
            endpoint: endpoint(&self.config),
            client_id: self.config.client_id,
            server_version: server_version.or(previous.server_version),
            connected_at: if state == ConnectionState::Ready {
                previous.connected_at.or_else(|| Some(Utc::now()))
            } else {
                None
            },
            managed_accounts: managed_accounts.unwrap_or(previous.managed_accounts),
            last_error: error.or_else(|| {
                if state == ConnectionState::Ready {
                    None
                } else {
                    previous.last_error
                }
            }),
            reconnect_attempt: self.reconnect_attempt,
        });
    }

    fn account_is_allowed(&self, accounts: &[String]) -> bool {
        self.config
            .account
            .as_ref()
            .is_none_or(|configured| accounts.iter().any(|account| account == configured))
    }

    fn stop_account_subscriptions(&mut self) {
        if let Some(cancellation) = self.subscription_cancellation.take() {
            cancellation.cancel();
        }
        self.market_data_cancellations.clear();
    }

    fn start_account_subscriptions(&mut self, client: Arc<Client>, accounts: &[String]) {
        self.stop_account_subscriptions();
        let cancellation = self.cancellation.child_token();
        self.subscription_cancellation = Some(cancellation.clone());

        spawn_position_subscription(
            client.clone(),
            self.events.clone(),
            cancellation.clone(),
            self.position_heartbeat_interval,
        );
        spawn_account_summary_subscription(
            client.clone(),
            self.events.clone(),
            cancellation.clone(),
        );
        for account in accounts {
            spawn_pnl_subscription(
                client.clone(),
                self.events.clone(),
                cancellation.clone(),
                account.clone(),
                self.pnl_idle_timeout,
            );
        }
        for contract in self.market_data_contracts.values().cloned() {
            let market_cancellation = cancellation.child_token();
            self.market_data_cancellations
                .insert(contract.conid, market_cancellation.clone());
            spawn_market_data_subscription(
                client.clone(),
                self.events.clone(),
                market_cancellation,
                contract,
            );
        }
    }
}

fn order_error_may_leave_order_active(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("[399]") && detail.contains("will not be placed at the exchange until")
}

fn spawn_position_subscription(
    client: Arc<Client>,
    events: mpsc::Sender<BrokerEvent>,
    cancellation: CancellationToken,
    heartbeat_interval: Duration,
) {
    tokio::spawn(async move {
        loop {
            // Mark the lease as synchronizing before awaiting the IBKR request.
            // Otherwise a failed or hung subscribe call could leave a previous
            // session's `ready` snapshot trusted indefinitely.
            let subscription_id = uuid::Uuid::now_v7();
            let started_at = Utc::now();
            if events
                .send(BrokerEvent::PositionSnapshotStarted {
                    subscription_id,
                    observed_at: started_at,
                })
                .await
                .is_err()
            {
                return;
            }
            let subscription = tokio::select! {
                _ = cancellation.cancelled() => return,
                result = client.positions() => result,
            };
            let mut subscription = match subscription {
                Ok(subscription) => subscription,
                Err(error) => {
                    let reason = format!("failed to subscribe to IBKR positions: {error}");
                    tracing::error!(%error, "failed to subscribe to IBKR positions; retrying");
                    if events
                        .send(BrokerEvent::PositionSubscriptionEnded {
                            subscription_id,
                            observed_at: Utc::now(),
                            reason,
                        })
                        .await
                        .is_err()
                        || wait_for_subscription_retry(&cancellation).await
                    {
                        return;
                    }
                    continue;
                }
            };
            let mut snapshot_complete = false;
            let mut heartbeat = tokio::time::interval(heartbeat_interval);
            heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
            // `interval` ticks immediately. Consume that tick so a heartbeat can
            // never race ahead of the authoritative PositionEnd marker.
            heartbeat.tick().await;
            let terminal_reason = loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = heartbeat.tick(), if snapshot_complete => {
                        if !client.is_connected() {
                            break "IBKR client disconnected while the position subscription was active".to_owned();
                        }
                        let observed_at = Utc::now();
                        if events.send(BrokerEvent::PositionSubscriptionHeartbeat {
                            subscription_id,
                            observed_at,
                        }).await.is_err() {
                            return;
                        }
                    }
                    item = subscription.next() => match item {
                        Some(Ok(SubscriptionItem::Data(PositionUpdate::Position(position)))) => {
                            if events.send(BrokerEvent::Position {
                                subscription_id,
                                position: position_snapshot(position),
                            }).await.is_err() {
                                return;
                            }
                        }
                        Some(Ok(SubscriptionItem::Data(PositionUpdate::PositionEnd))) => {
                            tracing::debug!("initial IBKR position snapshot completed");
                            let observed_at = Utc::now();
                            if events.send(BrokerEvent::PositionSnapshotCompleted {
                                subscription_id,
                                observed_at,
                            }).await.is_err() {
                                return;
                            }
                            snapshot_complete = true;
                        }
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            tracing::warn!(?notice, "IBKR position subscription notice");
                        }
                        Some(Err(error)) => {
                            tracing::error!(%error, "IBKR position subscription failed; invalidating the snapshot and retrying");
                            break format!("IBKR position subscription failed: {error}");
                        }
                        None => {
                            tracing::warn!("IBKR closed the position subscription; invalidating the snapshot and retrying");
                            break "IBKR closed the position subscription".to_owned();
                        }
                    }
                }
            };
            drop(subscription);
            if events
                .send(BrokerEvent::PositionSubscriptionEnded {
                    subscription_id,
                    observed_at: Utc::now(),
                    reason: terminal_reason,
                })
                .await
                .is_err()
                || wait_for_subscription_retry(&cancellation).await
            {
                return;
            }
        }
    });
}

fn position_heartbeat_interval(max_position_age_seconds: u64) -> Duration {
    // Refresh well inside the configured risk deadline. The upper bound keeps
    // stale-position protection responsive even when operators allow a large
    // account-data age, while the lower bound avoids a zero-duration timer.
    Duration::from_secs((max_position_age_seconds / 3).clamp(1, 30))
}

fn pnl_idle_timeout(max_account_data_age_seconds: u64) -> Duration {
    // PnL is a real broker value and must never be refreshed by a synthetic
    // local heartbeat. Re-subscribe well before the risk deadline so IBKR has
    // time to send a new authoritative snapshot.
    Duration::from_secs((max_account_data_age_seconds / 3).clamp(1, 60))
}

async fn fetch_fx_rate_snapshots(
    client: Arc<Client>,
    accounts: &[String],
    quote_currency: &str,
    timeout: Duration,
) -> CommandResult<Vec<FxRateSnapshot>> {
    let quote_currency = quote_currency.trim().to_ascii_uppercase();
    if quote_currency.len() != 3 {
        return Err("FX quote currency must be a three-letter code".into());
    }

    let mut merged = HashMap::<(String, String), FxRateSnapshot>::new();
    let mut failures = Vec::new();
    let mut valid_accounts = 0_usize;
    for account in accounts {
        match fetch_account_fx_rate_snapshots(client.clone(), account, &quote_currency, timeout)
            .await
        {
            Ok(snapshots) => {
                valid_accounts += 1;
                for snapshot in snapshots {
                    let key = (
                        snapshot.base_currency.clone(),
                        snapshot.quote_currency.clone(),
                    );
                    if let Some(previous) = merged.get(&key)
                        && (previous.rate - snapshot.rate).abs()
                            > previous.rate.abs().max(snapshot.rate.abs()) * 1e-6
                    {
                        tracing::warn!(
                            base_currency = %snapshot.base_currency,
                            quote_currency = %snapshot.quote_currency,
                            previous_account = %previous.account,
                            previous_rate = previous.rate,
                            account = %snapshot.account,
                            rate = snapshot.rate,
                            "IBKR managed accounts returned different FX rates; using the latest snapshot"
                        );
                    }
                    merged.insert(key, snapshot);
                }
            }
            Err(error) => {
                tracing::warn!(%account, %error, "failed to fetch IBKR account FX rates");
                failures.push(format!("{account}: {error}"));
            }
        }
    }
    if valid_accounts == 0 {
        return Err(format!(
            "IBKR FX-rate snapshot failed for every managed account: {}",
            failures.join("; ")
        ));
    }
    let mut snapshots = merged.into_values().collect::<Vec<_>>();
    snapshots.sort_by(|left, right| {
        left.base_currency
            .cmp(&right.base_currency)
            .then_with(|| left.quote_currency.cmp(&right.quote_currency))
    });
    Ok(snapshots)
}

async fn fetch_account_fx_rate_snapshots(
    client: Arc<Client>,
    account: &str,
    quote_currency: &str,
    timeout: Duration,
) -> CommandResult<Vec<FxRateSnapshot>> {
    let account_id = ibapi::accounts::types::AccountId(account.to_owned());
    let mut subscription = client
        .account_updates_multi(Some(&account_id), None)
        .await
        .map_err(|error| error.to_string())?;
    let collection = tokio::time::timeout(timeout, async {
        let mut values = Vec::new();
        loop {
            match subscription.next().await {
                Some(Ok(SubscriptionItem::Data(AccountUpdateMulti::AccountMultiValue(value)))) => {
                    values.push(value)
                }
                Some(Ok(SubscriptionItem::Data(AccountUpdateMulti::End))) => {
                    return Ok(values);
                }
                Some(Ok(SubscriptionItem::Notice(notice))) => {
                    tracing::warn!(?notice, %account, "IBKR account-updates snapshot notice");
                }
                Some(Err(error)) => return Err(error.to_string()),
                None => {
                    return Err(
                        "IBKR closed the account-updates stream before its end marker".into(),
                    );
                }
            }
        }
    })
    .await;
    subscription.cancel().await;
    let values = collection.map_err(|_| {
        format!(
            "IBKR account-updates snapshot timed out after {} seconds",
            timeout.as_secs()
        )
    })??;
    account_fx_rate_snapshots(account, quote_currency, values, Utc::now())
}

fn account_fx_rate_snapshots(
    account: &str,
    quote_currency: &str,
    values: Vec<AccountMultiValue>,
    observed_at: DateTime<Utc>,
) -> CommandResult<Vec<FxRateSnapshot>> {
    let quote_currency = quote_currency.trim().to_ascii_uppercase();
    let mut rates = HashMap::<String, f64>::new();
    let mut account_ready = true;
    for value in values {
        if value
            .key
            .trim()
            .split('-')
            .next()
            .is_some_and(|key| key.eq_ignore_ascii_case("AccountReady"))
            && value.value.trim().eq_ignore_ascii_case("false")
        {
            account_ready = false;
        }
        if !value.key.trim().ends_with("ExchangeRate") {
            continue;
        }
        let currency = value.currency.trim().to_ascii_uppercase();
        let Ok(rate) = value.value.trim().parse::<f64>() else {
            continue;
        };
        if currency.len() == 3 && rate.is_finite() && rate > 0.0 {
            rates.insert(currency, rate);
        }
    }

    if !account_ready {
        return Err(format!(
            "IBKR reports account {account} is not ready; refusing possibly stale FX rates"
        ));
    }

    let Some(account_base_rate) = rates.get(&quote_currency).copied() else {
        return Err(format!(
            "IBKR account updates did not identify {quote_currency} as the account base currency; \
             risk.base_currency must match the IBKR account base currency"
        ));
    };
    if (account_base_rate - 1.0).abs() > 1e-6 {
        return Err(format!(
            "IBKR reports {quote_currency} ExchangeRate={account_base_rate}, not 1; \
             risk.base_currency must match the IBKR account base currency"
        ));
    }

    let mut snapshots = rates
        .into_iter()
        .filter(|(currency, _)| currency != &quote_currency)
        .map(|(base_currency, rate)| FxRateSnapshot {
            account: account.to_owned(),
            base_currency,
            quote_currency: quote_currency.clone(),
            rate,
            observed_at,
        })
        .collect::<Vec<_>>();
    snapshots.sort_by(|left, right| left.base_currency.cmp(&right.base_currency));
    Ok(snapshots)
}

fn spawn_account_summary_subscription(
    client: Arc<Client>,
    events: mpsc::Sender<BrokerEvent>,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        let group = ibapi::accounts::types::AccountGroup("All".into());
        let mut subscription = match client
            .account_summary(&group, ibapi::accounts::AccountSummaryTags::ALL)
            .await
        {
            Ok(subscription) => subscription,
            Err(error) => {
                tracing::error!(%error, "failed to subscribe to IBKR account summary");
                return;
            }
        };
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                item = subscription.next() => match item {
                    Some(Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(summary)))) => {
                        if events.send(BrokerEvent::AccountSummary {
                            account: summary.account,
                            tag: summary.tag,
                            value: summary.value,
                            currency: summary.currency,
                            observed_at: Utc::now(),
                        }).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(SubscriptionItem::Data(AccountSummaryResult::End))) => {
                        tracing::debug!("initial IBKR account summary completed");
                    }
                    Some(Ok(SubscriptionItem::Notice(notice))) => {
                        tracing::warn!(?notice, "IBKR account summary subscription notice");
                    }
                    Some(Err(error)) => {
                        tracing::error!(%error, "IBKR account summary subscription failed");
                        break;
                    }
                    None => break,
                }
            }
        }
    });
}

fn spawn_pnl_subscription(
    client: Arc<Client>,
    events: mpsc::Sender<BrokerEvent>,
    cancellation: CancellationToken,
    account: String,
    idle_timeout: Duration,
) {
    tokio::spawn(async move {
        let account_id = ibapi::accounts::types::AccountId(account.clone());
        loop {
            let subscription = tokio::select! {
                _ = cancellation.cancelled() => return,
                result = client.pnl(&account_id, None) => result,
            };
            let mut subscription = match subscription {
                Ok(subscription) => subscription,
                Err(error) => {
                    tracing::warn!(%error, %account, "failed to subscribe to IBKR account PnL; retrying");
                    if wait_for_subscription_retry(&cancellation).await {
                        return;
                    }
                    continue;
                }
            };
            let idle = tokio::time::sleep(idle_timeout);
            tokio::pin!(idle);
            let mut receiver_closed = false;
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = &mut idle => {
                        tracing::warn!(
                            %account,
                            idle_seconds = idle_timeout.as_secs(),
                            "IBKR account PnL subscription became idle; resubscribing"
                        );
                        break;
                    }
                    item = subscription.next() => match item {
                        Some(Ok(SubscriptionItem::Data(pnl))) => {
                            idle.as_mut().reset(Instant::now() + idle_timeout);
                            if events.send(BrokerEvent::Pnl {
                                account: account.clone(),
                                daily_pnl: pnl.daily_pnl,
                                unrealized_pnl: pnl.unrealized_pnl,
                                realized_pnl: pnl.realized_pnl,
                                observed_at: Utc::now(),
                            }).await.is_err() {
                                receiver_closed = true;
                                break;
                            }
                        }
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            tracing::warn!(?notice, %account, "IBKR PnL subscription notice");
                        }
                        Some(Err(error)) => {
                            tracing::warn!(%error, %account, "IBKR PnL subscription failed; resubscribing");
                            break;
                        }
                        None => {
                            tracing::warn!(%account, "IBKR closed the account PnL subscription; resubscribing");
                            break;
                        }
                    }
                }
            }
            // Dropping the subscription sends CancelPnL before the next
            // request, preventing overlapping request IDs at the Gateway.
            drop(subscription);
            if receiver_closed || wait_for_subscription_retry(&cancellation).await {
                return;
            }
        }
    });
}

async fn wait_for_subscription_retry(cancellation: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => true,
        _ = tokio::time::sleep(Duration::from_secs(1)) => false,
    }
}

fn spawn_market_data_subscription(
    client: Arc<Client>,
    events: mpsc::Sender<BrokerEvent>,
    cancellation: CancellationToken,
    contract: ContractCandidate,
) {
    tokio::spawn(async move {
        let ib_contract = candidate_contract(&contract);
        let _ = send_market_status(&events, contract.conid, "subscribing", None).await;
        'retry: loop {
            let mut subscription = match client
                .market_data(&ib_contract)
                .streaming()
                .subscribe()
                .await
            {
                Ok(subscription) => subscription,
                Err(error) => {
                    tracing::error!(%error, conid = contract.conid, "failed to subscribe to IBKR market data");
                    if !send_market_status(
                        &events,
                        contract.conid,
                        "retrying",
                        Some(error.to_string()),
                    )
                    .await
                    {
                        break;
                    }
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(15)) => continue,
                    }
                }
            };
            let _ = send_market_status(&events, contract.conid, "awaiting_data", None).await;
            let mut received_data = false;
            let initial_data_timeout = tokio::time::sleep(Duration::from_secs(30));
            tokio::pin!(initial_data_timeout);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break 'retry,
                    _ = &mut initial_data_timeout, if !received_data => {
                        let detail = "IBKR market-data subscription produced no initial data \
                                      within 30 seconds; retrying the subscription";
                        tracing::warn!(
                            conid = contract.conid,
                            "IBKR market-data subscription timed out awaiting initial data"
                        );
                        if !send_market_status(
                            &events,
                            contract.conid,
                            "retrying",
                            Some(detail.into()),
                        )
                        .await
                        {
                            break 'retry;
                        }
                        break;
                    }
                    item = subscription.next() => match item {
                    Some(Ok(SubscriptionItem::Data(tick))) => {
                        let observed_at = Utc::now();
                        if !received_data {
                            if !send_market_status(&events, contract.conid, "active", None).await {
                                break 'retry;
                            }
                            received_data = true;
                        }
                            use ibapi::market_data::realtime::TickTypes;
                            let sent = match tick {
                                TickTypes::Price(tick) => send_market_tick(
                                    &events, contract.conid, format!("{:?}", tick.tick_type),
                                    Some(tick.price), None, observed_at,
                                ).await,
                                TickTypes::Size(tick) => send_market_tick(
                                    &events, contract.conid, format!("{:?}", tick.tick_type),
                                    Some(tick.size), None, observed_at,
                                ).await,
                                TickTypes::PriceSize(tick) => {
                                    let price_sent = send_market_tick(
                                        &events, contract.conid,
                                        format!("{:?}", tick.price_tick_type),
                                        Some(tick.price), None, observed_at,
                                    ).await;
                                    let size_sent = send_market_tick(
                                        &events, contract.conid,
                                        format!("{:?}", tick.size_tick_type),
                                        Some(tick.size), None, observed_at,
                                    ).await;
                                    price_sent && size_sent
                                }
                                TickTypes::String(tick) => send_market_tick(
                                    &events, contract.conid, format!("{:?}", tick.tick_type),
                                    None, Some(tick.value), observed_at,
                                ).await,
                                TickTypes::Generic(tick) => send_market_tick(
                                    &events, contract.conid, format!("{:?}", tick.tick_type),
                                    Some(tick.value), None, observed_at,
                                ).await,
                                TickTypes::MarketDataType(data_type) => send_market_tick(
                                    &events, contract.conid, "MarketDataType".into(),
                                    None, Some(format!("{data_type:?}")), observed_at,
                                ).await,
                                TickTypes::SnapshotEnd
                                | TickTypes::RequestParameters(_)
                                | TickTypes::OptionComputation(_) => true,
                            };
                            if !sent {
                                break 'retry;
                            }
                        }
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            tracing::warn!(?notice, conid = contract.conid, "IBKR market-data subscription notice");
                        }
                        Some(Err(error)) => {
                            tracing::error!(%error, conid = contract.conid, "IBKR market-data subscription failed");
                            if !send_market_status(
                                &events, contract.conid, "retrying", Some(error.to_string())
                            ).await {
                                break 'retry;
                            }
                            break;
                        }
                        None => {
                            let _ = send_market_status(
                                &events, contract.conid, "retrying",
                                Some("IBKR closed the market-data stream".into())
                            ).await;
                            break;
                        },
                    }
                }
            }
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(15)) => {}
            }
        }
    });
}

async fn send_market_status(
    events: &mpsc::Sender<BrokerEvent>,
    conid: i32,
    state: &str,
    error: Option<String>,
) -> bool {
    events
        .send(BrokerEvent::MarketDataStatus {
            conid,
            state: state.into(),
            error,
            observed_at: Utc::now(),
        })
        .await
        .is_ok()
}

async fn send_market_tick(
    events: &mpsc::Sender<BrokerEvent>,
    conid: i32,
    tick_type: String,
    numeric_value: Option<f64>,
    text_value: Option<String>,
    observed_at: DateTime<Utc>,
) -> bool {
    events
        .send(BrokerEvent::MarketDataTick {
            conid,
            tick_type,
            numeric_value,
            text_value,
            observed_at,
        })
        .await
        .is_ok()
}

fn order_snapshot(data: ibapi::orders::OrderData, completed: bool) -> OpenOrderSnapshot {
    let completed_time = completed.then_some(data.order_state.completed_time);
    let completed_status = (!data.order_state.completed_status.trim().is_empty())
        .then_some(data.order_state.completed_status);
    OpenOrderSnapshot {
        broker_order_id: data.order_id,
        perm_id: data.order.perm_id,
        client_id: data.order.client_id,
        account: data.order.account,
        conid: data.contract.contract_id,
        symbol: data.contract.symbol.to_string(),
        side: data.order.action.to_string(),
        quantity: data.order.total_quantity,
        order_type: data.order.order_type,
        limit_price: data.order.limit_price,
        status: data.order_state.status.to_string(),
        completed_time,
        completed_status,
    }
}

fn contract_candidate(contract: Contract) -> ContractCandidate {
    ContractCandidate {
        conid: contract.contract_id,
        symbol: contract.symbol.to_string(),
        security_type: contract.security_type.to_string(),
        currency: contract.currency.to_string(),
        exchange: contract.exchange.to_string(),
        primary_exchange: contract.primary_exchange.to_string(),
        local_symbol: contract.local_symbol,
        description: contract.description,
        derivative_security_types: Vec::new(),
    }
}

fn contract_schedule(
    details: &ibapi::contracts::ContractDetails,
) -> CommandResult<ContractSchedule> {
    let exchange = {
        let primary = details.contract.primary_exchange.to_string();
        if primary.trim().is_empty() || primary.eq_ignore_ascii_case("SMART") {
            details.contract.exchange.to_string()
        } else {
            primary
        }
    };
    let regular_sessions = parse_contract_sessions(&details.liquid_hours, &details.time_zone_id)?;
    if regular_sessions.is_empty() {
        return Err(format!(
            "IBKR returned no regular trading sessions for conid {} ({exchange})",
            details.contract.contract_id
        ));
    }
    Ok(ContractSchedule {
        conid: details.contract.contract_id,
        exchange: exchange.trim().to_ascii_uppercase(),
        time_zone_id: details.time_zone_id.clone(),
        regular_sessions,
        extended_sessions: parse_contract_sessions(&details.trading_hours, &details.time_zone_id)?,
        fetched_at: Utc::now(),
    })
}

fn parse_contract_sessions(
    values: &[String],
    time_zone_id: &str,
) -> CommandResult<Vec<ContractSession>> {
    let timezone = ibkr_timezone(time_zone_id)?;
    let mut sessions = Vec::new();
    for entry in values.iter().flat_map(|value| value.split(';')) {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (trading_date, hours) = entry
            .split_once(':')
            .ok_or_else(|| format!("invalid IBKR trading-hours entry: {entry}"))?;
        let trading_date = NaiveDate::parse_from_str(trading_date, "%Y%m%d")
            .map_err(|error| format!("invalid IBKR trading date {trading_date}: {error}"))?;
        if hours.eq_ignore_ascii_case("CLOSED") {
            continue;
        }
        for interval in hours.split(',') {
            let (open, close) = interval
                .split_once('-')
                .ok_or_else(|| format!("invalid IBKR trading-hours interval: {interval}"))?;
            let opens_at = parse_ibkr_local_datetime(open, trading_date, timezone, true)?;
            let closes_at = parse_ibkr_local_datetime(close, trading_date, timezone, false)?;
            if opens_at >= closes_at {
                return Err(format!(
                    "IBKR trading-hours interval does not increase: {interval}"
                ));
            }
            sessions.push(ContractSession {
                trading_date,
                opens_at,
                closes_at,
            });
        }
    }
    sessions.sort_by_key(|session| session.opens_at);
    sessions.dedup();
    Ok(sessions)
}

fn parse_ibkr_local_datetime(
    value: &str,
    default_date: NaiveDate,
    timezone: chrono_tz::Tz,
    opening: bool,
) -> CommandResult<DateTime<Utc>> {
    let value = value.trim();
    let naive = if value.len() == 4 {
        NaiveDateTime::parse_from_str(
            &format!("{} {value}", default_date.format("%Y%m%d")),
            "%Y%m%d %H%M",
        )
    } else {
        NaiveDateTime::parse_from_str(value, "%Y%m%d:%H%M")
    }
    .map_err(|error| format!("invalid IBKR local session time {value}: {error}"))?;
    let localized = timezone.from_local_datetime(&naive);
    let datetime = if opening {
        localized.earliest()
    } else {
        localized.latest()
    }
    .ok_or_else(|| format!("IBKR session time {value} does not exist in timezone {timezone}"))?;
    Ok(datetime.with_timezone(&Utc))
}

fn ibkr_timezone(value: &str) -> CommandResult<chrono_tz::Tz> {
    let canonical = match value.trim() {
        "US/Eastern" | "EST" | "EST5EDT" => "America/New_York",
        "US/Central" | "CST" | "CST6CDT" => "America/Chicago",
        "US/Mountain" | "MST" | "MST7MDT" => "America/Denver",
        "US/Pacific" | "PST" | "PST8PDT" => "America/Los_Angeles",
        "MET" | "CET" => "Europe/Paris",
        "GB-Eire" | "GMT" => "Europe/London",
        "Hongkong" => "Asia/Hong_Kong",
        "Japan" => "Asia/Tokyo",
        other => other,
    };
    canonical
        .parse()
        .map_err(|_| format!("unsupported IBKR contract timezone: {value}"))
}

pub(crate) fn parse_ibkr_execution_datetime(value: &str) -> CommandResult<DateTime<Utc>> {
    let value = value.trim();
    let (datetime, timezone) = value
        .rsplit_once(' ')
        .ok_or_else(|| format!("invalid IBKR execution time: {value}"))?;
    let naive = NaiveDateTime::parse_from_str(datetime.trim(), "%Y%m%d %H:%M:%S")
        .map_err(|error| format!("invalid IBKR execution time {value}: {error}"))?;
    let timezone = ibkr_timezone(timezone)?;
    timezone
        .from_local_datetime(&naive)
        .earliest()
        .map(|time| time.with_timezone(&Utc))
        .ok_or_else(|| format!("IBKR execution time {value} does not exist"))
}

fn execution_datetime_or_now(value: &str) -> DateTime<Utc> {
    parse_ibkr_execution_datetime(value).unwrap_or_else(|error| {
        tracing::warn!(%error, raw_time = value, "using receipt time for malformed IBKR execution time");
        Utc::now()
    })
}

fn normalize_search_candidate(candidate: &mut ContractCandidate) {
    candidate.normalize_streaming_subscription();
}

fn position_snapshot(position: ibapi::accounts::Position) -> PositionSnapshot {
    PositionSnapshot {
        account: position.account,
        conid: position.contract.contract_id,
        symbol: position.contract.symbol.to_string(),
        security_type: position.contract.security_type.to_string(),
        currency: position.contract.currency.to_string(),
        exchange: position.contract.exchange.to_string(),
        quantity: position.position,
        average_cost: position.average_cost,
        observed_at: Utc::now(),
    }
}

fn candidate_contract(candidate: &ContractCandidate) -> Contract {
    let mut contract = Contract::default();
    contract.contract_id = candidate.conid;
    contract.symbol = candidate.symbol.as_str().into();
    contract.security_type = ibapi::contracts::SecurityType::from(candidate.security_type.as_str());
    contract.currency = candidate.currency.as_str().into();
    contract.exchange = candidate.exchange.as_str().into();
    contract.primary_exchange = candidate.primary_exchange.as_str().into();
    contract.local_symbol = candidate.local_symbol.clone();
    contract
}

fn parse_bar_size(value: &str) -> CommandResult<BarSize> {
    match value {
        "5s" => Ok(BarSize::Sec5),
        "1m" => Ok(BarSize::Min),
        "5m" => Ok(BarSize::Min5),
        "15m" => Ok(BarSize::Min15),
        "30m" => Ok(BarSize::Min30),
        "1h" => Ok(BarSize::Hour),
        "1d" => Ok(BarSize::Day),
        _ => Err("unsupported timeframe; use 5s, 1m, 5m, 15m, 30m, 1h, or 1d".into()),
    }
}

fn historical_broker_window(request: &HistoricalBarsRequest) -> (DateTime<Utc>, DateTime<Utc>) {
    if historical_endpoint_is_dst_ambiguous(request.start)
        || historical_endpoint_is_dst_ambiguous(request.end)
    {
        (
            request.start - chrono::Duration::hours(2),
            request.end + chrono::Duration::hours(2),
        )
    } else {
        (request.start, request.end)
    }
}

fn historical_endpoint_is_dst_ambiguous(timestamp: DateTime<Utc>) -> bool {
    [
        chrono_tz::America::New_York,
        chrono_tz::America::Chicago,
        chrono_tz::America::Toronto,
        chrono_tz::Europe::London,
        chrono_tz::Europe::Paris,
        chrono_tz::Australia::Sydney,
    ]
    .into_iter()
    .any(|timezone| {
        let local = timestamp.with_timezone(&timezone).naive_local();
        matches!(
            timezone.from_local_datetime(&local),
            chrono::LocalResult::Ambiguous(_, _)
        )
    })
}

fn historical_duration(
    timeframe: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> CommandResult<HistoricalDuration> {
    let seconds = (end - start).num_seconds();
    if seconds <= 0 {
        return Err("historical end must be after start".into());
    }
    if timeframe == "5s" {
        return Ok(HistoricalDuration::seconds(
            seconds.clamp(1, i32::MAX as i64) as i32,
        ));
    }
    let days = ((seconds + 86_399) / 86_400).clamp(1, i32::MAX as i64) as i32;
    Ok(HistoricalDuration::days(days))
}

fn bar_timestamp_utc(timestamp: BarTimestamp) -> CommandResult<DateTime<Utc>> {
    match timestamp {
        BarTimestamp::DateTime(value) => DateTime::from_timestamp(value.unix_timestamp(), 0)
            .ok_or_else(|| "historical bar timestamp is out of range".into()),
        BarTimestamp::Date(value) => {
            let date = chrono::NaiveDate::parse_from_str(&value.to_string(), "%Y-%m-%d")
                .map_err(|error| error.to_string())?;
            Ok(date
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid")
                .and_utc())
        }
    }
}

async fn send_place_order_event(
    sender: &mpsc::Sender<BrokerEvent>,
    connection_session_id: uuid::Uuid,
    event: ibapi::orders::PlaceOrder,
) {
    let event = match event {
        ibapi::orders::PlaceOrder::OrderStatus(status) => BrokerEvent::OrderStatus {
            connection_session_id: Some(connection_session_id),
            broker_order_id: status.order_id,
            status: status.status.to_string(),
            filled: status.filled,
            remaining: status.remaining,
            average_fill_price: status.average_fill_price,
            last_fill_price: status.last_fill_price,
            perm_id: status.perm_id,
            why_held: status.why_held,
            market_cap_price: status.market_cap_price,
        },
        ibapi::orders::PlaceOrder::ExecutionData(data) => BrokerEvent::Execution {
            connection_session_id: Some(connection_session_id),
            broker_order_id: data.execution.order_id,
            perm_id: data.execution.perm_id,
            execution_id: data.execution.execution_id,
            conid: data.contract.contract_id,
            side: format!("{:?}", data.execution.side),
            quantity: data.execution.shares,
            price: data.execution.price,
            executed_at: execution_datetime_or_now(&data.execution.time),
        },
        ibapi::orders::PlaceOrder::CommissionReport(report) => BrokerEvent::Commission {
            execution_id: report.execution_id,
            commission: report.commission,
            currency: report.currency,
        },
        ibapi::orders::PlaceOrder::OpenOrder(data) => BrokerEvent::OpenOrder {
            connection_session_id: Some(connection_session_id),
            broker_order_id: data.order_id,
            perm_id: data.order.perm_id,
            status: data.order_state.status.to_string(),
            reject_reason: data.order_state.reject_reason,
            warning_text: data.order_state.warning_text,
            completed_time: data.order_state.completed_time,
            completed_status: data.order_state.completed_status,
        },
    };
    if sender.send(event).await.is_err() {
        tracing::error!("IBKR broker event consumer stopped");
    }
}

fn execution_event(event: ibapi::orders::Executions) -> BrokerEvent {
    match event {
        ibapi::orders::Executions::ExecutionData(data) => BrokerEvent::Execution {
            connection_session_id: None,
            broker_order_id: data.execution.order_id,
            perm_id: data.execution.perm_id,
            execution_id: data.execution.execution_id,
            conid: data.contract.contract_id,
            side: format!("{:?}", data.execution.side),
            quantity: data.execution.shares,
            price: data.execution.price,
            executed_at: execution_datetime_or_now(&data.execution.time),
        },
        ibapi::orders::Executions::CommissionReport(report) => BrokerEvent::Commission {
            execution_id: report.execution_id,
            commission: report.commission,
            currency: report.currency,
        },
    }
}

fn endpoint(config: &IbkrConfig) -> String {
    format!("{}:{}", config.host, config.port)
}

/// A small randomized delay added to the deterministic backoff so repeated
/// restarts do not retry in lockstep with other clients of the same gateway.
/// Derived from the wall clock to avoid pulling in a RNG dependency.
fn reconnect_jitter() -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Duration::from_millis(u64::from(nanos % 1_000))
}

fn reconnect_delay(attempt: u32, maximum_seconds: u64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(16);
    let seconds = 1_u64
        .checked_shl(exponent)
        .unwrap_or(u64::MAX)
        .min(maximum_seconds.max(1));
    Duration::from_secs(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_delay_is_exponential_and_capped() {
        assert_eq!(reconnect_delay(1, 60), Duration::from_secs(1));
        assert_eq!(reconnect_delay(2, 60), Duration::from_secs(2));
        assert_eq!(reconnect_delay(6, 60), Duration::from_secs(32));
        assert_eq!(reconnect_delay(7, 60), Duration::from_secs(60));
        assert_eq!(reconnect_delay(100, 60), Duration::from_secs(60));
    }

    #[test]
    fn five_second_historical_requests_use_seconds() {
        assert_eq!(parse_bar_size("5s").unwrap(), BarSize::Sec5);
        let start = Utc::now();
        assert_eq!(
            historical_duration("5s", start, start + chrono::Duration::hours(1)).unwrap(),
            HistoricalDuration::seconds(3_600)
        );
        assert_eq!(
            historical_duration("1m", start, start + chrono::Duration::hours(1)).unwrap(),
            HistoricalDuration::days(1)
        );
    }

    #[test]
    fn historical_requests_avoid_ambiguous_dst_metadata_endpoints() {
        let start = DateTime::parse_from_rfc3339("2025-11-02T05:51:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let request = HistoricalBarsRequest {
            contract: ContractCandidate {
                conid: 272093,
                symbol: "MSFT".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "NASDAQ".into(),
                primary_exchange: "NASDAQ".into(),
                local_symbol: "MSFT".into(),
                description: String::new(),
                derivative_security_types: Vec::new(),
            },
            timeframe: "5s".into(),
            start,
            end: start + chrono::Duration::hours(1),
            outside_rth: false,
        };
        let (broker_start, broker_end) = historical_broker_window(&request);
        assert_eq!(broker_start, request.start - chrono::Duration::hours(2));
        assert_eq!(broker_end, request.end + chrono::Duration::hours(2));

        let mut ordinary = request;
        ordinary.start += chrono::Duration::days(1);
        ordinary.end += chrono::Duration::days(1);
        assert_eq!(
            historical_broker_window(&ordinary),
            (ordinary.start, ordinary.end)
        );
    }

    #[test]
    fn stock_search_candidate_gets_safe_smart_defaults() {
        let mut candidate = ContractCandidate {
            conid: 123,
            symbol: "SAP".into(),
            security_type: "STK".into(),
            currency: "EUR".into(),
            exchange: String::new(),
            primary_exchange: "IBIS".into(),
            local_symbol: String::new(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };
        normalize_search_candidate(&mut candidate);
        assert_eq!(candidate.exchange, "SMART");
        assert_eq!(candidate.local_symbol, "SAP");
        assert_eq!(candidate.primary_exchange, "IBIS");
        assert!(candidate.validate_streaming_subscription().is_ok());
    }

    #[test]
    fn market_data_subscription_rejects_missing_exchange() {
        let candidate = ContractCandidate {
            conid: 14204,
            symbol: "TEST".into(),
            security_type: "STK".into(),
            currency: "EUR".into(),
            exchange: String::new(),
            primary_exchange: String::new(),
            local_symbol: String::new(),
            description: String::new(),
            derivative_security_types: Vec::new(),
        };
        assert_eq!(
            candidate.validate_streaming_subscription().unwrap_err(),
            "market-data contract requires an exchange; use SMART for STK routing"
        );
    }

    #[test]
    fn account_exchange_rates_are_converted_to_configured_base_currency_pairs() {
        let observed_at = Utc::now();
        let rates = account_fx_rate_snapshots(
            "DU123",
            "hkd",
            vec![
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "ExchangeRate".into(),
                    value: "1".into(),
                    currency: "HKD".into(),
                    ..Default::default()
                },
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "ExchangeRate".into(),
                    value: "7.8125".into(),
                    currency: "USD".into(),
                    ..Default::default()
                },
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "CashBalance".into(),
                    value: "1000".into(),
                    currency: "USD".into(),
                    ..Default::default()
                },
            ],
            observed_at,
        )
        .unwrap();

        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].base_currency, "USD");
        assert_eq!(rates[0].quote_currency, "HKD");
        assert_eq!(rates[0].rate, 7.8125);
        assert_eq!(rates[0].observed_at, observed_at);
    }

    #[test]
    fn account_exchange_rates_reject_a_mismatched_configured_base_currency() {
        let error = account_fx_rate_snapshots(
            "DU123",
            "HKD",
            vec![
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "ExchangeRate".into(),
                    value: "1".into(),
                    currency: "USD".into(),
                    ..Default::default()
                },
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "ExchangeRate".into(),
                    value: "0.128".into(),
                    currency: "HKD".into(),
                    ..Default::default()
                },
            ],
            Utc::now(),
        )
        .unwrap_err();

        assert!(error.contains("risk.base_currency must match"));
    }

    #[test]
    fn account_exchange_rates_fail_closed_while_ibkr_account_is_not_ready() {
        let error = account_fx_rate_snapshots(
            "DU123",
            "HKD",
            vec![
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "AccountReady".into(),
                    value: "false".into(),
                    currency: String::new(),
                    ..Default::default()
                },
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "ExchangeRate".into(),
                    value: "1".into(),
                    currency: "HKD".into(),
                    ..Default::default()
                },
                AccountMultiValue {
                    account: "DU123".into(),
                    key: "ExchangeRate".into(),
                    value: "7.8".into(),
                    currency: "USD".into(),
                    ..Default::default()
                },
            ],
            Utc::now(),
        )
        .unwrap_err();

        assert!(error.contains("not ready"));
    }

    #[test]
    fn ibkr_regular_sessions_are_converted_to_utc_and_keep_split_intervals() {
        let sessions = parse_contract_sessions(
            &[
                "20260731:0900-20260731:1200,20260731:1300-20260731:1730".into(),
                "20260801:CLOSED".into(),
            ],
            "MET",
        )
        .unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(
            sessions[0].opens_at,
            "2026-07-31T07:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            sessions[0].closes_at,
            "2026-07-31T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            sessions[1].opens_at,
            "2026-07-31T11:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn ibkr_legacy_session_format_uses_the_trading_date() {
        let sessions =
            parse_contract_sessions(&["20260115:0930-1600".into()], "US/Eastern").unwrap();
        assert_eq!(
            sessions[0].opens_at,
            "2026-01-15T14:30:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            sessions[0].closes_at,
            "2026-01-15T21:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn ibkr_execution_time_is_converted_from_exchange_timezone() {
        assert_eq!(
            parse_ibkr_execution_datetime("20260731 12:34:01 US/Eastern").unwrap(),
            "2026-07-31T16:34:01Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn deferred_until_open_warning_has_an_uncertain_order_outcome() {
        assert!(order_error_may_leave_order_active(
            "[399] Order Message: BUY 10 MSFT Warning: your order will not be placed at the \
             exchange until 2026-07-30 09:30:00 US/Eastern"
        ));
        assert!(!order_error_may_leave_order_active(
            "[201] Order rejected - reason: insufficient buying power"
        ));
    }

    #[test]
    fn position_heartbeat_runs_well_inside_the_risk_freshness_deadline() {
        assert_eq!(position_heartbeat_interval(1), Duration::from_secs(1));
        assert_eq!(position_heartbeat_interval(15), Duration::from_secs(5));
        assert_eq!(position_heartbeat_interval(120), Duration::from_secs(30));
        assert_eq!(position_heartbeat_interval(3_600), Duration::from_secs(30));
    }

    #[test]
    fn pnl_idle_watchdog_runs_before_the_risk_freshness_deadline() {
        assert_eq!(pnl_idle_timeout(1), Duration::from_secs(1));
        assert_eq!(pnl_idle_timeout(15), Duration::from_secs(5));
        assert_eq!(pnl_idle_timeout(120), Duration::from_secs(40));
        assert_eq!(pnl_idle_timeout(3_600), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn actor_stays_disconnected_until_requested() {
        let cancellation = CancellationToken::new();
        let handle = spawn(IbkrConfig::default(), 120, cancellation.clone());
        tokio::task::yield_now().await;
        assert_eq!(handle.status().state, ConnectionState::Disconnected);
        assert!(!handle.status().desired);
        cancellation.cancel();
    }
}
