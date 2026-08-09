use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use http::{Request as HttpRequest, Response as HttpResponse, StatusCode, header::ORIGIN};
use jsonrpsee::{
    RpcModule,
    core::client::ClientT,
    http_client::HttpClientBuilder,
    server::{HttpBody, Server, ServerConfig},
    types::ErrorObjectOwned,
};
use quant_rpc_types::{
    AcknowledgeDifferenceParams, CalendarListParams, CalendarStatusParams, CancelOrderParams,
    DataCoverageParams, DataJobIdParams, DatasetSnapshotParams, InstrumentSearchParams,
    LiveApprovalParams, LogsTailParams, MarketDataBarsParams, MarketDataConidParams,
    MonitoringAcknowledgeParams, MonitoringAlertsParams, PaginationParams, PerformanceReportParams,
    PerformanceSnapshotsParams, ResolveOrderIntentParams, SafetyModeParams, SafetyNoteParams,
    StrategyCreateParams, StrategyDeleteParams, StrategyExecutionActionsParams,
    StrategyExecutionToggleParams, StrategyIdParams, StrategyOrderProvenance, StrategyRenameParams,
    StrategySignalsParams,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::validate_request::{ValidateRequest, ValidateRequestHeaderLayer};

use crate::{
    config::{Environment, RiskConfig, RpcConfig},
    error::{AppError, Result},
    ibkr::{self, ConnectionStatus},
    storage::{ReconciliationHealth, Storage, StorageMutexExt},
};

#[derive(Clone, Debug, Serialize)]
pub struct SystemStatus {
    pub version: &'static str,
    pub pid: u32,
    pub started_at: DateTime<Utc>,
    pub uptime_seconds: u64,
    pub state: SystemState,
    pub environment: Environment,
    pub storage_schema_version: i64,
    pub trading_enabled: bool,
    pub ibkr: ConnectionStatus,
    pub reconciliation: ReconciliationHealth,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemState {
    Starting,
    Ready,
    Degraded,
    Draining,
}

#[derive(Debug, Deserialize)]
struct Request {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OrderParams {
    idempotency_key: String,
    account: String,
    #[serde(flatten)]
    order: ibkr::BrokerOrderRequest,
    estimated_price: Option<f64>,
    #[serde(default)]
    strategy_provenance: Option<StrategyOrderProvenance>,
}

#[derive(Clone)]
pub struct RpcServer {
    config: RpcConfig,
    status: watch::Receiver<SystemStatus>,
    ibkr: ibkr::Handle,
    storage: Arc<Mutex<Storage>>,
    strategy_order_coordination: Arc<AsyncMutex<()>>,
    risk_config: RiskConfig,
    lake_dir: std::path::PathBuf,
    staging_dir: std::path::PathBuf,
    backup_dir: std::path::PathBuf,
    cancellation: CancellationToken,
}

#[derive(Clone)]
struct AllowedWebOrigin {
    value: String,
}

impl<B> ValidateRequest<B> for AllowedWebOrigin {
    type ResponseBody = HttpBody;

    fn validate(
        &mut self,
        request: &mut HttpRequest<B>,
    ) -> std::result::Result<(), HttpResponse<Self::ResponseBody>> {
        let accepted = request
            .headers()
            .get(ORIGIN)
            .map(|origin| self.value == "*" || origin.as_bytes() == self.value.as_bytes())
            // Native clients such as the CLI do not send Origin.
            .unwrap_or(true);
        if accepted {
            return Ok(());
        }

        Err(HttpResponse::builder()
            .status(StatusCode::FORBIDDEN)
            .body(HttpBody::from("web origin is not allowed"))
            .expect("static forbidden response is valid"))
    }
}

impl RpcServer {
    pub fn new(
        config: RpcConfig,
        status: watch::Receiver<SystemStatus>,
        ibkr: ibkr::Handle,
        storage: Arc<Mutex<Storage>>,
        strategy_order_coordination: Arc<AsyncMutex<()>>,
        risk_config: RiskConfig,
        lake_dir: std::path::PathBuf,
        staging_dir: std::path::PathBuf,
        backup_dir: std::path::PathBuf,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config,
            status,
            ibkr,
            storage,
            strategy_order_coordination,
            risk_config,
            lake_dir,
            staging_dir,
            backup_dir,
            cancellation,
        }
    }

    pub async fn run(self) -> Result<()> {
        let address = self.config.http_listen;
        let server_config = ServerConfig::builder()
            .max_request_body_size(
                self.config
                    .max_request_bytes
                    .min(u32::MAX as usize)
                    .try_into()
                    .expect("request size was clamped"),
            )
            .max_connections(
                self.config
                    .max_concurrent_requests
                    .min(u32::MAX as usize)
                    .try_into()
                    .expect("connection count was clamped"),
            )
            .build();
        let middleware =
            ServiceBuilder::new().layer(ValidateRequestHeaderLayer::custom(AllowedWebOrigin {
                value: self.config.allowed_web_origin.clone(),
            }));
        let server = Server::builder()
            .set_config(server_config)
            .set_http_middleware(middleware)
            .build(address)
            .await
            .map_err(|error| AppError::Config(format!("cannot bind RPC HTTP listener: {error}")))?;
        let mut module = RpcModule::new(self.clone());
        for &method in quant_rpc_types::ALL_METHODS {
            let method_name = method.to_owned();
            module
                .register_async_method::<std::result::Result<Value, ErrorObjectOwned>, _, _>(
                    method,
                    move |params, context, _| {
                        let method_name = method_name.clone();
                        async move {
                            let params = params.parse::<Value>().unwrap_or_else(|_| json!({}));
                            let response = dispatch(
                                Request {
                                    jsonrpc: "2.0".into(),
                                    id: Value::Null,
                                    method: method_name,
                                    params,
                                },
                                &context.status,
                                &context.ibkr,
                                &context.storage,
                                &context.strategy_order_coordination,
                                &context.risk_config,
                                &context.lake_dir,
                                &context.staging_dir,
                                &context.backup_dir,
                                &context.cancellation,
                            )
                            .await;
                            match (response.result, response.error) {
                                (Some(result), None) => Ok(result),
                                (_, Some(error)) => Err(ErrorObjectOwned::owned(
                                    i32::try_from(error.code).unwrap_or(-32603),
                                    error.message,
                                    error.data,
                                )),
                                _ => Err(ErrorObjectOwned::owned(
                                    -32603,
                                    "malformed internal RPC response",
                                    None::<()>,
                                )),
                            }
                        }
                    },
                )
                .map_err(|error| {
                    AppError::Config(format!("cannot register RPC method: {error}"))
                })?;
        }
        let cancellation = self.cancellation.clone();
        let handle = server.start(module);
        tracing::info!(endpoint = %format!("http://{address}"), "RPC HTTP server listening");
        tokio::select! {
            _ = cancellation.cancelled() => {
                let _ = handle.stop();
                handle.stopped().await;
                Ok(())
            }
            _ = handle.clone().stopped() => Err(AppError::TaskFailed(
                "RPC HTTP server stopped unexpectedly".into()
            )),
        }
    }
}

fn strategy_order_coordination_required(method: &str) -> bool {
    matches!(
        method,
        "order.submit"
            | "strategy.start"
            | "strategy.pause"
            | "strategy.stop"
            | "strategy.delete"
            | "strategy.execution.configure"
            | "strategy.execution.configure_portfolio"
            | "strategy.execution.enable"
            | "strategy.execution.disable"
            | "execution_cost.model.upsert"
            | "execution_cost.model.delete"
            | "execution_cost.control.configure"
            | "execution_risk.control.configure"
            | "execution_risk.control.reset"
    )
}

async fn dispatch(
    request: Request,
    status: &watch::Receiver<SystemStatus>,
    ibkr: &ibkr::Handle,
    storage: &Arc<Mutex<Storage>>,
    strategy_order_coordination: &Arc<AsyncMutex<()>>,
    risk_config: &RiskConfig,
    lake_dir: &Path,
    staging_dir: &Path,
    backup_dir: &Path,
    cancellation: &CancellationToken,
) -> Response {
    if request.jsonrpc != "2.0" {
        return failure(request.id, -32600, "jsonrpc must be \"2.0\"");
    }

    // A strategy order becomes externally irreversible when it is handed to
    // IBKR.  Serialize that hand-off with every operation that can invalidate
    // its persisted target or execution semantics.  The strategy evaluator
    // uses this same gate, so a newer signal cannot supersede the target after
    // final local authorization but before the broker acknowledges the order.
    let _strategy_order_guard = if strategy_order_coordination_required(&request.method) {
        Some(strategy_order_coordination.lock().await)
    } else {
        None
    };

    match request.method.as_str() {
        "system.status" => {
            let mut current = status.borrow().clone();
            current.uptime_seconds = (Utc::now() - current.started_at).num_seconds().max(0) as u64;
            current.ibkr = ibkr.status();
            current.reconciliation = match storage
                .lock_safe()
                .reconciliation_health(current.ibkr.connection_session_id)
            {
                Ok(health) => health,
                Err(error) => return failure(request.id, -32030, &error.to_string()),
            };
            if current.reconciliation.state == "degraded" {
                current.state = SystemState::Degraded;
            }
            success(
                request.id,
                serde_json::to_value(current).expect("status serializes"),
            )
        }
        "logs.tail" => {
            let params = match serde_json::from_value::<LogsTailParams>(request.params) {
                Ok(params) if params.limit > 0 => params,
                Ok(_) => return failure(request.id, -32602, "limit must be greater than zero"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            success(
                request.id,
                json!({"entries": crate::telemetry::tail(params.after_cursor, params.limit)}),
            )
        }
        "system.health" => {
            let mut current = status.borrow().clone();
            current.uptime_seconds = (Utc::now() - current.started_at).num_seconds().max(0) as u64;
            current.ibkr = ibkr.status();
            current.reconciliation = match storage
                .lock_safe()
                .reconciliation_health(current.ibkr.connection_session_id)
            {
                Ok(health) => health,
                Err(error) => return failure(request.id, -32030, &error.to_string()),
            };
            if current.reconciliation.state == "degraded" {
                current.state = SystemState::Degraded;
            }
            match storage
                .lock_safe()
                .operational_health(lake_dir, staging_dir)
            {
                Ok(operations) => success(
                    request.id,
                    json!({
                        "healthy": matches!(current.state, SystemState::Ready),
                        "system": current,
                        "operations": operations
                    }),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "system.version" => success(
            request.id,
            json!({
                "version": env!("CARGO_PKG_VERSION"),
                "rpc_version": quant_rpc_types::RPC_VERSION,
                "capabilities": quant_rpc_types::ALL_METHODS,
            }),
        ),
        "ibkr.status" => success(
            request.id,
            serde_json::to_value(ibkr.status()).expect("IBKR status serializes"),
        ),
        "ibkr.connect" => match ibkr.connect().await {
            Ok(()) => success(request.id, json!({"accepted": true})),
            Err(error) => failure(request.id, -32010, &error),
        },
        "ibkr.disconnect" => match ibkr.disconnect().await {
            Ok(()) => success(request.id, json!({"accepted": true})),
            Err(error) => failure(request.id, -32010, &error),
        },
        "account.managed" => match ibkr.managed_accounts() {
            Ok(accounts) => success(request.id, json!({"accounts": accounts})),
            Err(error) => failure(request.id, -32011, &error),
        },
        "instrument.search" => {
            let params = match serde_json::from_value::<InstrumentSearchParams>(request.params) {
                Ok(params) if !params.pattern.trim().is_empty() => params,
                Ok(_) => return failure(request.id, -32602, "pattern cannot be empty"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match ibkr.search_contracts(params.pattern).await {
                Ok(candidates) => {
                    // `matching_symbols` may return unresolved descriptors
                    // (notably generic CASH symbols) with conid=0 and missing
                    // routing fields. They are search hints, not contracts
                    // that can be subscribed or ordered.
                    let candidates = candidates
                        .into_iter()
                        .filter(|candidate| candidate.conid > 0)
                        .collect::<Vec<_>>();
                    let mut persisted = Vec::new();
                    for candidate in &candidates {
                        if candidate.conid <= 0 {
                            continue;
                        }
                        match storage.lock_safe().upsert_instrument(candidate) {
                            Ok(instrument_id) => persisted.push(instrument_id),
                            Err(error) => {
                                return failure(request.id, -32030, &error.to_string());
                            }
                        }
                    }
                    success(
                        request.id,
                        json!({"candidates": candidates, "instrument_ids": persisted}),
                    )
                }
                Err(error) => failure(request.id, -32012, &error),
            }
        }
        "instrument.list" => match storage.lock_safe().list_instruments() {
            Ok(instruments) => success(request.id, json!({"instruments": instruments})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "account.summary" => match storage.lock_safe().list_account_summary() {
            Ok(summary) => success(request.id, json!({"summary": summary})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "account.pnl" => match storage.lock_safe().list_account_pnl() {
            Ok(pnl) => success(request.id, json!({"pnl": pnl})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "portfolio.positions" => match storage.lock_safe().list_positions() {
            Ok(positions) => success(request.id, json!({"positions": positions})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "data.backfill" => {
            let params = match serde_json::from_value::<ibkr::HistoricalBarsRequest>(request.params)
            {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            let job = crate::storage::BackfillJobRequest {
                contract: params.contract,
                timeframe: params.timeframe,
                start: params.start,
                end: params.end,
                outside_rth: params.outside_rth,
                fx_rate_pair: None,
            };
            match storage.lock_safe().create_unverified_backfill_jobs(&job) {
                Ok(created) => {
                    let jobs = created
                        .iter()
                        .map(|(gap, creation)| {
                            json!({
                                "job_id": creation.job_id,
                                "state": "pending",
                                "reused": creation.reused,
                                "range_expanded": creation.range_expanded,
                                "start": gap.start,
                                "end": gap.end
                            })
                        })
                        .collect::<Vec<_>>();
                    let first = created.first().map(|(_, creation)| creation);
                    success(
                        request.id,
                        json!({
                            // Keep the original single-job fields for older clients.
                            "job_id": first.map(|creation| creation.job_id),
                            "state": if created.is_empty() { "completed" } else { "pending" },
                            "reused": !created.is_empty()
                                && created.iter().all(|(_, creation)| creation.reused),
                            "range_expanded": created
                                .iter()
                                .any(|(_, creation)| creation.range_expanded),
                            "already_verified": created.is_empty(),
                            "job_count": created.len(),
                            "jobs": jobs
                        }),
                    )
                }
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "data.jobs" => {
            let params = match serde_json::from_value::<PaginationParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            let page = params.page.max(1);
            let page_size = params.limit.unwrap_or(params.page_size).clamp(1, 500);
            let worker_ready = ibkr.status().state == crate::ibkr::ConnectionState::Ready;
            let storage = storage.lock_safe();
            match (
                storage.list_data_jobs_page(worker_ready, page, page_size),
                storage.data_job_queue_status(),
            ) {
                (Ok((jobs, total_items)), Ok((active_job_id, active_job_count))) => {
                    let mut result = paginated("jobs", jobs, page, page_size, total_items);
                    result
                        .as_object_mut()
                        .expect("paginated result is an object")
                        .insert(
                            "queue".into(),
                            json!({
                                "worker_ready": worker_ready,
                                "active_job_id": active_job_id,
                                "active_job_count": active_job_count
                            }),
                        );
                    success(request.id, result)
                }
                (Err(error), _) | (_, Err(error)) => {
                    failure(request.id, -32030, &error.to_string())
                }
            }
        }
        "data.job.cancel" => {
            let params = match serde_json::from_value::<DataJobIdParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().cancel_data_job(params.job_id) {
                Ok(true) => success(
                    request.id,
                    json!({"job_id": params.job_id, "state": "cancelled"}),
                ),
                Ok(false) => failure(request.id, -32044, "cancelable data job not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "data.coverage" => {
            let params = match serde_json::from_value::<DataCoverageParams>(request.params) {
                Ok(params) if params.end > params.start => params,
                Ok(_) => return failure(request.id, -32602, "end must be after start"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().historical_coverage_for_session(
                params.conid,
                &params.timeframe,
                params.start,
                params.end,
                params.outside_rth,
            ) {
                Ok(coverage) => success(request.id, json!({"coverage": coverage})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "data.verify" => match storage.lock_safe().verify_dataset_files(lake_dir) {
            Ok(files) => {
                let healthy = files.iter().all(|file| file["healthy"] == true);
                success(request.id, json!({"healthy": healthy, "files": files}))
            }
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "data.snapshot.create" => {
            let params = match serde_json::from_value::<DatasetSnapshotParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .create_dataset_snapshot(&params.name, &params.dataset)
            {
                Ok(snapshot_id) => success(
                    request.id,
                    json!({"snapshot_id": snapshot_id, "dataset": params.dataset}),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "data.snapshot.list" => match storage.lock_safe().list_dataset_snapshots() {
            Ok(snapshots) => success(request.id, json!({"snapshots": snapshots})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "market_data.subscribe" => {
            let mut contract =
                match serde_json::from_value::<ibkr::ContractCandidate>(request.params) {
                    Ok(contract) => contract,
                    Err(error) => {
                        return failure(
                            request.id,
                            -32602,
                            &format!("invalid parameters: {error}"),
                        );
                    }
                };
            contract.normalize_streaming_subscription();
            if let Err(error) = contract.validate_streaming_subscription() {
                return failure(request.id, -32602, &error);
            }
            if let Err(error) = storage.lock_safe().add_market_data_subscription(&contract) {
                return failure(request.id, -32030, &error.to_string());
            }
            match ibkr.subscribe_market_data(contract.clone()).await {
                Ok(()) => success(
                    request.id,
                    json!({"subscribed": true, "contract": contract}),
                ),
                Err(error) => {
                    let _ = storage
                        .lock_safe()
                        .remove_market_data_subscription(contract.conid);
                    failure(request.id, -32050, &error)
                }
            }
        }
        "market_data.unsubscribe" => {
            let params = match serde_json::from_value::<MarketDataConidParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            if let Err(error) = ibkr.unsubscribe_market_data(params.conid).await {
                return failure(request.id, -32050, &error);
            }
            match storage
                .lock_safe()
                .remove_market_data_subscription(params.conid)
            {
                Ok(()) => success(
                    request.id,
                    json!({"unsubscribed": true, "conid": params.conid}),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "market_data.subscriptions" => match storage.lock_safe().market_data_subscriptions() {
            Ok(subscriptions) => success(request.id, json!({"subscriptions": subscriptions})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "market_data.quote" => {
            let params = match serde_json::from_value::<MarketDataConidParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().latest_quote(params.conid) {
                Ok(quote) => success(request.id, json!({"quote": quote})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "market_data.health" => {
            let params = match serde_json::from_value::<MarketDataConidParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().market_data_health(
                params.conid,
                risk_config.max_market_data_age_seconds,
                Utc::now(),
            ) {
                Ok(health) => success(request.id, json!({"health": health})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "market_data.bars" => {
            let params = match serde_json::from_value::<MarketDataBarsParams>(request.params) {
                Ok(params) if params.limit > 0 => params,
                Ok(_) => return failure(request.id, -32602, "limit must be greater than zero"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().list_market_bars(
                params.conid,
                &params.timeframe,
                params.limit,
            ) {
                Ok(bars) => success(
                    request.id,
                    json!({"bars": bars, "timeframe": params.timeframe}),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.create" => {
            let params = match serde_json::from_value::<StrategyCreateParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .create_strategy(&params.name, &params.kind, &params.config)
            {
                Ok(strategy_id) => success(
                    request.id,
                    json!({
                        "strategy_id": strategy_id,
                        "state": "stopped",
                        "kind": params.kind
                    }),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.kinds" => success(
            request.id,
            json!({
                "kinds": crate::strategy::registered_kinds(),
                "strategies": crate::strategy::metadata_json(),
            }),
        ),
        "strategy.list" => match storage.lock_safe().list_strategies() {
            Ok(strategies) => success(request.id, json!({"strategies": strategies})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "strategy.rename" => {
            let params = match serde_json::from_value::<StrategyRenameParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .rename_strategy(params.strategy_id, &params.name)
            {
                Ok(true) => success(
                    request.id,
                    json!({"strategy_id": params.strategy_id, "name": params.name.trim()}),
                ),
                Ok(false) => failure(request.id, -32044, "strategy not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.start" | "strategy.pause" | "strategy.stop" => {
            let params = match serde_json::from_value::<StrategyIdParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            let state = match request.method.as_str() {
                "strategy.start" => "running",
                "strategy.pause" => "paused",
                "strategy.stop" => "stopped",
                _ => unreachable!("matched strategy state method"),
            };
            match storage
                .lock_safe()
                .set_strategy_state(params.strategy_id, state)
            {
                Ok(true) => success(
                    request.id,
                    json!({"strategy_id": params.strategy_id, "state": state}),
                ),
                Ok(false) => failure(request.id, -32044, "strategy not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.delete" => {
            let params = match serde_json::from_value::<StrategyDeleteParams>(request.params) {
                Ok(params) if params.confirm => params,
                Ok(_) => {
                    return failure(
                        request.id,
                        -32602,
                        "strategy deletion requires confirm=true",
                    );
                }
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().delete_strategy(params.strategy_id) {
                Ok(true) => success(
                    request.id,
                    json!({"strategy_id": params.strategy_id, "deleted": true}),
                ),
                Ok(false) => failure(request.id, -32044, "strategy not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.signals" => {
            let params = match serde_json::from_value::<StrategySignalsParams>(request.params) {
                Ok(params) if params.limit > 0 => params,
                Ok(_) => return failure(request.id, -32602, "limit must be greater than zero"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .list_strategy_evaluations(params.strategy_id, params.limit)
            {
                Ok(evaluations) => success(
                    request.id,
                    json!({"strategy_id": params.strategy_id, "evaluations": evaluations}),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.execution.configure" => {
            let params = match serde_json::from_value::<crate::storage::StrategyExecutionConfig>(
                request.params,
            ) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            if status.borrow().environment != Environment::Paper {
                return failure(
                    request.id,
                    -32063,
                    "automatic strategy execution is supported only in paper environment",
                );
            }
            match storage
                .lock_safe()
                .configure_strategy_execution_with_capital_currency(
                    &params,
                    &risk_config.base_currency,
                ) {
                Ok(()) => success(
                    request.id,
                    json!({"strategy_id": params.strategy_id, "enabled": false}),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.execution.configure_portfolio" => {
            let params = match serde_json::from_value::<
                crate::storage::StrategyPortfolioExecutionConfig,
            >(request.params)
            {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            if status.borrow().environment != Environment::Paper {
                return failure(
                    request.id,
                    -32063,
                    "automatic strategy execution is supported only in paper environment",
                );
            }
            match storage
                .lock_safe()
                .configure_strategy_portfolio_execution_with_capital_currency(
                    &params,
                    &risk_config.base_currency,
                ) {
                Ok(()) => success(
                    request.id,
                    json!({
                        "strategy_id": params.strategy_id,
                        "enabled": false,
                        "leg_count": params.legs.len()
                    }),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.execution.enable" | "strategy.execution.disable" => {
            let params =
                match serde_json::from_value::<StrategyExecutionToggleParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return failure(
                            request.id,
                            -32602,
                            &format!("invalid parameters: {error}"),
                        );
                    }
                };
            let enabled = request.method == "strategy.execution.enable";
            if enabled
                && (!params.confirm
                    || status.borrow().environment != Environment::Paper
                    || !risk_config.trading_enabled)
            {
                return failure(
                    request.id,
                    -32063,
                    "enabling execution requires confirmation, paper environment, and trading_enabled=true",
                );
            }
            match storage
                .lock_safe()
                .set_strategy_execution_enabled_with_capital_currency(
                    params.strategy_id,
                    enabled,
                    &risk_config.base_currency,
                ) {
                Ok(true) => success(
                    request.id,
                    json!({"strategy_id": params.strategy_id, "enabled": enabled}),
                ),
                Ok(false) => failure(request.id, -32044, "strategy execution config not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "strategy.execution.list" => match storage.lock_safe().list_strategy_execution_configs() {
            Ok(configs) => success(request.id, json!({"configs": configs})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "strategy.execution.actions" => {
            let params =
                match serde_json::from_value::<StrategyExecutionActionsParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return failure(
                            request.id,
                            -32602,
                            &format!("invalid parameters: {error}"),
                        );
                    }
                };
            let page = params.page.max(1);
            let page_size = params.limit.unwrap_or(params.page_size).clamp(1, 500);
            match storage
                .lock_safe()
                .list_strategy_execution_actions_page(page, page_size)
            {
                Ok((actions, total_items)) => success(
                    request.id,
                    paginated("actions", actions, page, page_size, total_items),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "execution_cost.model.upsert" => {
            let params = match serde_json::from_value::<crate::storage::ExecutionCostModelInput>(
                request.params,
            ) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().upsert_execution_cost_model(&params) {
                Ok(cost_model_id) => success(request.id, json!({"cost_model_id": cost_model_id})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "execution_cost.model.list" => match storage.lock_safe().list_execution_cost_models() {
            Ok(models) => success(request.id, json!({"models": models})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "execution_cost.model.delete" => {
            #[derive(serde::Deserialize)]
            struct Params {
                cost_model_id: uuid::Uuid,
                confirm: bool,
            }
            let params = match serde_json::from_value::<Params>(request.params) {
                Ok(params) if params.confirm => params,
                Ok(_) => return failure(request.id, -32602, "delete requires confirm=true"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .delete_execution_cost_model(params.cost_model_id)
            {
                Ok(deleted) => success(request.id, json!({"deleted": deleted})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "execution_cost.control.configure" => {
            let params = match serde_json::from_value::<crate::storage::StrategyCostControlInput>(
                request.params,
            ) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().configure_strategy_cost_control(&params) {
                Ok(()) => success(request.id, json!({"strategy_id": params.strategy_id})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "execution_cost.control.list" => match storage.lock_safe().list_strategy_cost_controls() {
            Ok(controls) => success(request.id, json!({"controls": controls})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "execution_risk.control.configure" => {
            let mut params = match serde_json::from_value::<crate::storage::StrategyRiskControlInput>(
                request.params,
            ) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            // RPC v2 clients created before schema 34 did not send this field.
            // Saving is an explicit operator action, so binding that submitted
            // amount to the daemon's current validated base currency is both
            // backward compatible and unambiguous.
            if params
                .capital_currency
                .as_deref()
                .is_none_or(|currency| currency.trim().is_empty())
            {
                params.capital_currency = Some(risk_config.base_currency.clone());
            }
            if !params.capital_currency.as_deref().is_some_and(|currency| {
                currency
                    .trim()
                    .eq_ignore_ascii_case(risk_config.base_currency.trim())
            }) {
                return failure(
                    request.id,
                    -32602,
                    "capital_currency must match the daemon risk.base_currency; reload the page before saving",
                );
            }
            match storage.lock_safe().configure_strategy_risk_control(&params) {
                Ok(()) => success(request.id, json!({"strategy_id": params.strategy_id})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "execution_risk.control.list" => match storage.lock_safe().list_strategy_risk_controls(
            &risk_config.base_currency,
            risk_config.max_fx_rate_age_seconds,
            Utc::now(),
        ) {
            Ok(controls) => success(
                request.id,
                json!({
                    "controls": controls,
                    "base_currency": risk_config.base_currency.to_ascii_uppercase()
                }),
            ),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "execution_risk.control.reset" => {
            let params = match serde_json::from_value::<crate::storage::StrategyRiskResetInput>(
                request.params,
            ) {
                Ok(params) if params.confirm && !params.note.trim().is_empty() => params,
                Ok(_) => {
                    return failure(
                        request.id,
                        -32602,
                        "reset requires confirm=true and a non-empty note",
                    );
                }
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().reset_strategy_risk_statistics(&params) {
                Ok(true) => success(request.id, json!({"strategy_id": params.strategy_id})),
                Ok(false) => failure(request.id, -32044, "strategy risk control not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "performance.repair_history" => {
            let params = match serde_json::from_value::<StrategyIdParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            // Reconciliation is the only authoritative API repair for missing
            // execution rows. A failure does not prevent the independent FX
            // repair from being queued, and is returned explicitly for review.
            let (reconciliation, reconciliation_error) = match ibkr.reconcile().await {
                Ok(snapshot) => match storage.lock_safe().reconcile(&snapshot) {
                    Ok(report) => (serde_json::to_value(report).ok(), None),
                    Err(error) => (None, Some(error.to_string())),
                },
                Err(error) => (None, Some(error)),
            };
            match storage.lock_safe().create_strategy_historical_fx_jobs(
                params.strategy_id,
                &risk_config.base_currency,
                risk_config.max_fx_rate_age_seconds,
            ) {
                Ok((gaps, jobs)) => success(
                    request.id,
                    json!({
                        "strategy_id": params.strategy_id,
                        "reconciliation": reconciliation,
                        "reconciliation_error": reconciliation_error,
                        "fx_gaps": gaps,
                        "jobs": jobs.into_iter().map(|job| json!({
                            "job_id": job.job_id,
                            "reused": job.reused,
                            "range_expanded": job.range_expanded,
                        })).collect::<Vec<_>>()
                    }),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "performance.report" => {
            let params = match serde_json::from_value::<PerformanceReportParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().strategy_performance_report(
                params.strategy_id,
                params.initial_capital,
                &risk_config.base_currency,
                risk_config.max_fx_rate_age_seconds,
                risk_config.max_market_data_age_seconds,
                risk_config.max_account_data_age_seconds,
                params.benchmark_conid,
                Utc::now(),
            ) {
                Ok(report) => success(request.id, report),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "performance.snapshots" => {
            let params = match serde_json::from_value::<PerformanceSnapshotsParams>(request.params)
            {
                Ok(params) if params.limit > 0 => params,
                Ok(_) => return failure(request.id, -32602, "limit must be greater than zero"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .list_strategy_performance_snapshots(params.strategy_id, params.limit)
            {
                Ok(snapshots) => success(request.id, json!({"snapshots": snapshots})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "fx.set" => {
            let params = match serde_json::from_value::<crate::storage::FxRateInput>(request.params)
            {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().upsert_fx_rate(&params) {
                Ok(()) => success(request.id, json!({"updated": true})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "fx.list" => match storage.lock_safe().list_fx_rates() {
            Ok(rates) => success(request.id, json!({"rates": rates})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "calendar.add" => {
            let params = match serde_json::from_value::<crate::storage::MarketSessionInput>(
                request.params,
            ) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().upsert_market_session(&params) {
                Ok(()) => success(request.id, json!({"updated": true})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "calendar.refresh" => {
            #[derive(serde::Deserialize)]
            struct Params {
                contract: crate::ibkr::ContractCandidate,
            }
            let params = match serde_json::from_value::<Params>(request.params) {
                Ok(params) if params.contract.conid > 0 => params,
                Ok(_) => return failure(request.id, -32602, "contract conid must be positive"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match ibkr.contract_schedule(params.contract).await {
                Ok(schedule) => match storage.lock_safe().replace_ibkr_market_sessions(&schedule) {
                    Ok(intervals) => success(
                        request.id,
                        json!({
                            "updated": true,
                            "conid": schedule.conid,
                            "exchange": schedule.exchange,
                            "time_zone_id": schedule.time_zone_id,
                            "regular_intervals": schedule.regular_sessions.len(),
                            "extended_intervals": schedule.extended_sessions.len(),
                            "intervals": intervals,
                            "fetched_at": schedule.fetched_at
                        }),
                    ),
                    Err(error) => failure(request.id, -32030, &error.to_string()),
                },
                Err(error) => failure(request.id, -32024, &error),
            }
        }
        "calendar.list" => {
            let params = match serde_json::from_value::<CalendarListParams>(request.params) {
                Ok(params) if params.limit > 0 => params,
                Ok(_) => return failure(request.id, -32602, "limit must be greater than zero"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .list_market_sessions(params.exchange.as_deref(), params.limit)
            {
                Ok(sessions) => success(request.id, json!({"sessions": sessions})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "calendar.status" => {
            let params = match serde_json::from_value::<CalendarStatusParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().market_session_is_open_for(
                &params.exchange,
                Utc::now(),
                params.outside_rth,
            ) {
                Ok(open) => success(
                    request.id,
                    json!({
                        "exchange": params.exchange,
                        "session_kind": if params.outside_rth { "extended" } else { "regular" },
                        "open": open,
                        "configured": open.is_some()
                    }),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "monitor.alerts" => {
            let params = match serde_json::from_value::<MonitoringAlertsParams>(request.params) {
                Ok(params) if params.limit > 0 => params,
                Ok(_) => return failure(request.id, -32602, "limit must be greater than zero"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .list_monitoring_alerts(params.active_only, params.limit)
            {
                Ok(alerts) => success(request.id, json!({"alerts": alerts})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "monitor.acknowledge" => {
            let params = match serde_json::from_value::<MonitoringAcknowledgeParams>(request.params)
            {
                Ok(params) if !params.note.trim().is_empty() => params,
                Ok(_) => return failure(request.id, -32602, "note cannot be empty"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .acknowledge_monitoring_alert(params.alert_id, &params.note)
            {
                Ok(true) => success(request.id, json!({"acknowledged": true})),
                Ok(false) => failure(request.id, -32044, "active alert not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "monitor.metrics" => {
            let current = status.borrow().clone();
            let operational = storage
                .lock_safe()
                .operational_health(lake_dir, staging_dir);
            match operational {
                Ok(operational) => {
                    let alerts = storage
                        .lock_safe()
                        .list_monitoring_alerts(true, 10_000)
                        .unwrap_or_default();
                    success(
                        request.id,
                        json!({
                            "daemon_ready": matches!(current.state, SystemState::Ready),
                            "ibkr_ready": current.ibkr.state == crate::ibkr::ConnectionState::Ready,
                            "active_alert_count": alerts.len(),
                            "operational": operational,
                            "observed_at": Utc::now()
                        }),
                    )
                }
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "backtest.run" => {
            let params =
                match serde_json::from_value::<crate::storage::BacktestRequest>(request.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return failure(
                            request.id,
                            -32602,
                            &format!("invalid parameters: {error}"),
                        );
                    }
                };
            match storage
                .lock_safe()
                .run_moving_average_backtest(lake_dir, &params)
            {
                Ok(result) => success(request.id, result),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "backtest.list" => match storage.lock_safe().list_backtests() {
            Ok(backtests) => success(request.id, json!({"backtests": backtests})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "backtest.get" => {
            #[derive(serde::Deserialize)]
            struct Params {
                backtest_id: uuid::Uuid,
                #[serde(default)]
                trade_page: Option<usize>,
                #[serde(default)]
                trade_page_size: Option<usize>,
                #[serde(default)]
                max_equity_points: Option<usize>,
            }
            let params = match serde_json::from_value::<Params>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            let options = crate::storage::BacktestDetailOptions {
                trade_page: params.trade_page.unwrap_or(1),
                trade_page_size: params.trade_page_size.unwrap_or(200),
                max_equity_points: params.max_equity_points.unwrap_or(2_000),
            };
            match storage
                .lock_safe()
                .backtest_details_with_options(params.backtest_id, options)
            {
                Ok(Some(details)) => success(request.id, details),
                Ok(None) => failure(request.id, -32044, "backtest not found"),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "backup.create" => match storage.lock_safe().create_backup(backup_dir, lake_dir) {
            Ok(backup) => success(request.id, backup),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "backup.list" => match Storage::list_backups(backup_dir) {
            Ok(backups) => success(request.id, json!({"backups": backups})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "safety.status" => match storage.lock_safe().trading_control() {
            Ok(control) => success(request.id, control),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "safety.set" => {
            let params = match serde_json::from_value::<SafetyModeParams>(request.params) {
                Ok(params) if !params.note.trim().is_empty() => params,
                Ok(_) => return failure(request.id, -32602, "operator note cannot be empty"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .set_trading_control(&params.mode, &params.note)
            {
                Ok(control) => success(request.id, control),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "safety.live_approve" => {
            let params = match serde_json::from_value::<LiveApprovalParams>(request.params) {
                Ok(params) if params.confirm_live_risk => params,
                Ok(_) => return failure(request.id, -32602, "live approval requires confirmation"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .approve_live_trading(&params.conids, &params.note)
            {
                Ok(control) => success(request.id, control),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "safety.live_revoke" => {
            let params = match serde_json::from_value::<SafetyNoteParams>(request.params) {
                Ok(params) if !params.note.trim().is_empty() => params,
                Ok(_) => return failure(request.id, -32602, "operator note cannot be empty"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage.lock_safe().revoke_live_trading(&params.note) {
                Ok(control) => success(request.id, control),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "order.preview" | "order.submit" => {
            let submit = request.method == "order.submit";
            let params = match serde_json::from_value::<OrderParams>(request.params) {
                Ok(params) if !params.idempotency_key.trim().is_empty() => params,
                Ok(_) => return failure(request.id, -32602, "idempotency_key cannot be empty"),
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            if submit
                && params.idempotency_key.starts_with("strategy:")
                && params.strategy_provenance.is_none()
            {
                return failure(
                    request.id,
                    -32602,
                    "automatic strategy orders require persisted action provenance",
                );
            }
            let ibkr_status = ibkr.status();
            let account_managed = ibkr
                .managed_accounts()
                .is_ok_and(|accounts| accounts.iter().any(|item| item == &params.account));
            let environment = status.borrow().environment;

            // Every storage-backed check plus intent persistence runs under a
            // single lock acquisition so concurrent submissions cannot race
            // past shared limits (open-order count, rate limit, exposure)
            // between check and persist. The lock guard lives inside this block
            // and is released before the broker call below.
            let (decision, portfolio_risk, intent_id) = {
                let mut guard = storage.lock_safe();
                if let Some(provenance) = &params.strategy_provenance
                    && let Err(error) = guard.ensure_strategy_order_submission_authorized(
                        provenance,
                        &params.idempotency_key,
                        &params.account,
                        &params.order,
                    )
                {
                    // This failure deliberately does not create an order intent:
                    // the persisted desired target remains active and the next
                    // worker pass will recompute a safe delta from fresh state.
                    return failure(request.id, -32027, &error.to_string());
                }
                let trading_control = match guard.trading_control() {
                    Ok(control) => control,
                    Err(error) => return failure(request.id, -32030, &error.to_string()),
                };
                let health = match guard.reconciliation_health(ibkr_status.connection_session_id) {
                    Ok(health) => health,
                    Err(error) => return failure(request.id, -32030, &error.to_string()),
                };
                let close_only = match ibkr_status.connected_at {
                    Some(connected_at) => match guard.evaluate_close_only(
                        &params.account,
                        params.order.contract.conid,
                        &params.order.side,
                        params.order.quantity,
                        connected_at,
                        risk_config.max_account_data_age_seconds,
                        Utc::now(),
                    ) {
                        Ok(close_only) => Some(close_only),
                        Err(error) => return failure(request.id, -32030, &error.to_string()),
                    },
                    None => None,
                };
                let close_only_allowed =
                    close_only.as_ref().is_some_and(|decision| decision.allowed);
                let market_data = match guard.market_data_health(
                    params.order.contract.conid,
                    risk_config.max_market_data_age_seconds,
                    Utc::now(),
                ) {
                    Ok(health) => health,
                    Err(error) => return failure(request.id, -32030, &error.to_string()),
                };
                // Prefer the locally observed market price over the caller-supplied
                // estimate so an understated estimate cannot weaken the per-order
                // notional check for market orders.
                let effective_estimated_price = market_data.latest_price.or(params.estimated_price);
                let fx_rate = match guard.currency_conversion_rate(
                    &params.order.contract.currency,
                    &risk_config.base_currency,
                    risk_config.max_fx_rate_age_seconds,
                    Utc::now(),
                ) {
                    // A strict reduction may proceed without FX, but the
                    // audit must not invent a 1:1 conversion rate merely to
                    // satisfy an opening-risk calculation.
                    Ok(rate) => rate,
                    Err(error) => return failure(request.id, -32030, &error.to_string()),
                };
                let decision = if close_only_allowed {
                    crate::risk::allow_position_reduction(
                        risk_config,
                        &params.order,
                        effective_estimated_price,
                        fx_rate,
                        submit,
                    )
                } else {
                    crate::risk::evaluate(
                        risk_config,
                        &params.order,
                        effective_estimated_price,
                        fx_rate,
                        submit,
                    )
                };
                let reconciliation_allowed =
                    health.state == "healthy" || (health.state == "degraded" && close_only_allowed);
                let market_data_allowed = market_data.state == "fresh" || close_only_allowed;
                let readiness_allowed = reconciliation_allowed && market_data_allowed;
                let portfolio_risk = match guard.evaluate_portfolio_risk(
                    risk_config,
                    &params.account,
                    &params.order,
                    params.estimated_price,
                    market_data.latest_price,
                    close_only_allowed,
                    Utc::now(),
                ) {
                    Ok(decision) => decision,
                    Err(error) => return failure(request.id, -32030, &error.to_string()),
                };
                if !submit {
                    if !account_managed {
                        return failure(
                            request.id,
                            -32020,
                            "account is not managed by this session",
                        );
                    }
                    return success(
                        request.id,
                        json!({
                            "decision": decision,
                            "portfolio_risk": portfolio_risk,
                            "trading_readiness": {
                                "allowed": readiness_allowed
                                    && decision.allowed
                                    && portfolio_risk.allowed,
                                "reconciliation": health,
                                "close_only": close_only,
                                "market_data": market_data
                            }
                        }),
                    );
                }
                // Hard gates ahead of the risk decisions. Each rejection persists
                // an intent and an audit record before the RPC error is returned.
                let gate: Option<(i64, &'static str, String)> = if trading_control["reject_new_orders"]
                    == true
                    || trading_control["emergency_stop"] == true
                {
                    Some((
                        -32061,
                        "TRADING_CONTROL_BLOCKED",
                        "new orders are blocked by trading control".into(),
                    ))
                } else if environment == Environment::Live && {
                    let whitelist = trading_control["live_conid_whitelist"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    trading_control["live_approved"] != true
                        || !whitelist
                            .iter()
                            .any(|value| value.as_i64() == Some(params.order.contract.conid as i64))
                } {
                    Some((
                        -32062,
                        "LIVE_NOT_APPROVED",
                        "live trading is not approved for this conid".into(),
                    ))
                } else if !account_managed {
                    Some((
                        -32020,
                        "ACCOUNT_NOT_MANAGED",
                        "account is not managed by this session".into(),
                    ))
                } else if !readiness_allowed {
                    Some((
                        -32024,
                        "READINESS_BLOCKED",
                        format!(
                            "trading readiness blocked: reconciliation={}, market_data={}; only a fresh, strictly position-reducing order may bypass degraded inputs",
                            health.state, market_data.state,
                        ),
                    ))
                } else {
                    None
                };
                if let Some((code, reason_code, detail)) = gate {
                    match guard.create_order_intent(
                        &params.idempotency_key,
                        &params.account,
                        &params.order,
                        "blocked",
                        Some(detail.as_str()),
                    ) {
                        Ok(id) => {
                            if let Some(provenance) = &params.strategy_provenance
                                && let Err(error) = guard.bind_strategy_action_leg_order_intent(
                                    provenance.action_id,
                                    provenance.leg_index,
                                    id,
                                )
                            {
                                return failure(request.id, -32030, &error.to_string());
                            }
                            if let Err(error) =
                                guard.record_risk_decision(id, "reject", reason_code, &detail)
                            {
                                return failure(request.id, -32030, &error.to_string());
                            }
                        }
                        Err(error) => return failure(request.id, -32021, &error.to_string()),
                    }
                    return failure(request.id, code, &detail);
                }
                let intent_id = {
                    let status = if decision.allowed && portfolio_risk.allowed {
                        "approved"
                    } else {
                        "risk_rejected"
                    };
                    match guard.create_order_intent(
                        &params.idempotency_key,
                        &params.account,
                        &params.order,
                        status,
                        (!decision.allowed)
                            .then_some(decision.detail.as_str())
                            .or_else(|| {
                                (!portfolio_risk.allowed).then_some(portfolio_risk.detail.as_str())
                            }),
                    ) {
                        Ok(id) => {
                            if let Some(provenance) = &params.strategy_provenance
                                && let Err(error) = guard.bind_strategy_action_leg_order_intent(
                                    provenance.action_id,
                                    provenance.leg_index,
                                    id,
                                )
                            {
                                return failure(request.id, -32030, &error.to_string());
                            }
                            if let Err(error) = guard.record_risk_decision(
                                id,
                                if decision.allowed { "allow" } else { "reject" },
                                decision.reason_code,
                                &decision.detail,
                            ) {
                                return failure(request.id, -32030, &error.to_string());
                            }
                            if let Err(error) = guard.record_risk_decision(
                                id,
                                if portfolio_risk.allowed {
                                    "allow"
                                } else {
                                    "reject"
                                },
                                portfolio_risk.reason_code,
                                &portfolio_risk.detail,
                            ) {
                                return failure(request.id, -32030, &error.to_string());
                            }
                            id
                        }
                        Err(error) => return failure(request.id, -32021, &error.to_string()),
                    }
                };
                (decision, portfolio_risk, intent_id)
            };
            if !decision.allowed {
                return failure(request.id, -32020, &decision.detail);
            }
            if !portfolio_risk.allowed {
                return failure(request.id, -32025, &portfolio_risk.detail);
            }
            match ibkr.place_order(params.order).await {
                Ok(broker_order_id) => {
                    // The broker has acknowledged the order. Any local failure
                    // after this point must NOT be reported as a rejection: the
                    // order is live at IBKR even though it could not be recorded
                    // locally. Mark the intent 'unknown' and return -32026 so
                    // callers resolve it through reconciliation instead of
                    // resubmitting under a new idempotency key.
                    let Some(connection_session_id) = ibkr.status().connection_session_id else {
                        let detail = format!(
                            "IBKR acknowledged broker order {broker_order_id} but the \
                             connection lost its session before it could be recorded"
                        );
                        if let Err(storage_error) = storage
                            .lock_safe()
                            .mark_order_intent_unknown(intent_id, &detail)
                        {
                            tracing::error!(
                                %storage_error,
                                %intent_id,
                                "failed to persist unknown broker order outcome"
                            );
                        }
                        return failure(
                            request.id,
                            -32026,
                            &format!(
                                "{detail}; the order intent is recorded as 'unknown'. Do not \
                                 retry with a new idempotency key; run reconcile and inspect \
                                 open orders first"
                            ),
                        );
                    };
                    let order_id = match storage.lock_safe().record_submitted_order(
                        intent_id,
                        broker_order_id,
                        connection_session_id,
                    ) {
                        Ok(id) => id,
                        Err(error) => {
                            let detail = format!(
                                "IBKR acknowledged broker order {broker_order_id} but recording \
                                 it locally failed: {error}"
                            );
                            if let Err(storage_error) = storage
                                .lock_safe()
                                .mark_order_intent_unknown(intent_id, &detail)
                            {
                                tracing::error!(
                                    %storage_error,
                                    %intent_id,
                                    "failed to persist unknown broker order outcome"
                                );
                            }
                            return failure(
                                request.id,
                                -32026,
                                &format!(
                                    "{detail}; the order intent is recorded as 'unknown'. Do not \
                                     retry with a new idempotency key; run reconcile and inspect \
                                     open orders first"
                                ),
                            );
                        }
                    };
                    success(
                        request.id,
                        json!({
                            "order_intent_id": intent_id,
                            "order_id": order_id,
                            "broker_order_id": broker_order_id,
                            "status": "submitted"
                        }),
                    )
                }
                Err(error) => {
                    // Distinguish a definitive broker rejection from an
                    // ambiguous outcome (acknowledgement timeout). The latter
                    // may still be live at IBKR and must be resolved through
                    // reconciliation, never resubmitted under a new key.
                    let unknown_outcome = error.starts_with(ibkr::UNKNOWN_OUTCOME_PREFIX);
                    let persisted = if unknown_outcome {
                        storage
                            .lock_safe()
                            .mark_order_intent_unknown(intent_id, &error)
                    } else {
                        storage
                            .lock_safe()
                            .mark_order_intent_rejected(intent_id, &error)
                    };
                    if let Err(storage_error) = persisted {
                        tracing::error!(
                            %storage_error,
                            %intent_id,
                            "failed to persist broker order outcome"
                        );
                    }
                    if unknown_outcome {
                        failure(
                            request.id,
                            -32026,
                            &format!(
                                "{error}; the order intent is recorded as 'unknown'. Do not retry \
                                 with a new idempotency key; run reconcile and inspect open \
                                 orders first"
                            ),
                        )
                    } else {
                        failure(request.id, -32022, &error)
                    }
                }
            }
        }
        "order.cancel" => {
            let params = match serde_json::from_value::<CancelOrderParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            let Some(connection_session_id) = ibkr.status().connection_session_id else {
                return failure(request.id, -32010, "IBKR is not connected");
            };
            // Refresh IBKR open orders before cancellation. This safely re-associates
            // a still-open order with the current connection session after a daemon
            // or Gateway reconnect, and prevents cancelling a stale local row whose
            // broker order id may have been reused.
            let snapshot = match ibkr.reconcile().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return failure(
                        request.id,
                        -32040,
                        &format!("cannot verify the order with IBKR before cancellation: {error}"),
                    );
                }
            };
            if let Err(error) = storage.lock_safe().reconcile(&snapshot) {
                return failure(request.id, -32030, &error.to_string());
            }
            // Keep the mutex guard in an explicit scope. Matching directly on
            // `storage.lock_safe().mark_cancel_pending(...)` extends the
            // temporary guard through the entire match expression; attempting
            // another storage lock in the error arm would then self-deadlock.
            let mark_result = {
                let mut guard = storage.lock_safe();
                guard.mark_cancel_pending(params.broker_order_id, connection_session_id)
            };
            let previous_status = match mark_result {
                Ok(status) => status,
                Err(error) => {
                    let broker_is_open = snapshot
                        .open_orders
                        .iter()
                        .any(|order| order.broker_order_id == params.broker_order_id);
                    if !broker_is_open {
                        match storage.lock_safe().mark_previous_session_order_not_open(
                            params.broker_order_id,
                            connection_session_id,
                        ) {
                            Ok(true) => {
                                return failure(
                                    request.id,
                                    -32024,
                                    &format!(
                                        "IBKR does not report broker order {} as open. \
                                         The stale previous-session local order was marked \
                                         'not_open'; no cancellation was sent.",
                                        params.broker_order_id
                                    ),
                                );
                            }
                            Ok(false) => {}
                            Err(mark_error) => {
                                return failure(request.id, -32030, &mark_error.to_string());
                            }
                        }
                    }
                    return failure(request.id, -32030, &error.to_string());
                }
            };
            match ibkr.cancel_order(params.broker_order_id).await {
                Ok(()) => success(
                    request.id,
                    json!({"accepted": true, "status": "cancel_pending"}),
                ),
                Err(error) => {
                    let restore_result = storage.lock_safe().restore_cancel_status(
                        params.broker_order_id,
                        connection_session_id,
                        &previous_status,
                    );
                    let message = match restore_result {
                        Ok(()) => error,
                        Err(restore_error) => format!(
                            "{error}; additionally failed to restore local order status: {restore_error}"
                        ),
                    };
                    failure(request.id, -32023, &message)
                }
            }
        }
        "order.intent.resolve" => {
            let params = match serde_json::from_value::<ResolveOrderIntentParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            if !params.confirm {
                return failure(
                    request.id,
                    -32602,
                    "manual intent resolution requires confirm=true after verifying the true \
                     outcome against IBKR open orders and executions",
                );
            }
            match storage
                .lock_safe()
                .resolve_order_intent(params.order_intent_id, &params.note)
            {
                Ok(result) => success(request.id, result),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "order.intent.list" | "order.list" | "execution.list" => {
            let params = match serde_json::from_value::<PaginationParams>(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            let page = params.page.max(1);
            let page_size = params.limit.unwrap_or(params.page_size).clamp(1, 500);
            if request.method == "order.intent.list" {
                match storage
                    .lock_safe()
                    .list_unknown_order_intents_page(page, page_size)
                {
                    Ok((intents, total_items)) => success(
                        request.id,
                        paginated("intents", intents, page, page_size, total_items),
                    ),
                    Err(error) => failure(request.id, -32030, &error.to_string()),
                }
            } else if request.method == "order.list" {
                match storage.lock_safe().list_orders_page(page, page_size) {
                    Ok((orders, total_items)) => success(
                        request.id,
                        paginated("orders", orders, page, page_size, total_items),
                    ),
                    Err(error) => failure(request.id, -32030, &error.to_string()),
                }
            } else {
                match storage.lock_safe().list_executions_page(page, page_size) {
                    Ok((executions, total_items)) => success(
                        request.id,
                        paginated("executions", executions, page, page_size, total_items),
                    ),
                    Err(error) => failure(request.id, -32030, &error.to_string()),
                }
            }
        }
        "reconcile.run" => match ibkr.reconcile().await {
            Ok(snapshot) => match storage.lock_safe().reconcile(&snapshot) {
                Ok(report) => success(request.id, json!({"report": report})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            },
            Err(error) => failure(request.id, -32040, &error),
        },
        "reconcile.status" => {
            match storage
                .lock_safe()
                .reconciliation_health(ibkr.status().connection_session_id)
            {
                Ok(health) => success(request.id, json!({"reconciliation": health})),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "reconcile.differences" => match storage.lock_safe().list_reconciliation_differences() {
            Ok(differences) => success(request.id, json!({"differences": differences})),
            Err(error) => failure(request.id, -32030, &error.to_string()),
        },
        "reconcile.acknowledge" => {
            let params = match serde_json::from_value::<AcknowledgeDifferenceParams>(request.params)
            {
                Ok(params) => params,
                Err(error) => {
                    return failure(request.id, -32602, &format!("invalid parameters: {error}"));
                }
            };
            match storage
                .lock_safe()
                .acknowledge_reconciliation_difference(params.difference_id, &params.note)
            {
                Ok(()) => success(
                    request.id,
                    json!({
                        "acknowledged": true,
                        "difference_id": params.difference_id,
                        "requires_reconciliation": true
                    }),
                ),
                Err(error) => failure(request.id, -32030, &error.to_string()),
            }
        }
        "system.shutdown" => {
            if !request.params.is_null() && request.params != json!({}) {
                return failure(request.id, -32602, "system.shutdown takes no parameters");
            }
            cancellation.cancel();
            success(request.id, json!({"accepted": true}))
        }
        _ => failure(request.id, -32601, "method not found"),
    }
}

fn success(id: Value, result: Value) -> Response {
    Response {
        jsonrpc: "2.0".into(),
        id,
        result: Some(result),
        error: None,
    }
}

fn paginated(
    key: &str,
    rows: Vec<Value>,
    page: usize,
    page_size: usize,
    total_items: usize,
) -> Value {
    let total_pages = total_items.div_ceil(page_size).max(1);
    let mut result = serde_json::Map::new();
    result.insert(key.to_owned(), Value::Array(rows));
    result.insert("page".into(), json!(page));
    result.insert("page_size".into(), json!(page_size));
    result.insert("total_items".into(), json!(total_items));
    result.insert("total_pages".into(), json!(total_pages));
    Value::Object(result)
}

fn failure(id: Value, code: i64, message: &str) -> Response {
    Response {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(RpcError {
            code,
            message: message.into(),
            data: None,
        }),
    }
}

pub async fn call(
    address: SocketAddr,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value> {
    let endpoint = format!("http://{address}");
    let client = HttpClientBuilder::default()
        .request_timeout(timeout)
        .build(&endpoint)
        .map_err(|error| AppError::DaemonUnavailable {
            endpoint: endpoint.clone(),
            reason: error.to_string(),
        })?;
    let params = params.as_object().cloned().ok_or_else(|| AppError::Rpc {
        code: -32602,
        message: "RPC params must be a JSON object".into(),
    })?;
    client
        .request(method, params)
        .await
        .map_err(|error| match error {
            jsonrpsee::core::client::Error::Call(error) => AppError::Rpc {
                code: error.code().into(),
                message: error.message().into(),
            },
            other => AppError::DaemonUnavailable {
                endpoint,
                reason: other.to_string(),
            },
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IbkrConfig;

    #[tokio::test]
    async fn rejects_unknown_method() {
        let (sender, receiver) = watch::channel(test_status());
        drop(sender);
        let ibkr = ibkr::spawn(
            IbkrConfig::default(),
            RiskConfig::default().max_account_data_age_seconds,
            CancellationToken::new(),
        );
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(Mutex::new(
            Storage::open(&directory.path().join("rpc.duckdb")).unwrap(),
        ));
        let strategy_order_coordination = Arc::new(AsyncMutex::new(()));
        let response = dispatch(
            Request {
                jsonrpc: "2.0".into(),
                id: json!(1),
                method: "unknown".into(),
                params: Value::Null,
            },
            &receiver,
            &ibkr,
            &storage,
            &strategy_order_coordination,
            &RiskConfig::default(),
            directory.path(),
            directory.path(),
            directory.path(),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(response.error.unwrap().code, -32601);
    }

    #[tokio::test]
    async fn jsonrpsee_http_server_and_client_share_the_contract() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let cancellation = CancellationToken::new();
        let (sender, receiver) = watch::channel(test_status());
        drop(sender);
        let ibkr = ibkr::spawn(
            IbkrConfig::default(),
            RiskConfig::default().max_account_data_age_seconds,
            cancellation.clone(),
        );
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(Mutex::new(
            Storage::open(&directory.path().join("rpc-http.duckdb")).unwrap(),
        ));
        let mut config = RpcConfig::default();
        config.http_listen = address;
        let server = RpcServer::new(
            config,
            receiver,
            ibkr,
            storage,
            Arc::new(AsyncMutex::new(())),
            RiskConfig::default(),
            directory.path().into(),
            directory.path().into(),
            directory.path().into(),
            cancellation.clone(),
        );
        let task = tokio::spawn(server.run());

        let mut response = None;
        for _ in 0..20 {
            match call(
                address,
                quant_rpc_types::method::SYSTEM_VERSION,
                json!({}),
                Duration::from_secs(1),
            )
            .await
            {
                Ok(value) => {
                    response = Some(value);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        assert_eq!(
            response.unwrap()["rpc_version"],
            quant_rpc_types::RPC_VERSION
        );
        cancellation.cancel();
        assert!(task.await.unwrap().is_ok());
    }

    #[test]
    fn strategy_order_coordination_covers_target_and_execution_mutations() {
        for method in [
            "order.submit",
            "strategy.start",
            "strategy.pause",
            "strategy.stop",
            "strategy.execution.configure",
            "strategy.execution.configure_portfolio",
            "strategy.execution.enable",
            "strategy.execution.disable",
            "execution_cost.model.upsert",
            "execution_cost.control.configure",
            "execution_risk.control.configure",
        ] {
            assert!(
                strategy_order_coordination_required(method),
                "{method} must serialize with broker submission"
            );
        }
        assert!(!strategy_order_coordination_required("strategy.list"));
        assert!(!strategy_order_coordination_required("order.preview"));
    }

    #[test]
    fn web_origin_validation_allows_cli_and_configured_ui_only() {
        let mut validator = AllowedWebOrigin {
            value: "http://127.0.0.1:8080".into(),
        };
        let mut cli_request = HttpRequest::new(());
        assert!(validator.validate(&mut cli_request).is_ok());

        let mut web_request = HttpRequest::builder()
            .header(ORIGIN, "http://127.0.0.1:8080")
            .body(())
            .unwrap();
        assert!(validator.validate(&mut web_request).is_ok());

        let mut foreign_request = HttpRequest::builder()
            .header(ORIGIN, "https://example.invalid")
            .body(())
            .unwrap();
        let response = validator.validate(&mut foreign_request).unwrap_err();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn wildcard_web_origin_allows_any_browser_origin() {
        let mut validator = AllowedWebOrigin { value: "*".into() };
        let mut request = HttpRequest::builder()
            .header(ORIGIN, "https://any-site.example")
            .body(())
            .unwrap();
        assert!(validator.validate(&mut request).is_ok());
    }

    fn test_status() -> SystemStatus {
        SystemStatus {
            version: "test",
            pid: 1,
            started_at: Utc::now(),
            uptime_seconds: 0,
            state: SystemState::Ready,
            environment: Environment::Development,
            storage_schema_version: 1,
            trading_enabled: false,
            ibkr: ConnectionStatus::new(&IbkrConfig::default()),
            reconciliation: ReconciliationHealth {
                state: "pending",
                reconciliation_id: None,
                connection_session_id: None,
                blocking_difference_count: 0,
                completed_at: None,
            },
        }
    }
}
