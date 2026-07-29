use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use crate::api::call_method;

use super::{error_modal::ErrorModal, value::localize_json_times};

#[derive(Properties, PartialEq)]
pub struct RpcToolsPageProps {
    pub endpoint: String,
}

#[function_component(RpcToolsPage)]
pub fn rpc_tools_page(props: &RpcToolsPageProps) -> Html {
    let method = use_state(|| "system.status".to_owned());
    let params = use_state(|| "{}".to_owned());
    let confirmed = use_state(|| false);
    let busy = use_state(|| false);
    let result = use_state(|| None::<Result<Value, String>>);
    let mutation = is_mutation(&method);

    let submit = {
        let method = method.clone();
        let params = params.clone();
        let confirmed = confirmed.clone();
        let busy = busy.clone();
        let result = result.clone();
        let endpoint = props.endpoint.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let parsed = match serde_json::from_str::<Value>(&params) {
                Ok(Value::Object(object)) => Value::Object(object),
                Ok(_) => {
                    result.set(Some(Err("RPC 参数必须是 JSON object".into())));
                    return;
                }
                Err(error) => {
                    result.set(Some(Err(format!("JSON 参数无效：{error}"))));
                    return;
                }
            };
            if is_mutation(&method) && !*confirmed {
                result.set(Some(Err("变更方法必须勾选确认后才能执行".into())));
                return;
            }
            let method_name = (*method).clone();
            let endpoint = endpoint.clone();
            let busy = busy.clone();
            let result = result.clone();
            busy.set(true);
            spawn_local(async move {
                let response = call_method(&endpoint, &method_name, parsed)
                    .await
                    .map(|value| localize_json_times(&value));
                result.set(Some(response));
                busy.set(false);
            });
        })
    };

    html! {
        <>
            <ErrorModal
                message={result.as_ref().and_then(|value| value.as_ref().err()).cloned()}
                on_close={{
                    let result = result.clone();
                    Callback::from(move |_| result.set(None))
                }}
            />
            <Alert style={Color::Warning}>
                {"高级 RPC 工具覆盖 CLI 使用的全部共享 RPC 方法。优先使用专用页面；订单、安全控制和关闭 daemon 等方法会直接改变后台状态。"}
            </Alert>
            <div class="card shadow-sm"><div class="card-body">
                <form onsubmit={submit}>
                    <div class="row g-3">
                        <div class="col-12 col-lg-5">
                            <label class="form-label" for="rpc-method">{"RPC 方法"}</label>
                            <select
                                id="rpc-method"
                                class="form-select"
                                value={(*method).clone()}
                                onchange={{
                                    let method = method.clone();
                                    let params = params.clone();
                                    let confirmed = confirmed.clone();
                                    let result = result.clone();
                                    Callback::from(move |event: Event| {
                                        let select: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                        let next = select.value();
                                        params.set(parameter_template(&next).to_string());
                                        method.set(next);
                                        confirmed.set(false);
                                        result.set(None);
                                    })
                                }}
                            >
                                {quant_rpc_types::ALL_METHODS.iter().map(|name| html! {
                                    <option value={*name}>{*name}</option>
                                }).collect::<Html>()}
                            </select>
                        </div>
                        <div class="col-12 col-lg-7">
                            <label class="form-label" for="rpc-endpoint-readonly">{"Endpoint"}</label>
                            <input id="rpc-endpoint-readonly" class="form-control" value={props.endpoint.clone()} readonly=true />
                        </div>
                        <div class="col-12">
                            <label class="form-label" for="rpc-params">{"参数（JSON object）"}</label>
                            <textarea
                                id="rpc-params"
                                class="form-control rpc-params"
                                rows="10"
                                value={(*params).clone()}
                                oninput={{
                                    let params = params.clone();
                                    Callback::from(move |event: InputEvent| {
                                        let input: web_sys::HtmlTextAreaElement = event.target_unchecked_into();
                                        params.set(input.value());
                                    })
                                }}
                            />
                        </div>
                        {
                            mutation.then(|| html! {
                                <div class="col-12">
                                    <div class="form-check">
                                        <input
                                            id="rpc-confirm"
                                            class="form-check-input"
                                            type="checkbox"
                                            checked={*confirmed}
                                            onchange={{
                                                let confirmed = confirmed.clone();
                                                Callback::from(move |event: Event| {
                                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                                    confirmed.set(input.checked());
                                                })
                                            }}
                                        />
                                        <label class="form-check-label text-danger fw-semibold" for="rpc-confirm">
                                            {format!("我确认执行变更方法 {}", *method)}
                                        </label>
                                    </div>
                                </div>
                            }).unwrap_or_default()
                        }
                        <div class="col-12">
                            <button class="btn btn-primary" type="submit" disabled={*busy || (mutation && !*confirmed)}>
                                {
                                    if *busy {
                                        html! {
                                            <>
                                                <span class="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                                                {"执行中…"}
                                            </>
                                        }
                                    } else {
                                        html! { "调用 RPC" }
                                    }
                                }
                            </button>
                        </div>
                    </div>
                </form>
            </div></div>
            {
                match &*result {
                    Some(Ok(value)) => html! {
                        <section class="mt-4">
                            <h2 class="h5">{"响应"}</h2>
                            <div class="card shadow-sm"><div class="card-body">
                                <pre class="rpc-result mb-0">{serde_json::to_string_pretty(value).unwrap_or_else(|_| "无法显示响应".into())}</pre>
                            </div></div>
                        </section>
                    },
                    Some(Err(_)) => Html::default(),
                    None => Html::default(),
                }
            }
        </>
    }
}

fn is_mutation(method: &str) -> bool {
    !matches!(
        method,
        "system.status"
            | "system.health"
            | "system.version"
            | "ibkr.status"
            | "account.managed"
            | "account.summary"
            | "account.pnl"
            | "instrument.search"
            | "instrument.list"
            | "portfolio.positions"
            | "data.jobs"
            | "data.coverage"
            | "data.snapshot.list"
            | "data.verify"
            | "market_data.subscriptions"
            | "market_data.quote"
            | "market_data.health"
            | "market_data.bars"
            | "strategy.kinds"
            | "strategy.list"
            | "strategy.signals"
            | "strategy.execution.list"
            | "strategy.execution.actions"
            | "performance.report"
            | "performance.snapshots"
            | "fx.list"
            | "calendar.list"
            | "calendar.status"
            | "monitor.alerts"
            | "monitor.metrics"
            | "backtest.list"
            | "backtest.get"
            | "backup.list"
            | "order.preview"
            | "order.list"
            | "execution.list"
            | "reconcile.status"
            | "reconcile.differences"
            | "safety.status"
    )
}

fn parameter_template(method: &str) -> Value {
    match method {
        "instrument.search" => json!({"pattern": "EUR"}),
        "data.backfill" => json!({
            "contract": contract_template(),
            "timeframe": "1m",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "outside_rth": false
        }),
        "data.job.cancel" => json!({"job_id": ""}),
        "data.coverage" => json!({
            "conid": 0, "timeframe": "1m",
            "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"
        }),
        "data.snapshot.create" => json!({"name": "snapshot-name", "dataset": "bars"}),
        "market_data.subscribe" => contract_template(),
        "market_data.unsubscribe" | "market_data.quote" | "market_data.health" => {
            json!({"conid": 0})
        }
        "market_data.bars" => json!({"conid": 0, "timeframe": "1m", "limit": 100}),
        "strategy.create" => json!({
            "name": "paper-web-round-trip",
            "kind": "paper_round_trip",
            "config": {"conid": 0, "phase_bars": 1}
        }),
        "strategy.start" | "strategy.pause" | "strategy.stop" => json!({"strategy_id": ""}),
        "strategy.delete" => json!({"strategy_id": "", "confirm": true}),
        "strategy.signals" => json!({"strategy_id": "", "limit": 100}),
        "strategy.execution.configure" => json!({
            "strategy_id": "", "account": "", "target_quantity": 1.0,
            "short_target_quantity": 0.0, "allow_short": false,
            "order_type": "market", "paper_only": true, "contract": contract_template()
        }),
        "strategy.execution.configure_portfolio" => json!({
            "strategy_id": "", "account": "", "order_type": "market",
            "paper_only": true, "legs": []
        }),
        "strategy.execution.enable" | "strategy.execution.disable" => {
            json!({"strategy_id": "", "confirm": true})
        }
        "strategy.execution.actions" => json!({"page": 1, "page_size": 25}),
        "performance.report" => {
            json!({"strategy_id": "", "initial_capital": 100000.0, "benchmark_conid": null})
        }
        "performance.snapshots" => json!({"strategy_id": "", "limit": 100}),
        "fx.set" => json!({
            "base_currency": "EUR", "quote_currency": "USD", "rate": 1.0,
            "source": "manual", "observed_at": "2026-01-01T00:00:00Z"
        }),
        "calendar.add" => json!({
            "exchange": "NYSE", "trading_date": "2026-01-02",
            "opens_at": "2026-01-02T14:30:00Z", "closes_at": "2026-01-02T21:00:00Z",
            "state": "open", "source": "manual"
        }),
        "calendar.list" => json!({"exchange": null, "limit": 100}),
        "calendar.status" => json!({"exchange": "NYSE"}),
        "monitor.alerts" => json!({"active_only": true, "limit": 100}),
        "monitor.acknowledge" => json!({"alert_id": "", "note": ""}),
        "backtest.run" => json!({
            "strategy_id": null,
            "conid": 0, "timeframe": "1m",
            "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z",
            "strategy_kind": "paper_round_trip",
            "strategy_config": {"conid": 0, "phase_bars": 1},
            "quantity": 1.0, "initial_cash": 100000.0,
            "slippage_bps": 0.0, "commission_per_order": 0.0, "seed": 0
        }),
        "backtest.get" => json!({"backtest_id": ""}),
        "safety.set" => json!({"mode": "normal", "note": ""}),
        "safety.live_approve" => {
            json!({"conids": [], "note": "", "confirm_live_risk": false})
        }
        "safety.live_revoke" => json!({"note": ""}),
        "order.preview" | "order.submit" => json!({
            "idempotency_key": "",
            "account": "",
            "contract": contract_template(),
            "side": "buy",
            "quantity": 1.0,
            "order_type": "market",
            "limit_price": null,
            "outside_rth": false,
            "estimated_price": null
        }),
        "order.cancel" => json!({"broker_order_id": 0}),
        "reconcile.acknowledge" => json!({"difference_id": "", "note": ""}),
        "order.list" | "execution.list" => json!({"page": 1, "page_size": 25}),
        _ => json!({}),
    }
}

fn contract_template() -> Value {
    json!({
        "conid": 0,
        "symbol": "",
        "security_type": "STK",
        "currency": "USD",
        "exchange": "SMART",
        "primary_exchange": "",
        "local_symbol": "",
        "description": "",
        "derivative_security_types": []
    })
}
