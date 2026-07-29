use chrono::{Local, NaiveDateTime, TimeDelta, TimeZone, Utc};
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use crate::api::call_method;

use super::{
    backtest_data_panel::BacktestDataPanel,
    error_modal::ErrorModal,
    instrument_search::InstrumentSearch,
    value::{
        array, format_number, integer, local_time, number, official_security_name,
        security_exchange, text,
    },
};

#[derive(Properties, PartialEq)]
pub struct BacktestPageProps {
    pub endpoint: String,
    pub strategies: Value,
}

#[function_component(BacktestPage)]
pub fn backtest_page(props: &BacktestPageProps) -> Html {
    let strategies = array(&props.strategies, "strategies");
    let strategy_id = use_state(|| {
        strategies
            .first()
            .map(|strategy| text(strategy, "strategy_id"))
            .filter(|value| value != "—")
            .unwrap_or_default()
    });
    let instrument = use_state(|| None::<Value>);
    let timeframe = use_state(|| "1m".to_owned());
    let start = use_state(|| local_datetime_value(-7.0 * 86_400_000.0));
    let end = use_state(|| local_datetime_value(0.0));
    let quantity = use_state(|| "1".to_owned());
    let initial_cash = use_state(|| "100000".to_owned());
    let slippage_bps = use_state(|| "5".to_owned());
    let commission = use_state(|| "1".to_owned());
    let seed = use_state(|| "42".to_owned());
    let busy = use_state(|| false);
    let data_ready = use_state(|| false);
    let list_busy = use_state(|| true);
    let runs = use_state(Vec::<Value>::new);
    let detail = use_state(|| None::<Value>);
    let detail_busy = use_state(|| None::<String>);
    let notice = use_state(|| None::<Result<String, String>>);

    {
        let endpoint = props.endpoint.clone();
        let runs = runs.clone();
        let list_busy = list_busy.clone();
        let notice = notice.clone();
        use_effect_with(endpoint.clone(), move |_| {
            load_runs(endpoint, runs, list_busy, notice);
            || ()
        });
    }

    let run = {
        let endpoint = props.endpoint.clone();
        let strategies = strategies.clone();
        let strategy_id = strategy_id.clone();
        let instrument = instrument.clone();
        let timeframe = timeframe.clone();
        let start = start.clone();
        let end = end.clone();
        let quantity = quantity.clone();
        let initial_cash = initial_cash.clone();
        let slippage_bps = slippage_bps.clone();
        let commission = commission.clone();
        let seed = seed.clone();
        let busy = busy.clone();
        let data_ready = data_ready.clone();
        let runs = runs.clone();
        let list_busy = list_busy.clone();
        let notice = notice.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let Some(contract) = (*instrument).clone() else {
                notice.set(Some(Err("请先搜索并选择证券".into())));
                return;
            };
            if !*data_ready {
                notice.set(Some(Err(
                    "所选证券、周期和时间范围尚无可用于回测的本地历史数据。请先在“准备本地历史数据”中完成下载。"
                        .into(),
                )));
                return;
            }
            let Some(strategy) = strategies
                .iter()
                .find(|strategy| text(strategy, "strategy_id") == *strategy_id)
            else {
                notice.set(Some(Err("请选择策略".into())));
                return;
            };
            let parse_positive = |value: &str, label: &str| {
                value
                    .parse::<f64>()
                    .ok()
                    .filter(|number| number.is_finite() && *number > 0.0)
                    .ok_or_else(|| format!("{label}必须大于 0"))
            };
            let quantity_value = match parse_positive(&quantity, "交易数量") {
                Ok(value) => value,
                Err(error) => {
                    notice.set(Some(Err(error)));
                    return;
                }
            };
            let cash_value = match parse_positive(&initial_cash, "初始资金") {
                Ok(value) => value,
                Err(error) => {
                    notice.set(Some(Err(error)));
                    return;
                }
            };
            let Some(start_utc) = datetime_local_to_utc(&start) else {
                notice.set(Some(Err("回测开始时间无效".into())));
                return;
            };
            let Some(end_utc) = datetime_local_to_utc(&end) else {
                notice.set(Some(Err("回测结束时间无效".into())));
                return;
            };
            if start_utc >= end_utc {
                notice.set(Some(Err("结束时间必须晚于开始时间".into())));
                return;
            }
            let conid = contract.get("conid").and_then(Value::as_i64).unwrap_or(0);
            if conid <= 0 {
                notice.set(Some(Err("所选证券没有有效 Conid".into())));
                return;
            }
            let mut strategy_config = strategy.get("config").cloned().unwrap_or_else(|| json!({}));
            strategy_config["conid"] = Value::from(conid);
            let params = json!({
                "strategy_id": *strategy_id,
                "conid": conid,
                "timeframe": *timeframe,
                "start": start_utc,
                "end": end_utc,
                "strategy_kind": text(strategy, "kind"),
                "strategy_config": strategy_config,
                "quantity": quantity_value,
                "initial_cash": cash_value,
                "slippage_bps": slippage_bps.parse::<f64>().unwrap_or(0.0),
                "commission_per_order": commission.parse::<f64>().unwrap_or(0.0),
                "seed": seed.parse::<i64>().unwrap_or(0)
            });
            let endpoint = endpoint.clone();
            let busy = busy.clone();
            let runs = runs.clone();
            let list_busy = list_busy.clone();
            let notice = notice.clone();
            busy.set(true);
            notice.set(None);
            spawn_local(async move {
                match call_method(&endpoint, "backtest.run", params).await {
                    Ok(response) => {
                        let id = text(&response, "backtest_id");
                        notice.set(Some(Ok(format!("回测已完成：{id}"))));
                        load_runs(endpoint, runs, list_busy, notice.clone());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    let refresh_runs = {
        let endpoint = props.endpoint.clone();
        let runs = runs.clone();
        let list_busy = list_busy.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            load_runs(
                endpoint.clone(),
                runs.clone(),
                list_busy.clone(),
                notice.clone(),
            )
        })
    };

    html! {
        <>
            <ErrorModal
                message={notice.as_ref().and_then(|value| value.as_ref().err()).cloned()}
                on_close={{
                    let notice = notice.clone();
                    Callback::from(move |_| notice.set(None))
                }}
            />
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"运行回测"}</h2>
                <p class="text-secondary">
                    {"使用本地 Parquet Bar，按收盘信号并在下一根 Bar 开盘成交，避免未来函数。请先确保所选时间范围已有历史数据。"}
                </p>
                <form onsubmit={run}>
                    <div class="row g-3">
                        <div class="col-12">
                            <label class="form-label">{"1. 选择证券"}</label>
                            <InstrumentSearch endpoint={props.endpoint.clone()} on_select={{
                                let instrument = instrument.clone();
                                Callback::from(move |value| instrument.set(Some(value)))
                            }} />
                            {instrument.as_ref().map(|value| html! {
                                <div class="alert alert-success mt-3 mb-0">
                                    {format!("已选择：{} ({}) · {} · Conid {}",
                                        official_security_name(value), text(value, "symbol"),
                                        security_exchange(value), integer(value, "conid"))}
                                </div>
                            }).unwrap_or_default()}
                        </div>
                        <div class="col-12">
                            <label class="form-label" for="backtest-strategy">{"2. 选择策略"}</label>
                            <select id="backtest-strategy" class="form-select" value={(*strategy_id).clone()}
                                onchange={{
                                    let strategy_id = strategy_id.clone();
                                    Callback::from(move |event: Event| {
                                        let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                        strategy_id.set(input.value());
                                    })
                                }}>
                                {strategies.iter().map(|strategy| {
                                    let id = text(strategy, "strategy_id");
                                    html! { <option value={id.clone()}>{format!("{} · {} · {}", text(strategy, "name"), text(strategy, "kind"), id)}</option> }
                                }).collect::<Html>()}
                            </select>
                            <div class="form-text strategy-id">{format!("完整策略 UUID：{}", *strategy_id)}</div>
                        </div>
                        <Field label="时间周期" value={timeframe.clone()} kind="text" />
                        <Field label="开始时间（本地）" value={start.clone()} kind="datetime-local" />
                        <Field label="结束时间（本地）" value={end.clone()} kind="datetime-local" />
                        <BacktestDataPanel
                            endpoint={props.endpoint.clone()}
                            instrument={(*instrument).clone()}
                            timeframe={(*timeframe).clone()}
                            start={(*start).clone()}
                            end={(*end).clone()}
                            on_ready={{
                                let data_ready = data_ready.clone();
                                Callback::from(move |ready| data_ready.set(ready))
                            }}
                            on_error={{
                                let notice = notice.clone();
                                Callback::from(move |error| notice.set(Some(Err(error))))
                            }}
                        />
                        <Field label="每次交易数量" value={quantity.clone()} kind="number" />
                        <Field label="初始资金" value={initial_cash.clone()} kind="number" />
                        <Field label="滑点（bps）" value={slippage_bps.clone()} kind="number" />
                        <Field label="每单佣金" value={commission.clone()} kind="number" />
                        <Field label="随机种子" value={seed.clone()} kind="number" />
                        <div class="col-12">
                            <button class="btn btn-primary" type="submit"
                                disabled={*busy || instrument.is_none() || strategy_id.is_empty() || !*data_ready}>
                                {if *busy {
                                    html! { <><span class="spinner-border spinner-border-sm me-2" />{"回测运行中…"}</> }
                                } else { html! { "运行回测" } }}
                            </button>
                        </div>
                    </div>
                </form>
                {notice.as_ref().map(|result| match result {
                    Ok(message) => html! { <Alert style={Color::Success} class="mt-3"><span>{message}</span></Alert> },
                    Err(_) => Html::default(),
                }).unwrap_or_default()}
            </div></section>

            <section class="card shadow-sm"><div class="card-body">
                <div class="d-flex justify-content-between align-items-center mb-3">
                    <h2 class="h5 mb-0">{"回测历史"}</h2>
                    <button class="btn btn-sm btn-outline-primary" disabled={*list_busy} onclick={refresh_runs}>
                        {if *list_busy {
                            html! { <><span class="spinner-border spinner-border-sm me-2" />{"刷新中…"}</> }
                        } else { html! { "刷新" } }}
                    </button>
                </div>
                <div class="table-responsive"><table class="table table-hover align-middle mb-0">
                    <thead><tr>
                        <th>{"开始时间（本地）"}</th><th>{"Backtest ID"}</th><th>{"策略 UUID"}</th>
                        <th>{"证券（官方名称）"}</th><th>{"交易所"}</th><th>{"周期"}</th>
                        <th>{"状态"}</th><th class="text-end">{"收益率"}</th>
                        <th class="text-end">{"最大回撤"}</th><th class="text-end">{"交易数"}</th><th>{"操作"}</th>
                    </tr></thead>
                    <tbody>
                        {if runs.is_empty() {
                            html! { <tr><td colspan="11" class="text-center text-secondary py-4">{"暂无回测记录"}</td></tr> }
                        } else {
                            runs.iter().map(|run| {
                                let id = text(run, "backtest_id");
                                html! { <tr>
                                    <td class="text-nowrap">{local_time(run, "started_at")}</td>
                                    <td class="strategy-id"><code>{id.clone()}</code></td>
                                    <td class="strategy-id"><code>{run.pointer("/parameters/strategy_id").and_then(Value::as_str).unwrap_or("—")}</code></td>
                                    <td><div class="fw-semibold">{official_security_name(run)}</div><div class="small text-secondary">{text(run, "symbol")}</div></td>
                                    <td>{security_exchange(run)}</td>
                                    <td>{run.pointer("/parameters/timeframe").and_then(Value::as_str).unwrap_or("—")}</td>
                                    <td><span class="badge bg-secondary">{text(run, "state")}</span></td>
                                    <td class="text-end">{metric_percent(run, "total_return")}</td>
                                    <td class="text-end">{metric_percent(run, "maximum_drawdown")}</td>
                                    <td class="text-end">{metric_integer(run, "trade_count")}</td>
                                    <td><button class="btn btn-sm btn-outline-primary" disabled={detail_busy.is_some()} onclick={{
                                        let endpoint = props.endpoint.clone();
                                        let detail = detail.clone();
                                        let detail_busy = detail_busy.clone();
                                        let notice = notice.clone();
                                        let id_for_request = id.clone();
                                        Callback::from(move |_| {
                                            detail_busy.set(Some(id_for_request.clone()));
                                            let endpoint = endpoint.clone();
                                            let detail = detail.clone();
                                            let detail_busy = detail_busy.clone();
                                            let notice = notice.clone();
                                            let id = id_for_request.clone();
                                            spawn_local(async move {
                                                match call_method(&endpoint, "backtest.get", json!({"backtest_id": id})).await {
                                                    Ok(value) => detail.set(Some(value)),
                                                    Err(error) => notice.set(Some(Err(error))),
                                                }
                                                detail_busy.set(None);
                                            });
                                        })
                                    }}>
                                        {if detail_busy.as_deref() == Some(id.as_str()) {
                                            html! { <><span class="spinner-border spinner-border-sm me-2" />{"加载中…"}</> }
                                        } else { html! { "查看结果" } }}
                                    </button></td>
                                </tr> }
                            }).collect::<Html>()
                        }}
                    </tbody>
                </table></div>
            </div></section>
            {detail.as_ref().map(|value| backtest_modal(value, {
                let detail = detail.clone();
                Callback::from(move |_| detail.set(None))
            })).unwrap_or_default()}
        </>
    }
}

#[derive(Properties, PartialEq)]
struct FieldProps {
    label: &'static str,
    value: UseStateHandle<String>,
    kind: &'static str,
}

#[function_component(Field)]
fn field(props: &FieldProps) -> Html {
    html! {
        <div class="col-12 col-md-6 col-xl-3">
            <label class="form-label">{props.label}</label>
            <input class="form-control" type={props.kind} value={(*props.value).clone()}
                step={if props.kind == "number" { "any" } else { "1" }}
                oninput={{
                    let value = props.value.clone();
                    Callback::from(move |event: InputEvent| {
                        let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                        value.set(input.value());
                    })
                }} />
        </div>
    }
}

fn load_runs(
    endpoint: String,
    runs: UseStateHandle<Vec<Value>>,
    busy: UseStateHandle<bool>,
    notice: UseStateHandle<Option<Result<String, String>>>,
) {
    busy.set(true);
    spawn_local(async move {
        match call_method(&endpoint, "backtest.list", json!({})).await {
            Ok(response) => runs.set(array(&response, "backtests")),
            Err(error) => notice.set(Some(Err(error))),
        }
        busy.set(false);
    });
}

fn backtest_modal(run: &Value, on_close: Callback<MouseEvent>) -> Html {
    let trades = array(run, "trades");
    html! {
        <>
            <div class="modal fade show d-block" tabindex="-1" role="dialog" aria-modal="true">
                <div class="modal-dialog modal-xl modal-dialog-centered modal-dialog-scrollable">
                    <div class="modal-content">
                        <div class="modal-header">
                            <div>
                                <h2 class="modal-title h5">{"回测结果"}</h2>
                                <code class="strategy-id">{text(run, "backtest_id")}</code>
                            </div>
                            <button class="btn-close" type="button" onclick={on_close.clone()} />
                        </div>
                        <div class="modal-body">
                            <div class="row g-3 mb-4">
                                {[
                                    ("最终权益", metric_number(run, "final_equity")),
                                    ("总收益率", metric_percent(run, "total_return")),
                                    ("最大回撤", metric_percent(run, "maximum_drawdown")),
                                    ("交易次数", metric_integer(run, "trade_count")),
                                    ("换手率", metric_percent(run, "turnover")),
                                    ("持仓数量", metric_number(run, "open_position")),
                                ].into_iter().map(|(label, value)| html! {
                                    <div class="col-6 col-lg-2"><div class="card h-100"><div class="card-body">
                                        <div class="small text-secondary">{label}</div><div class="fw-semibold">{value}</div>
                                    </div></div></div>
                                }).collect::<Html>()}
                            </div>
                            {equity_chart(run)}
                            <h3 class="h6 mt-4">{"成交记录"}</h3>
                            <div class="table-responsive"><table class="table table-sm table-hover">
                                <thead><tr><th>{"信号时间（本地）"}</th><th>{"成交时间（本地）"}</th><th>{"方向"}</th><th>{"数量"}</th><th>{"价格"}</th><th>{"佣金"}</th><th>{"滑点"}</th></tr></thead>
                                <tbody>{if trades.is_empty() {
                                    html! { <tr><td colspan="7" class="text-center text-secondary">{"没有成交"}</td></tr> }
                                } else {
                                    trades.iter().map(|trade| html! { <tr>
                                        <td>{local_time(trade, "signal_time")}</td><td>{local_time(trade, "fill_time")}</td>
                                        <td>{text(trade, "side")}</td><td>{number(trade, "quantity")}</td>
                                        <td>{number(trade, "price")}</td><td>{number(trade, "commission")}</td><td>{number(trade, "slippage")}</td>
                                    </tr> }).collect::<Html>()
                                }}</tbody>
                            </table></div>
                        </div>
                        <div class="modal-footer"><button class="btn btn-secondary" onclick={on_close.clone()}>{"关闭"}</button></div>
                    </div>
                </div>
            </div>
            <div class="modal-backdrop fade show" onclick={on_close} />
        </>
    }
}

fn equity_chart(run: &Value) -> Html {
    let points = array(run, "equity");
    if points.len() < 2 {
        return html! { <div class="text-secondary">{"权益数据不足，无法绘制曲线。"}</div> };
    }
    let values = points
        .iter()
        .filter_map(|point| point.get("equity").and_then(Value::as_f64))
        .collect::<Vec<_>>();
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let range = (max - min).max(f64::EPSILON);
    let last = (values.len() - 1) as f64;
    let polyline = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            format!(
                "{:.2},{:.2}",
                index as f64 / last * 1000.0,
                220.0 - (value - min) / range * 200.0
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    html! {
        <div>
            <h3 class="h6">{"权益曲线"}</h3>
            <svg viewBox="0 0 1000 240" class="w-100 border rounded bg-light" role="img" aria-label="回测权益曲线">
                <polyline points={polyline} fill="none" stroke="#0d6efd" stroke-width="3" />
            </svg>
            <div class="d-flex justify-content-between small text-secondary">
                <span>{format!("最低 {}", format_number(min))}</span><span>{format!("最高 {}", format_number(max))}</span>
            </div>
        </div>
    }
}

fn metric_value<'a>(run: &'a Value, key: &str) -> Option<&'a Value> {
    run.get("metrics").and_then(|metrics| metrics.get(key))
}

fn metric_number(run: &Value, key: &str) -> String {
    metric_value(run, key)
        .and_then(Value::as_f64)
        .map(format_number)
        .unwrap_or_else(|| "—".into())
}

fn metric_integer(run: &Value, key: &str) -> String {
    metric_value(run, key)
        .and_then(Value::as_i64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "—".into())
}

fn metric_percent(run: &Value, key: &str) -> String {
    metric_value(run, key)
        .and_then(Value::as_f64)
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn datetime_local_to_utc(value: &str) -> Option<String> {
    let local = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M").ok()?;
    Local
        .from_local_datetime(&local)
        .single()
        .map(|date| date.with_timezone(&Utc).to_rfc3339())
}

fn local_datetime_value(offset_ms: f64) -> String {
    (Local::now() + TimeDelta::milliseconds(offset_ms as i64))
        .format("%Y-%m-%dT%H:%M")
        .to_string()
}
