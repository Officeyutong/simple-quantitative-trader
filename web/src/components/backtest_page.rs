use chrono::{Local, NaiveDateTime, TimeDelta, TimeZone, Utc};
use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use crate::api::call_method;

use super::{
    backtest_data_panel::BacktestDataPanel,
    error_modal::ErrorModal,
    value::{
        array, boolean, format_number, integer, local_time, number, official_security_name,
        security_exchange, text,
    },
};

#[derive(Properties, PartialEq)]
pub struct BacktestPageProps {
    pub endpoint: String,
    pub strategies: Value,
    pub execution_configs: Value,
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
    let start = use_state(|| local_datetime_value(-7.0 * 86_400_000.0));
    let end = use_state(|| local_datetime_value(0.0));
    let initial_cash = use_state(|| "100000".to_owned());
    let seed = use_state(|| "42".to_owned());
    let cost_gate_mode = use_state(|| "match_strategy".to_owned());
    let busy = use_state(|| false);
    let data_ready = use_state(|| false);
    let list_busy = use_state(|| true);
    let runs = use_state(Vec::<Value>::new);
    let detail = use_state(|| None::<Value>);
    let detail_busy = use_state(|| None::<String>);
    let cost_controls = use_state(Vec::<Value>::new);
    let cost_models = use_state(Vec::<Value>::new);
    let cost_loading = use_state(|| true);
    let notice = use_state(|| None::<Result<String, String>>);
    {
        let strategy_ids = strategies
            .iter()
            .map(|strategy| text(strategy, "strategy_id"))
            .filter(|id| id != "—")
            .collect::<Vec<_>>();
        let strategy_id = strategy_id.clone();
        let data_ready = data_ready.clone();
        use_effect_with(strategy_ids.clone(), move |_| {
            if !strategy_ids.iter().any(|id| id == strategy_id.as_str()) {
                strategy_id.set(strategy_ids.first().cloned().unwrap_or_default());
                data_ready.set(false);
            }
            || ()
        });
    }
    let selected_strategy = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == *strategy_id)
        .cloned();
    let instrument = selected_strategy.as_ref().and_then(strategy_instrument);
    let timeframe = selected_strategy
        .as_ref()
        .and_then(|strategy| strategy.get("bar_timeframe"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let minimum_history = selected_strategy
        .as_ref()
        .and_then(|strategy| strategy.get("minimum_history"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let is_portfolio = selected_strategy
        .as_ref()
        .is_some_and(|strategy| boolean(strategy, "is_portfolio"));
    let strategy_metadata_ready = !timeframe.is_empty() && minimum_history > 0;
    let execution_configs = array(&props.execution_configs, "configs");
    let execution_config = execution_configs
        .iter()
        .find(|config| text(config, "strategy_id") == *strategy_id)
        .cloned();
    let outside_rth = execution_config
        .as_ref()
        .is_some_and(|config| boolean(config, "outside_rth"));
    let execution_config_ready = execution_config.is_some();
    let cost_control = cost_controls
        .iter()
        .find(|control| text(control, "strategy_id") == *strategy_id);
    let cost_model = cost_control.and_then(|control| {
        let model_id = text(control, "cost_model_id");
        cost_models
            .iter()
            .find(|model| text(model, "cost_model_id") == model_id)
    });
    // The saved execution contract is the same currency source used by live
    // order submission. Fall back to the catalog instrument only for a
    // strategy that has not yet saved an execution configuration.
    let strategy_currency = execution_config
        .as_ref()
        .and_then(|config| config.pointer("/contract/currency"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            selected_strategy
                .as_ref()
                .map(|strategy| text(strategy, "currency"))
        })
        .unwrap_or_default();
    let initial_cash_label = initial_cash_label(&strategy_currency);
    let cost_currency_matches = cost_model
        .is_some_and(|model| text(model, "currency").eq_ignore_ascii_case(&strategy_currency));
    let cost_model_ready = cost_model.is_some() && cost_currency_matches;

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
    {
        let endpoint = props.endpoint.clone();
        let controls = cost_controls.clone();
        let models = cost_models.clone();
        let loading = cost_loading.clone();
        let notice = notice.clone();
        use_effect_with(endpoint.clone(), move |_| {
            loading.set(true);
            load_cost_configuration(
                endpoint.clone(),
                controls.clone(),
                models.clone(),
                loading.clone(),
                notice.clone(),
            );
            let interval = Interval::new(5_000, move || {
                load_cost_configuration(
                    endpoint.clone(),
                    controls.clone(),
                    models.clone(),
                    loading.clone(),
                    notice.clone(),
                )
            });
            move || drop(interval)
        });
    }

    let run = {
        let endpoint = props.endpoint.clone();
        let strategies = strategies.clone();
        let strategy_id = strategy_id.clone();
        let instrument = instrument.clone();
        let timeframe = timeframe.clone();
        let minimum_history = minimum_history;
        let is_portfolio = is_portfolio;
        let outside_rth = outside_rth;
        let execution_config = execution_config.clone();
        let start = start.clone();
        let end = end.clone();
        let initial_cash = initial_cash.clone();
        let cost_gate_mode = cost_gate_mode.clone();
        let cost_model_ready = cost_model_ready;
        let seed = seed.clone();
        let busy = busy.clone();
        let data_ready = data_ready.clone();
        let runs = runs.clone();
        let list_busy = list_busy.clone();
        let notice = notice.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            if is_portfolio {
                notice.set(Some(Err(
                    "策略绑定回测当前只支持单腿执行配置；该策略使用组合执行配置，无法运行回测。"
                        .into(),
                )));
                return;
            }
            if timeframe.is_empty() || minimum_history == 0 {
                notice.set(Some(Err(
                    "后端没有返回策略实现的 Bar 周期或最少历史 Bar 数；请确认 Web 与后端版本一致后重试。"
                        .into(),
                )));
                return;
            }
            let Some(contract) = instrument.clone() else {
                notice.set(Some(Err(
                    "策略绑定的证券资料不完整；请先通过证券搜索或行情订阅保存该合约资料。".into(),
                )));
                return;
            };
            if !*data_ready {
                notice.set(Some(Err(
                    "所选证券、周期和时间范围尚未通过完整下载验证。请先在“准备本地历史数据”中补齐未抓取范围。"
                        .into(),
                )));
                return;
            }
            if !cost_model_ready {
                notice.set(Some(Err(
                    "该策略没有可用且币种匹配的费用模型；请先在“交易成本”页面完成绑定。".into(),
                )));
                return;
            }
            let Some(execution_config) = execution_config.as_ref() else {
                notice.set(Some(Err(
                    "该策略尚未保存自动执行配置；回测无法确定与实时运行一致的多头目标、空头目标和做空权限。请先在策略执行配置中保存这些参数。"
                        .into(),
                )));
                return;
            };
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
            let Some(quantity_value) = execution_config
                .get("target_quantity")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value > 0.0)
            else {
                notice.set(Some(Err(
                    "策略执行配置中的多头目标仓位无效；请暂停策略并重新保存执行配置。".into(),
                )));
                return;
            };
            let Some(short_target_quantity) = execution_config
                .get("short_target_quantity")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value <= 0.0)
            else {
                notice.set(Some(Err(
                    "策略执行配置中的空头目标仓位无效；它必须小于或等于 0。".into(),
                )));
                return;
            };
            let allow_short = boolean(execution_config, "allow_short");
            if !allow_short && short_target_quantity < 0.0 {
                notice.set(Some(Err(
                    "策略执行配置禁止做空，但空头目标仓位小于 0；请先修正执行配置。".into(),
                )));
                return;
            }
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
            let params = json!({
                "strategy_id": *strategy_id,
                "cost_gate_mode": (*cost_gate_mode).clone(),
                "conid": conid,
                "timeframe": timeframe.clone(),
                "outside_rth": outside_rth,
                "start": start_utc,
                "end": end_utc,
                "strategy_kind": text(strategy, "kind"),
                "strategy_config": strategy.get("config").cloned().unwrap_or_else(|| json!({})),
                "quantity": quantity_value,
                "short_target_quantity": short_target_quantity,
                "allow_short": allow_short,
                "initial_cash": cash_value,
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
                    {"使用本地 Parquet Bar，按收盘信号并在下一根 Bar 开盘成交，避免未来函数。仓位目标强制使用所选策略已保存的执行配置。"}
                </p>
                <form onsubmit={run}>
                    <div class="row g-3">
                        <div class="col-12">
                            <label class="form-label" for="backtest-strategy">{"1. 选择策略"}</label>
                            <select id="backtest-strategy" class="form-select" value={(*strategy_id).clone()}
                                onchange={{
                                    let strategy_id = strategy_id.clone();
                                    let data_ready = data_ready.clone();
                                    Callback::from(move |event: Event| {
                                        let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                        strategy_id.set(input.value());
                                        data_ready.set(false);
                                    })
                                }}>
                                {strategies.iter().map(|strategy| {
                                    let id = text(strategy, "strategy_id");
                                    html! {
                                        <option
                                            key={id.clone()}
                                            value={id.clone()}
                                            selected={id == *strategy_id}
                                        >
                                            {text(strategy, "name")}
                                        </option>
                                    }
                                }).collect::<Html>()}
                            </select>
                        </div>
                        <div class="col-12">
                            <label class="form-label">{"2. 策略绑定的证券与周期"}</label>
                            {instrument.as_ref().map(|value| html! {
                                <div class="alert alert-success mb-0">
                                    <div>{format!("{} ({}) · {} · Conid {}",
                                        official_security_name(value), text(value, "symbol"),
                                        security_exchange(value), integer(value, "conid"))}</div>
                                    <div class="small mt-1">{format!(
                                        "Bar 周期：{}；最少历史：{} 根（由后端策略实现锁定）",
                                        if timeframe.is_empty() { "—" } else { timeframe.as_str() },
                                        if minimum_history == 0 { "—".into() } else { minimum_history.to_string() },
                                    )}</div>
                                    <div class="small mt-1">{format!("下载及回测交易时段：{}", if outside_rth { "含盘前盘后" } else { "常规交易时段" })}</div>
                                </div>
                            }).unwrap_or_else(|| html! {
                                <div class="alert alert-danger mb-0">
                                    {"该策略的 Conid 或本地证券资料不完整，暂时无法准备回测数据。"}
                                </div>
                            })}
                        </div>
                        {if is_portfolio {
                            html! {
                                <div class="col-12">
                                    <div class="alert alert-warning mb-0">
                                        <div class="fw-semibold">{"当前不能对组合执行策略运行绑定回测"}</div>
                                        <div class="small mt-1">
                                            {"当前回测引擎只支持单腿执行配置；组合策略需要逐腿成交、费用和权益计算支持后才能启用。"}
                                        </div>
                                    </div>
                                </div>
                            }
                        } else { Html::default() }}
                        {if !strategy_metadata_ready {
                            html! {
                                <div class="col-12">
                                    <div class="alert alert-danger mb-0">
                                        {"后端未提供策略实现的 Bar 周期或最少历史 Bar 数。请更新并重启后端，Web 不会自行猜测这些参数。"}
                                    </div>
                                </div>
                            }
                        } else { Html::default() }}
                        <Field label="开始时间（本地）" value={start.clone()} kind="datetime-local" />
                        <Field label="结束时间（本地）" value={end.clone()} kind="datetime-local" />
                        {if timeframe == "5s" {
                            html! {
                                <div class="col-12">
                                    <div class="alert alert-info mb-0">
                                        {"5 秒历史数据将按每小时分片从 IBKR 下载。较长范围会产生较多请求，请等待下载任务完成。"}
                                    </div>
                                </div>
                            }
                        } else { Html::default() }}
                        {if !is_portfolio && strategy_metadata_ready {
                            html! {
                                <BacktestDataPanel
                                    endpoint={props.endpoint.clone()}
                                    strategy_id={(*strategy_id).clone()}
                                    instrument={instrument.clone()}
                                    timeframe={timeframe.clone()}
                                    start={(*start).clone()}
                                    end={(*end).clone()}
                                    outside_rth={outside_rth}
                                    on_ready={{
                                        let data_ready = data_ready.clone();
                                        Callback::from(move |ready| data_ready.set(ready))
                                    }}
                                    on_error={{
                                        let notice = notice.clone();
                                        Callback::from(move |error| notice.set(Some(Err(error))))
                                    }}
                                />
                            }
                        } else { Html::default() }}
                        <div class="col-12">
                            {if is_portfolio {
                                Html::default()
                            } else {
                                execution_target_panel(execution_config.as_ref())
                            }}
                        </div>
                        <Field label={initial_cash_label} value={initial_cash.clone()} kind="number" />
                        <Field label="随机种子" value={seed.clone()} kind="number" />
                        <div class="col-12">
                            {cost_model_panel(cost_control, cost_model, &strategy_currency, *cost_loading)}
                        </div>
                        <div class="col-12">
                            <label class="form-label" for="backtest-cost-gate-mode">{"成本门控模拟模式"}</label>
                            <select
                                id="backtest-cost-gate-mode"
                                class="form-select"
                                value={(*cost_gate_mode).clone()}
                                onchange={{
                                    let cost_gate_mode = cost_gate_mode.clone();
                                    Callback::from(move |event: Event| {
                                        let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                        cost_gate_mode.set(input.value());
                                    })
                                }}
                            >
                                <option value="match_strategy">{"按当前策略成本门控模拟（默认）"}</option>
                                <option value="fees_only">{"仅扣除交易费用，不拦截信号"}</option>
                            </select>
                            <div class="form-text">
                                {if *cost_gate_mode == "match_strategy" {
                                    "冻结当前策略的安全倍数、佣金/已完成周期毛利润阈值和实时佣金 P90；减仓和平仓仍始终绕过成本门控。"
                                } else {
                                    "只计算佣金、税费、点差和滑点，所有策略信号都不会被成本门控过滤。"
                                }}
                            </div>
                        </div>
                        <div class="col-12">
                            <button class="btn btn-primary" type="submit"
                                disabled={*busy || *cost_loading || instrument.is_none() || strategy_id.is_empty() || !*data_ready || !cost_model_ready || !execution_config_ready || is_portfolio || !strategy_metadata_ready}>
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
                                                match call_method(&endpoint, "backtest.get", json!({
                                                    "backtest_id": id,
                                                    "trade_page": 1,
                                                    "trade_page_size": 500,
                                                    "max_equity_points": 2000
                                                })).await {
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

fn strategy_instrument(strategy: &Value) -> Option<Value> {
    let conid = strategy
        .get("conid")
        .and_then(Value::as_i64)
        .or_else(|| strategy.pointer("/config/conid").and_then(Value::as_i64))
        .filter(|value| *value > 0)?;
    let symbol = strategy
        .get("symbol")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())?;
    Some(json!({
        "conid": conid,
        "symbol": symbol,
        "security_type": strategy.get("security_type").and_then(Value::as_str).unwrap_or("STK"),
        "currency": strategy.get("currency").and_then(Value::as_str).unwrap_or_default(),
        "exchange": strategy.get("exchange").and_then(Value::as_str).unwrap_or("SMART"),
        "primary_exchange": strategy.get("primary_exchange").and_then(Value::as_str).unwrap_or_default(),
        "local_symbol": strategy.get("local_symbol").and_then(Value::as_str).unwrap_or(symbol),
        "description": strategy.get("description").and_then(Value::as_str).unwrap_or_default(),
        "derivative_security_types": []
    }))
}

fn execution_target_panel(config: Option<&Value>) -> Html {
    let Some(config) = config else {
        return html! {
            <div class="alert alert-danger mb-0">
                <div class="fw-semibold">{"尚未保存策略执行配置，无法运行回测"}</div>
                <div class="small mt-1">
                    {"请先保存该策略的自动执行配置。回测必须与实时运行使用相同的多头目标、空头目标和做空权限，不能在这里临时覆盖。"}
                </div>
            </div>
        };
    };
    html! {
        <div class="card bg-light">
            <div class="card-body">
                <div class="fw-semibold mb-1">{"策略执行目标（只读）"}</div>
                <div class="small text-secondary mb-3">
                    {"这些值来自已保存的策略执行配置，并随回测参数保存。修改时请先到策略执行配置页面暂停策略并重新保存。"}
                </div>
                <div class="row g-3">
                    <div class="col-12 col-md-4">
                        <label class="form-label">{"多头目标仓位"}</label>
                        <input class="form-control" type="number" value={number(config, "target_quantity")} readonly={true} />
                    </div>
                    <div class="col-12 col-md-4">
                        <label class="form-label">{"空头目标仓位"}</label>
                        <input class="form-control" type="number" value={number(config, "short_target_quantity")} readonly={true} />
                    </div>
                    <div class="col-12 col-md-4 d-flex align-items-end">
                        <div class="form-check mb-2">
                            <input class="form-check-input" type="checkbox" checked={boolean(config, "allow_short")} disabled={true} />
                            <label class="form-check-label">{"允许做空"}</label>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct FieldProps {
    label: String,
    value: UseStateHandle<String>,
    kind: &'static str,
}

#[function_component(Field)]
fn field(props: &FieldProps) -> Html {
    html! {
        <div class="col-12 col-md-6 col-xl-3">
            <label class="form-label">{props.label.clone()}</label>
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

fn load_cost_configuration(
    endpoint: String,
    controls: UseStateHandle<Vec<Value>>,
    models: UseStateHandle<Vec<Value>>,
    loading: UseStateHandle<bool>,
    notice: UseStateHandle<Option<Result<String, String>>>,
) {
    spawn_local(async move {
        let controls_result =
            call_method(&endpoint, "execution_cost.control.list", json!({})).await;
        let models_result = call_method(&endpoint, "execution_cost.model.list", json!({})).await;
        match (controls_result, models_result) {
            (Ok(control_value), Ok(model_value)) => {
                controls.set(array(&control_value, "controls"));
                models.set(array(&model_value, "models"));
            }
            (Err(error), _) | (_, Err(error)) => notice.set(Some(Err(error))),
        }
        loading.set(false);
    });
}

fn cost_model_panel(
    control: Option<&Value>,
    model: Option<&Value>,
    strategy_currency: &str,
    loading: bool,
) -> Html {
    if loading {
        return html! {
            <div class="alert alert-info mb-0">
                <span class="spinner-border spinner-border-sm me-2" />
                {"正在读取策略绑定的费用模型…"}
            </div>
        };
    }
    let (Some(control), Some(model)) = (control, model) else {
        return html! {
            <div class="alert alert-danger mb-0">
                <div class="fw-semibold">{"尚未绑定费用模型，无法运行回测"}</div>
                <div class="small mt-1">{"请先到“交易成本”页面为该策略保存费用模型。回测不再接受独立的手填佣金和滑点。"}</div>
            </div>
        };
    };
    let model_currency = text(model, "currency");
    if !model_currency.eq_ignore_ascii_case(strategy_currency) {
        return html! {
            <div class="alert alert-danger mb-0">
                <div class="fw-semibold">{"费用模型币种与证券币种不匹配"}</div>
                <div class="small mt-1">{format!("模型币种：{model_currency}；证券币种：{strategy_currency}。请先修正绑定。")}</div>
            </div>
        };
    }
    html! {
        <div class="card bg-light">
            <div class="card-body">
                <div class="d-flex flex-wrap justify-content-between align-items-center gap-2 mb-2">
                    <div>
                        <div class="fw-semibold">{format!("回测费用模型：{} ({model_currency})", text(model, "name"))}</div>
                        <div class="small text-secondary">{"模型值会随本次回测保存，之后修改费用模型不会改写历史结果。"}</div>
                    </div>
                    <span class={classes!("badge", if boolean(control, "enabled") { "bg-success" } else { "bg-secondary" })}>
                        {if boolean(control, "enabled") { "策略成本门控已启用" } else { "策略成本门控已停用" }}
                    </span>
                </div>
                <div class="row g-2 small">
                    <div class="col-12 col-lg-4">
                        <span class="text-secondary">{"买入："}</span>
                        {fee_model_summary(model, "buy")}
                    </div>
                    <div class="col-12 col-lg-4">
                        <span class="text-secondary">{"卖出："}</span>
                        {format!("{}；税费 {} bps", fee_model_summary(model, "sell"), number(model, "sell_tax_bps"))}
                    </div>
                    <div class="col-12 col-lg-4">
                        <span class="text-secondary">{"价格冲击："}</span>
                        {format!("点差 {} bps；单边滑点 {} bps", number(model, "estimated_spread_bps"), number(model, "estimated_slippage_bps"))}
                    </div>
                </div>
                <div class="alert alert-info py-2 mt-3 mb-0">
                    {"回测始终按此费用模型扣除佣金、税费、点差和滑点。“按当前策略成本门控模拟”还会复现信号强度/往返成本安全门槛，以及路径依赖的佣金/已完成周期毛利润门控。策略风险、账户状态、行情新鲜度、活动订单冲突和交易日历门控不在回测范围内。"}
                </div>
            </div>
        </div>
    }
}

fn initial_cash_label(currency: &str) -> String {
    let currency = currency.trim().to_ascii_uppercase();
    if currency.is_empty() {
        "初始资金（证券币种未知）".into()
    } else {
        format!("初始资金（{currency}）")
    }
}

fn fee_model_summary(model: &Value, side: &str) -> String {
    format!(
        "固定 {} / 每股 {} / 比例 {} bps / 最低 {}",
        number(model, &format!("{side}_fixed_fee")),
        number(model, &format!("{side}_per_share_fee")),
        number(model, &format!("{side}_rate_bps")),
        number(model, &format!("{side}_min_fee")),
    )
}

fn backtest_modal(run: &Value, on_close: Callback<MouseEvent>) -> Html {
    let trades = array(run, "trades");
    let trade_total = run
        .pointer("/trades_page/total_items")
        .and_then(Value::as_u64)
        .unwrap_or(trades.len() as u64);
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
                                    ("佣金及税费", metric_number(run, "total_commission")),
                                    ("点差成本", metric_number(run, "total_spread")),
                                    ("滑点成本", metric_number(run, "total_slippage")),
                                    ("总执行成本", metric_number(run, "total_execution_cost")),
                                ].into_iter().map(|(label, value)| html! {
                                    <div class="col-6 col-lg-3"><div class="card h-100"><div class="card-body">
                                        <div class="small text-secondary">{label}</div><div class="fw-semibold">{value}</div>
                                    </div></div></div>
                                }).collect::<Html>()}
                            </div>
                            <div class="alert alert-light border">
                                <span class="fw-semibold">{"费用模型快照："}</span>
                                {run.pointer("/parameters/cost_model/name").and_then(Value::as_str).unwrap_or("历史回测未保存费用模型")}
                                {run.pointer("/parameters/cost_model/currency").and_then(Value::as_str).map(|currency| format!(" ({currency})")).unwrap_or_default()}
                            </div>
                            {equity_chart(run)}
                            <h3 class="h6 mt-4">{"成交记录"}</h3>
                            {if trade_total > trades.len() as u64 {
                                html! { <div class="alert alert-info py-2">
                                    {format!("共有 {trade_total} 条成交，当前显示前 {} 条；可通过 backtest.get 的 trade_page 参数读取后续记录。", trades.len())}
                                </div> }
                            } else { Html::default() }}
                            <div class="table-responsive"><table class="table table-sm table-hover">
                                <thead><tr><th>{"信号时间（本地）"}</th><th>{"成交时间（本地）"}</th><th>{"方向"}</th><th>{"数量"}</th><th>{"价格"}</th><th>{"佣金/税费"}</th><th>{"点差成本"}</th><th>{"滑点成本"}</th></tr></thead>
                                <tbody>{if trades.is_empty() {
                                    html! { <tr><td colspan="8" class="text-center text-secondary">{"没有成交"}</td></tr> }
                                } else {
                                    trades.iter().map(|trade| html! { <tr>
                                        <td>{local_time(trade, "signal_time")}</td><td>{local_time(trade, "fill_time")}</td>
                                        <td>{text(trade, "side")}</td><td>{number(trade, "quantity")}</td>
                                        <td>{number(trade, "price")}</td><td>{number(trade, "commission")}</td>
                                        <td>{number(trade, "spread")}</td><td>{number(trade, "slippage")}</td>
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
    let sampling = run.get("equity_sampling");
    let downsampled = sampling.is_some_and(|value| boolean(value, "downsampled"));
    let total_points = sampling
        .and_then(|value| value.get("total_points"))
        .and_then(Value::as_u64)
        .unwrap_or(points.len() as u64);
    html! {
        <div>
            <h3 class="h6">{"权益曲线"}</h3>
            {if downsampled {
                html! { <div class="small text-secondary mb-2">
                    {format!("为控制响应大小，曲线已从 {total_points} 个权益点均匀抽样为 {} 个点；收益指标仍基于完整数据。", points.len())}
                </div> }
            } else { Html::default() }}
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

#[cfg(test)]
mod tests {
    use super::initial_cash_label;

    #[test]
    fn initial_cash_label_uses_the_instrument_currency() {
        assert_eq!(initial_cash_label("usd"), "初始资金（USD）");
        assert_eq!(initial_cash_label(" HKD "), "初始资金（HKD）");
    }

    #[test]
    fn initial_cash_label_does_not_silently_imply_a_currency() {
        assert_eq!(initial_cash_label(""), "初始资金（证券币种未知）");
    }
}
