use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    strategy_chart::StrategyChart,
    value::{
        array, boolean, integer, local_time, number, official_security_name, security_exchange,
        text,
    },
};

#[derive(Properties, PartialEq)]
pub struct StrategyStatusPageProps {
    pub endpoint: String,
    pub strategies: Value,
}

#[function_component(StrategyStatusPage)]
pub fn strategy_status_page(props: &StrategyStatusPageProps) -> Html {
    let strategies = array(&props.strategies, "strategies");
    let selected_id = use_state(|| {
        strategies
            .first()
            .map(|strategy| text(strategy, "strategy_id"))
            .unwrap_or_default()
    });
    let evaluations = use_state(Vec::<Value>::new);
    let bars = use_state(Vec::<Value>::new);
    let cost_controls = use_state(Vec::<Value>::new);
    let cost_models = use_state(Vec::<Value>::new);
    let error = use_state(|| None::<String>);
    let busy = use_state(|| false);

    {
        let selected_id = selected_id.clone();
        let strategy_ids = strategies
            .iter()
            .map(|strategy| text(strategy, "strategy_id"))
            .collect::<Vec<_>>();
        use_effect_with(strategy_ids, move |strategy_ids| {
            if !strategy_ids.iter().any(|id| id == &*selected_id) {
                selected_id.set(strategy_ids.first().cloned().unwrap_or_default());
            }
            || ()
        });
    }

    {
        let endpoint = props.endpoint.clone();
        let selected_id_value = (*selected_id).clone();
        let strategies = strategies.clone();
        let evaluations = evaluations.clone();
        let bars = bars.clone();
        let cost_controls = cost_controls.clone();
        let cost_models = cost_models.clone();
        let error = error.clone();
        let busy = busy.clone();
        let current_selection = selected_id.clone();
        use_effect_with((endpoint.clone(), selected_id_value.clone()), move |_| {
            refresh_status(
                endpoint.clone(),
                selected_id_value.clone(),
                strategies.clone(),
                evaluations.clone(),
                bars.clone(),
                cost_controls.clone(),
                cost_models.clone(),
                error.clone(),
                busy.clone(),
                current_selection.clone(),
            );
            let interval = Interval::new(5_000, move || {
                refresh_status(
                    endpoint.clone(),
                    selected_id_value.clone(),
                    strategies.clone(),
                    evaluations.clone(),
                    bars.clone(),
                    cost_controls.clone(),
                    cost_models.clone(),
                    error.clone(),
                    busy.clone(),
                    current_selection.clone(),
                );
            });
            move || drop(interval)
        });
    }

    let strategy = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == *selected_id);
    let required_bars = strategy.map(strategy_required_bars).unwrap_or(0);
    let available_bars = bars.len() as u64;
    let bar_timeframe = strategy.map(strategy_timeframe).unwrap_or("1m");
    let bar_timeframe_label = if bar_timeframe == "5s" {
        "5 秒"
    } else {
        "1 分钟"
    };
    let latest = evaluations.first();
    let cost_control = cost_controls
        .iter()
        .find(|control| text(control, "strategy_id") == *selected_id);
    let cost_model = cost_control.and_then(|control| {
        let cost_model_id = text(control, "cost_model_id");
        cost_models
            .iter()
            .find(|model| text(model, "cost_model_id") == cost_model_id)
    });
    let progress = if required_bars == 0 {
        0
    } else {
        (available_bars.saturating_mul(100) / required_bars).min(100)
    };

    html! {
        <>
            <ErrorModal message={(*error).clone()} on_close={{
                let error = error.clone();
                Callback::from(move |_| error.set(None))
            }} />
            <section class="card shadow-sm mb-4"><div class="card-body">
                <div class="d-flex flex-wrap justify-content-between align-items-end gap-3">
                    <div class="flex-grow-1">
                        <label class="form-label" for="status-strategy">{"选择策略"}</label>
                        <select id="status-strategy" class="form-select" value={(*selected_id).clone()} onchange={{
                            let selected_id = selected_id.clone();
                            let evaluations = evaluations.clone();
                            let bars = bars.clone();
                            Callback::from(move |event: Event| {
                                let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                evaluations.set(Vec::new());
                                bars.set(Vec::new());
                                selected_id.set(input.value());
                            })
                        }}>
                            {strategies.iter().map(|strategy| html! {
                                <option value={text(strategy, "strategy_id")}>
                                    {format!("{} · {}", text(strategy, "name"), text(strategy, "strategy_id"))}
                                </option>
                            }).collect::<Html>()}
                        </select>
                    </div>
                    <span class="text-secondary">{if *busy { "刷新中…" } else { "每 5 秒自动刷新" }}</span>
                </div>
            </div></section>

            {strategy.map(|strategy| html! {
                <>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"运行状态"}</h2>
                        <div class="row g-3">
                            <Status label="完整策略 UUID" value={text(strategy, "strategy_id")} />
                            <Status label="名称" value={text(strategy, "name")} />
                            <Status label="类型" value={text(strategy, "kind")} />
                            <Status label="Bar 周期" value={bar_timeframe_label.to_owned()} />
                            <Status label="状态" value={text(strategy, "state")} />
                            <Status label="证券（官方名称）" value={official_security_name(strategy)} />
                            <Status label="所属交易所" value={security_exchange(strategy)} />
                            <Status label="Conid" value={integer(strategy, "conid")} />
                            <Status label="最后处理 Bar（本地）" value={local_time(strategy, "last_evaluated_bar")} />
                            <Status label="最近错误" value={text(strategy, "last_error")} />
                            {
                                (text(strategy, "kind") == "moving_average_cross_v2").then(|| html! {
                                    <>
                                        <Status label="均线算法" value={text_at(strategy, "/config/average_type")} />
                                        <Status label="最小均线差" value={format!("{}%", number_at(strategy, "/config/min_gap_percent"))} />
                                        <Status label="连续确认 Bar" value={integer_at(strategy, "/config/confirmation_bars")} />
                                        <Status label="信号冷却 Bar" value={integer_at(strategy, "/config/cooldown_bars")} />
                                        <Status label="ATR 窗口" value={integer_at(strategy, "/config/atr_window")} />
                                        <Status label="最小 ATR" value={format!("{}%", number_at(strategy, "/config/min_atr_percent"))} />
                                        <Status label="趋势窗口" value={integer_at(strategy, "/config/trend_window")} />
                                    </>
                                }).unwrap_or_default()
                            }
                        </div>
                    </div></section>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"成本控制"}</h2>
                        {cost_control.map(|control| html! {
                            <>
                                <div class="row g-3">
                                    <Status label="成本门控" value={if boolean(control, "enabled") { "已启用" } else { "已停用" }.to_owned()} />
                                    <Status label="费用模型" value={text(control, "cost_model_name")} />
                                    <Status label="模型币种" value={cost_model.map(|model| text(model, "currency")).unwrap_or_else(|| "—".into())} />
                                    <Status label="成本安全倍数" value={number(control, "minimum_cost_multiple")} />
                                    <Status label="佣金/毛利润上限" value={format_ratio(control, "maximum_commission_to_gross_profit_ratio")} />
                                    <Status label="熔断最少交易数" value={integer(control, "minimum_completed_trades")} />
                                    <Status label="买入费用（固定/股/比例/最低）" value={cost_model.map(|model| fee_summary(model, "buy")).unwrap_or_else(|| "—".into())} />
                                    <Status label="卖出费用（固定/股/比例/最低）" value={cost_model.map(|model| fee_summary(model, "sell")).unwrap_or_else(|| "—".into())} />
                                    <Status label="卖出税费" value={cost_model.map(|model| format!("{} bps", number(model, "sell_tax_bps"))).unwrap_or_else(|| "—".into())} />
                                    <Status label="预计点差" value={cost_model.map(|model| format!("{} bps", number(model, "estimated_spread_bps"))).unwrap_or_else(|| "—".into())} />
                                    <Status label="单边预计滑点" value={cost_model.map(|model| format!("{} bps", number(model, "estimated_slippage_bps"))).unwrap_or_else(|| "—".into())} />
                                </div>
                                {(!boolean(control, "enabled")).then(|| html! {
                                    <div class="alert alert-warning mt-3 mb-0">{"该策略已关联费用模型，但成本门控当前未启用。"}</div>
                                }).unwrap_or_default()}
                            </>
                        }).unwrap_or_else(|| html! {
                            <div class="alert alert-secondary mb-0">{"尚未给该策略配置成本控制；当前不会执行成本门控。"}</div>
                        })}
                    </div></section>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"Bar 准备进度"}</h2>
                        <div class="mb-2">{format!("已有 {} / 需要 {} 根已完成的 {} Bar", available_bars, required_bars, bar_timeframe_label)}</div>
                        <div class="progress" role="progressbar" aria-valuenow={progress.to_string()} aria-valuemin="0" aria-valuemax="100">
                            <div class="progress-bar" style={format!("width: {progress}%")}>{format!("{progress}%")}</div>
                        </div>
                    </div></section>
                </>
            }).unwrap_or_default()}

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"价格与均线图"}</h2>
                <p class="text-secondary">{"K 线、短均线和长均线共享时间轴；可缩放、平移并悬停查看数据。"}</p>
                <StrategyChart
                    bars={(*bars).clone()}
                    evaluations={(*evaluations).clone()}
                    symbol={strategy.map(|value| text(value, "symbol")).unwrap_or_else(|| "—".into())}
                    view_key={strategy.map(|value| text(value, "strategy_id")).unwrap_or_default()}
                />
            </div></section>

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"最新均线计算"}</h2>
                {latest.map(|row| html! {
                    <div class="row g-3">
                        <Status label="Bar 时间（本地）" value={local_time(row, "bar_time")} />
                        <Status label="当前信号" value={text(row, "signal")} />
                        <Status label="当前短均线" value={number(row, "short_value")} />
                        <Status label="当前长均线" value={number(row, "long_value")} />
                        <Status label="上一根短均线" value={number(row, "previous_short_value")} />
                        <Status label="上一根长均线" value={number(row, "previous_long_value")} />
                        <Status label="收盘价" value={row.pointer("/output/bar/close").and_then(Value::as_f64).map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())} />
                        <Status label="均线差" value={row.pointer("/output/gap_percent").and_then(Value::as_f64).map(|v| format!("{v:.4}%")).unwrap_or_else(|| "—".into())} />
                        <Status label="ATR" value={row.pointer("/output/atr").and_then(Value::as_f64).map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())} />
                        <Status label="ATR 占价格" value={row.pointer("/output/atr_percent").and_then(Value::as_f64).map(|v| format!("{v:.4}%")).unwrap_or_else(|| "—".into())} />
                        <Status label="合格方向" value={text_at(row, "/output/qualified_direction")} />
                        <Status label="信号原因" value={text_at(row, "/output/signal_reason")} />
                        <Status label="计算写入时间（本地）" value={local_time(row, "created_at")} />
                    </div>
                }).unwrap_or_else(|| html! {
                    <div class="text-secondary">{"尚无计算结果。请确认策略已启动、行情订阅有效，并等待 Bar 数量达到要求。"}</div>
                })}
            </div></section>

            <section>
                <h2 class="h5">{"最近计算历史"}</h2>
                <div class="card shadow-sm table-responsive"><table class="table table-hover align-middle mb-0">
                    <thead><tr><th>{"Bar 时间（本地）"}</th><th>{"短均线"}</th><th>{"长均线"}</th><th>{"信号"}</th><th>{"收盘价"}</th><th>{"计算时间（本地）"}</th></tr></thead>
                    <tbody>
                        {if evaluations.is_empty() {
                            html! { <tr><td colspan="6" class="text-center text-secondary py-4">{"暂无计算记录"}</td></tr> }
                        } else {
                            evaluations.iter().map(|row| html! {
                                <tr>
                                    <td>{local_time(row, "bar_time")}</td><td>{number(row, "short_value")}</td>
                                    <td>{number(row, "long_value")}</td><td>{text(row, "signal")}</td>
                                    <td>{row.pointer("/output/bar/close").and_then(Value::as_f64).map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())}</td>
                                    <td>{local_time(row, "created_at")}</td>
                                </tr>
                            }).collect::<Html>()
                        }}
                    </tbody>
                </table></div>
            </section>
        </>
    }
}

fn refresh_status(
    endpoint: String,
    strategy_id: String,
    strategies: Vec<Value>,
    evaluations: UseStateHandle<Vec<Value>>,
    bars: UseStateHandle<Vec<Value>>,
    cost_controls: UseStateHandle<Vec<Value>>,
    cost_models: UseStateHandle<Vec<Value>>,
    error: UseStateHandle<Option<String>>,
    busy: UseStateHandle<bool>,
    current_selection: UseStateHandle<String>,
) {
    if strategy_id.is_empty() {
        return;
    }
    let conid = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
        .and_then(|strategy| strategy.get("conid"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let timeframe = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
        .map(strategy_timeframe)
        .unwrap_or("1m");
    let required = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
        .map(strategy_required_bars)
        .unwrap_or(100)
        .max(200);
    busy.set(true);
    spawn_local(async move {
        let requested_strategy_id = strategy_id.clone();
        match call_method(
            &endpoint,
            "strategy.signals",
            json!({"strategy_id": strategy_id, "limit": 100}),
        )
        .await
        {
            Ok(value) if *current_selection == requested_strategy_id => {
                evaluations.set(array(&value, "evaluations"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        match call_method(&endpoint, "execution_cost.control.list", json!({})).await {
            Ok(value) if *current_selection == requested_strategy_id => {
                cost_controls.set(array(&value, "controls"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        match call_method(&endpoint, "execution_cost.model.list", json!({})).await {
            Ok(value) if *current_selection == requested_strategy_id => {
                cost_models.set(array(&value, "models"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        if conid > 0 {
            match call_method(
                &endpoint,
                "market_data.bars",
                json!({"conid": conid, "timeframe": timeframe, "limit": required.max(1)}),
            )
            .await
            {
                Ok(value) if *current_selection == requested_strategy_id => bars.set(
                    array(&value, "bars")
                        .into_iter()
                        .filter(|bar| bar.get("final").and_then(Value::as_bool).unwrap_or(false))
                        .collect(),
                ),
                Ok(_) => return,
                Err(message) => error.set(Some(message)),
            }
        }
        busy.set(false);
    });
}

fn fee_summary(model: &Value, side: &str) -> String {
    format!(
        "{} / {} / {} bps / {} {}",
        number(model, &format!("{side}_fixed_fee")),
        number(model, &format!("{side}_per_share_fee")),
        number(model, &format!("{side}_rate_bps")),
        number(model, &format!("{side}_min_fee")),
        text(model, "currency")
    )
}

fn format_ratio(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|ratio| format!("{} ({:.2}%)", number(value, key), ratio * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn strategy_timeframe(strategy: &Value) -> &'static str {
    if text(strategy, "kind") == "moving_average_cross_5s"
        || (text(strategy, "kind") == "moving_average_cross_v2"
            && strategy
                .pointer("/config/bar_timeframe")
                .and_then(Value::as_str)
                == Some("5s"))
    {
        "5s"
    } else {
        "1m"
    }
}

fn strategy_required_bars(strategy: &Value) -> u64 {
    let config = strategy.get("config").unwrap_or(&Value::Null);
    let long = config
        .get("long_window")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if text(strategy, "kind") != "moving_average_cross_v2" {
        return long + 1;
    }
    let atr = config
        .get("atr_window")
        .and_then(Value::as_u64)
        .unwrap_or(14)
        + 1;
    let trend = config
        .get("trend_window")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let confirmation = config
        .get("confirmation_bars")
        .and_then(Value::as_u64)
        .unwrap_or(2);
    let cooldown = config
        .get("cooldown_bars")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    long.max(atr).max(trend) + confirmation + cooldown
}

fn text_at(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or("—")
        .to_owned()
}

fn integer_at(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "—".into())
}

fn number_at(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_f64)
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "—".into())
}

#[derive(Properties, PartialEq)]
struct StatusProps {
    label: &'static str,
    value: String,
}

#[function_component(Status)]
fn status(props: &StatusProps) -> Html {
    html! {
        <div class="col-12 col-md-6 col-xl-3">
            <div class="small text-secondary">{props.label}</div>
            <div class="text-break strategy-id">{props.value.clone()}</div>
        </div>
    }
}
