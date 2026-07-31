use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    instrument_search::InstrumentSearch,
    value::{integer, local_time, number, official_security_name, security_exchange, text},
};

fn market_data_state(health: &Value) -> (&'static str, &'static str) {
    match health.get("state").and_then(Value::as_str) {
        Some("fresh") => ("实时且新鲜", "text-success"),
        Some("delayed") => ("延迟行情（不可自动交易）", "text-danger"),
        Some("stale") => ("行情已过期", "text-warning"),
        Some("missing") => ("尚无行情", "text-secondary"),
        _ => ("未知", "text-secondary"),
    }
}

#[derive(Properties, PartialEq)]
pub struct MovingAverageWizardPageProps {
    pub endpoint: String,
    pub system: Value,
    pub on_completed: Callback<()>,
}

#[function_component(MovingAverageWizardPage)]
pub fn moving_average_wizard_page(props: &MovingAverageWizardPageProps) -> Html {
    let selected = use_state(|| None::<Value>);
    let health = use_state(|| None::<Value>);
    let name = use_state(|| "moving-average-strategy".to_owned());
    let strategy_version = use_state(|| "v2".to_owned());
    let bar_timeframe = use_state(|| "1m".to_owned());
    let bar_timeframe_ref = use_node_ref();
    let short_window = use_state(|| "5".to_owned());
    let long_window = use_state(|| "20".to_owned());
    let average_type = use_state(|| "ema".to_owned());
    let min_gap_percent = use_state(|| "0.05".to_owned());
    let confirmation_bars = use_state(|| "2".to_owned());
    let cooldown_bars = use_state(|| "3".to_owned());
    let atr_window = use_state(|| "14".to_owned());
    let min_atr_percent = use_state(|| "0".to_owned());
    let trend_window = use_state(|| "0".to_owned());
    let strategy_id = use_state(String::new);
    let account = use_state(|| {
        props
            .system
            .pointer("/ibkr/managed_accounts/0")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    });
    let long_target = use_state(|| "1".to_owned());
    let short_target = use_state(|| "0".to_owned());
    let allow_short = use_state(|| false);
    let outside_rth = use_state(|| false);
    let execution_configured = use_state(|| false);
    let execution_confirmed = use_state(|| false);
    let busy_action = use_state(String::new);
    let notice = use_state(|| None::<Result<String, String>>);
    {
        let managed_account = props
            .system
            .pointer("/ibkr/managed_accounts/0")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let account = account.clone();
        use_effect_with(managed_account.clone(), move |_| {
            if account.trim().is_empty() && !managed_account.is_empty() {
                account.set(managed_account);
            }
            || ()
        });
    }

    let subscribe = wizard_rpc(
        props.endpoint.clone(),
        "market_data.subscribe",
        (*selected).clone().unwrap_or_else(|| json!({})),
        "subscribe",
        "行情订阅请求已接受；收到 Tick 后请检查行情状态",
        busy_action.clone(),
        notice.clone(),
        props.on_completed.clone(),
    );
    let check_health = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let health = health.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            let Some(conid) = selected
                .as_ref()
                .and_then(|item| item.get("conid"))
                .and_then(Value::as_i64)
            else {
                notice.set(Some(Err("请先选择股票".into())));
                return;
            };
            let endpoint = endpoint.clone();
            let health = health.clone();
            let busy_action = busy_action.clone();
            let notice = notice.clone();
            busy_action.set("health".into());
            spawn_local(async move {
                match call_method(&endpoint, "market_data.health", json!({"conid": conid})).await {
                    Ok(response) => {
                        health.set(response.get("health").cloned());
                        notice.set(Some(Ok("行情健康状态已刷新".into())));
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy_action.set(String::new());
            });
        })
    };

    let create_strategy = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let name = name.clone();
        let strategy_version = strategy_version.clone();
        let bar_timeframe = bar_timeframe.clone();
        let bar_timeframe_ref = bar_timeframe_ref.clone();
        let short_window = short_window.clone();
        let long_window = long_window.clone();
        let average_type = average_type.clone();
        let min_gap_percent = min_gap_percent.clone();
        let confirmation_bars = confirmation_bars.clone();
        let cooldown_bars = cooldown_bars.clone();
        let atr_window = atr_window.clone();
        let min_atr_percent = min_atr_percent.clone();
        let trend_window = trend_window.clone();
        let strategy_id = strategy_id.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        let on_completed = props.on_completed.clone();
        Callback::from(move |_| {
            let Some(contract) = (*selected).clone() else {
                notice.set(Some(Err("请先选择股票".into())));
                return;
            };
            let Some(short) = short_window
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
            else {
                notice.set(Some(Err("短期窗口必须大于 0".into())));
                return;
            };
            let Some(long) = long_window
                .parse::<usize>()
                .ok()
                .filter(|value| *value > short && *value <= 10_000)
            else {
                notice.set(Some(Err("长期窗口必须大于短期窗口且不超过 10000".into())));
                return;
            };
            if name.trim().is_empty() {
                notice.set(Some(Err("策略名称不能为空".into())));
                return;
            }
            let parse_percentage = |value: &str, label: &str| {
                value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && (0.0..=100.0).contains(value))
                    .ok_or_else(|| format!("{label}必须是 0 到 100 之间的数字"))
            };
            let parse_count = |value: &str, label: &str, min: usize, max: usize| {
                value
                    .parse::<usize>()
                    .ok()
                    .filter(|value| (min..=max).contains(value))
                    .ok_or_else(|| format!("{label}必须在 {min} 到 {max} 之间"))
            };
            let conid = contract.get("conid").and_then(Value::as_i64).unwrap_or(0);
            // Read the select element at submission time. This avoids creating
            // the previous render's strategy kind when selection and creation
            // happen before Yew has completed another render.
            let selected_timeframe = bar_timeframe_ref
                .cast::<web_sys::HtmlSelectElement>()
                .map(|select| select.value())
                .unwrap_or_else(|| (*bar_timeframe).clone());
            let kind = if *strategy_version == "v2" {
                "moving_average_cross_v2"
            } else if selected_timeframe == "5s" {
                "moving_average_cross_5s"
            } else {
                "moving_average_cross"
            };
            let mut config = json!({
                "conid": conid,
                "short_window": short,
                "long_window": long
            });
            if *strategy_version == "v2" {
                let advanced = (
                    parse_percentage(&min_gap_percent, "最小均线差"),
                    parse_count(&confirmation_bars, "确认 Bar 数", 1, 1_000),
                    parse_count(&cooldown_bars, "冷却 Bar 数", 0, 10_000),
                    parse_count(&atr_window, "ATR 窗口", 1, 10_000),
                    parse_percentage(&min_atr_percent, "最小 ATR"),
                    parse_count(&trend_window, "趋势窗口", 0, 10_000),
                );
                let (Ok(gap), Ok(confirm), Ok(cooldown), Ok(atr), Ok(min_atr), Ok(trend)) =
                    advanced
                else {
                    let error = [
                        advanced.0.err(),
                        advanced.1.err(),
                        advanced.2.err(),
                        advanced.3.err(),
                        advanced.4.err(),
                        advanced.5.err(),
                    ]
                    .into_iter()
                    .flatten()
                    .next()
                    .unwrap_or_else(|| "V2 参数无效".into());
                    notice.set(Some(Err(error)));
                    return;
                };
                config = json!({
                    "conid": conid,
                    "short_window": short,
                    "long_window": long,
                    "bar_timeframe": selected_timeframe,
                    "average_type": &*average_type,
                    "min_gap_percent": gap,
                    "confirmation_bars": confirm,
                    "cooldown_bars": cooldown,
                    "atr_window": atr,
                    "min_atr_percent": min_atr,
                    "trend_window": trend
                });
            }
            let params = json!({
                "name": name.trim(),
                "kind": kind,
                "config": config
            });
            let endpoint = endpoint.clone();
            let strategy_id = strategy_id.clone();
            let busy_action = busy_action.clone();
            let notice = notice.clone();
            let on_completed = on_completed.clone();
            busy_action.set("create".into());
            spawn_local(async move {
                match call_method(&endpoint, "strategy.create", params).await {
                    Ok(response) => {
                        let id = text(&response, "strategy_id");
                        let created_kind = text(&response, "kind");
                        strategy_id.set(id.clone());
                        notice.set(Some(Ok(format!(
                            "均线策略已创建：{id}（类型：{created_kind}）"
                        ))));
                        on_completed.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy_action.set(String::new());
            });
        })
    };

    let configure_execution = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let strategy_id = strategy_id.clone();
        let account = account.clone();
        let long_target = long_target.clone();
        let short_target = short_target.clone();
        let allow_short = allow_short.clone();
        let outside_rth = outside_rth.clone();
        let execution_configured = execution_configured.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        let on_completed = props.on_completed.clone();
        Callback::from(move |_| {
            let Some(contract) = (*selected).clone() else {
                notice.set(Some(Err("请先选择股票".into())));
                return;
            };
            let Some(long_quantity) = long_target
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite() && *value > 0.0)
            else {
                notice.set(Some(Err("多头目标必须大于 0".into())));
                return;
            };
            let Some(short_quantity) = short_target
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite() && *value <= 0.0)
            else {
                notice.set(Some(Err("空头目标必须小于或等于 0".into())));
                return;
            };
            if *allow_short && short_quantity >= 0.0 {
                notice.set(Some(Err("允许做空时，空头目标必须是负数".into())));
                return;
            }
            if !*allow_short && short_quantity < 0.0 {
                notice.set(Some(Err("负数空头目标需要开启允许做空".into())));
                return;
            }
            if strategy_id.is_empty() || account.trim().is_empty() {
                notice.set(Some(Err("策略 ID 和 Paper 账户不能为空".into())));
                return;
            }
            let params = json!({
                "strategy_id": *strategy_id,
                "account": account.trim(),
                "target_quantity": long_quantity,
                "short_target_quantity": short_quantity,
                "allow_short": *allow_short,
                "outside_rth": *outside_rth,
                "order_type": if *outside_rth { "limit" } else { "market" },
                "paper_only": true,
                "contract": contract
            });
            let endpoint = endpoint.clone();
            let execution_configured = execution_configured.clone();
            let busy_action = busy_action.clone();
            let notice = notice.clone();
            let on_completed = on_completed.clone();
            busy_action.set("configure".into());
            spawn_local(async move {
                match call_method(&endpoint, "strategy.execution.configure", params).await {
                    Ok(_) => {
                        execution_configured.set(true);
                        notice.set(Some(Ok("Paper 执行配置已保存，但尚未启用".into())));
                        on_completed.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy_action.set(String::new());
            });
        })
    };

    let start = wizard_rpc(
        props.endpoint.clone(),
        "strategy.start",
        json!({"strategy_id": *strategy_id}),
        "start",
        "策略信号计算已启动",
        busy_action.clone(),
        notice.clone(),
        props.on_completed.clone(),
    );
    let enable = wizard_rpc(
        props.endpoint.clone(),
        "strategy.execution.enable",
        json!({"strategy_id": *strategy_id, "confirm": true}),
        "enable",
        "Paper 自动执行已启用",
        busy_action.clone(),
        notice.clone(),
        props.on_completed.clone(),
    );
    let busy = !busy_action.is_empty();
    let market_data_is_fresh = health
        .as_ref()
        .and_then(|value| value.get("state"))
        .and_then(Value::as_str)
        == Some("fresh");
    let error_message = notice
        .as_ref()
        .and_then(|result| result.as_ref().err())
        .cloned();
    let close_error = {
        let notice = notice.clone();
        Callback::from(move |_| notice.set(None))
    };

    html! {
        <>
            <ErrorModal message={error_message} on_close={close_error} />
            <Alert style={Color::Info}>
                {"可选择现有的 1 分钟均线或新的 5 秒均线。两者都只使用已完成的 Bar：短均线上穿长均线产生买入信号，下穿产生卖出信号；至少需要“长期窗口 + 1”根完整 Bar。"}
            </Alert>
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"1. 搜索并选择股票"}</h2>
                <InstrumentSearch endpoint={props.endpoint.clone()} stock_only={true} on_select={{
                    let selected = selected.clone();
                    let health = health.clone();
                    Callback::from(move |instrument| {
                        selected.set(Some(instrument));
                        health.set(None);
                    })
                }} />
                {selected.as_ref().map(|instrument| html! {
                    <div class="alert alert-success mt-3 mb-0">
                        {format!("已选择：{} ({}) · {} · Conid {}",
                            official_security_name(instrument), text(instrument, "symbol"),
                            security_exchange(instrument), integer(instrument, "conid"))}
                    </div>
                }).unwrap_or_default()}
            </div></section>

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"2. 订阅并检查行情"}</h2>
                <p class="text-secondary">
                    {format!(
                        "均线策略需要持续接收成交 Tick，daemon 才能聚合出已完成的 {} Bar。订阅后等待收到 Tick，再检查行情状态。",
                        if *bar_timeframe == "5s" { "5 秒" } else { "1 分钟" }
                    )}
                </p>
                <div class="d-flex flex-wrap gap-2 mb-3">
                    <button class="btn btn-outline-primary" disabled={selected.is_none() || busy} onclick={subscribe}>
                        {button_content(&busy_action, "subscribe", "订阅行情")}
                    </button>
                    <button class="btn btn-outline-secondary" disabled={selected.is_none() || busy} onclick={check_health}>
                        {button_content(&busy_action, "health", "检查行情")}
                    </button>
                </div>
                {
                    health.as_ref().map(|health| {
                        let (state_label, state_class) = market_data_state(health);
                        html! {
                            <>
                                {
                                    (health.get("state").and_then(Value::as_str) == Some("delayed")).then(|| html! {
                                        <Alert style={Color::Danger}>
                                            {"IBKR 返回的是延迟行情：可以观察信号，但系统不会允许它驱动自动下单。"}
                                        </Alert>
                                    }).unwrap_or_default()
                                }
                                <div class="row g-3">
                                    <div class="col-md-3"><strong>{"行情状态："}</strong><span class={state_class}>{state_label}</span></div>
                                    <div class="col-md-2"><strong>{"Tick 类型："}</strong>{text(health, "latest_price_type")}</div>
                                    <div class="col-md-2"><strong>{"价格："}</strong>{number(health, "latest_price")}</div>
                                    <div class="col-md-2"><strong>{"接收距今秒数："}</strong>{integer(health, "age_seconds")}</div>
                                    <div class="col-md-3"><strong>{"程序接收时间："}</strong>{local_time(health, "observed_at")}</div>
                                </div>
                            </>
                        }
                    }).unwrap_or_default()
                }
            </div></section>

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"3. 设置参数并创建策略"}</h2>
                <div class="row g-3 align-items-end">
                    <div class="col-12 col-lg-3">
                        <label class="form-label">{"策略名称"}</label>
                        <input class="form-control" value={(*name).clone()} oninput={state_input(name.clone())} />
                    </div>
                    <div class="col-12 col-lg-3">
                        <label class="form-label" for="ma-strategy-version">{"策略版本"}</label>
                        <select id="ma-strategy-version" class="form-select"
                            value={(*strategy_version).clone()}
                            disabled={!strategy_id.is_empty()}
                            onchange={{
                                let strategy_version = strategy_version.clone();
                                Callback::from(move |event: Event| {
                                    let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                    strategy_version.set(input.value());
                                })
                            }}>
                            <option value="v2">{"V2（推荐，过滤噪声）"}</option>
                            <option value="v1">{"V1（简单交叉）"}</option>
                        </select>
                    </div>
                    <div class="col-12 col-lg-2">
                        <label class="form-label" for="ma-bar-timeframe">{"Bar 周期"}</label>
                        <select id="ma-bar-timeframe" class="form-select" ref={bar_timeframe_ref}
                            value={(*bar_timeframe).clone()}
                            disabled={!strategy_id.is_empty()}
                            onchange={{
                                let bar_timeframe = bar_timeframe.clone();
                                Callback::from(move |event: Event| {
                                    let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                    bar_timeframe.set(input.value());
                                })
                            }}>
                            <option value="1m">{"1 分钟（现有策略）"}</option>
                            <option value="5s">{"5 秒（秒级策略）"}</option>
                        </select>
                    </div>
                    <div class="col-6 col-lg-2">
                        <label class="form-label">{"短期窗口"}</label>
                        <input class="form-control" type="number" min="1" value={(*short_window).clone()} oninput={state_input(short_window.clone())} />
                    </div>
                    <div class="col-6 col-lg-2">
                        <label class="form-label">{"长期窗口"}</label>
                        <input class="form-control" type="number" min="2" value={(*long_window).clone()} oninput={state_input(long_window.clone())} />
                    </div>
                    <div class="col-12 col-lg-2">
                        <button class="btn btn-primary w-100" disabled={selected.is_none() || busy || !strategy_id.is_empty()} onclick={create_strategy}>
                            {button_content(&busy_action, "create", "创建策略")}
                        </button>
                    </div>
                    {
                        (*strategy_version == "v2").then(|| html! {
                            <>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"均线类型"}</label>
                                    <select class="form-select" value={(*average_type).clone()}
                                        onchange={{
                                            let average_type = average_type.clone();
                                            Callback::from(move |event: Event| {
                                                let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                                average_type.set(input.value());
                                            })
                                        }}>
                                        <option value="ema">{"EMA（推荐）"}</option>
                                        <option value="sma">{"SMA"}</option>
                                    </select>
                                </div>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"最小均线差（%）"}</label>
                                    <input class="form-control" type="number" min="0" max="100" step="0.01"
                                        value={(*min_gap_percent).clone()} oninput={state_input(min_gap_percent.clone())} />
                                </div>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"确认 Bar 数"}</label>
                                    <input class="form-control" type="number" min="1" max="1000"
                                        value={(*confirmation_bars).clone()} oninput={state_input(confirmation_bars.clone())} />
                                </div>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"冷却 Bar 数"}</label>
                                    <input class="form-control" type="number" min="0" max="10000"
                                        value={(*cooldown_bars).clone()} oninput={state_input(cooldown_bars.clone())} />
                                </div>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"ATR 窗口"}</label>
                                    <input class="form-control" type="number" min="1" max="10000"
                                        value={(*atr_window).clone()} oninput={state_input(atr_window.clone())} />
                                </div>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"最小 ATR（%）"}</label>
                                    <input class="form-control" type="number" min="0" max="100" step="0.01"
                                        value={(*min_atr_percent).clone()} oninput={state_input(min_atr_percent.clone())} />
                                </div>
                                <div class="col-6 col-lg-2">
                                    <label class="form-label">{"趋势窗口（0=关闭）"}</label>
                                    <input class="form-control" type="number" min="0" max="10000"
                                        value={(*trend_window).clone()} oninput={state_input(trend_window.clone())} />
                                </div>
                                <div class="col-12">
                                    <div class="form-text">
                                        {"V2 只有在均线差、ATR、趋势和连续确认条件全部满足时才产生信号；冷却期可阻止短时间内反向交易。"}
                                    </div>
                                </div>
                            </>
                        }).unwrap_or_default()
                    }
                    <div class="col-12">
                        <div class="form-text">
                            {format!(
                                "例如短期 5、长期 20，需要至少 21 根已完成的 {} Bar 才会开始计算；对应约 {} 的长均线。",
                                if *bar_timeframe == "5s" { "5 秒" } else { "1 分钟" },
                                if *bar_timeframe == "5s" { "100 秒" } else { "20 分钟" }
                            )}
                        </div>
                    </div>
                    <div class="col-12">
                        <label class="form-label">{"完整策略 UUID"}</label>
                        <input class="form-control" value={(*strategy_id).clone()} placeholder="创建后自动填写" readonly=true />
                    </div>
                </div>
            </div></section>

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"4. 可选：配置 Paper 自动执行"}</h2>
                <p class="text-secondary">{"只想观察信号时可以跳过本步骤。多头和空头字段表示目标持仓，而不是每次下单数量。"}</p>
                <div class="row g-3 align-items-end">
                    <div class="col-12 col-lg-4"><label class="form-label">{"Paper 账户"}</label>
                        <input class="form-control" value={(*account).clone()} oninput={state_input(account.clone())} /></div>
                    <div class="col-6 col-lg-2"><label class="form-label">{"多头目标"}</label>
                        <input class="form-control" type="number" min="0.0001" step="any" value={(*long_target).clone()} oninput={state_input(long_target.clone())} /></div>
                    <div class="col-6 col-lg-2"><label class="form-label">{"空头目标"}</label>
                        <input class="form-control" type="number" max="0" step="any" value={(*short_target).clone()} oninput={state_input(short_target.clone())} /></div>
                    <div class="col-6 col-lg-2"><div class="form-check mb-2">
                        <input id="ma-allow-short" class="form-check-input" type="checkbox" checked={*allow_short} onchange={{
                            let allow_short = allow_short.clone();
                            let short_target = short_target.clone();
                            Callback::from(move |event: Event| {
                                let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                allow_short.set(input.checked());
                                if !input.checked() { short_target.set("0".into()); }
                            })
                        }} />
                        <label class="form-check-label" for="ma-allow-short">{"允许做空"}</label>
                    </div></div>
                    <div class="col-6 col-lg-2"><div class="form-check mb-2">
                        <input id="ma-outside-rth" class="form-check-input" type="checkbox"
                            checked={*outside_rth} onchange={{
                                let outside_rth = outside_rth.clone();
                                Callback::from(move |event: Event| {
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                    outside_rth.set(input.checked());
                                })
                            }} />
                        <label class="form-check-label" for="ma-outside-rth">{"允许盘前盘后"}</label>
                    </div></div>
                    <div class="col-12 col-lg-2">
                        <button class="btn btn-outline-primary w-100" disabled={strategy_id.is_empty() || busy} onclick={configure_execution}>
                            {button_content(&busy_action, "configure", "保存执行配置")}
                        </button>
                    </div>
                </div>
                {(*outside_rth).then(|| html! {
                    <div class="alert alert-warning mt-3 mb-0">
                        {"盘前盘后自动执行使用最新 Ask（买入）或 Bid（卖出）作为限价；流动性较低时可能不会立即成交。"}
                    </div>
                }).unwrap_or_default()}
            </div></section>

            <section class="card shadow-sm"><div class="card-body">
                <h2 class="h5">{"5. 启动"}</h2>
                <div class="d-flex flex-wrap gap-2 mb-3">
                    <button class="btn btn-success" disabled={strategy_id.is_empty() || busy} onclick={start}>
                        {button_content(&busy_action, "start", "启动信号")}
                    </button>
                </div>
                <div class="form-check mb-3">
                    <input id="ma-execution-confirm" class="form-check-input" type="checkbox"
                        checked={*execution_confirmed} onchange={{
                            let confirmed = execution_confirmed.clone();
                            Callback::from(move |event: Event| {
                                let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                confirmed.set(input.checked());
                            })
                        }} />
                    <label class="form-check-label text-danger fw-semibold" for="ma-execution-confirm">
                        {"我确认这是 Paper 账户，并理解策略可能自动提交订单"}
                    </label>
                </div>
                <button class="btn btn-danger" disabled={!*execution_confirmed || !*execution_configured || !market_data_is_fresh || busy} onclick={enable}>
                    {button_content(&busy_action, "enable", "启用 Paper 自动执行")}
                </button>
                {
                    (!market_data_is_fresh).then(|| html! {
                        <div class="form-text text-danger mt-2">
                            {"自动执行要求第 2 步检查结果为“实时且新鲜”。行情延迟、过期或尚未检查时不能启用。"}
                        </div>
                    }).unwrap_or_default()
                }
                {notice.as_ref().map(|result| match result {
                    Ok(message) => html! { <Alert style={Color::Success} class="mt-3"><span>{message}</span></Alert> },
                    Err(_) => Html::default(),
                }).unwrap_or_default()}
            </div></section>
        </>
    }
}

fn wizard_rpc(
    endpoint: String,
    method: &'static str,
    params: Value,
    action: &'static str,
    success: &'static str,
    busy_action: UseStateHandle<String>,
    notice: UseStateHandle<Option<Result<String, String>>>,
    on_completed: Callback<()>,
) -> Callback<MouseEvent> {
    Callback::from(move |_| {
        let endpoint = endpoint.clone();
        let params = params.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        let on_completed = on_completed.clone();
        busy_action.set(action.into());
        spawn_local(async move {
            match call_method(&endpoint, method, params).await {
                Ok(_) => {
                    notice.set(Some(Ok(success.into())));
                    on_completed.emit(());
                }
                Err(error) => notice.set(Some(Err(error))),
            }
            busy_action.set(String::new());
        });
    })
}

fn state_input(state: UseStateHandle<String>) -> Callback<InputEvent> {
    Callback::from(move |event: InputEvent| {
        let input: web_sys::HtmlInputElement = event.target_unchecked_into();
        state.set(input.value());
    })
}

fn button_content(busy_action: &UseStateHandle<String>, action: &str, label: &str) -> Html {
    if busy_action.as_str() == action {
        html! {
            <>
                <span class="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                {"处理中…"}
            </>
        }
    } else {
        html! { label.to_owned() }
    }
}
