use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    instrument_search::InstrumentSearch,
    value::{integer, local_time, number, text},
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
pub struct PaperValidationPageProps {
    pub endpoint: String,
    pub system: Value,
    pub on_completed: Callback<()>,
}

#[function_component(PaperValidationPage)]
pub fn paper_validation_page(props: &PaperValidationPageProps) -> Html {
    let selected = use_state(|| None::<Value>);
    let health = use_state(|| None::<Value>);
    let strategy_name = use_state(|| "paper-web-round-trip".to_owned());
    let phase_bars = use_state(|| "1".to_owned());
    let account = use_state(|| {
        props
            .system
            .pointer("/ibkr/managed_accounts/0")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    });
    let target_quantity = use_state(|| "1".to_owned());
    let strategy_id = use_state(String::new);
    let execution_confirmed = use_state(|| false);
    let busy = use_state(|| false);
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

    let subscribe = rpc_button(
        props.endpoint.clone(),
        "market_data.subscribe",
        (*selected).clone().unwrap_or_else(|| json!({})),
        busy.clone(),
        busy_action.clone(),
        "subscribe",
        notice.clone(),
        "行情订阅请求已接受",
        props.on_completed.clone(),
    );
    let check_health = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let health = health.clone();
        let busy = busy.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            let Some(conid) = selected
                .as_ref()
                .and_then(|item| item.get("conid"))
                .and_then(Value::as_i64)
            else {
                notice.set(Some(Err("请先选择合约".into())));
                return;
            };
            let endpoint = endpoint.clone();
            let health = health.clone();
            let busy = busy.clone();
            let busy_action = busy_action.clone();
            let notice = notice.clone();
            busy.set(true);
            busy_action.set("health".into());
            spawn_local(async move {
                match call_method(&endpoint, "market_data.health", json!({"conid": conid})).await {
                    Ok(response) => {
                        health.set(response.get("health").cloned());
                        notice.set(Some(Ok("行情健康状态已刷新".into())));
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
                busy_action.set(String::new());
            });
        })
    };

    let create_strategy = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let strategy_name = strategy_name.clone();
        let phase_bars = phase_bars.clone();
        let strategy_id = strategy_id.clone();
        let busy = busy.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        let on_completed = props.on_completed.clone();
        Callback::from(move |_| {
            let Some(conid) = selected
                .as_ref()
                .and_then(|item| item.get("conid"))
                .and_then(Value::as_i64)
            else {
                notice.set(Some(Err("请先选择合约".into())));
                return;
            };
            let Some(phase) = phase_bars.parse::<u32>().ok().filter(|value| *value > 0) else {
                notice.set(Some(Err("Phase bars 必须大于 0".into())));
                return;
            };
            let endpoint = endpoint.clone();
            let name = (*strategy_name).clone();
            let strategy_id = strategy_id.clone();
            let busy = busy.clone();
            let busy_action = busy_action.clone();
            let notice = notice.clone();
            let on_completed = on_completed.clone();
            busy.set(true);
            busy_action.set("create".into());
            spawn_local(async move {
                let params = json!({
                    "name": name,
                    "kind": "paper_round_trip",
                    "config": {"conid": conid, "phase_bars": phase}
                });
                match call_method(&endpoint, "strategy.create", params).await {
                    Ok(response) => {
                        let id = response
                            .get("strategy_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        strategy_id.set(id.to_owned());
                        notice.set(Some(Ok(format!("策略已创建：{id}"))));
                        on_completed.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
                busy_action.set(String::new());
            });
        })
    };

    let configure = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let strategy_id = strategy_id.clone();
        let account = account.clone();
        let target_quantity = target_quantity.clone();
        let busy = busy.clone();
        let busy_action = busy_action.clone();
        let notice = notice.clone();
        let on_completed = props.on_completed.clone();
        Callback::from(move |_| {
            let Some(quantity) = target_quantity
                .parse::<f64>()
                .ok()
                .filter(|value| *value > 0.0)
            else {
                notice.set(Some(Err("目标数量必须大于 0".into())));
                return;
            };
            let Some(contract) = (*selected).clone() else {
                notice.set(Some(Err("请先选择合约".into())));
                return;
            };
            if strategy_id.is_empty() || account.trim().is_empty() {
                notice.set(Some(Err("策略 ID 和 Paper 账户不能为空".into())));
                return;
            }
            let params = json!({
                "strategy_id": *strategy_id,
                "account": *account,
                "target_quantity": quantity,
                "short_target_quantity": 0.0,
                "allow_short": false,
                "order_type": "market",
                "paper_only": true,
                "contract": contract
            });
            run_rpc(
                endpoint.clone(),
                "strategy.execution.configure",
                params,
                busy.clone(),
                busy_action.clone(),
                "configure",
                notice.clone(),
                "执行配置已保存（尚未启用）",
                on_completed.clone(),
            );
        })
    };
    let start_strategy = rpc_button(
        props.endpoint.clone(),
        "strategy.start",
        json!({"strategy_id": *strategy_id}),
        busy.clone(),
        busy_action.clone(),
        "start",
        notice.clone(),
        "策略信号计算已启动",
        props.on_completed.clone(),
    );
    let enable_execution = rpc_button(
        props.endpoint.clone(),
        "strategy.execution.enable",
        json!({"strategy_id": *strategy_id, "confirm": true}),
        busy.clone(),
        busy_action.clone(),
        "enable",
        notice.clone(),
        "Paper 自动执行已启用",
        props.on_completed.clone(),
    );

    html! {
        <>
            <ErrorModal
                message={notice.as_ref().and_then(|value| value.as_ref().err()).cloned()}
                on_close={{
                    let notice = notice.clone();
                    Callback::from(move |_| notice.set(None))
                }}
            />
            <Alert style={Color::Warning}>
                {"此向导会创建可自动下单的 paper 策略。只有行情类型为实时 Bid/Ask、账户为 paper 且风控允许时才应启用执行。该策略仅用于验证，不代表盈利能力。"}
            </Alert>
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"1. 搜索并选择当前可交易合约"}</h2>
                <InstrumentSearch
                    endpoint={props.endpoint.clone()}
                    initial_pattern="EUR"
                    stock_only={true}
                    on_select={{
                        let selected = selected.clone();
                        Callback::from(move |instrument| selected.set(Some(instrument)))
                    }}
                />
            </div></section>
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"2. 订阅并检查实时行情"}</h2>
                <div class="d-flex gap-2 mb-3">
                    <button class="btn btn-outline-primary" disabled={selected.is_none() || *busy} onclick={subscribe}>
                        {button_content(&busy_action, "subscribe", "订阅行情")}
                    </button>
                    <button class="btn btn-outline-secondary" disabled={selected.is_none() || *busy} onclick={check_health}>
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
                                            {"IBKR 返回的是延迟行情。即使刚刚收到，也不能用于自动策略下单。"}
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
                <h2 class="h5">{"3. 创建策略"}</h2>
                <div class="row g-3 align-items-end">
                    <div class="col-md-7"><label class="form-label">{"名称"}</label><input class="form-control" value={(*strategy_name).clone()} oninput={{
                        let value = strategy_name.clone(); Callback::from(move |event: InputEvent| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
                        })
                    }} /></div>
                    <div class="col-md-2"><label class="form-label">{"Phase bars"}</label><input class="form-control" type="number" min="1" value={(*phase_bars).clone()} oninput={{
                        let value = phase_bars.clone(); Callback::from(move |event: InputEvent| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
                        })
                    }} /></div>
                    <div class="col-md-3"><button class="btn btn-primary w-100" disabled={selected.is_none() || *busy} onclick={create_strategy}>
                        {button_content(&busy_action, "create", "创建验证策略")}
                    </button></div>
                    <div class="col-12"><label class="form-label">{"Strategy ID"}</label><input class="form-control" value={(*strategy_id).clone()} placeholder="创建后自动填写，也可以粘贴已有 ID" oninput={{
                        let value = strategy_id.clone(); Callback::from(move |event: InputEvent| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
                        })
                    }} /></div>
                </div>
            </div></section>
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"4. 配置并启动"}</h2>
                <div class="row g-3 align-items-end">
                    <div class="col-md-6"><label class="form-label">{"Paper 账户"}</label><input class="form-control" value={(*account).clone()} oninput={{
                        let value = account.clone(); Callback::from(move |event: InputEvent| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
                        })
                    }} /></div>
                    <div class="col-md-3"><label class="form-label">{"Buy 目标数量"}</label><input class="form-control" type="number" min="0.0001" step="any" value={(*target_quantity).clone()} oninput={{
                        let value = target_quantity.clone(); Callback::from(move |event: InputEvent| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
                        })
                    }} /></div>
                    <div class="col-md-3"><button class="btn btn-outline-primary w-100" disabled={strategy_id.is_empty() || *busy} onclick={configure}>
                        {button_content(&busy_action, "configure", "保存执行配置")}
                    </button></div>
                    <div class="col-md-3"><button class="btn btn-outline-success w-100" disabled={strategy_id.is_empty() || *busy} onclick={start_strategy}>
                        {button_content(&busy_action, "start", "启动策略")}
                    </button></div>
                    <div class="col-md-9">
                        <div class="form-check">
                            <input id="execution-confirm" class="form-check-input" type="checkbox" checked={*execution_confirmed} onchange={{
                                let value = execution_confirmed.clone(); Callback::from(move |event: Event| {
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.checked());
                                })
                            }} />
                            <label class="form-check-label text-danger fw-semibold" for="execution-confirm">{"我确认这是 paper 账户，并允许该策略自动提交订单"}</label>
                        </div>
                    </div>
                    <div class="col-md-3"><button class="btn btn-danger w-100" disabled={!*execution_confirmed || strategy_id.is_empty() || *busy} onclick={enable_execution}>
                        {button_content(&busy_action, "enable", "启用 Paper 执行")}
                    </button></div>
                </div>
            </div></section>
            {
                match &*notice {
                    Some(Ok(message)) => html! { <Alert style={Color::Success}><span>{message}</span></Alert> },
                    Some(Err(_)) => Html::default(),
                    None => Html::default(),
                }
            }
        </>
    }
}

fn rpc_button(
    endpoint: String,
    method: &'static str,
    params: Value,
    busy: UseStateHandle<bool>,
    busy_action: UseStateHandle<String>,
    action: &'static str,
    notice: UseStateHandle<Option<Result<String, String>>>,
    success: &'static str,
    on_completed: Callback<()>,
) -> Callback<MouseEvent> {
    Callback::from(move |_| {
        run_rpc(
            endpoint.clone(),
            method,
            params.clone(),
            busy.clone(),
            busy_action.clone(),
            action,
            notice.clone(),
            success,
            on_completed.clone(),
        );
    })
}

fn run_rpc(
    endpoint: String,
    method: &'static str,
    params: Value,
    busy: UseStateHandle<bool>,
    busy_action: UseStateHandle<String>,
    action: &'static str,
    notice: UseStateHandle<Option<Result<String, String>>>,
    success: &'static str,
    on_completed: Callback<()>,
) {
    busy.set(true);
    busy_action.set(action.into());
    spawn_local(async move {
        match call_method(&endpoint, method, params).await {
            Ok(_) => {
                notice.set(Some(Ok(success.into())));
                on_completed.emit(());
            }
            Err(error) => notice.set(Some(Err(error))),
        }
        busy.set(false);
        busy_action.set(String::new());
    });
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
