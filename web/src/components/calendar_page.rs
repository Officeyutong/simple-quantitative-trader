use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::value::{array, local_time, security_exchange, text};

const REFRESH_INTERVAL_MS: u32 = 5_000;

#[derive(Properties, PartialEq)]
pub struct CalendarPageProps {
    pub endpoint: String,
    pub execution_configs: Value,
}

#[derive(Clone, PartialEq)]
enum CalendarState {
    Loading,
    Ready {
        sessions: Vec<Value>,
        regular: Option<Value>,
        extended: Option<Value>,
    },
    Error(String),
}

#[function_component(CalendarPage)]
pub fn calendar_page(props: &CalendarPageProps) -> Html {
    let configs = array(&props.execution_configs, "configs");
    let first_contract = configs
        .first()
        .and_then(|config| config.get("contract"))
        .cloned()
        .unwrap_or(Value::Null);
    let exchange = use_state(|| {
        let exchange = security_exchange(&first_contract);
        (exchange != "—").then_some(exchange).unwrap_or_default()
    });
    let selected_conid = use_state(|| {
        first_contract
            .get("conid")
            .and_then(Value::as_i64)
            .map(|value| value.to_string())
            .unwrap_or_default()
    });
    let contract_select_ref = use_node_ref();
    let state = use_state(|| CalendarState::Loading);
    let busy = use_state(|| false);
    let notice = use_state(|| None::<Result<String, String>>);
    let manual_date = use_state(String::new);
    let manual_open = use_state(String::new);
    let manual_close = use_state(String::new);

    {
        let endpoint = props.endpoint.clone();
        let exchange_value = (*exchange).clone();
        let state = state.clone();
        use_effect_with((endpoint.clone(), exchange_value.clone()), move |_| {
            load_calendar(
                endpoint.clone(),
                exchange_value.clone(),
                state.clone(),
                true,
            );
            let interval = Interval::new(REFRESH_INTERVAL_MS, move || {
                load_calendar(
                    endpoint.clone(),
                    exchange_value.clone(),
                    state.clone(),
                    false,
                );
            });
            move || drop(interval)
        });
    }

    let selected_contract = configs.iter().find_map(|config| {
        let contract = config.get("contract")?;
        (contract
            .get("conid")
            .and_then(Value::as_i64)
            .map(|value| value.to_string())
            == Some((*selected_conid).clone()))
        .then(|| contract.clone())
    });

    let refresh_ibkr = {
        let endpoint = props.endpoint.clone();
        let configs = configs.clone();
        let contract_select_ref = contract_select_ref.clone();
        let exchange = exchange.clone();
        let state = state.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            // Read the DOM value at submission time. Browsers may restore a
            // select's visual value before Yew has completed the corresponding
            // state render, so state alone can otherwise refresh the previous
            // contract.
            let current_conid = contract_select_ref
                .cast::<web_sys::HtmlSelectElement>()
                .map(|select| select.value())
                .unwrap_or_default();
            let contract = configs.iter().find_map(|config| {
                let contract = config.get("contract")?;
                (contract
                    .get("conid")
                    .and_then(Value::as_i64)
                    .map(|value| value.to_string())
                    == Some(current_conid.clone()))
                .then(|| contract.clone())
            });
            let Some(contract) = contract else {
                notice.set(Some(Err("请先选择一个已经配置自动执行的证券".into())));
                return;
            };
            let endpoint = endpoint.clone();
            let exchange = exchange.clone();
            let state = state.clone();
            let busy = busy.clone();
            let notice = notice.clone();
            busy.set(true);
            spawn_local(async move {
                match call_method(&endpoint, "calendar.refresh", json!({"contract": contract}))
                    .await
                {
                    Ok(result) => {
                        let refreshed_exchange = text(&result, "exchange");
                        if refreshed_exchange != "—" {
                            exchange.set(refreshed_exchange.clone());
                        }
                        notice.set(Some(Ok(format!(
                            "已从 IBKR 更新 {}：正常时段 {} 段，扩展时段 {} 段",
                            refreshed_exchange,
                            result
                                .get("regular_intervals")
                                .and_then(Value::as_u64)
                                .unwrap_or(0),
                            result
                                .get("extended_intervals")
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                        ))));
                        load_calendar(endpoint, refreshed_exchange, state, false);
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    let add_manual = {
        let endpoint = props.endpoint.clone();
        let exchange = exchange.clone();
        let date = manual_date.clone();
        let opens_at = manual_open.clone();
        let closes_at = manual_close.clone();
        let state = state.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            let exchange_value = exchange.trim().to_ascii_uppercase();
            let date_value = date.trim().to_owned();
            let opens = local_datetime_to_utc(&opens_at);
            let closes = local_datetime_to_utc(&closes_at);
            if exchange_value.is_empty() || date_value.is_empty() {
                notice.set(Some(Err("交易所和交易日期不能为空".into())));
                return;
            }
            let (Ok(opens), Ok(closes)) = (opens, closes) else {
                notice.set(Some(Err("开市和收市时间必须是有效的本地日期时间".into())));
                return;
            };
            let endpoint = endpoint.clone();
            let exchange = exchange.clone();
            let state = state.clone();
            let busy = busy.clone();
            let notice = notice.clone();
            busy.set(true);
            spawn_local(async move {
                match call_method(
                    &endpoint,
                    "calendar.add",
                    json!({
                        "exchange": exchange_value,
                        "trading_date": date_value,
                        "opens_at": opens,
                        "closes_at": closes,
                        "state": "open",
                        "source": "web_manual"
                    }),
                )
                .await
                {
                    Ok(_) => {
                        notice.set(Some(Ok("人工正常交易时段已保存".into())));
                        load_calendar(endpoint, (*exchange).clone(), state, false);
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    let (sessions, regular, extended, load_error) = match &*state {
        CalendarState::Loading => (Vec::new(), None, None, None),
        CalendarState::Ready {
            sessions,
            regular,
            extended,
        } => (sessions.clone(), regular.clone(), extended.clone(), None),
        CalendarState::Error(error) => (Vec::new(), None, None, Some(error.clone())),
    };

    html! {
        <>
            <section class="card shadow-sm mb-4">
                <div class="card-body">
                    <h2 class="h5">{"IBKR 交易日历"}</h2>
                    <p class="text-secondary">
                        {"正常订单使用 IBKR liquidHours，允许盘前盘后的订单使用 tradingHours。系统按合约时区处理夏令时，并将结果转换为 UTC 缓存。"}
                    </p>
                    <div class="row g-3 align-items-end">
                        <div class="col-12 col-xl-5">
                            <label class="form-label" for="calendar-contract">{"策略执行证券"}</label>
                            <select ref={contract_select_ref} id="calendar-contract" class="form-select"
                                onchange={{
                                    let selected_conid = selected_conid.clone();
                                    let exchange = exchange.clone();
                                    let configs = configs.clone();
                                    Callback::from(move |event: Event| {
                                        let select: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                        let conid = select.value();
                                        if let Some(contract) = configs.iter().find_map(|config| {
                                            let contract = config.get("contract")?;
                                            (contract.get("conid").and_then(Value::as_i64).map(|value| value.to_string()) == Some(conid.clone()))
                                                .then_some(contract)
                                        }) {
                                            let selected_exchange = security_exchange(contract);
                                            if selected_exchange != "—" {
                                                exchange.set(selected_exchange);
                                            }
                                        }
                                        selected_conid.set(conid);
                                    })
                                }}>
                                {
                                    if configs.is_empty() {
                                        html! { <option value="">{"暂无策略执行证券"}</option> }
                                    } else {
                                        configs.iter().filter_map(|config| {
                                            let contract = config.get("contract")?;
                                            let conid = contract.get("conid")?.as_i64()?.to_string();
                                            Some(html! {
                                                <option value={conid.clone()} selected={conid == *selected_conid}>
                                                    {format!("{} · {} · Conid {}", text(contract, "symbol"), security_exchange(contract), conid)}
                                                </option>
                                            })
                                        }).collect::<Html>()
                                    }
                                }
                            </select>
                        </div>
                        <div class="col-12 col-md-6 col-xl-3">
                            <label class="form-label" for="calendar-exchange">{"交易所筛选"}</label>
                            <input id="calendar-exchange" class="form-control"
                                placeholder="例如 SBF、NASDAQ"
                                value={(*exchange).clone()}
                                oninput={{
                                    let exchange = exchange.clone();
                                    Callback::from(move |event: InputEvent| {
                                        let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                        exchange.set(input.value().to_ascii_uppercase());
                                    })
                                }} />
                        </div>
                        <div class="col-12 col-md-6 col-xl-4">
                            <button class="btn btn-primary w-100"
                                disabled={*busy || selected_contract.is_none()}
                                onclick={refresh_ibkr}>
                                {if *busy { "正在读取 IBKR…" } else { "立即从 IBKR 更新日历" }}
                            </button>
                        </div>
                    </div>
                    {notice.as_ref().map(|result| match result {
                        Ok(message) => html! { <div class="alert alert-success mt-3 mb-0">{message}</div> },
                        Err(message) => html! { <div class="alert alert-danger mt-3 mb-0">{message}</div> },
                    }).unwrap_or_default()}
                    {load_error.map(|error| html! {
                        <div class="alert alert-danger mt-3 mb-0">{error}</div>
                    }).unwrap_or_default()}
                </div>
            </section>

            <section class="row g-3 mb-4">
                {status_card("正常交易时段", regular.as_ref(), false)}
                {status_card("扩展交易时段", extended.as_ref(), true)}
            </section>

            <section class="card shadow-sm mb-4">
                <div class="card-header fw-semibold">{"已缓存交易时段"}</div>
                <div class="table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"交易所"}</th><th>{"类型"}</th><th>{"交易日期"}</th>
                            <th>{"开市（本地）"}</th><th>{"收市（本地）"}</th>
                            <th>{"来源"}</th><th>{"更新时间（本地）"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if matches!(&*state, CalendarState::Loading) {
                                html! { <tr><td colspan="7" class="text-center text-secondary py-4">{"正在读取日历…"}</td></tr> }
                            } else if sessions.is_empty() {
                                html! { <tr><td colspan="7" class="text-center text-secondary py-4">{"该交易所尚无缓存；请选择证券并从 IBKR 更新"}</td></tr> }
                            } else {
                                sessions.iter().map(|row| html! {
                                    <tr>
                                        <td class="fw-semibold">{text(row, "exchange")}</td>
                                        <td>{session_kind_badge(&text(row, "session_kind"))}</td>
                                        <td>{text(row, "trading_date")}</td>
                                        <td class="text-nowrap">{local_time(row, "opens_at")}</td>
                                        <td class="text-nowrap">{local_time(row, "closes_at")}</td>
                                        <td><code>{text(row, "source")}</code></td>
                                        <td class="text-nowrap">{local_time(row, "updated_at")}</td>
                                    </tr>
                                }).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                </div>
            </section>

            <section class="card shadow-sm">
                <div class="card-body">
                    <h2 class="h5">{"人工补录正常交易时段"}</h2>
                    <p class="text-secondary small">
                        {"仅用于 IBKR 暂时无法返回日历时的诊断或应急。时间按浏览器本地时区输入，保存时自动转换为 UTC；后续 IBKR 更新同一日期时会以 IBKR 数据为准。"}
                    </p>
                    <div class="row g-3 align-items-end">
                        <div class="col-12 col-md-3">
                            <label class="form-label" for="calendar-date">{"交易日期"}</label>
                            <input id="calendar-date" type="date" class="form-control"
                                value={(*manual_date).clone()}
                                oninput={state_input(manual_date.clone())} />
                        </div>
                        <div class="col-12 col-md-3">
                            <label class="form-label" for="calendar-open">{"开市时间（本地）"}</label>
                            <input id="calendar-open" type="datetime-local" class="form-control"
                                value={(*manual_open).clone()}
                                oninput={state_input(manual_open.clone())} />
                        </div>
                        <div class="col-12 col-md-3">
                            <label class="form-label" for="calendar-close">{"收市时间（本地）"}</label>
                            <input id="calendar-close" type="datetime-local" class="form-control"
                                value={(*manual_close).clone()}
                                oninput={state_input(manual_close.clone())} />
                        </div>
                        <div class="col-12 col-md-3">
                            <button class="btn btn-outline-primary w-100" disabled={*busy}
                                onclick={add_manual}>{"保存人工时段"}</button>
                        </div>
                    </div>
                </div>
            </section>
        </>
    }
}

fn load_calendar(
    endpoint: String,
    exchange: String,
    state: UseStateHandle<CalendarState>,
    show_loading: bool,
) {
    if show_loading {
        state.set(CalendarState::Loading);
    }
    spawn_local(async move {
        let exchange = exchange.trim().to_ascii_uppercase();
        let filter = (!exchange.is_empty()).then_some(exchange.clone());
        let sessions = call_method(
            &endpoint,
            "calendar.list",
            json!({"exchange": filter, "limit": 1000}),
        )
        .await;
        let result = match sessions {
            Ok(sessions) => {
                let rows = array(&sessions, "sessions");
                if exchange.is_empty() {
                    Ok(CalendarState::Ready {
                        sessions: rows,
                        regular: None,
                        extended: None,
                    })
                } else {
                    let regular = call_method(
                        &endpoint,
                        "calendar.status",
                        json!({"exchange": exchange, "outside_rth": false}),
                    )
                    .await;
                    let extended = call_method(
                        &endpoint,
                        "calendar.status",
                        json!({"exchange": exchange, "outside_rth": true}),
                    )
                    .await;
                    match (regular, extended) {
                        (Ok(regular), Ok(extended)) => Ok(CalendarState::Ready {
                            sessions: rows,
                            regular: Some(regular),
                            extended: Some(extended),
                        }),
                        (Err(error), _) | (_, Err(error)) => Err(error),
                    }
                }
            }
            Err(error) => Err(error),
        };
        state.set(result.unwrap_or_else(CalendarState::Error));
    });
}

fn status_card(label: &str, status: Option<&Value>, extended: bool) -> Html {
    let configured = status
        .and_then(|value| value.get("configured"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let open = status
        .and_then(|value| value.get("open"))
        .and_then(Value::as_bool);
    let (badge, detail) = match (configured, open) {
        (true, Some(true)) => ("bg-success", "当前可交易"),
        (true, Some(false)) => ("bg-secondary", "当前休市"),
        _ => ("bg-warning text-dark", "尚未配置"),
    };
    html! {
        <div class="col-12 col-lg-6">
            <div class="card shadow-sm h-100">
                <div class="card-body d-flex justify-content-between align-items-center gap-3">
                    <div>
                        <div class="fw-semibold">{label}</div>
                        <div class="small text-secondary">
                            {if extended { "用于 outside_rth=true 的限价单" } else { "用于常规时段市价单" }}
                        </div>
                    </div>
                    <span class={classes!("badge", badge)}>{detail}</span>
                </div>
            </div>
        </div>
    }
}

fn session_kind_badge(kind: &str) -> Html {
    let (label, class) = if kind == "extended" {
        ("扩展", "bg-info text-dark")
    } else {
        ("正常", "bg-primary")
    };
    html! { <span class={classes!("badge", class)}>{label}</span> }
}

fn state_input(state: UseStateHandle<String>) -> Callback<InputEvent> {
    Callback::from(move |event: InputEvent| {
        let input: web_sys::HtmlInputElement = event.target_unchecked_into();
        state.set(input.value());
    })
}

fn local_datetime_to_utc(value: &str) -> Result<String, ()> {
    if value.trim().is_empty() {
        return Err(());
    }
    let date = js_sys::Date::new(&JsValue::from_str(value));
    if date.get_time().is_nan() {
        return Err(());
    }
    date.to_iso_string().as_string().ok_or(())
}
