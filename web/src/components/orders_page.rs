use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    cancel_order_button::CancelOrderButton,
    error_modal::ErrorModal,
    pagination::{Pagination, load_saved_page, save_page},
    value::{
        array, integer, local_time, number, official_security_name, security_exchange, short_id,
        text,
    },
};

#[derive(Properties, PartialEq)]
pub struct OrdersPageProps {
    pub endpoint: String,
}

#[function_component(OrdersPage)]
pub fn orders_page(props: &OrdersPageProps) -> Html {
    let orders = use_state(Vec::<Value>::new);
    let order_page = use_state(|| load_saved_page("quant-trader.orders-page"));
    let order_total_pages = use_state(|| 1_usize);
    let order_total_items = use_state(|| 0_usize);
    let executions = use_state(Vec::<Value>::new);
    let execution_page = use_state(|| load_saved_page("quant-trader.executions-page"));
    let execution_total_pages = use_state(|| 1_usize);
    let execution_total_items = use_state(|| 0_usize);
    let error = use_state(|| None::<String>);
    {
        let endpoint = props.endpoint.clone();
        let page = *order_page;
        let rows = orders.clone();
        let pages = order_total_pages.clone();
        let items = order_total_items.clone();
        let error = error.clone();
        use_effect_with((endpoint.clone(), page), move |_| {
            load_page(
                endpoint.clone(),
                "order.list",
                "orders",
                page,
                rows.clone(),
                pages.clone(),
                items.clone(),
                error.clone(),
            );
            let interval = Interval::new(5_000, move || {
                load_page(
                    endpoint.clone(),
                    "order.list",
                    "orders",
                    page,
                    rows.clone(),
                    pages.clone(),
                    items.clone(),
                    error.clone(),
                )
            });
            move || drop(interval)
        });
    }
    {
        let endpoint = props.endpoint.clone();
        let page = *execution_page;
        let rows = executions.clone();
        let pages = execution_total_pages.clone();
        let items = execution_total_items.clone();
        let error = error.clone();
        use_effect_with((endpoint.clone(), page), move |_| {
            load_page(
                endpoint.clone(),
                "execution.list",
                "executions",
                page,
                rows.clone(),
                pages.clone(),
                items.clone(),
                error.clone(),
            );
            let interval = Interval::new(5_000, move || {
                load_page(
                    endpoint.clone(),
                    "execution.list",
                    "executions",
                    page,
                    rows.clone(),
                    pages.clone(),
                    items.clone(),
                    error.clone(),
                )
            });
            move || drop(interval)
        });
    }
    html! {
        <>
            <ErrorModal message={(*error).clone()} on_close={{
                let error = error.clone();
                Callback::from(move |_| error.set(None))
            }} />
            <section class="mb-4">
                <h2 class="h5">{"订单"}</h2>
                <div class="card shadow-sm table-responsive">
                    <Pagination page={*order_page} total_pages={*order_total_pages}
                        total_items={*order_total_items} on_page={{
                            let page = order_page.clone();
                            Callback::from(move |next| {
                                save_page("quant-trader.orders-page", next);
                                page.set(next);
                            })
                        }} />
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"创建时间（本地）"}</th><th>{"订单 ID"}</th><th>{"证券（官方名称）"}</th><th>{"所属交易所"}</th><th>{"Broker ID"}</th>
                            <th>{"状态"}</th><th class="text-end">{"已成交数量"}</th>
                            <th class="text-end">{"平均成交价"}</th><th>{"更新时间（本地）"}</th>
                            <th>{"操作"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if orders.is_empty() {
                                html! { <tr><td colspan="10" class="text-center text-secondary py-4">{"暂无订单"}</td></tr> }
                            } else {
                                orders.iter().map(|row| {
                                    let broker_order_id = row.get("broker_order_id").and_then(Value::as_i64).unwrap_or(0);
                                    let status = text(row, "status");
                                    let symbol = text(row, "symbol");
                                    html! { <tr>
                                        <td class="text-nowrap">{local_time(row, "created_at")}</td>
                                        <td title={text(row, "order_id")}><code>{short_id(row, "order_id")}</code></td>
                                        <td>
                                            <div class="fw-semibold">{official_security_name(row)}</div>
                                            <div class="small text-secondary">{symbol.clone()}</div>
                                        </td>
                                        <td>{security_exchange(row)}</td>
                                        <td>{integer(row, "broker_order_id")}</td>
                                        <td>
                                            <span class={classes!("badge", order_status_class(&status))}
                                                title={order_status_explanation(&status)}>
                                                {order_status_label(&status)}
                                            </span>
                                            {order_diagnostics(row).into_iter().map(|detail| html! {
                                                <div class="small text-secondary mt-1 text-break">{detail}</div>
                                            }).collect::<Html>()}
                                        </td>
                                        <td class="text-end">{number(row, "filled_quantity")}</td>
                                        <td class="text-end">{number(row, "average_fill_price")}</td>
                                        <td class="text-nowrap">{local_time(row, "updated_at")}</td>
                                        <td>
                                            <CancelOrderButton endpoint={props.endpoint.clone()}
                                                broker_order_id={broker_order_id}
                                                symbol={symbol}
                                                status={status}
                                                on_cancelled={{
                                                    let endpoint = props.endpoint.clone();
                                                    let page = *order_page;
                                                    let rows = orders.clone();
                                                    let pages = order_total_pages.clone();
                                                    let items = order_total_items.clone();
                                                    let error = error.clone();
                                                    Callback::from(move |_| load_page(
                                                        endpoint.clone(), "order.list", "orders", page,
                                                        rows.clone(), pages.clone(), items.clone(), error.clone()
                                                    ))
                                                }}
                                                on_error={{
                                                    let error = error.clone();
                                                    Callback::from(move |message| error.set(Some(message)))
                                                }} />
                                        </td>
                                    </tr>
                                }}).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                    <Pagination page={*order_page} total_pages={*order_total_pages}
                        total_items={*order_total_items} on_page={{
                            let page = order_page.clone();
                            Callback::from(move |next| {
                                save_page("quant-trader.orders-page", next);
                                page.set(next);
                            })
                        }} />
                </div>
            </section>
            <section class="mb-4">
                <h2 class="h5">{"成交"}</h2>
                <div class="card shadow-sm table-responsive">
                    <Pagination page={*execution_page} total_pages={*execution_total_pages}
                        total_items={*execution_total_items} on_page={{
                            let page = execution_page.clone();
                            Callback::from(move |next| {
                                save_page("quant-trader.executions-page", next);
                                page.set(next);
                            })
                        }} />
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"成交时间（本地）"}</th><th>{"Broker Execution ID"}</th>
                            <th>{"证券（官方名称）"}</th><th>{"所属交易所"}</th><th>{"Conid"}</th><th>{"方向"}</th><th class="text-end">{"数量"}</th>
                            <th class="text-end">{"价格"}</th><th class="text-end">{"佣金"}</th><th>{"币种"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if executions.is_empty() {
                                html! { <tr><td colspan="10" class="text-center text-secondary py-4">{"暂无成交"}</td></tr> }
                            } else {
                                executions.iter().map(|row| html! {
                                    <tr>
                                        <td class="text-nowrap">{local_time(row, "executed_at")}</td>
                                        <td><code>{text(row, "broker_execution_id")}</code></td>
                                        <td>
                                            <div class="fw-semibold">{official_security_name(row)}</div>
                                            <div class="small text-secondary">{text(row, "symbol")}</div>
                                        </td>
                                        <td>{security_exchange(row)}</td>
                                        <td>{integer(row, "conid")}</td>
                                        <td><span class={classes!("badge", if text(row, "side").to_uppercase().contains("BUY") { "bg-success" } else { "bg-danger" })}>{text(row, "side")}</span></td>
                                        <td class="text-end">{number(row, "quantity")}</td>
                                        <td class="text-end">{number(row, "price")}</td>
                                        <td class="text-end">{number(row, "commission")}</td>
                                        <td>{text(row, "currency")}</td>
                                    </tr>
                                }).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                    <Pagination page={*execution_page} total_pages={*execution_total_pages}
                        total_items={*execution_total_items} on_page={{
                            let page = execution_page.clone();
                            Callback::from(move |next| {
                                save_page("quant-trader.executions-page", next);
                                page.set(next);
                            })
                        }} />
                </div>
            </section>
        </>
    }
}

fn order_status_label(status: &str) -> &'static str {
    match status.to_ascii_lowercase().as_str() {
        "submitted" => "已提交",
        "presubmitted" => "预提交",
        "pendingsubmit" | "apipending" => "等待提交",
        "pendingcancel" | "cancel_pending" => "取消处理中",
        "filled" => "已成交",
        "cancelled" | "canceled" => "已取消",
        "inactive" => "未激活",
        "rejected" => "已拒绝",
        "not_open" => "IBKR 无活动订单",
        _ => "未知状态",
    }
}

fn order_status_class(status: &str) -> &'static str {
    match status.to_ascii_lowercase().as_str() {
        "filled" => "bg-success",
        "cancelled" | "canceled" | "inactive" | "not_open" => "bg-secondary",
        "rejected" => "bg-danger",
        "pendingcancel" | "cancel_pending" => "bg-warning text-dark",
        _ => "bg-primary",
    }
}

fn order_status_explanation(status: &str) -> &'static str {
    match status.to_ascii_lowercase().as_str() {
        "not_open" => {
            "该订单是旧会话记录；最近一次 IBKR all-open-orders 对账未发现活动订单，因此没有发送取消请求"
        }
        "pendingcancel" | "cancel_pending" => "取消请求已经发送，正在等待 IBKR 最终状态",
        "filled" => "订单已经成交，不能取消",
        "cancelled" | "canceled" => "IBKR 已确认订单取消",
        _ => "订单状态来自本地记录或 IBKR 回报",
    }
}

fn order_diagnostics(order: &Value) -> Vec<String> {
    let mut details = Vec::new();
    if let Some(remaining) = order.get("remaining_quantity").and_then(Value::as_f64) {
        details.push(format!(
            "剩余数量：{}",
            super::value::format_number(remaining)
        ));
    }
    if let Some(reason) = order
        .get("why_held")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        details.push(format!("IBKR Hold：{reason}"));
    }
    if let Some(price) = order.get("last_fill_price").and_then(Value::as_f64) {
        details.push(format!(
            "最近成交价：{}",
            super::value::format_number(price)
        ));
    }
    if let Some(price) = order.get("market_cap_price").and_then(Value::as_f64) {
        details.push(format!(
            "IBKR 限价上限：{}",
            super::value::format_number(price)
        ));
    }
    let event = order.get("latest_broker_event").unwrap_or(&Value::Null);
    for (key, label) in [
        ("reject_reason", "拒绝原因"),
        ("warning_text", "IBKR 警告"),
        ("completed_status", "完成说明"),
    ] {
        if let Some(value) = event
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            details.push(format!("{label}：{value}"));
        }
    }
    details
}

fn load_page(
    endpoint: String,
    method: &'static str,
    key: &'static str,
    page: usize,
    rows: UseStateHandle<Vec<Value>>,
    total_pages: UseStateHandle<usize>,
    total_items: UseStateHandle<usize>,
    error: UseStateHandle<Option<String>>,
) {
    spawn_local(async move {
        match call_method(&endpoint, method, json!({"page": page, "page_size": 25})).await {
            Ok(value) => {
                rows.set(array(&value, key));
                total_pages.set(
                    value
                        .get("total_pages")
                        .and_then(Value::as_u64)
                        .unwrap_or(1) as usize,
                );
                total_items.set(
                    value
                        .get("total_items")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                );
            }
            Err(message) => error.set(Some(message)),
        }
    });
}
