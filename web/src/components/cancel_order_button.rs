use serde_json::json;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

#[derive(Properties, PartialEq)]
pub struct CancelOrderButtonProps {
    pub endpoint: String,
    pub broker_order_id: i64,
    pub symbol: String,
    pub status: String,
    pub on_cancelled: Callback<()>,
    pub on_error: Callback<String>,
}

#[function_component(CancelOrderButton)]
pub fn cancel_order_button(props: &CancelOrderButtonProps) -> Html {
    let confirming = use_state(|| false);
    let loading = use_state(|| false);
    let cancellable = matches!(
        props.status.to_ascii_lowercase().as_str(),
        "submitted" | "presubmitted" | "pendingsubmit" | "apipending"
    ) && props.broker_order_id > 0;

    let close = {
        let confirming = confirming.clone();
        let loading = loading.clone();
        Callback::from(move |_| {
            if !*loading {
                confirming.set(false);
            }
        })
    };

    html! {
        <>
            <button class="btn btn-sm btn-outline-danger" disabled={!cancellable || *loading}
                onclick={{
                    let confirming = confirming.clone();
                    Callback::from(move |_| confirming.set(true))
                }}>
                {if props.status.eq_ignore_ascii_case("cancel_pending")
                    || props.status.eq_ignore_ascii_case("pendingcancel") {
                    "取消处理中"
                } else {
                    "取消订单"
                }}
            </button>
            {if *confirming {
                html! {
                    <>
                        <div class="modal fade show d-block" tabindex="-1" role="dialog"
                            aria-modal="true" aria-labelledby="cancel-order-title">
                            <div class="modal-dialog modal-dialog-centered">
                                <div class="modal-content shadow">
                                    <div class="modal-header bg-danger text-white">
                                        <h2 id="cancel-order-title" class="modal-title h5">{"确认取消订单"}</h2>
                                        <button class="btn-close btn-close-white" type="button"
                                            aria-label="关闭" disabled={*loading} onclick={close.clone()} />
                                    </div>
                                    <div class="modal-body">
                                        <p>{"取消请求将发送到当前连接的 IBKR 会话。订单可能在请求到达前已经成交。"}</p>
                                        <dl class="mb-0">
                                            <dt>{"证券"}</dt><dd>{props.symbol.clone()}</dd>
                                            <dt>{"Broker Order ID"}</dt><dd>{props.broker_order_id}</dd>
                                            <dt>{"当前状态"}</dt><dd>{props.status.clone()}</dd>
                                        </dl>
                                    </div>
                                    <div class="modal-footer">
                                        <button class="btn btn-secondary" disabled={*loading}
                                            onclick={close.clone()}>{"返回"}</button>
                                        <button class="btn btn-danger" disabled={*loading}
                                            onclick={{
                                                let endpoint = props.endpoint.clone();
                                                let broker_order_id = props.broker_order_id;
                                                let loading = loading.clone();
                                                let confirming = confirming.clone();
                                                let on_cancelled = props.on_cancelled.clone();
                                                let on_error = props.on_error.clone();
                                                Callback::from(move |_| {
                                                    loading.set(true);
                                                    let endpoint = endpoint.clone();
                                                    let loading = loading.clone();
                                                    let confirming = confirming.clone();
                                                    let on_cancelled = on_cancelled.clone();
                                                    let on_error = on_error.clone();
                                                    spawn_local(async move {
                                                        match call_method(
                                                            &endpoint,
                                                            "order.cancel",
                                                            json!({"broker_order_id": broker_order_id}),
                                                        ).await {
                                                            Ok(_) => on_cancelled.emit(()),
                                                            Err(error) => on_error.emit(error),
                                                        }
                                                        loading.set(false);
                                                        confirming.set(false);
                                                    });
                                                })
                                            }}>
                                            {if *loading {
                                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"取消中…"}</> }
                                            } else { html! { "确认取消" } }}
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="modal-backdrop fade show" onclick={close.clone()} />
                    </>
                }
            } else { Html::default() }}
        </>
    }
}
