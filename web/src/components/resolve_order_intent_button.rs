use serde_json::json;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;
use web_sys::HtmlTextAreaElement;
use yew::prelude::*;

use crate::api::call_method;

#[derive(Properties, PartialEq)]
pub struct ResolveOrderIntentButtonProps {
    pub endpoint: String,
    pub order_intent_id: String,
    pub symbol: String,
    pub on_resolved: Callback<()>,
    pub on_error: Callback<String>,
}

#[function_component(ResolveOrderIntentButton)]
pub fn resolve_order_intent_button(props: &ResolveOrderIntentButtonProps) -> Html {
    let open = use_state(|| false);
    let loading = use_state(|| false);
    let note = use_state(String::new);
    let verified = use_state(|| false);
    let can_submit = *verified && !note.trim().is_empty() && !*loading;

    let close = {
        let open = open.clone();
        let loading = loading.clone();
        Callback::from(move |_| {
            if !*loading {
                open.set(false);
            }
        })
    };

    html! {
        <>
            <button class="btn btn-sm btn-outline-danger" disabled={*loading}
                onclick={{
                    let open = open.clone();
                    let note = note.clone();
                    let verified = verified.clone();
                    Callback::from(move |_| {
                        note.set(String::new());
                        verified.set(false);
                        open.set(true);
                    })
                }}>
                {"标记已解决"}
            </button>
            {if *open {
                html! {
                    <>
                        <div class="modal fade show d-block" tabindex="-1" role="dialog"
                            aria-modal="true" aria-labelledby="resolve-intent-title">
                            <div class="modal-dialog modal-dialog-centered modal-lg">
                                <div class="modal-content shadow">
                                    <div class="modal-header bg-danger text-white">
                                        <h2 id="resolve-intent-title" class="modal-title h5">
                                            {"确认人工解决未知订单意图"}
                                        </h2>
                                        <button class="btn-close btn-close-white" type="button"
                                            aria-label="关闭" disabled={*loading} onclick={close.clone()} />
                                    </div>
                                    <div class="modal-body">
                                        <div class="alert alert-danger">
                                            {"此操作只解除本地重复下单保护，不会取消 IBKR 中可能存在的订单。核查不完整可能造成重复交易。"}
                                        </div>
                                        <dl>
                                            <dt>{"证券"}</dt><dd>{props.symbol.clone()}</dd>
                                            <dt>{"Intent ID"}</dt>
                                            <dd><code class="text-break">{props.order_intent_id.clone()}</code></dd>
                                        </dl>
                                        <p class="mb-2">{"请先核查 IBKR 的活动订单、已完成订单、成交记录和当前持仓。"}</p>
                                        <label class="form-label fw-semibold" for="intent-resolution-note">
                                            {"核查说明（必填）"}
                                        </label>
                                        <textarea id="intent-resolution-note" class="form-control" rows="3"
                                            value={(*note).clone()}
                                            placeholder="例如：已核对 IBKR 活动订单、已完成订单、成交和持仓，未发现对应订单或成交。"
                                            disabled={*loading}
                                            oninput={{
                                                let note = note.clone();
                                                Callback::from(move |event: InputEvent| {
                                                    let target = event.target().and_then(|value| value.dyn_into::<HtmlTextAreaElement>().ok());
                                                    if let Some(target) = target {
                                                        note.set(target.value());
                                                    }
                                                })
                                            }} />
                                        <div class="form-check mt-3">
                                            <input id="intent-resolution-confirm" class="form-check-input"
                                                type="checkbox" checked={*verified} disabled={*loading}
                                                onchange={{
                                                    let verified = verified.clone();
                                                    Callback::from(move |_| verified.set(!*verified))
                                                }} />
                                            <label class="form-check-label" for="intent-resolution-confirm">
                                                {"我已完成上述核查，并确认可以解除本地保护"}
                                            </label>
                                        </div>
                                    </div>
                                    <div class="modal-footer">
                                        <button class="btn btn-secondary" disabled={*loading}
                                            onclick={close.clone()}>{"返回"}</button>
                                        <button class="btn btn-danger" disabled={!can_submit}
                                            onclick={{
                                                let endpoint = props.endpoint.clone();
                                                let order_intent_id = props.order_intent_id.clone();
                                                let note_value = (*note).clone();
                                                let loading = loading.clone();
                                                let open = open.clone();
                                                let on_resolved = props.on_resolved.clone();
                                                let on_error = props.on_error.clone();
                                                Callback::from(move |_| {
                                                    loading.set(true);
                                                    let endpoint = endpoint.clone();
                                                    let order_intent_id = order_intent_id.clone();
                                                    let note = note_value.trim().to_owned();
                                                    let loading = loading.clone();
                                                    let open = open.clone();
                                                    let on_resolved = on_resolved.clone();
                                                    let on_error = on_error.clone();
                                                    spawn_local(async move {
                                                        match call_method(&endpoint, "order.intent.resolve", json!({
                                                            "order_intent_id": order_intent_id,
                                                            "note": note,
                                                            "confirm": true,
                                                        })).await {
                                                            Ok(_) => on_resolved.emit(()),
                                                            Err(error) => on_error.emit(error),
                                                        }
                                                        loading.set(false);
                                                        open.set(false);
                                                    });
                                                })
                                            }}>
                                            {if *loading {
                                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"处理中…"}</> }
                                            } else { html! { "确认标记已解决" } }}
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
