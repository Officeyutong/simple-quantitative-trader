use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::error_modal::ErrorModal;
use super::value::{integer, official_security_name, security_exchange, text};

#[derive(Properties, PartialEq)]
pub struct InstrumentSearchProps {
    pub endpoint: String,
    #[prop_or_default]
    pub initial_pattern: String,
    #[prop_or_default]
    pub on_select: Callback<Value>,
    #[prop_or(false)]
    pub stock_only: bool,
}

#[function_component(InstrumentSearch)]
pub fn instrument_search(props: &InstrumentSearchProps) -> Html {
    let pattern = use_state(|| props.initial_pattern.clone());
    let candidates = use_state(Vec::<Value>::new);
    let selected_conid = use_state(|| None::<i64>);
    let detail = use_state(|| None::<Value>);
    let busy = use_state(|| false);
    let error = use_state(|| None::<String>);

    let search = {
        let endpoint = props.endpoint.clone();
        let pattern = pattern.clone();
        let candidates = candidates.clone();
        let selected_conid = selected_conid.clone();
        let detail = detail.clone();
        let busy = busy.clone();
        let error = error.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let query = pattern.trim().to_owned();
            if query.is_empty() {
                error.set(Some("请输入证券代码或名称".into()));
                return;
            }
            let endpoint = endpoint.clone();
            let candidates = candidates.clone();
            let selected_conid = selected_conid.clone();
            let detail = detail.clone();
            let busy = busy.clone();
            let error = error.clone();
            busy.set(true);
            error.set(None);
            spawn_local(async move {
                match call_method(&endpoint, "instrument.search", json!({"pattern": query})).await {
                    Ok(response) => {
                        let rows = response
                            .get("candidates")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        if rows.is_empty() {
                            error.set(Some(
                                "没有找到已解析证券；IBKR 返回的 conid=0 占位结果已过滤".into(),
                            ));
                        }
                        candidates.set(rows);
                        selected_conid.set(None);
                        detail.set(None);
                    }
                    Err(message) => error.set(Some(message)),
                }
                busy.set(false);
            });
        })
    };

    html! {
        <>
            <ErrorModal message={(*error).clone()} on_close={{
                let error = error.clone();
                Callback::from(move |_| error.set(None))
            }} />
            <form onsubmit={search} class="d-flex gap-2 mb-3">
                <input
                    class="form-control"
                    value={(*pattern).clone()}
                    placeholder="输入代码或名称，例如 OR、AAPL、L'Oreal"
                    oninput={{
                        let pattern = pattern.clone();
                        Callback::from(move |event: InputEvent| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                            pattern.set(input.value());
                        })
                    }}
                />
                <button class="btn btn-primary" type="submit" disabled={*busy}>
                    {
                        if *busy {
                            html! {
                                <>
                                    <span class="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                                    {"搜索中…"}
                                </>
                            }
                        } else {
                            html! { "搜索" }
                        }
                    }
                </button>
            </form>
            {
                (!candidates.is_empty()).then(|| html! {
                    <div class="table-responsive"><table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"操作"}</th><th>{"证券（官方名称）"}</th><th>{"Conid"}</th>
                            <th>{"类型"}</th><th>{"币种"}</th><th>{"所属交易所"}</th>
                            <th>{"路由"}</th><th>{"本地代码"}</th>
                        </tr></thead>
                        <tbody>
                            {candidates.iter().map(|candidate| {
                                let conid = candidate.get("conid").and_then(Value::as_i64);
                                let active = *selected_conid == conid;
                                let selectable = conid.is_some_and(|value| value > 0)
                                    && (!props.stock_only || text(candidate, "security_type") == "STK");
                                let detail_value = candidate.clone();
                                let select_value = candidate.clone();
                                html! {
                                    <tr class={active.then_some("table-primary")}>
                                        <td><div class="d-flex flex-wrap gap-2">
                                            <button class="btn btn-sm btn-outline-secondary" onclick={{
                                                let detail = detail.clone();
                                                Callback::from(move |_| detail.set(Some(detail_value.clone())))
                                            }}>{"查看详情"}</button>
                                            <button class="btn btn-sm btn-outline-primary" disabled={!selectable}
                                                title={(!selectable).then_some("当前向导只支持已解析的 STK 合约")}
                                                onclick={{
                                                let selected_conid = selected_conid.clone();
                                                let detail = detail.clone();
                                                let on_select = props.on_select.clone();
                                                Callback::from(move |_| {
                                                    selected_conid.set(conid);
                                                    detail.set(Some(select_value.clone()));
                                                    on_select.emit(select_value.clone());
                                                })
                                            }}>{"选择"}</button>
                                        </div></td>
                                        <td>
                                            <div class="fw-semibold">{official_security_name(candidate)}</div>
                                            <div class="small text-secondary">{text(candidate, "symbol")}</div>
                                        </td>
                                        <td>{integer(candidate, "conid")}</td>
                                        <td>{text(candidate, "security_type")}</td>
                                        <td>{text(candidate, "currency")}</td>
                                        <td>{security_exchange(candidate)}</td>
                                        <td>{text(candidate, "exchange")}</td>
                                        <td>{text(candidate, "local_symbol")}</td>
                                    </tr>
                                }
                            }).collect::<Html>()}
                        </tbody>
                    </table></div>
                }).unwrap_or_default()
            }
            {
                detail.as_ref().map(|instrument| {
                    let close_detail = {
                        let detail = detail.clone();
                        Callback::from(move |_| detail.set(None))
                    };
                    instrument_detail_modal(instrument, close_detail)
                }).unwrap_or_default()
            }
        </>
    }
}

fn instrument_detail_modal(instrument: &Value, on_close: Callback<MouseEvent>) -> Html {
    let derivatives = instrument
        .get("derivative_security_types")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "—".into());
    html! {
        <>
            <div class="modal fade show d-block" tabindex="-1" role="dialog" aria-modal="true"
                aria-labelledby="instrument-detail-title">
                <div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable">
                    <div class="modal-content shadow">
                        <div class="modal-header">
                            <div>
                                <h2 id="instrument-detail-title" class="modal-title h5 mb-1">
                                    {official_security_name(instrument)}
                                </h2>
                                <div class="text-secondary">{text(instrument, "symbol")}</div>
                            </div>
                            <button type="button" class="btn-close" aria-label="关闭" onclick={on_close.clone()} />
                        </div>
                        <div class="modal-body">
                            <dl class="row mb-0">
                                <dt class="col-sm-3">{"Conid"}</dt><dd class="col-sm-9">{integer(instrument, "conid")}</dd>
                                <dt class="col-sm-3">{"官方完整名称"}</dt><dd class="col-sm-9">{official_security_name(instrument)}</dd>
                                <dt class="col-sm-3">{"Symbol"}</dt><dd class="col-sm-9">{text(instrument, "symbol")}</dd>
                                <dt class="col-sm-3">{"证券类型"}</dt><dd class="col-sm-9">{text(instrument, "security_type")}</dd>
                                <dt class="col-sm-3">{"币种"}</dt><dd class="col-sm-9">{text(instrument, "currency")}</dd>
                                <dt class="col-sm-3">{"主交易所"}</dt><dd class="col-sm-9">{text(instrument, "primary_exchange")}</dd>
                                <dt class="col-sm-3">{"请求路由"}</dt><dd class="col-sm-9">{text(instrument, "exchange")}</dd>
                                <dt class="col-sm-3">{"本地代码"}</dt><dd class="col-sm-9">{text(instrument, "local_symbol")}</dd>
                                <dt class="col-sm-3">{"可用衍生品类型"}</dt><dd class="col-sm-9">{derivatives}</dd>
                            </dl>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" onclick={on_close.clone()}>{"关闭"}</button>
                        </div>
                    </div>
                </div>
            </div>
            <div class="modal-backdrop fade show" onclick={on_close} />
        </>
    }
}
