use serde_json::json;
use yew::prelude::*;

use super::MutationRequest;

#[derive(Properties, PartialEq)]
pub struct RenameStrategyButtonProps {
    pub strategy_id: String,
    pub strategy_name: String,
    pub on_mutation: Callback<MutationRequest>,
}

#[function_component(RenameStrategyButton)]
pub fn rename_strategy_button(props: &RenameStrategyButtonProps) -> Html {
    let editing = use_state(|| false);
    let name = use_state(|| props.strategy_name.clone());
    let loading = use_state(|| false);

    let close = {
        let editing = editing.clone();
        let loading = loading.clone();
        Callback::from(move |_| {
            if !*loading {
                editing.set(false);
            }
        })
    };
    let trimmed = name.trim();
    let unchanged = trimmed == props.strategy_name;

    html! {
        <>
            <button
                class="btn btn-sm btn-outline-primary"
                disabled={*loading}
                onclick={{
                    let editing = editing.clone();
                    let name = name.clone();
                    let current_name = props.strategy_name.clone();
                    Callback::from(move |_| {
                        name.set(current_name.clone());
                        editing.set(true);
                    })
                }}
            >
                {"重命名"}
            </button>
            {
                if *editing {
                    html! {
                        <>
                            <div class="modal fade show d-block" tabindex="-1" role="dialog"
                                aria-modal="true" aria-labelledby="rename-strategy-title">
                                <div class="modal-dialog modal-dialog-centered">
                                    <div class="modal-content shadow">
                                        <div class="modal-header">
                                            <h2 id="rename-strategy-title" class="modal-title h5">
                                                {"重命名策略"}
                                            </h2>
                                            <button type="button" class="btn-close" aria-label="关闭"
                                                disabled={*loading} onclick={close.clone()} />
                                        </div>
                                        <div class="modal-body">
                                            <label class="form-label" for="rename-strategy-name">
                                                {"新名称"}
                                            </label>
                                            <input
                                                id="rename-strategy-name"
                                                class="form-control"
                                                maxlength="200"
                                                value={(*name).clone()}
                                                oninput={{
                                                    let name = name.clone();
                                                    Callback::from(move |event: InputEvent| {
                                                        let input: web_sys::HtmlInputElement =
                                                            event.target_unchecked_into();
                                                        name.set(input.value());
                                                    })
                                                }}
                                            />
                                            <div class="form-text">
                                                {"名称必须非空且全局唯一。重命名不会改变策略 UUID、配置、订单或历史绩效。"}
                                            </div>
                                        </div>
                                        <div class="modal-footer">
                                            <button class="btn btn-secondary" disabled={*loading}
                                                onclick={close.clone()}>{"取消"}</button>
                                            <button
                                                class="btn btn-primary"
                                                disabled={*loading || trimmed.is_empty() || unchanged}
                                                onclick={{
                                                    let callback = props.on_mutation.clone();
                                                    let strategy_id = props.strategy_id.clone();
                                                    let name = name.clone();
                                                    let loading = loading.clone();
                                                    let editing = editing.clone();
                                                    Callback::from(move |_| {
                                                        loading.set(true);
                                                        let loading = loading.clone();
                                                        let editing = editing.clone();
                                                        callback.emit(MutationRequest {
                                                            method: "strategy.rename".into(),
                                                            params: json!({
                                                                "strategy_id": strategy_id,
                                                                "name": name.trim()
                                                            }),
                                                            on_complete: Callback::from(move |_| {
                                                                loading.set(false);
                                                                editing.set(false);
                                                            }),
                                                        });
                                                    })
                                                }}
                                            >
                                                {
                                                    if *loading {
                                                        html! {
                                                            <>
                                                                <span class="spinner-border spinner-border-sm me-2"
                                                                    aria-hidden="true" />
                                                                {"保存中…"}
                                                            </>
                                                        }
                                                    } else {
                                                        html! { "保存名称" }
                                                    }
                                                }
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="modal-backdrop fade show" onclick={close.clone()} />
                        </>
                    }
                } else {
                    Html::default()
                }
            }
        </>
    }
}
