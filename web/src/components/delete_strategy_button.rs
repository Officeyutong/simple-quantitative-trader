use serde_json::json;
use yew::prelude::*;

use super::MutationRequest;

#[derive(Properties, PartialEq)]
pub struct DeleteStrategyButtonProps {
    pub strategy_id: String,
    pub strategy_name: String,
    pub disabled: bool,
    pub on_mutation: Callback<MutationRequest>,
}

#[function_component(DeleteStrategyButton)]
pub fn delete_strategy_button(props: &DeleteStrategyButtonProps) -> Html {
    let confirming = use_state(|| false);
    let loading = use_state(|| false);

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
            <button
                class="btn btn-sm btn-danger"
                disabled={props.disabled || *loading}
                onclick={{
                    let confirming = confirming.clone();
                    Callback::from(move |_| confirming.set(true))
                }}
            >
                {"删除策略"}
            </button>
            {
                if *confirming {
                    html! {
                        <>
                            <div class="modal fade show d-block" tabindex="-1" role="dialog"
                                aria-modal="true" aria-labelledby="delete-strategy-title">
                                <div class="modal-dialog modal-dialog-centered">
                                    <div class="modal-content shadow">
                                        <div class="modal-header bg-danger text-white">
                                            <h2 id="delete-strategy-title" class="modal-title h5">
                                                {"确认删除策略"}
                                            </h2>
                                            <button type="button" class="btn-close btn-close-white"
                                                aria-label="关闭" disabled={*loading}
                                                onclick={close.clone()} />
                                        </div>
                                        <div class="modal-body">
                                            <p>{"此操作会永久删除策略定义、执行配置、信号评估、执行动作和绩效快照。"}</p>
                                            <dl class="mb-0">
                                                <dt>{"名称"}</dt>
                                                <dd class="text-break">{props.strategy_name.clone()}</dd>
                                                <dt>{"完整策略 UUID"}</dt>
                                                <dd class="strategy-id text-break"><code>{props.strategy_id.clone()}</code></dd>
                                            </dl>
                                            <p class="text-secondary small mb-0">
                                                {"已有订单与成交记录将保留。仅允许删除已停止且已关闭自动执行的策略。"}
                                            </p>
                                        </div>
                                        <div class="modal-footer">
                                            <button class="btn btn-secondary" disabled={*loading}
                                                onclick={close.clone()}>{"取消"}</button>
                                            <button
                                                class="btn btn-danger"
                                                disabled={*loading}
                                                onclick={{
                                                    let callback = props.on_mutation.clone();
                                                    let strategy_id = props.strategy_id.clone();
                                                    let loading = loading.clone();
                                                    let confirming = confirming.clone();
                                                    Callback::from(move |_| {
                                                        loading.set(true);
                                                        let loading = loading.clone();
                                                        let confirming = confirming.clone();
                                                        callback.emit(MutationRequest {
                                                            method: "strategy.delete".into(),
                                                            params: json!({
                                                                "strategy_id": strategy_id,
                                                                "confirm": true
                                                            }),
                                                            on_complete: Callback::from(move |_| {
                                                                loading.set(false);
                                                                confirming.set(false);
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
                                                                {"删除中…"}
                                                            </>
                                                        }
                                                    } else {
                                                        html! { "永久删除" }
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
