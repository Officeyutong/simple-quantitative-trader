use serde_json::json;
use yew::prelude::*;

use super::MutationRequest;

#[derive(Properties, PartialEq)]
pub struct ActionButtonProps {
    pub label: &'static str,
    pub class: &'static str,
    pub method: &'static str,
    pub strategy_id: String,
    #[prop_or(false)]
    pub confirm: bool,
    #[prop_or(false)]
    pub disabled: bool,
    pub on_mutation: Callback<MutationRequest>,
}

#[function_component(ActionButton)]
pub fn action_button(props: &ActionButtonProps) -> Html {
    let loading = use_state(|| false);
    let request = if props.confirm {
        json!({"strategy_id": props.strategy_id, "confirm": true})
    } else {
        json!({"strategy_id": props.strategy_id})
    };
    html! {
        <button
            class={classes!("btn", "btn-sm", props.class)}
            disabled={props.disabled || props.strategy_id == "—" || *loading}
            onclick={{
                let callback = props.on_mutation.clone();
                let method = props.method.to_owned();
                let loading = loading.clone();
                Callback::from(move |_| {
                    loading.set(true);
                    let loading = loading.clone();
                    callback.emit(MutationRequest {
                        method: method.clone(),
                        params: request.clone(),
                        on_complete: Callback::from(move |_| loading.set(false)),
                    });
                })
            }}
        >
            {
                if *loading {
                    html! {
                        <>
                            <span class="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                            {"处理中…"}
                        </>
                    }
                } else {
                    html! { props.label }
                }
            }
        </button>
    }
}
