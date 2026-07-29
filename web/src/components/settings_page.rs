use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use super::error_modal::ErrorModal;
use crate::api::{save_rpc_endpoint, validate_rpc_endpoint};

#[derive(Properties, PartialEq)]
pub struct SettingsPageProps {
    pub endpoint: String,
    pub endpoint_handle: UseStateHandle<String>,
}

#[function_component(SettingsPage)]
pub fn settings_page(props: &SettingsPageProps) -> Html {
    let draft = use_state(|| props.endpoint.clone());
    let message = use_state(|| None::<Result<String, String>>);
    let save = {
        let draft = draft.clone();
        let message = message.clone();
        let endpoint_handle = props.endpoint_handle.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            match save_rpc_endpoint(&draft) {
                Ok(endpoint) => {
                    endpoint_handle.set(endpoint.clone());
                    message.set(Some(Ok(format!(
                        "已保存并切换到 {endpoint}，正在重新连接。"
                    ))));
                }
                Err(error) => message.set(Some(Err(error))),
            }
        })
    };
    let validation_hint = validate_rpc_endpoint(&draft).err();
    html! {
        <>
        <ErrorModal
            message={message.as_ref().and_then(|value| value.as_ref().err()).cloned()}
            on_close={{
                let message = message.clone();
                Callback::from(move |_| message.set(None))
            }}
        />
        <div class="row"><div class="col-12 col-xl-8">
            <div class="card shadow-sm"><div class="card-body">
                <h2 class="h5">{"Daemon RPC 地址"}</h2>
                <p class="text-secondary">
                    {"地址保存在当前浏览器的 LocalStorage 中。保存后，所有页面读取和操作都会立即使用新地址。"}
                </p>
                <form onsubmit={save}>
                    <label class="form-label" for="rpc-endpoint">{"WebSocket URL"}</label>
                    <input
                        id="rpc-endpoint"
                        class={classes!("form-control", validation_hint.is_some().then_some("is-invalid"))}
                        type="text" autocomplete="off" spellcheck="false"
                        placeholder="ws://192.168.1.10:8787"
                        value={(*draft).clone()}
                        oninput={{
                            let draft = draft.clone();
                            let message = message.clone();
                            Callback::from(move |event: InputEvent| {
                                let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                draft.set(input.value());
                                message.set(None);
                            })
                        }}
                    />
                    {validation_hint.map(|error| html! { <div class="invalid-feedback">{error}</div> }).unwrap_or_default()}
                    <div class="form-text">{"示例：ws://127.0.0.1:8787 或 wss://quant.example.com/rpc。"}</div>
                    <button class="btn btn-primary mt-3" type="submit" disabled={validate_rpc_endpoint(&draft).is_err()}>
                        {"保存并连接"}
                    </button>
                </form>
                {
                    match &*message {
                        Some(Ok(text)) => html! { <div class="alert alert-success mt-3 mb-0">{text}</div> },
                        Some(Err(_)) => Html::default(),
                        None => Html::default(),
                    }
                }
            </div></div>
            <Alert style={Color::Warning} class="mt-3">
                {"跨机器连接请使用 SSH 隧道，或带 TLS 和身份认证的 WebSocket 反向代理。不要把交易 RPC 直接暴露到公网。"}
            </Alert>
        </div></div>
        </>
    }
}
