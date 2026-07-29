use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{error_modal::ErrorModal, value::array};

#[derive(Properties, PartialEq)]
pub struct LogsPageProps {
    pub endpoint: String,
}

#[function_component(LogsPage)]
pub fn logs_page(props: &LogsPageProps) -> Html {
    let entries = use_state(Vec::<Value>::new);
    let cursor = use_state(|| 0_u64);
    let paused = use_state(|| false);
    let error = use_state(|| None::<String>);

    {
        let endpoint = props.endpoint.clone();
        let entries = entries.clone();
        let cursor = cursor.clone();
        let paused = paused.clone();
        let error = error.clone();
        use_effect_with(endpoint.clone(), move |_| {
            load_logs(
                endpoint.clone(),
                entries.clone(),
                cursor.clone(),
                error.clone(),
            );
            let interval = Interval::new(1_000, move || {
                if !*paused {
                    load_logs(
                        endpoint.clone(),
                        entries.clone(),
                        cursor.clone(),
                        error.clone(),
                    );
                }
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
            <section class="card shadow-sm"><div class="card-body">
                <div class="d-flex flex-wrap justify-content-between align-items-center gap-2 mb-3">
                    <div>
                        <h2 class="h5 mb-1">{"Daemon 实时日志"}</h2>
                        <div class="text-secondary">{"通过当前 JSON-RPC WebSocket 每秒增量读取；服务端保留最近 2000 行。"}</div>
                    </div>
                    <div class="d-flex gap-2">
                        <button class="btn btn-outline-secondary" onclick={{
                            let paused = paused.clone();
                            Callback::from(move |_| paused.set(!*paused))
                        }}>{if *paused { "继续" } else { "暂停" }}</button>
                        <button class="btn btn-outline-danger" onclick={{
                            let entries = entries.clone();
                            Callback::from(move |_| entries.set(Vec::new()))
                        }}>{"清空屏幕"}</button>
                    </div>
                </div>
                <pre class="bg-dark text-light rounded p-3 mb-0 overflow-auto" style="height: 65vh; white-space: pre-wrap; word-break: break-word;">{
                    entries.iter()
                        .filter_map(|entry| entry.get("line").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n")
                }</pre>
            </div></section>
        </>
    }
}

fn load_logs(
    endpoint: String,
    entries: UseStateHandle<Vec<Value>>,
    cursor: UseStateHandle<u64>,
    error: UseStateHandle<Option<String>>,
) {
    let after_cursor = *cursor;
    spawn_local(async move {
        match call_method(
            &endpoint,
            "logs.tail",
            json!({"after_cursor": after_cursor, "limit": 500}),
        )
        .await
        {
            Ok(value) => {
                let additions = array(&value, "entries");
                if let Some(next) = additions
                    .last()
                    .and_then(|entry| entry.get("cursor"))
                    .and_then(Value::as_u64)
                {
                    cursor.set(next);
                }
                if !additions.is_empty() {
                    let mut combined = (*entries).clone();
                    combined.extend(additions);
                    if combined.len() > 1_000 {
                        combined.drain(..combined.len() - 1_000);
                    }
                    entries.set(combined);
                }
            }
            Err(message) => error.set(Some(message)),
        }
    });
}
