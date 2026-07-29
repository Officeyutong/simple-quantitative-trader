use chrono::{Local, NaiveDateTime, TimeZone, Utc};
use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::value::{array, integer, local_time, text};

#[derive(Properties, PartialEq)]
pub struct BacktestDataPanelProps {
    pub endpoint: String,
    pub instrument: Option<Value>,
    pub timeframe: String,
    pub start: String,
    pub end: String,
    pub on_ready: Callback<bool>,
    pub on_error: Callback<String>,
}

#[function_component(BacktestDataPanel)]
pub fn backtest_data_panel(props: &BacktestDataPanelProps) -> Html {
    let coverage = use_state(|| None::<Value>);
    let jobs = use_state(Vec::<Value>::new);
    let checking = use_state(|| false);
    let downloading = use_state(|| false);
    let created_job_id = use_state(|| None::<String>);
    let success = use_state(|| None::<String>);

    let key = format!(
        "{}|{}|{}|{}|{}",
        props.endpoint,
        props
            .instrument
            .as_ref()
            .and_then(|value| value.get("conid"))
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        props.timeframe,
        props.start,
        props.end
    );
    {
        let endpoint = props.endpoint.clone();
        let instrument = props.instrument.clone();
        let timeframe = props.timeframe.clone();
        let start = props.start.clone();
        let end = props.end.clone();
        let coverage = coverage.clone();
        let jobs = jobs.clone();
        let checking = checking.clone();
        let on_ready = props.on_ready.clone();
        let on_error = props.on_error.clone();
        use_effect_with(key, move |_| {
            on_ready.emit(false);
            coverage.set(None);
            refresh(
                endpoint.clone(),
                instrument.clone(),
                timeframe.clone(),
                start.clone(),
                end.clone(),
                coverage.clone(),
                jobs.clone(),
                checking.clone(),
                on_ready.clone(),
                on_error.clone(),
            );
            let interval = Interval::new(5_000, move || {
                refresh(
                    endpoint.clone(),
                    instrument.clone(),
                    timeframe.clone(),
                    start.clone(),
                    end.clone(),
                    coverage.clone(),
                    jobs.clone(),
                    checking.clone(),
                    on_ready.clone(),
                    on_error.clone(),
                );
            });
            move || drop(interval)
        });
    }

    let selected_job = created_job_id
        .as_ref()
        .and_then(|id| jobs.iter().find(|job| text(job, "job_id") == *id))
        .or_else(|| {
            let conid = props
                .instrument
                .as_ref()
                .and_then(|value| value.get("conid"))
                .and_then(Value::as_i64)?;
            jobs.iter().find(|job| {
                job.pointer("/request/contract/conid")
                    .and_then(Value::as_i64)
                    == Some(conid)
                    && job.pointer("/request/timeframe").and_then(Value::as_str)
                        == Some(props.timeframe.as_str())
            })
        });
    let job_active = selected_job
        .map(|job| {
            matches!(
                text(job, "state").as_str(),
                "pending" | "running" | "retrying"
            )
        })
        .unwrap_or(false);

    let download = {
        let endpoint = props.endpoint.clone();
        let instrument = props.instrument.clone();
        let timeframe = props.timeframe.clone();
        let start = props.start.clone();
        let end = props.end.clone();
        let downloading = downloading.clone();
        let created_job_id = created_job_id.clone();
        let success = success.clone();
        let on_error = props.on_error.clone();
        Callback::from(move |_| {
            let Some(contract) = instrument.as_ref().and_then(contract_request) else {
                on_error.emit("请先选择具有完整合约信息的证券".into());
                return;
            };
            let Some(start) = local_to_utc(&start) else {
                on_error.emit("历史数据开始时间无效".into());
                return;
            };
            let Some(end) = local_to_utc(&end) else {
                on_error.emit("历史数据结束时间无效".into());
                return;
            };
            downloading.set(true);
            success.set(None);
            let endpoint = endpoint.clone();
            let timeframe = timeframe.clone();
            let downloading = downloading.clone();
            let created_job_id = created_job_id.clone();
            let success = success.clone();
            let on_error = on_error.clone();
            spawn_local(async move {
                match call_method(
                    &endpoint,
                    "data.backfill",
                    json!({
                        "contract": contract,
                        "timeframe": timeframe,
                        "start": start,
                        "end": end,
                        "outside_rth": false
                    }),
                )
                .await
                {
                    Ok(value) => {
                        let id = text(&value, "job_id");
                        created_job_id.set(Some(id.clone()));
                        success.set(Some(format!("历史数据下载任务已创建：{id}")));
                    }
                    Err(error) => on_error.emit(error),
                }
                downloading.set(false);
            });
        })
    };

    let cancel = selected_job.map(|job| {
        let endpoint = props.endpoint.clone();
        let job_id = text(job, "job_id");
        let on_error = props.on_error.clone();
        let success = success.clone();
        Callback::from(move |_| {
            let endpoint = endpoint.clone();
            let job_id = job_id.clone();
            let on_error = on_error.clone();
            let success = success.clone();
            spawn_local(async move {
                match call_method(&endpoint, "data.job.cancel", json!({"job_id": job_id})).await {
                    Ok(_) => success.set(Some("下载任务已取消".into())),
                    Err(error) => on_error.emit(error),
                }
            });
        })
    });

    let has_files = coverage
        .as_ref()
        .and_then(|value| value.get("files"))
        .and_then(Value::as_array)
        .is_some_and(|files| !files.is_empty());
    let covered = coverage
        .as_ref()
        .and_then(|value| value.get("covered"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let gaps = coverage
        .as_ref()
        .map(|value| array(value, "raw_gaps"))
        .unwrap_or_default();

    html! {
        <div class="col-12">
            <div class="card border-primary">
                <div class="card-body">
                    <div class="d-flex flex-wrap justify-content-between align-items-center gap-2">
                        <div>
                            <h3 class="h6 mb-1">{"3. 准备本地历史数据"}</h3>
                            <p class="small text-secondary mb-0">
                                {"页面每 5 秒检查一次 Parquet 覆盖和下载任务状态。历史数据由 daemon 从 IBKR 下载。"}
                            </p>
                        </div>
                        <button class="btn btn-sm btn-outline-primary" disabled={*checking}
                            onclick={{
                                let endpoint = props.endpoint.clone();
                                let instrument = props.instrument.clone();
                                let timeframe = props.timeframe.clone();
                                let start = props.start.clone();
                                let end = props.end.clone();
                                let coverage = coverage.clone();
                                let jobs = jobs.clone();
                                let checking = checking.clone();
                                let on_ready = props.on_ready.clone();
                                let on_error = props.on_error.clone();
                                Callback::from(move |_| refresh(
                                    endpoint.clone(), instrument.clone(), timeframe.clone(),
                                    start.clone(), end.clone(), coverage.clone(), jobs.clone(),
                                    checking.clone(), on_ready.clone(), on_error.clone()
                                ))
                            }}>
                            {if *checking {
                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"检查中…"}</> }
                            } else { html! { "立即检查" } }}
                        </button>
                    </div>
                    <div class="row g-3 mt-1">
                        <Status label="回测可用" value={if has_files { "是" } else { "否" }}
                            class={if has_files { "text-success" } else { "text-danger" }} />
                        <Status label="完整连续覆盖" value={if covered { "是" } else { "否" }}
                            class={if covered { "text-success" } else { "text-warning" }} />
                        <Status label="本地 Bar 数" value={coverage.as_ref().map(|v| integer(v, "row_count")).unwrap_or_else(|| "—".into())}
                            class="" />
                        <Status label="原始缺口数" value={gaps.len().to_string()} class="" />
                    </div>
                    {if !has_files {
                        html! {
                            <div class="alert alert-warning mt-3 mb-0">
                                {"所选范围没有可用于回测的本地 Parquet 文件。请创建下载任务，并等待任务完成。"}
                            </div>
                        }
                    } else if !covered {
                        html! {
                            <div class="alert alert-info mt-3 mb-0">
                                {"已有 Bar 可用于回测，但检测到原始时间缺口。周末、休市和非交易时段也会被计为缺口，请结合交易日历判断。"}
                            </div>
                        }
                    } else { Html::default() }}
                    <div class="d-flex flex-wrap gap-2 mt-3">
                        <button class="btn btn-primary" disabled={props.instrument.is_none() || *downloading || job_active}
                            onclick={download}>
                            {if *downloading {
                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"创建中…"}</> }
                            } else if job_active { html! { "下载任务进行中" } }
                            else { html! { "下载所选范围历史数据" } }}
                        </button>
                        {if job_active {
                            html! { <button class="btn btn-outline-danger" onclick={cancel.unwrap()}>{"取消任务"}</button> }
                        } else { Html::default() }}
                    </div>
                    {success.as_ref().map(|message| html! {
                        <div class="alert alert-success mt-3 mb-0 text-break">{message}</div>
                    }).unwrap_or_default()}
                    {selected_job.map(|job| html! {
                        <div class="table-responsive mt-3">
                            <table class="table table-sm mb-0">
                                <thead><tr><th>{"Job ID"}</th><th>{"状态"}</th><th>{"已完成分片"}</th>
                                    <th>{"进度时间（本地）"}</th><th>{"错误"}</th></tr></thead>
                                <tbody><tr>
                                    <td class="strategy-id"><code>{text(job, "job_id")}</code></td>
                                    <td><span class="badge bg-secondary">{text(job, "state")}</span></td>
                                    <td>{integer(job, "completed_slices")}</td>
                                    <td>{local_time(job, "cursor_time")}</td>
                                    <td class="text-break">{text(job, "last_error")}</td>
                                </tr></tbody>
                            </table>
                        </div>
                    }).unwrap_or_default()}
                </div>
            </div>
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct StatusProps {
    label: &'static str,
    value: String,
    class: &'static str,
}

#[function_component(Status)]
fn status(props: &StatusProps) -> Html {
    html! {
        <div class="col-6 col-lg-3">
            <div class="small text-secondary">{props.label}</div>
            <div class={classes!("fw-semibold", props.class)}>{props.value.clone()}</div>
        </div>
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh(
    endpoint: String,
    instrument: Option<Value>,
    timeframe: String,
    start: String,
    end: String,
    coverage: UseStateHandle<Option<Value>>,
    jobs: UseStateHandle<Vec<Value>>,
    checking: UseStateHandle<bool>,
    on_ready: Callback<bool>,
    on_error: Callback<String>,
) {
    let Some(conid) = instrument
        .as_ref()
        .and_then(|value| value.get("conid"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
    else {
        on_ready.emit(false);
        return;
    };
    let (Some(start), Some(end)) = (local_to_utc(&start), local_to_utc(&end)) else {
        on_ready.emit(false);
        return;
    };
    checking.set(true);
    spawn_local(async move {
        match call_method(
            &endpoint,
            "data.coverage",
            json!({"conid": conid, "timeframe": timeframe, "start": start, "end": end}),
        )
        .await
        {
            Ok(value) => {
                let value = value.get("coverage").cloned().unwrap_or(Value::Null);
                let ready = value
                    .get("files")
                    .and_then(Value::as_array)
                    .is_some_and(|files| !files.is_empty());
                on_ready.emit(ready);
                coverage.set(Some(value));
            }
            Err(error) => {
                on_ready.emit(false);
                on_error.emit(error);
            }
        }
        match call_method(&endpoint, "data.jobs", json!({})).await {
            Ok(value) => jobs.set(array(&value, "jobs")),
            Err(error) => on_error.emit(error),
        }
        checking.set(false);
    });
}

fn local_to_utc(value: &str) -> Option<String> {
    let local = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M").ok()?;
    Local
        .from_local_datetime(&local)
        .single()
        .map(|date| date.with_timezone(&Utc).to_rfc3339())
}

fn contract_request(value: &Value) -> Option<Value> {
    let conid = value.get("conid")?.as_i64()?;
    Some(json!({
        "conid": conid,
        "symbol": value.get("symbol").and_then(Value::as_str).unwrap_or_default(),
        "security_type": value.get("security_type").and_then(Value::as_str).unwrap_or("STK"),
        "currency": value.get("currency").and_then(Value::as_str).unwrap_or_default(),
        "exchange": value.get("exchange").and_then(Value::as_str).unwrap_or("SMART"),
        "primary_exchange": value.get("primary_exchange").and_then(Value::as_str).unwrap_or_default(),
        "local_symbol": value.get("local_symbol").and_then(Value::as_str).unwrap_or_default(),
        "description": value.get("description").and_then(Value::as_str).unwrap_or_default(),
        "derivative_security_types": value.get("derivative_security_types")
            .cloned().unwrap_or_else(|| json!([]))
    }))
}
