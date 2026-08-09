use std::{cell::RefCell, rc::Rc};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::value::{array, integer, local_time, text};

#[derive(Properties, PartialEq)]
pub struct BacktestDataPanelProps {
    pub endpoint: String,
    pub strategy_id: String,
    pub instrument: Option<Value>,
    pub timeframe: String,
    pub start: String,
    pub end: String,
    pub outside_rth: bool,
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
    let refresh_generation = use_mut_ref(|| 0_u64);

    let key = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        props.endpoint,
        props.strategy_id,
        props
            .instrument
            .as_ref()
            .and_then(|value| value.get("conid"))
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        props.timeframe,
        props.start,
        props.end,
        props.outside_rth
    );
    {
        let endpoint = props.endpoint.clone();
        let strategy_id = props.strategy_id.clone();
        let instrument = props.instrument.clone();
        let timeframe = props.timeframe.clone();
        let start = props.start.clone();
        let end = props.end.clone();
        let outside_rth = props.outside_rth;
        let coverage = coverage.clone();
        let jobs = jobs.clone();
        let checking = checking.clone();
        let created_job_id = created_job_id.clone();
        let success = success.clone();
        let refresh_generation = refresh_generation.clone();
        let on_ready = props.on_ready.clone();
        let on_error = props.on_error.clone();
        use_effect_with(key, move |_| {
            on_ready.emit(false);
            coverage.set(None);
            created_job_id.set(None);
            success.set(None);
            refresh(
                endpoint.clone(),
                strategy_id.clone(),
                instrument.clone(),
                timeframe.clone(),
                start.clone(),
                end.clone(),
                outside_rth,
                coverage.clone(),
                jobs.clone(),
                checking.clone(),
                refresh_generation.clone(),
                on_ready.clone(),
                on_error.clone(),
            );
            let interval = Interval::new(5_000, move || {
                refresh(
                    endpoint.clone(),
                    strategy_id.clone(),
                    instrument.clone(),
                    timeframe.clone(),
                    start.clone(),
                    end.clone(),
                    outside_rth,
                    coverage.clone(),
                    jobs.clone(),
                    checking.clone(),
                    refresh_generation.clone(),
                    on_ready.clone(),
                    on_error.clone(),
                );
            });
            move || drop(interval)
        });
    }

    let conid = props
        .instrument
        .as_ref()
        .and_then(|value| value.get("conid"))
        .and_then(Value::as_i64);
    let selected_start = local_to_utc(&props.start);
    let selected_end = local_to_utc(&props.end);
    let selected_job = match (conid, selected_start.as_deref(), selected_end.as_deref()) {
        (Some(conid), Some(start), Some(end)) => created_job_id
            .as_ref()
            .and_then(|id| jobs.iter().find(|job| text(job, "job_id") == *id))
            .or_else(|| {
                jobs.iter().find(|job| {
                    job_is_active(job)
                        && job_covers_range(
                            job,
                            conid,
                            &props.timeframe,
                            start,
                            end,
                            props.outside_rth,
                        )
                })
            })
            .or_else(|| {
                jobs.iter().find(|job| {
                    job_matches_range(job, conid, &props.timeframe, start, end, props.outside_rth)
                })
            }),
        _ => None,
    };
    let job_active = selected_job.is_some_and(job_is_active);
    let running_job = jobs
        .iter()
        .find(|job| text(job, "runtime_state") == "running");

    let download = {
        let endpoint = props.endpoint.clone();
        let instrument = props.instrument.clone();
        let timeframe = props.timeframe.clone();
        let start = props.start.clone();
        let end = props.end.clone();
        let outside_rth = props.outside_rth;
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
                        "outside_rth": outside_rth
                    }),
                )
                .await
                {
                    Ok(value) => {
                        let already_verified = value
                            .get("already_verified")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let created_jobs = array(&value, "jobs");
                        let id = value
                            .get("job_id")
                            .and_then(Value::as_str)
                            .or_else(|| {
                                created_jobs
                                    .first()
                                    .and_then(|job| job.get("job_id").and_then(Value::as_str))
                            })
                            .unwrap_or_default()
                            .to_owned();
                        created_job_id.set((!id.is_empty()).then_some(id.clone()));
                        let reused = value
                            .get("reused")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let range_expanded = value
                            .get("range_expanded")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let job_count = value
                            .get("job_count")
                            .and_then(Value::as_u64)
                            .unwrap_or(created_jobs.len() as u64);
                        let message = if already_verified || job_count == 0 {
                            "所选范围已经完成下载验证，无需创建新任务。".to_owned()
                        } else if job_count > 1 {
                            format!(
                                "已按缺失范围创建或复用 {job_count} 个下载任务；已有数据不会重复下载。"
                            )
                        } else {
                            match (reused, range_expanded) {
                                (true, true) => {
                                    format!("已合并到现有下载任务并扩展其缺失范围：{id}")
                                }
                                (true, false) => {
                                    format!("该缺失范围已在队列中，已复用下载任务：{id}")
                                }
                                (false, _) => format!("缺失范围下载任务已创建：{id}"),
                            }
                        };
                        success.set(Some(message));
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
    let ready = coverage.as_ref().is_some_and(coverage_is_ready);
    let missing_ranges = coverage
        .as_ref()
        .map(coverage_missing_ranges)
        .unwrap_or_default();
    let fetched_ranges = coverage
        .as_ref()
        .map(coverage_fetched_ranges)
        .unwrap_or_default();
    let raw_gaps = coverage
        .as_ref()
        .map(|value| array(value, "raw_gaps"))
        .unwrap_or_default();
    let coverage_basis = coverage
        .as_ref()
        .map(coverage_basis_label)
        .unwrap_or_else(|| "—".into());
    let coverage_error = coverage
        .as_ref()
        .and_then(|value| value.get("coverage_error"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);

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
                                let strategy_id = props.strategy_id.clone();
                                let instrument = props.instrument.clone();
                                let timeframe = props.timeframe.clone();
                                let start = props.start.clone();
                                let end = props.end.clone();
                                let outside_rth = props.outside_rth;
                                let coverage = coverage.clone();
                                let jobs = jobs.clone();
                                let checking = checking.clone();
                                let refresh_generation = refresh_generation.clone();
                                let on_ready = props.on_ready.clone();
                                let on_error = props.on_error.clone();
                                Callback::from(move |_| refresh(
                                    endpoint.clone(), strategy_id.clone(), instrument.clone(), timeframe.clone(),
                                    start.clone(), end.clone(), outside_rth, coverage.clone(), jobs.clone(),
                                    checking.clone(), refresh_generation.clone(), on_ready.clone(), on_error.clone()
                                ))
                            }}>
                            {if *checking {
                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"检查中…"}</> }
                            } else { html! { "立即检查" } }}
                        </button>
                    </div>
                    <div class="row g-3 mt-1">
                        <Status label="完整可回测" value={if ready { "是" } else { "否" }}
                            class={if ready { "text-success" } else { "text-danger" }} />
                        <Status label="完整性校验依据" value={coverage_basis}
                            class={if ready { "text-success" } else { "text-warning" }} />
                        <Status label="重叠文件记录数" value={coverage.as_ref().map(|v| integer(v, "row_count")).unwrap_or_else(|| "—".into())}
                            class="" />
                        <Status label="成功抓取范围" value={coverage.as_ref().map(|_| fetched_ranges.len().to_string()).unwrap_or_else(|| "—".into())}
                            class="" />
                        <Status label="未验证完整范围" value={coverage.as_ref().map(|_| missing_ranges.len().to_string()).unwrap_or_else(|| "—".into())}
                            class={if missing_ranges.is_empty() { "" } else { "text-danger" }} />
                        <Status label="重叠文件首 Bar（本地）" value={coverage.as_ref().map(|v| local_time(v, "first_bar_time")).unwrap_or_else(|| "—".into())}
                            class="" />
                        <Status label="重叠文件末 Bar（本地）" value={coverage.as_ref().map(|v| local_time(v, "last_bar_time")).unwrap_or_else(|| "—".into())}
                            class="" />
                        <Status label="交易时段" value={coverage.as_ref().map(coverage_session_kind).unwrap_or_else(|| "—".into())}
                            class="" />
                        <Status label="自然时间缺口" value={coverage.as_ref().map(|_| raw_gaps.len().to_string()).unwrap_or_else(|| "—".into())}
                            class="" />
                    </div>
                    {if !has_files {
                        html! {
                            <div class="alert alert-warning mt-3 mb-0">
                                {"所选范围没有可用于回测的本地 Parquet 文件。请创建下载任务，并等待任务完成。"}
                            </div>
                        }
                    } else if !ready {
                        html! {
                            <div class="alert alert-danger mt-3 mb-0">
                                <div class="fw-semibold">{"无法证明所选范围已经完整下载，回测已禁用。"}</div>
                                <div class="small mt-1">
                                    {coverage_error.as_deref().unwrap_or("请下载未验证的范围并等待任务完成。系统不会再使用少量重叠文件运行范围不完整的回测。")}
                                </div>
                            </div>
                        }
                    } else { Html::default() }}
                    {if !missing_ranges.is_empty() {
                        html! {
                            <div class="table-responsive mt-3">
                                <table class="table table-sm table-warning mb-0">
                                    <thead><tr><th>{"缺失范围起点（本地）"}</th><th>{"缺失范围终点（本地）"}</th></tr></thead>
                                    <tbody>
                                        {missing_ranges.iter().take(20).map(|gap| html! {
                                            <tr>
                                                <td>{local_time(gap, "start")}</td>
                                                <td>{local_time(gap, "end")}</td>
                                            </tr>
                                        }).collect::<Html>()}
                                    </tbody>
                                </table>
                                {if missing_ranges.len() > 20 {
                                    html! { <div class="small text-secondary mt-1">{format!("仅显示前 20 个，共 {} 个缺失范围。", missing_ranges.len())}</div> }
                                } else { Html::default() }}
                            </div>
                        }
                    } else { Html::default() }}
                    {running_job
                        .filter(|running| {
                            selected_job.is_some_and(|selected| {
                                text(selected, "job_id") != text(running, "job_id")
                            })
                        })
                        .map(|running| html! {
                            <div class="alert alert-info mt-3 mb-0">
                                <div class="fw-semibold">{"当前下载 worker 正在处理另一项任务"}</div>
                                <div class="small mt-1 text-break">
                                    {format!(
                                        "Job {}，{} 至 {}，当前进度 {}。所选任务会按队列位置自动开始。",
                                        text(running, "job_id"),
                                        nested_local_time(running, "/request/start"),
                                        nested_local_time(running, "/request/end"),
                                        job_progress(running),
                                    )}
                                </div>
                            </div>
                        })
                        .unwrap_or_default()}
                    <div class="d-flex flex-wrap gap-2 mt-3">
                        <button class="btn btn-primary" disabled={props.instrument.is_none() || *downloading || job_active}
                            onclick={download}>
                            {if *downloading {
                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"创建中…"}</> }
                            } else if let Some(job) = selected_job.filter(|_| job_active) {
                                html! { {job_button_label(job)} }
                            }
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
                                <thead><tr><th>{"Job ID"}</th><th>{"真实状态"}</th><th>{"队列位置"}</th><th>{"请求开始（本地）"}</th>
                                    <th>{"请求结束（本地）"}</th><th>{"下载进度"}</th><th>{"已完成分片"}</th>
                                    <th>{"进度时间（本地）"}</th><th>{"错误"}</th></tr></thead>
                                <tbody><tr>
                                    <td class="strategy-id"><code>{text(job, "job_id")}</code></td>
                                    <td>
                                        <span class={format!("badge {}", job_status_class(job))}>{job_status_label(job)}</span>
                                        <div class="small text-secondary mt-1">{format!("数据库状态：{}", text(job, "state"))}</div>
                                    </td>
                                    <td class="text-nowrap">{queue_position_label(job)}</td>
                                    <td>{nested_local_time(job, "/request/start")}</td>
                                    <td>{nested_local_time(job, "/request/end")}</td>
                                    <td class="text-nowrap">{job_progress(job)}</td>
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
    strategy_id: String,
    instrument: Option<Value>,
    timeframe: String,
    start: String,
    end: String,
    outside_rth: bool,
    coverage: UseStateHandle<Option<Value>>,
    jobs: UseStateHandle<Vec<Value>>,
    checking: UseStateHandle<bool>,
    refresh_generation: Rc<RefCell<u64>>,
    on_ready: Callback<bool>,
    on_error: Callback<String>,
) {
    let request_generation = {
        let mut generation = refresh_generation.borrow_mut();
        *generation += 1;
        *generation
    };
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
        let coverage_result = call_method(
            &endpoint,
            "data.coverage",
            json!({
                "strategy_id": strategy_id,
                "conid": conid,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "outside_rth": outside_rth
            }),
        )
        .await;
        if *refresh_generation.borrow() != request_generation {
            return;
        }
        match coverage_result {
            Ok(value) => {
                let value = value.get("coverage").cloned().unwrap_or(Value::Null);
                let ready = coverage_is_ready(&value);
                on_ready.emit(ready);
                coverage.set(Some(value));
            }
            Err(error) => {
                on_ready.emit(false);
                on_error.emit(error);
            }
        }
        let jobs_result =
            call_method(&endpoint, "data.jobs", json!({"page": 1, "page_size": 200})).await;
        if *refresh_generation.borrow() != request_generation {
            return;
        }
        match jobs_result {
            Ok(value) => jobs.set(array(&value, "jobs")),
            Err(error) => on_error.emit(error),
        }
        checking.set(false);
    });
}

fn local_to_utc(value: &str) -> Option<String> {
    let local = parse_local_datetime(value)?;
    Local
        .from_local_datetime(&local)
        .single()
        .map(|date| date.with_timezone(&Utc).to_rfc3339())
}

fn parse_local_datetime(value: &str) -> Option<NaiveDateTime> {
    ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]
        .into_iter()
        .find_map(|format| NaiveDateTime::parse_from_str(value, format).ok())
}

fn coverage_is_ready(value: &Value) -> bool {
    value
        .get("backtest_ready")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn coverage_missing_ranges(value: &Value) -> Vec<Value> {
    ["unfetched_ranges", "verified_gaps", "missing_sessions"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_array))
        .cloned()
        .unwrap_or_default()
}

fn coverage_fetched_ranges(value: &Value) -> Vec<Value> {
    ["fetched_ranges", "verified_ranges"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_array))
        .cloned()
        .unwrap_or_default()
}

fn coverage_session_kind(value: &Value) -> String {
    match value.get("session_kind").and_then(Value::as_str) {
        Some("regular") => "常规交易时段".into(),
        Some("extended") => "含盘前盘后".into(),
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => "—".into(),
    }
}

fn coverage_basis_label(value: &Value) -> String {
    match value.get("coverage_basis").and_then(Value::as_str) {
        Some("successful_backfill_ranges") => "IBKR 成功抓取区间".into(),
        Some("no_data") => "没有本地数据".into(),
        Some("incomplete") => "尚未完整抓取".into(),
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => "—".into(),
    }
}

pub(super) fn nested_local_time(value: &Value, pointer: &str) -> String {
    format_local_timestamp(value.pointer(pointer).and_then(Value::as_str))
}

fn format_local_timestamp(value: Option<&str>) -> String {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|date| {
            date.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "—".into())
}

pub(super) fn job_progress_percent(job: &Value) -> Option<f64> {
    let parse = |pointer: &str| {
        job.pointer(pointer)
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
    };
    let (Some(start), Some(cursor), Some(end)) = (
        parse("/request/start"),
        parse("/cursor_time"),
        parse("/request/end"),
    ) else {
        return None;
    };
    let total = (end - start).num_milliseconds();
    if total <= 0 {
        return None;
    }
    let completed = (cursor - start).num_milliseconds().clamp(0, total);
    Some(completed as f64 * 100.0 / total as f64)
}

pub(super) fn job_progress(job: &Value) -> String {
    job_progress_percent(job)
        .map(|progress| format!("{progress:.1}%"))
        .unwrap_or_else(|| "—".into())
}

pub(super) fn job_is_active(job: &Value) -> bool {
    matches!(
        text(job, "state").as_str(),
        "pending" | "running" | "retrying"
    )
}

pub(super) fn job_is_cancellable(job: &Value) -> bool {
    matches!(text(job, "state").as_str(), "pending" | "retrying")
}

pub(super) fn job_status_label(job: &Value) -> String {
    match text(job, "runtime_state").as_str() {
        "running" => "正在下载".into(),
        "queued" => "排队中".into(),
        "waiting_for_ibkr" => "等待 IBKR 就绪".into(),
        "pending" => "待处理".into(),
        "retrying" => "重试中".into(),
        "completed" => "已完成".into(),
        "failed" => "失败".into(),
        "cancelled" => "已取消".into(),
        _ => text(job, "state"),
    }
}

pub(super) fn job_status_class(job: &Value) -> &'static str {
    match text(job, "runtime_state").as_str() {
        "running" => "bg-primary",
        "queued" => "bg-warning text-dark",
        "waiting_for_ibkr" => "bg-warning text-dark",
        "pending" => "bg-secondary",
        "retrying" => "bg-warning text-dark",
        "completed" => "bg-success",
        "failed" => "bg-danger",
        "cancelled" => "bg-secondary",
        _ => "bg-secondary",
    }
}

pub(super) fn queue_position_label(job: &Value) -> String {
    let Some(position) = job.get("queue_position").and_then(Value::as_u64) else {
        return "—".into();
    };
    let ahead = job
        .get("jobs_ahead")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| position.saturating_sub(1));
    if ahead == 0 {
        "第 1 位（当前任务）".into()
    } else {
        format!("第 {position} 位（前方 {ahead} 项）")
    }
}

fn job_button_label(job: &Value) -> String {
    match text(job, "runtime_state").as_str() {
        "running" => "正在下载".into(),
        "queued" => job
            .get("queue_position")
            .and_then(Value::as_u64)
            .map(|position| format!("排队中（第 {position} 位）"))
            .unwrap_or_else(|| "排队中".into()),
        "waiting_for_ibkr" => "等待 IBKR 就绪".into(),
        _ => "下载任务进行中".into(),
    }
}

fn job_covers_range(
    job: &Value,
    conid: i64,
    timeframe: &str,
    start: &str,
    end: &str,
    outside_rth: bool,
) -> bool {
    if !job_matches_scope(job, conid, timeframe, outside_rth) {
        return false;
    }
    let parse =
        |value: Option<&str>| value.and_then(|value| DateTime::parse_from_rfc3339(value).ok());
    let (Some(job_start), Some(job_end), Some(start), Some(end)) = (
        parse(job.pointer("/request/start").and_then(Value::as_str)),
        parse(job.pointer("/request/end").and_then(Value::as_str)),
        parse(Some(start)),
        parse(Some(end)),
    ) else {
        return false;
    };
    job_start <= start && job_end >= end
}

fn job_matches_scope(job: &Value, conid: i64, timeframe: &str, outside_rth: bool) -> bool {
    text(job, "job_type") == "historical_backfill"
        && job
            .pointer("/request/contract/conid")
            .and_then(Value::as_i64)
            == Some(conid)
        && job.pointer("/request/timeframe").and_then(Value::as_str) == Some(timeframe)
        && job.pointer("/request/outside_rth").and_then(Value::as_bool) == Some(outside_rth)
}

fn job_matches_range(
    job: &Value,
    conid: i64,
    timeframe: &str,
    start: &str,
    end: &str,
    outside_rth: bool,
) -> bool {
    job_matches_scope(job, conid, timeframe, outside_rth)
        && job
            .pointer("/request/start")
            .and_then(Value::as_str)
            .is_some_and(|value| same_instant(value, start))
        && job
            .pointer("/request/end")
            .and_then(Value::as_str)
            .is_some_and(|value| same_instant(value, end))
}

fn same_instant(left: &str, right: &str) -> bool {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{coverage_is_ready, job_covers_range, job_matches_range, job_progress};

    #[test]
    fn coverage_requires_explicit_backtest_ready_proof() {
        assert!(coverage_is_ready(&json!({
            "backtest_ready": true
        })));
        assert!(!coverage_is_ready(&json!({
            "files": [{"path": "one-overlapping-file.parquet"}],
            "covered": true
        })));
        assert!(!coverage_is_ready(&json!({
            "backtest_ready": false
        })));
    }

    #[test]
    fn data_job_must_match_the_entire_requested_range() {
        let job = json!({
            "job_type": "historical_backfill",
            "request": {
                "contract": {"conid": 272093},
                "timeframe": "5s",
                "start": "2025-07-26T09:51:00Z",
                "end": "2026-08-02T09:51:00Z",
                "outside_rth": false
            }
        });
        assert!(job_matches_range(
            &job,
            272093,
            "5s",
            "2025-07-26T17:51:00+08:00",
            "2026-08-02T17:51:00+08:00",
            false
        ));
        assert!(!job_matches_range(
            &job,
            272093,
            "5s",
            "2026-07-26T09:51:00Z",
            "2026-08-02T09:51:00Z",
            false
        ));
        assert!(!job_matches_range(
            &job,
            272093,
            "5s",
            "2025-07-26T09:51:00Z",
            "2026-08-02T09:51:00Z",
            true
        ));
    }

    #[test]
    fn data_job_progress_uses_the_full_requested_range() {
        let job = json!({
            "request": {
                "start": "2025-01-01T00:00:00Z",
                "end": "2026-01-01T00:00:00Z"
            },
            "cursor_time": "2025-07-02T12:00:00Z"
        });
        assert_eq!(job_progress(&job), "50.0%");
    }

    #[test]
    fn merged_larger_job_covers_the_selected_range() {
        let job = json!({
            "job_type": "historical_backfill",
            "state": "pending",
            "request": {
                "contract": {"conid": 272093},
                "timeframe": "5s",
                "start": "2025-07-26T09:51:00Z",
                "end": "2026-08-02T11:07:00Z",
                "outside_rth": false
            }
        });
        assert!(job_covers_range(
            &job,
            272093,
            "5s",
            "2026-07-26T11:07:00Z",
            "2026-08-02T11:07:00Z",
            false,
        ));
        assert!(!job_covers_range(
            &job,
            272093,
            "5s",
            "2026-07-26T11:07:00Z",
            "2026-08-02T12:07:00Z",
            false,
        ));
    }
}
