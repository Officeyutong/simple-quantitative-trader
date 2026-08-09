use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    backtest_data_panel::{
        job_is_cancellable, job_progress, job_progress_percent, job_status_class, job_status_label,
        nested_local_time, queue_position_label,
    },
    error_modal::ErrorModal,
    pagination::{Pagination, load_saved_page, save_page},
    value::{array, integer, local_time, text},
};

const PAGE_SIZE: usize = 25;
const PAGE_STORAGE_KEY: &str = "quant-trader.download-jobs-page";

#[derive(Properties, PartialEq)]
pub struct DownloadJobsPageProps {
    pub endpoint: String,
}

#[function_component(DownloadJobsPage)]
pub fn download_jobs_page(props: &DownloadJobsPageProps) -> Html {
    let jobs = use_state(Vec::<Value>::new);
    let queue = use_state(|| Value::Null);
    let page = use_state(|| load_saved_page(PAGE_STORAGE_KEY));
    let total_pages = use_state(|| 1_usize);
    let total_items = use_state(|| 0_usize);
    let loading = use_state(|| false);
    let cancelling = use_state(|| None::<String>);
    let error = use_state(|| None::<String>);
    let success = use_state(|| None::<String>);

    {
        let endpoint = props.endpoint.clone();
        let current_page = *page;
        let jobs = jobs.clone();
        let queue = queue.clone();
        let total_pages = total_pages.clone();
        let total_items = total_items.clone();
        let loading = loading.clone();
        let error = error.clone();
        use_effect_with((endpoint.clone(), current_page), move |_| {
            load_jobs(
                endpoint.clone(),
                current_page,
                jobs.clone(),
                queue.clone(),
                total_pages.clone(),
                total_items.clone(),
                loading.clone(),
                error.clone(),
            );
            let interval = Interval::new(5_000, move || {
                load_jobs(
                    endpoint.clone(),
                    current_page,
                    jobs.clone(),
                    queue.clone(),
                    total_pages.clone(),
                    total_items.clone(),
                    loading.clone(),
                    error.clone(),
                );
            });
            move || drop(interval)
        });
    }

    let refresh = {
        let endpoint = props.endpoint.clone();
        let current_page = *page;
        let jobs = jobs.clone();
        let queue = queue.clone();
        let total_pages = total_pages.clone();
        let total_items = total_items.clone();
        let loading = loading.clone();
        let error = error.clone();
        Callback::from(move |_| {
            load_jobs(
                endpoint.clone(),
                current_page,
                jobs.clone(),
                queue.clone(),
                total_pages.clone(),
                total_items.clone(),
                loading.clone(),
                error.clone(),
            )
        })
    };

    let worker_ready = queue
        .get("worker_ready")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let active_count = queue
        .get("active_job_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let active_job_id = queue
        .get("active_job_id")
        .and_then(Value::as_str)
        .unwrap_or("—");

    html! {
        <>
            <ErrorModal message={(*error).clone()} on_close={{
                let error = error.clone();
                Callback::from(move |_| error.set(None))
            }} />
            <section class="card shadow-sm">
                <div class="card-body border-bottom">
                    <div class="d-flex flex-wrap justify-content-between align-items-center gap-3">
                        <div>
                            <h2 class="h5 mb-1">{"历史行情与汇率下载任务"}</h2>
                            <div class="text-secondary">
                                {"展示数据库中的全部任务；真实状态、队列位置和下载进度每 5 秒更新。"}
                            </div>
                        </div>
                        <button class="btn btn-outline-primary" disabled={*loading} onclick={refresh}>
                            {if *loading {
                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"刷新中…"}</> }
                            } else {
                                html! { "立即刷新" }
                            }}
                        </button>
                    </div>
                    {success.as_ref().map(|message| html! {
                        <div class="alert alert-success mt-3 mb-0">{message}</div>
                    }).unwrap_or_default()}
                    <div class="row g-3 mt-1">
                        <SummaryCard label="任务总数" value={(*total_items).to_string()} class="text-primary" />
                        <SummaryCard label="活动任务" value={active_count.to_string()} class={if active_count > 0 { "text-primary" } else { "" }} />
                        <SummaryCard label="下载 Worker" value={if worker_ready { "IBKR 已就绪" } else { "等待 IBKR" }} class={if worker_ready { "text-success" } else { "text-warning" }} />
                        <SummaryCard label="当前任务" value={short_job_id(active_job_id)} class="" />
                    </div>
                </div>
                <Pagination page={*page} total_pages={*total_pages} total_items={*total_items}
                    on_page={{
                        let page = page.clone();
                        Callback::from(move |next| {
                            save_page(PAGE_STORAGE_KEY, next);
                            page.set(next);
                        })
                    }} />
                <div class="table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"创建时间（本地）"}</th>
                            <th>{"Job ID"}</th>
                            <th>{"证券"}</th>
                            <th>{"数据范围"}</th>
                            <th style="min-width: 180px;">{"下载进度"}</th>
                            <th>{"真实状态"}</th>
                            <th>{"重试/错误"}</th>
                            <th>{"更新时间（本地）"}</th>
                            <th>{"操作"}</th>
                        </tr></thead>
                        <tbody>
                        {if jobs.is_empty() {
                            html! { <tr><td colspan="9" class="text-center text-secondary py-5">{"暂无下载任务"}</td></tr> }
                        } else {
                            jobs.iter().map(|job| {
                                let job_id = text(job, "job_id");
                                let progress = job_progress_percent(job);
                                let fx_base = job.pointer("/request/fx_rate_pair/base_currency")
                                    .and_then(Value::as_str);
                                let fx_quote = job.pointer("/request/fx_rate_pair/quote_currency")
                                    .and_then(Value::as_str);
                                let is_fx = fx_base.is_some() && fx_quote.is_some();
                                let security_title = match (fx_base, fx_quote) {
                                    (Some(base), Some(quote)) => format!("{base}/{quote} 历史汇率"),
                                    _ => request_text(job, "/request/contract/symbol"),
                                };
                                let security_detail = if is_fx {
                                    "IBKR IDEALPRO · MIDPOINT".to_owned()
                                } else {
                                    format!(
                                        "{} · Conid {} · {}",
                                        request_exchange(job),
                                        request_integer(job, "/request/contract/conid"),
                                        request_text(job, "/request/contract/currency"),
                                    )
                                };
                                let cancel_in_progress = (*cancelling).as_deref() == Some(job_id.as_str());
                                let cancel = {
                                    let endpoint = props.endpoint.clone();
                                    let job_id = job_id.clone();
                                    let current_page = *page;
                                    let jobs = jobs.clone();
                                    let queue = queue.clone();
                                    let total_pages = total_pages.clone();
                                    let total_items = total_items.clone();
                                    let loading = loading.clone();
                                    let cancelling = cancelling.clone();
                                    let error = error.clone();
                                    let success = success.clone();
                                    Callback::from(move |_| cancel_job(
                                        endpoint.clone(), job_id.clone(), current_page,
                                        jobs.clone(), queue.clone(), total_pages.clone(),
                                        total_items.clone(), loading.clone(), cancelling.clone(),
                                        error.clone(), success.clone(),
                                    ))
                                };
                                html! { <tr>
                                    <td class="text-nowrap">{local_time(job, "created_at")}</td>
                                    <td title={job_id.clone()}><code>{short_job_id(&job_id)}</code></td>
                                    <td>
                                        <div class="fw-semibold">{security_title}</div>
                                        <div class="small text-secondary">
                                            {security_detail}
                                        </div>
                                    </td>
                                    <td class="text-nowrap">
                                        <div>{format!(
                                            "{} · {}",
                                            request_text(job, "/request/timeframe"),
                                            if is_fx {
                                                "历史绩效汇率"
                                            } else if job.pointer("/request/outside_rth").and_then(Value::as_bool).unwrap_or(false) {
                                                "含盘前盘后"
                                            } else {
                                                "常规交易时段"
                                            },
                                        )}</div>
                                        <div class="small text-secondary">{nested_local_time(job, "/request/start")}</div>
                                        <div class="small text-secondary">{format!("至 {}", nested_local_time(job, "/request/end"))}</div>
                                    </td>
                                    <td>
                                        <div class="d-flex justify-content-between gap-2 small mb-1">
                                            <span>{job_progress(job)}</span>
                                            <span>{format!("{} 个分片", integer(job, "completed_slices"))}</span>
                                        </div>
                                        <div class="progress" style="height: 8px;">
                                            <div class="progress-bar" role="progressbar"
                                                style={format!("width: {:.1}%", progress.unwrap_or(0.0))}
                                                title={job_progress(job)} />
                                        </div>
                                        <div class="small text-secondary mt-1">
                                            {format!("已推进至 {}", local_time(job, "cursor_time"))}
                                        </div>
                                    </td>
                                    <td>
                                        <span class={format!("badge {}", job_status_class(job))}>{job_status_label(job)}</span>
                                        <div class="small mt-1">{queue_position_label(job)}</div>
                                        <div class="small text-secondary">{format!("数据库状态：{}", text(job, "state"))}</div>
                                    </td>
                                    <td>
                                        <div class="text-nowrap">{format!("尝试次数：{}", integer(job, "attempts"))}</div>
                                        <div class="small text-danger text-break" style="min-width: 180px; max-width: 320px;">{text(job, "last_error")}</div>
                                    </td>
                                    <td class="text-nowrap">{local_time(job, "updated_at")}</td>
                                    <td>
                                        {if job_is_cancellable(job) {
                                            html! { <button class="btn btn-sm btn-outline-danger text-nowrap"
                                                disabled={cancel_in_progress} onclick={cancel}>
                                                {if cancel_in_progress { "取消中…" } else { "取消任务" }}
                                            </button> }
                                        } else if text(job, "state") == "running" {
                                            html! { <span class="small text-secondary text-nowrap">{"当前分片执行中"}</span> }
                                        } else {
                                            Html::default()
                                        }}
                                    </td>
                                </tr> }
                            }).collect::<Html>()
                        }}
                        </tbody>
                    </table>
                </div>
                <Pagination page={*page} total_pages={*total_pages} total_items={*total_items}
                    on_page={{
                        let page = page.clone();
                        Callback::from(move |next| {
                            save_page(PAGE_STORAGE_KEY, next);
                            page.set(next);
                        })
                    }} />
            </section>
        </>
    }
}

#[derive(Properties, PartialEq)]
struct SummaryCardProps {
    label: &'static str,
    value: String,
    class: &'static str,
}

#[function_component(SummaryCard)]
fn summary_card(props: &SummaryCardProps) -> Html {
    html! {
        <div class="col-6 col-xl-3">
            <div class="border rounded p-3 h-100">
                <div class="small text-secondary">{props.label}</div>
                <div class={classes!("fw-semibold", "text-break", props.class)}>{props.value.clone()}</div>
            </div>
        </div>
    }
}

#[allow(clippy::too_many_arguments)]
fn load_jobs(
    endpoint: String,
    page: usize,
    jobs: UseStateHandle<Vec<Value>>,
    queue: UseStateHandle<Value>,
    total_pages: UseStateHandle<usize>,
    total_items: UseStateHandle<usize>,
    loading: UseStateHandle<bool>,
    error: UseStateHandle<Option<String>>,
) {
    loading.set(true);
    spawn_local(async move {
        match call_method(
            &endpoint,
            "data.jobs",
            json!({"page": page, "page_size": PAGE_SIZE}),
        )
        .await
        {
            Ok(value) => {
                jobs.set(array(&value, "jobs"));
                queue.set(value.get("queue").cloned().unwrap_or(Value::Null));
                total_pages.set(
                    value
                        .get("total_pages")
                        .and_then(Value::as_u64)
                        .unwrap_or(1) as usize,
                );
                total_items.set(
                    value
                        .get("total_items")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                );
            }
            Err(message) => error.set(Some(message)),
        }
        loading.set(false);
    });
}

#[allow(clippy::too_many_arguments)]
fn cancel_job(
    endpoint: String,
    job_id: String,
    page: usize,
    jobs: UseStateHandle<Vec<Value>>,
    queue: UseStateHandle<Value>,
    total_pages: UseStateHandle<usize>,
    total_items: UseStateHandle<usize>,
    loading: UseStateHandle<bool>,
    cancelling: UseStateHandle<Option<String>>,
    error: UseStateHandle<Option<String>>,
    success: UseStateHandle<Option<String>>,
) {
    cancelling.set(Some(job_id.clone()));
    success.set(None);
    spawn_local(async move {
        match call_method(&endpoint, "data.job.cancel", json!({"job_id": job_id})).await {
            Ok(_) => {
                success.set(Some("下载任务已取消".into()));
                load_jobs(
                    endpoint,
                    page,
                    jobs,
                    queue,
                    total_pages,
                    total_items,
                    loading,
                    error.clone(),
                );
            }
            Err(message) => error.set(Some(message)),
        }
        cancelling.set(None);
    });
}

fn request_text(job: &Value, pointer: &str) -> String {
    job.pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("—")
        .to_owned()
}

fn request_integer(job: &Value, pointer: &str) -> String {
    job.pointer(pointer)
        .and_then(Value::as_i64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "—".into())
}

fn request_exchange(job: &Value) -> String {
    [
        "/request/contract/primary_exchange",
        "/request/contract/exchange",
    ]
    .into_iter()
    .find_map(|pointer| {
        job.pointer(pointer)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    })
    .unwrap_or("—")
    .to_owned()
}

fn short_job_id(job_id: &str) -> String {
    if job_id == "—" || job_id.len() <= 13 {
        return job_id.to_owned();
    }
    format!("{}…{}", &job_id[..8], &job_id[job_id.len() - 4..])
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{request_exchange, short_job_id};

    #[test]
    fn job_id_is_shortened_without_losing_both_ends() {
        assert_eq!(
            short_job_id("019fc1e3-246d-7a00-be31-03cc5b73ebff"),
            "019fc1e3…ebff"
        );
        assert_eq!(short_job_id("—"), "—");
    }

    #[test]
    fn primary_exchange_is_preferred_for_job_contract() {
        let job = json!({
            "request": {"contract": {"exchange": "SMART", "primary_exchange": "NASDAQ"}}
        });
        assert_eq!(request_exchange(&job), "NASDAQ");
    }
}
