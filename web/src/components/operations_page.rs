use serde_json::json;
use yew::prelude::*;

use crate::api::DashboardData;

use super::{
    MutationRequest,
    key_value::KeyValue,
    metric_row::MetricRow,
    value::{array, boolean, bytes, integer, local_time, nested_text, text},
};

#[derive(Properties, PartialEq)]
pub struct OperationsPageProps {
    pub data: DashboardData,
    pub on_mutation: Callback<MutationRequest>,
}

#[function_component(OperationsPage)]
pub fn operations_page(props: &OperationsPageProps) -> Html {
    let pending = use_state(|| None::<String>);
    let alerts = array(&props.data.alerts, "alerts");
    let metrics = &props.data.metrics;
    let operational = metrics
        .get("operational")
        .unwrap_or(&serde_json::Value::Null);
    let system = &props.data.system;
    let operations = [
        ("ibkr.connect", "连接 IBKR", "btn-success"),
        ("reconcile.run", "立即对账", "btn-outline-primary"),
        ("backup.create", "创建备份", "btn-outline-secondary"),
    ];
    html! {
        <>
            <div class="d-flex flex-wrap gap-2 mb-4">
                {operations.into_iter().map(|(method, label, class)| html! {
                    <button
                        class={classes!("btn", class)}
                        disabled={pending.is_some()}
                        onclick={{
                            let callback = props.on_mutation.clone();
                            let pending = pending.clone();
                            Callback::from(move |_| {
                                pending.set(Some(method.into()));
                                let pending = pending.clone();
                                callback.emit(MutationRequest {
                                    method: method.into(),
                                    params: json!({}),
                                    on_complete: Callback::from(move |_| pending.set(None)),
                                });
                            })
                        }}
                    >
                        {
                            if pending.as_deref() == Some(method) {
                                html! {
                                    <>
                                        <span class="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                                        {"处理中…"}
                                    </>
                                }
                            } else {
                                html! { label }
                            }
                        }
                    </button>
                }).collect::<Html>()}
            </div>
            <section class="mb-4">
                <h2 class="h5">{"活动告警"}</h2>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"级别"}</th><th>{"告警"}</th><th>{"状态"}</th>
                            <th>{"信息"}</th><th>{"首次出现（本地）"}</th><th>{"最后出现（本地）"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if alerts.is_empty() {
                                html! { <tr><td colspan="6" class="text-center text-secondary py-4">{"当前没有活动告警"}</td></tr> }
                            } else {
                                alerts.iter().map(|row| {
                                    let severity = text(row, "severity");
                                    html! {
                                        <tr>
                                            <td><span class={classes!("badge", severity_class(&severity))}>{severity}</span></td>
                                            <td>{text(row, "alert_key")}</td>
                                            <td>{text(row, "state")}</td>
                                            <td>{text(row, "message")}</td>
                                            <td class="text-nowrap">{local_time(row, "first_observed_at")}</td>
                                            <td class="text-nowrap">{local_time(row, "last_observed_at")}</td>
                                        </tr>
                                    }
                                }).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                </div>
            </section>
            <section class="mb-4">
                <h2 class="h5">{"监控指标"}</h2>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr><th>{"指标"}</th><th>{"值"}</th><th>{"状态"}</th></tr></thead>
                        <tbody>
                            <MetricRow label="Daemon Ready" value={yes_no(boolean(metrics, "daemon_ready"))} healthy={boolean(metrics, "daemon_ready")} />
                            <MetricRow label="IBKR Ready" value={yes_no(boolean(metrics, "ibkr_ready"))} healthy={boolean(metrics, "ibkr_ready")} />
                            <MetricRow label="活动告警数" value={integer(metrics, "active_alert_count")} healthy={integer(metrics, "active_alert_count") == "0"} />
                            <MetricRow label="运行策略数" value={integer(operational, "running_strategies")} healthy={true} />
                            <MetricRow label="待处理数据任务" value={integer(operational, "pending_data_jobs")} healthy={integer(operational, "pending_data_jobs") == "0"} />
                            <MetricRow label="失败行情订阅" value={integer(operational, "failed_market_data_subscriptions")} healthy={integer(operational, "failed_market_data_subscriptions") == "0"} />
                            <MetricRow label="数据库大小" value={bytes(operational, "database_bytes")} healthy={true} />
                            <MetricRow label="Parquet Lake 大小" value={bytes(operational, "lake_bytes")} healthy={true} />
                            <MetricRow label="Staging 大小" value={bytes(operational, "staging_bytes")} healthy={true} />
                        </tbody>
                    </table>
                </div>
            </section>
            <section class="mb-4">
                <h2 class="h5">{"系统状态"}</h2>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr><th>{"项目"}</th><th>{"值"}</th></tr></thead>
                        <tbody>
                            <KeyValue label="Daemon 状态" value={text(system, "state")} />
                            <KeyValue label="运行环境" value={text(system, "environment")} />
                            <KeyValue label="版本" value={text(system, "version")} />
                            <KeyValue label="PID" value={integer(system, "pid")} />
                            <KeyValue label="运行秒数" value={integer(system, "uptime_seconds")} />
                            <KeyValue label="交易开关" value={yes_no(boolean(system, "trading_enabled"))} />
                            <KeyValue label="IBKR 状态" value={nested_text(system, "/ibkr/state")} />
                            <KeyValue label="IBKR Endpoint" value={nested_text(system, "/ibkr/endpoint")} />
                            <KeyValue label="IBKR Client ID" value={system.pointer("/ibkr/client_id").and_then(|v| v.as_i64()).map(|v| v.to_string()).unwrap_or_else(|| "—".into())} />
                            <KeyValue label="对账状态" value={nested_text(system, "/reconciliation/state")} />
                            <KeyValue label="阻塞差异数" value={system.pointer("/reconciliation/blocking_difference_count").and_then(|v| v.as_i64()).map(|v| v.to_string()).unwrap_or_else(|| "—".into())} />
                            <KeyValue label="启动时间（本地）" value={local_time(system, "started_at")} />
                        </tbody>
                    </table>
                </div>
            </section>
        </>
    }
}

fn yes_no(value: bool) -> String {
    if value { "是" } else { "否" }.into()
}

fn severity_class(severity: &str) -> &'static str {
    match severity {
        "critical" => "bg-danger",
        "warning" => "bg-warning text-dark",
        _ => "bg-info text-dark",
    }
}
