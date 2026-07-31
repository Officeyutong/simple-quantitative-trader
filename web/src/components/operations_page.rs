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
    let acknowledge_note = use_state(String::new);
    let alerts = array(&props.data.alerts, "alerts");
    let metrics = &props.data.metrics;
    let operational = metrics
        .get("operational")
        .unwrap_or(&serde_json::Value::Null);
    let system = &props.data.system;
    let reconciliation_state = system
        .pointer("/reconciliation/state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("pending");
    let current_reconciliation_id = system
        .pointer("/reconciliation/reconciliation_id")
        .and_then(serde_json::Value::as_str);
    let reconciliation_differences = array(&props.data.reconciliation_differences, "differences")
        .into_iter()
        .filter(|difference| {
            current_reconciliation_id.is_some_and(|id| {
                difference
                    .get("reconciliation_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(id)
            })
        })
        .collect::<Vec<_>>();
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
                <div class="d-flex flex-wrap justify-content-between align-items-end gap-3 mb-2">
                    <h2 class="h5 mb-0">{"活动告警"}</h2>
                    <div class="flex-grow-1" style="max-width: 36rem;">
                        <label class="form-label small" for="alert-acknowledge-note">{"确认备注"}</label>
                        <input id="alert-acknowledge-note" class="form-control form-control-sm"
                            placeholder="填写复核结果后，可确认对应告警"
                            value={(*acknowledge_note).clone()}
                            oninput={{
                                let note = acknowledge_note.clone();
                                Callback::from(move |event: InputEvent| {
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                    note.set(input.value());
                                })
                            }} />
                    </div>
                </div>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"级别"}</th><th>{"告警"}</th><th>{"状态"}</th>
                            <th>{"信息"}</th><th>{"首次出现（本地）"}</th><th>{"最后出现（本地）"}</th>
                            <th>{"操作"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if alerts.is_empty() {
                                html! { <tr><td colspan="7" class="text-center text-secondary py-4">{"当前没有活动告警"}</td></tr> }
                            } else {
                                alerts.iter().map(|row| {
                                    let severity = text(row, "severity");
                                    let alert_key = text(row, "alert_key");
                                    let alert_id = text(row, "alert_id");
                                    let request_key = format!("monitor.acknowledge:{alert_id}");
                                    html! {
                                        <tr>
                                            <td><span class={classes!("badge", severity_class(&severity))}>{severity}</span></td>
                                            <td>{alert_label(&alert_key)}</td>
                                            <td>{text(row, "state")}</td>
                                            <td>{text(row, "message")}</td>
                                            <td class="text-nowrap">{local_time(row, "first_observed_at")}</td>
                                            <td class="text-nowrap">{local_time(row, "last_observed_at")}</td>
                                            <td>
                                                <button class="btn btn-sm btn-outline-primary text-nowrap"
                                                    disabled={pending.is_some() || acknowledge_note.trim().is_empty()}
                                                    onclick={{
                                                        let callback = props.on_mutation.clone();
                                                        let pending = pending.clone();
                                                        let note = acknowledge_note.clone();
                                                        let alert_id = alert_id.clone();
                                                        let request_key = request_key.clone();
                                                        Callback::from(move |_| {
                                                            pending.set(Some(request_key.clone()));
                                                            let pending = pending.clone();
                                                            callback.emit(MutationRequest {
                                                                method: "monitor.acknowledge".into(),
                                                                params: json!({
                                                                    "alert_id": alert_id,
                                                                    "note": (*note).clone()
                                                                }),
                                                                on_complete: Callback::from(move |_| pending.set(None)),
                                                            });
                                                        })
                                                    }}>
                                                    {if pending.as_deref() == Some(request_key.as_str()) {
                                                        "确认中…"
                                                    } else {
                                                        "确认告警"
                                                    }}
                                                </button>
                                            </td>
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
                {reconciliation_guidance(reconciliation_state, &reconciliation_differences)}
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

fn alert_label(alert_key: &str) -> String {
    match alert_key {
        "market_data_competing_live_session" => "IBKR 实时行情被其他会话占用",
        "market_data_failed" => "行情订阅失败或正在重试",
        _ => alert_key,
    }
    .into()
}

fn severity_class(severity: &str) -> &'static str {
    match severity {
        "critical" => "bg-danger",
        "warning" => "bg-warning text-dark",
        _ => "bg-info text-dark",
    }
}

fn reconciliation_guidance(state: &str, differences: &[serde_json::Value]) -> Html {
    if state == "healthy" {
        return html! {
            <div class="alert alert-success py-2">
                {"IBKR 与本地订单状态一致，可以提交开仓和加仓订单。"}
            </div>
        };
    }
    if state == "pending" {
        return html! {
            <div class="alert alert-info">
                <strong>{"尚未完成本次连接的订单对账。"}</strong>
                <div class="mt-1">{"请先连接 IBKR，然后点击上方“立即对账”。对账完成前，系统会限制开仓和加仓。"}</div>
            </div>
        };
    }

    html! {
        <div class="alert alert-warning">
            <h3 class="h6">{"对账已降级：开仓和加仓暂时被禁止"}</h3>
            <p class="mb-2">
                {"本地数据库中的订单与 IBKR 当前订单不一致。清空数据库后，IBKR 中原有的活动订单会被识别为外部订单，这是该状态最常见的原因。行情新鲜时，系统仍只允许严格减少现有仓位的订单。"}
            </p>
            <ol class="mb-3">
                <li>{"检查下方当前对账差异。"}</li>
                <li>{"外部活动订单：前往“订单与成交”或 IBKR TWS，确认其用途；不再需要时取消，仍需保留时先人工核对。"}</li>
                <li>{"本地活动订单缺失：在 IBKR 确认该订单已经成交、取消或不存在，然后重新连接以刷新订单事件。"}</li>
                <li>{"处理完成后点击上方“立即对账”。只有新的对账结果变为 healthy 才会恢复开仓和加仓。"}</li>
            </ol>
            <div class="small mb-2">
                <strong>{"注意："}</strong>
                {"“确认差异”只记录人工复核，不会直接解除交易限制；最终仍以重新对账结果为准。"}
            </div>
            <div class="table-responsive bg-white rounded border">
                <table class="table table-sm align-middle mb-0">
                    <thead>
                        <tr>
                            <th>{"差异类型"}</th>
                            <th>{"Broker Order ID"}</th>
                            <th>{"说明"}</th>
                            <th>{"建议处理"}</th>
                        </tr>
                    </thead>
                    <tbody>
                        {
                            if differences.is_empty() {
                                html! {
                                    <tr><td colspan="4" class="text-center text-secondary py-3">
                                        {"未加载到当前批次的差异明细，请点击“立即对账”刷新。"}
                                    </td></tr>
                                }
                            } else {
                                differences.iter().map(|difference| {
                                    let difference_type = text(difference, "difference_type");
                                    html! {
                                        <tr>
                                            <td><code>{difference_type.clone()}</code></td>
                                            <td>{integer(difference, "broker_order_id")}</td>
                                            <td>{text(difference, "detail")}</td>
                                            <td>{difference_action(&difference_type)}</td>
                                        </tr>
                                    }
                                }).collect::<Html>()
                            }
                        }
                    </tbody>
                </table>
            </div>
        </div>
    }
}

fn difference_action(difference_type: &str) -> &'static str {
    match difference_type {
        "external_open_order" => "在 IBKR/TWS 核实，取消不再需要的订单，然后重新对账",
        "missing_broker_order" => "确认订单终态，重新连接 IBKR 后再次对账",
        "external_completed_order" => "仅为历史信息，不阻塞交易",
        _ => "核实 IBKR 与本地记录后重新对账",
    }
}
