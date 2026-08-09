use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::{PerformanceData, call_method, load_performance};

use super::{
    error_modal::ErrorModal,
    value::{array, format_number, integer, local_time, number, text},
};

const REFRESH_INTERVAL_MS: u32 = 5_000;

#[derive(Properties, PartialEq)]
pub struct PerformancePageProps {
    pub strategies: Value,
    pub endpoint: String,
}

#[derive(Clone, PartialEq)]
enum PerformanceState {
    Loading,
    Ready(PerformanceData),
    Error(String),
}

fn refresh(
    state: UseStateHandle<PerformanceState>,
    endpoint: String,
    strategy_id: String,
    initial_capital: f64,
    show_loading: bool,
) {
    if strategy_id.is_empty() {
        state.set(PerformanceState::Error("请先创建策略".into()));
        return;
    }
    if show_loading {
        state.set(PerformanceState::Loading);
    }
    spawn_local(async move {
        state.set(
            match load_performance(&endpoint, &strategy_id, initial_capital).await {
                Ok(data) => PerformanceState::Ready(data),
                Err(error) => PerformanceState::Error(error),
            },
        );
    });
}

#[function_component(PerformancePage)]
pub fn performance_page(props: &PerformancePageProps) -> Html {
    let strategies = array(&props.strategies, "strategies");
    let first_strategy = strategies
        .first()
        .map(|item| text(item, "strategy_id"))
        .filter(|item| item != "—")
        .unwrap_or_default();
    let selected = use_state(|| first_strategy);
    let capital = use_state(|| "100000".to_owned());
    let state = use_state(|| PerformanceState::Loading);
    let repairing = use_state(|| false);
    let repair_notice = use_state(|| None::<String>);
    let repair_error = use_state(|| None::<String>);
    let parsed_capital = capital.parse::<f64>().ok().filter(|value| *value > 0.0);

    {
        let strategy_ids = strategies
            .iter()
            .map(|strategy| text(strategy, "strategy_id"))
            .filter(|id| id != "—")
            .collect::<Vec<_>>();
        let selected = selected.clone();
        use_effect_with(strategy_ids.clone(), move |_| {
            if !strategy_ids.iter().any(|id| id == selected.as_str()) {
                selected.set(strategy_ids.first().cloned().unwrap_or_default());
            }
            || ()
        });
    }

    {
        let state = state.clone();
        let endpoint = props.endpoint.clone();
        let strategy_id = (*selected).clone();
        let initial_capital = parsed_capital.unwrap_or(100_000.0);
        use_effect_with(
            (endpoint.clone(), strategy_id.clone(), initial_capital),
            move |_| {
                refresh(
                    state.clone(),
                    endpoint.clone(),
                    strategy_id.clone(),
                    initial_capital,
                    true,
                );
                let interval = Interval::new(REFRESH_INTERVAL_MS, move || {
                    refresh(
                        state.clone(),
                        endpoint.clone(),
                        strategy_id.clone(),
                        initial_capital,
                        false,
                    );
                });
                move || drop(interval)
            },
        );
    }

    html! {
        <>
            <ErrorModal
                message={repair_error.as_ref().cloned().or_else(|| match &*state {
                    PerformanceState::Error(error) => Some(error.clone()),
                    _ => None,
                })}
                on_close={{
                    let state = state.clone();
                    let repair_error = repair_error.clone();
                    Callback::from(move |_| {
                        repair_error.set(None);
                        if matches!(&*state, PerformanceState::Error(_)) {
                            state.set(PerformanceState::Loading);
                        }
                    })
                }}
            />
            <div class="card shadow-sm mb-4"><div class="card-body">
                <div class="row g-3 align-items-end">
                    <div class="col-12 col-lg-5">
                        <label class="form-label" for="performance-strategy">{"策略"}</label>
                        <select
                            id="performance-strategy"
                            class="form-select"
                            value={(*selected).clone()}
                            onchange={{
                                let selected = selected.clone();
                                Callback::from(move |event: Event| {
                                    let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                    selected.set(input.value());
                                })
                            }}
                        >
                            {strategies.iter().map(|strategy| {
                                let id = text(strategy, "strategy_id");
                                html! {
                                    <option
                                        key={id.clone()}
                                        value={id.clone()}
                                        selected={id == *selected}
                                    >
                                        {text(strategy, "name")}
                                    </option>
                                }
                            }).collect::<Html>()}
                        </select>
                    </div>
                    <div class="col-12 col-lg-3">
                        <label class="form-label" for="initial-capital">{"初始资金"}</label>
                        <input
                            id="initial-capital"
                            class={classes!("form-control", parsed_capital.is_none().then_some("is-invalid"))}
                            type="number" min="0.01" step="1000"
                            value={(*capital).clone()}
                            oninput={{
                                let capital = capital.clone();
                                Callback::from(move |event: InputEvent| {
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                    capital.set(input.value());
                                })
                            }}
                        />
                    </div>
                    <div class="col-12 col-lg-2">
                        <button
                            class="btn btn-outline-primary w-100"
                            disabled={parsed_capital.is_none() || matches!(&*state, PerformanceState::Loading)}
                            onclick={{
                                let state = state.clone();
                                let endpoint = props.endpoint.clone();
                                let strategy_id = (*selected).clone();
                                Callback::from(move |_| {
                                    if let Some(initial_capital) = parsed_capital {
                                        refresh(state.clone(), endpoint.clone(), strategy_id.clone(), initial_capital, true);
                                    }
                                })
                            }}
                        >
                            {
                                if matches!(&*state, PerformanceState::Loading) {
                                    html! {
                                        <>
                                            <span class="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                                            {"刷新中…"}
                                        </>
                                    }
                                } else {
                                    html! { "立即刷新" }
                                }
                            }
                        </button>
                    </div>
                    <div class="col-12 col-lg-2">
                        <button
                            class="btn btn-outline-warning w-100"
                            disabled={selected.is_empty() || *repairing}
                            onclick={{
                                let endpoint = props.endpoint.clone();
                                let strategy_id = (*selected).clone();
                                let repairing = repairing.clone();
                                let repair_notice = repair_notice.clone();
                                let repair_error = repair_error.clone();
                                Callback::from(move |_| {
                                    repairing.set(true);
                                    repair_notice.set(None);
                                    repair_error.set(None);
                                    let endpoint = endpoint.clone();
                                    let strategy_id = strategy_id.clone();
                                    let repairing = repairing.clone();
                                    let repair_notice = repair_notice.clone();
                                    let repair_error = repair_error.clone();
                                    spawn_local(async move {
                                        match call_method(
                                            &endpoint,
                                            "performance.repair_history",
                                            json!({"strategy_id": strategy_id}),
                                        ).await {
                                            Ok(result) => {
                                                let jobs = result.get("jobs")
                                                    .and_then(Value::as_array)
                                                    .map(Vec::len)
                                                    .unwrap_or_default();
                                                let reconciliation_error = result
                                                    .get("reconciliation_error")
                                                    .and_then(Value::as_str);
                                                repair_notice.set(Some(match reconciliation_error {
                                                    Some(error) => format!(
                                                        "已创建/复用 {jobs} 个历史汇率任务；成交对账暂未完成：{error}"
                                                    ),
                                                    None => format!(
                                                        "成交对账已完成，已创建/复用 {jobs} 个历史汇率任务。下载完成后页面会自动重新计算。"
                                                    ),
                                                }));
                                            }
                                            Err(error) => repair_error.set(Some(error)),
                                        }
                                        repairing.set(false);
                                    });
                                })
                            }}
                        >
                            {if *repairing {
                                html! { <><span class="spinner-border spinner-border-sm me-2" />{"修复中…"}</> }
                            } else {
                                html! { "修复历史数据" }
                            }}
                        </button>
                    </div>
                </div>
                <div class="form-text mt-2">
                    {"收益指标只归因于该策略产生并成交的订单；总净损益包含未平仓盯市损益，只有成交、持仓、行情和汇率数据均可靠时才会显示。每 5 秒自动刷新。"}
                </div>
                {repair_notice.as_ref().map(|message| html! {
                    <div class="alert alert-info mt-3 mb-0">{message}</div>
                }).unwrap_or_default()}
            </div></div>
            {
                match &*state {
                    PerformanceState::Loading => html! {
                        <div class="d-flex align-items-center gap-3 py-5">
                            <div class="spinner-border text-primary" role="status" />
                            <span>{"正在计算策略绩效…"}</span>
                        </div>
                    },
                    PerformanceState::Error(_) => Html::default(),
                    PerformanceState::Ready(data) => performance_content(data),
                }
            }
        </>
    }
}

fn performance_content(data: &PerformanceData) -> Html {
    let report = &data.report;
    let snapshots = array(&data.snapshots, "snapshots");
    let currency = text(report, "base_currency");
    let warnings = report
        .get("data_warnings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let warning_groups = report
        .get("data_warning_groups")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let valuation_warnings = report
        .get("valuation_warnings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let data_complete = report.get("data_complete").and_then(Value::as_bool);
    let warning_total = report
        .get("data_warning_total")
        .and_then(Value::as_u64)
        .unwrap_or(warnings.len() as u64);
    let warnings_truncated = report
        .get("data_warnings_truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let valuation_complete = report.get("valuation_complete").and_then(Value::as_bool);
    let cards = [
        (
            "总净损益",
            money_or_unavailable(report, "total_net_pnl", &currency),
        ),
        (
            "已实现净损益",
            compatible_money(report, "realized_net_pnl", "net_pnl", &currency),
        ),
        (
            "未实现损益",
            money_or_unavailable(report, "unrealized_pnl", &currency),
        ),
        ("总收益率", percent_or_unavailable(report, "total_return")),
        (
            "已实现收益率",
            compatible_percent(report, "realized_return", "return"),
        ),
        (
            "已实现权益最大回撤",
            compatible_percent(
                report,
                "realized_maximum_drawdown_pct",
                "maximum_drawdown_pct",
            ),
        ),
    ];
    html! {
        <>
            if !warnings.is_empty() || data_complete == Some(false) {
                <div class="alert alert-warning" role="alert">
                    <div class="fw-semibold mb-1">{"成交归因数据不完整，已实现指标可能只覆盖能够可靠配对的成交"}</div>
                    if !warning_groups.is_empty() {
                        <ul class="mb-0">
                            {warning_groups.iter().map(|group| html! {
                                <li class="mb-1">
                                    <span class="fw-semibold">{text(group, "title")}</span>
                                    <span>{format!("：{}", text(group, "detail"))}</span>
                                </li>
                            }).collect::<Html>()}
                        </ul>
                        if !warnings.is_empty() {
                            <details class="mt-2">
                                <summary class="text-decoration-underline" style="cursor: pointer;">
                                    {if warnings_truncated {
                                        format!("查看前 {} 条逐笔审计明细（共 {warning_total} 条）", warnings.len())
                                    } else {
                                        format!("查看 {warning_total} 条逐笔审计明细")
                                    }}
                                </summary>
                                <ul class="mb-0 mt-2">
                                    {warnings.iter().map(|warning| html! {
                                        <li>{warning.as_str().unwrap_or("检测到无法配对的成交")}</li>
                                    }).collect::<Html>()}
                                </ul>
                            </details>
                        }
                    } else {
                        <ul class="mb-0">
                            if warnings.is_empty() {
                                <li>{"后端未提供具体原因；请核对订单、成交和佣金记录。"}</li>
                            } else {
                                {warnings.iter().map(|warning| html! {
                                    <li>{warning.as_str().unwrap_or("检测到无法配对的成交")}</li>
                                }).collect::<Html>()}
                            }
                        </ul>
                    }
                </div>
            }
            if valuation_complete != Some(true) {
                <div class="alert alert-danger" role="alert">
                    <div class="fw-semibold mb-1">{"当前总净损益不可计算"}</div>
                    <div>
                        {
                            if valuation_complete.is_none() {
                                "当前结果未包含新版盯市估值状态，已实现损益仍可查看，但不能作为账户当前总损益。"
                            } else {
                                "未平仓仓位缺少可靠的新鲜持仓、实时行情或汇率数据；页面不会用已实现损益冒充总净损益。"
                            }
                        }
                    </div>
                    if !valuation_warnings.is_empty() {
                        <ul class="mb-0 mt-1">
                            {valuation_warnings.iter().map(|warning| html! {
                                <li>{warning.as_str().unwrap_or("当前盯市估值不可用")}</li>
                            }).collect::<Html>()}
                        </ul>
                    }
                </div>
            }
            <div class="row g-3 mb-4">
                {cards.into_iter().map(|(label, value)| html! {
                    <div class="col-12 col-sm-6 col-xl-2">
                        <div class="card h-100 shadow-sm"><div class="card-body">
                            <div class="small text-secondary">{label}</div>
                            <div class="fs-5 fw-semibold mt-1">{value}</div>
                        </div></div>
                    </div>
                }).collect::<Html>()}
            </div>
            <section class="mb-4">
                <h2 class="h5">{"当前报告"}</h2>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr><th>{"指标"}</th><th>{"值"}</th><th>{"指标"}</th><th>{"值"}</th></tr></thead>
                        <tbody>
                            <tr><td>{"初始资金"}</td><td>{money(report, "initial_capital", &currency)}</td><td>{"总净损益（含未平仓）"}</td><td>{money_or_unavailable(report, "total_net_pnl", &currency)}</td></tr>
                            <tr><td>{"已实现毛损益"}</td><td>{compatible_money(report, "realized_gross_pnl", "gross_pnl", &currency)}</td><td>{"已实现净损益"}</td><td>{compatible_money(report, "realized_net_pnl", "net_pnl", &currency)}</td></tr>
                            <tr><td>{"未实现损益"}</td><td>{money_or_unavailable(report, "unrealized_pnl", &currency)}</td><td>{"佣金"}</td><td>{money(report, "commissions", &currency)}</td></tr>
                            <tr><td>{"总收益率"}</td><td>{percent_or_unavailable(report, "total_return")}</td><td>{"已实现收益率"}</td><td>{compatible_percent(report, "realized_return", "return")}</td></tr>
                            <tr><td>{"换手金额"}</td><td>{money(report, "turnover", &currency)}</td><td>{"已实现权益最大回撤"}</td><td>{compatible_percent(report, "realized_maximum_drawdown_pct", "maximum_drawdown_pct")}</td></tr>
                            <tr><td>{"完整往返交易"}</td><td>{integer(report, "realized_trade_count")}</td><td>{"未平仓证券数"}</td><td>{integer(report, "open_position_count")}</td></tr>
                            <tr><td>{"盈利交易"}</td><td>{integer(report, "winning_trade_count")}</td><td>{"亏损交易"}</td><td>{integer(report, "losing_trade_count")}</td></tr>
                            <tr><td>{"胜率"}</td><td>{percent(report, "win_rate")}</td><td>{"已实现权益 Sharpe / Sortino"}</td><td>{format!("{} / {}", number(report, "sharpe"), number(report, "sortino"))}</td></tr>
                            <tr><td>{"生成时间（本地）"}</td><td>{local_time(report, "generated_at")}</td><td>{"基础币种"}</td><td>{currency.clone()}</td></tr>
                        </tbody>
                    </table>
                </div>
            </section>
            <section class="mb-4">
                <h2 class="h5">{"历史绩效快照"}</h2>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"时间（本地）"}</th><th>{"账户"}</th><th>{"口径状态"}</th>
                            <th class="text-end">{"总净损益"}</th><th class="text-end">{"已实现净损益"}</th>
                            <th class="text-end">{"未实现损益"}</th><th class="text-end">{"佣金"}</th>
                            <th class="text-end">{"换手金额"}</th><th class="text-end">{"交易数"}</th>
                            <th class="text-end">{"盈利"}</th><th class="text-end">{"亏损"}</th><th class="text-end">{"未平仓"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if snapshots.is_empty() {
                                html! { <tr><td colspan="12" class="text-center text-secondary py-4">{"暂无绩效快照"}</td></tr> }
                            } else {
                                snapshots.iter().map(|row| {
                                    let row_currency = text(row, "base_currency");
                                    let legacy = legacy_snapshot(row);
                                    html! {
                                        <tr>
                                            <td class="text-nowrap">{local_time(row, "observed_at")}</td>
                                            <td>{text(row, "account")}</td>
                                            <td>{snapshot_status(row)}</td>
                                            <td class="text-end">{
                                                if legacy {
                                                    "—".to_owned()
                                                } else {
                                                    money_or_unavailable(row, "total_net_pnl", &row_currency)
                                                }
                                            }</td>
                                            <td class="text-end">{compatible_money(row, "realized_net_pnl", "net_pnl", &row_currency)}</td>
                                            <td class="text-end">{
                                                if legacy {
                                                    "—".to_owned()
                                                } else {
                                                    money_or_unavailable(row, "unrealized_pnl", &row_currency)
                                                }
                                            }</td>
                                            <td class="text-end">{money(row, "commissions", &row_currency)}</td>
                                            <td class="text-end">{money(row, "turnover", &row_currency)}</td>
                                            <td class="text-end">{integer(row, "realized_trade_count")}</td>
                                            <td class="text-end">{integer(row, "winning_trade_count")}</td>
                                            <td class="text-end">{integer(row, "losing_trade_count")}</td>
                                            <td class="text-end">{integer(row, "open_position_count")}</td>
                                        </tr>
                                    }
                                }).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                </div>
            </section>
        </>
    }
}

fn money(value: &Value, key: &str, currency: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{} {currency}", format_number(value)))
        .unwrap_or_else(|| "—".into())
}

fn money_or_unavailable(value: &Value, key: &str, currency: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{} {currency}", format_number(value)))
        .unwrap_or_else(|| "不可计算".into())
}

fn compatible_money(value: &Value, key: &str, legacy_key: &str, currency: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .or_else(|| value.get(legacy_key).and_then(Value::as_f64))
        .map(|value| format!("{} {currency}", format_number(value)))
        .unwrap_or_else(|| "—".into())
}

fn percent(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn percent_or_unavailable(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "不可计算".into())
}

fn compatible_percent(value: &Value, key: &str, legacy_key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .or_else(|| value.get(legacy_key).and_then(Value::as_f64))
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn legacy_snapshot(snapshot: &Value) -> bool {
    snapshot
        .get("valuation_complete")
        .and_then(Value::as_bool)
        .is_none()
}

fn snapshot_status(snapshot: &Value) -> Html {
    if legacy_snapshot(snapshot) {
        html! { <span class="badge bg-secondary text-white">{"旧版已实现口径"}</span> }
    } else if snapshot.get("data_complete").and_then(Value::as_bool) == Some(false) {
        html! { <span class="badge bg-danger text-white">{"成交数据不完整"}</span> }
    } else if snapshot.get("valuation_complete").and_then(Value::as_bool) == Some(false) {
        html! { <span class="badge bg-warning text-dark">{"估值不可用"}</span> }
    } else {
        html! { <span class="badge bg-success text-white">{"完整盯市口径"}</span> }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{compatible_money, legacy_snapshot, money_or_unavailable};

    #[test]
    fn total_pnl_never_falls_back_to_legacy_realized_pnl() {
        let report = json!({"net_pnl": 123.45, "total_net_pnl": null});

        assert_eq!(
            money_or_unavailable(&report, "total_net_pnl", "HKD"),
            "不可计算"
        );
        assert_eq!(
            compatible_money(&report, "realized_net_pnl", "net_pnl", "HKD"),
            "123.45 HKD"
        );
    }

    #[test]
    fn snapshot_without_valuation_flag_uses_legacy_label() {
        assert!(legacy_snapshot(&json!({"net_pnl": 10.0})));
        assert!(legacy_snapshot(
            &json!({"total_net_pnl": null, "valuation_complete": null})
        ));
        assert!(!legacy_snapshot(
            &json!({"total_net_pnl": null, "valuation_complete": false})
        ));
    }
}
