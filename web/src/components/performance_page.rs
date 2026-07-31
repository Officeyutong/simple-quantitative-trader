use gloo_timers::callback::Interval;
use serde_json::Value;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::{PerformanceData, load_performance};

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
                message={match &*state {
                    PerformanceState::Error(error) => Some(error.clone()),
                    _ => None,
                }}
                on_close={{
                    let state = state.clone();
                    Callback::from(move |_| state.set(PerformanceState::Loading))
                }}
            />
            <div class="card shadow-sm mb-4"><div class="card-body">
                <div class="row g-3 align-items-end">
                    <div class="col-12 col-lg-7">
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
                </div>
                <div class="form-text mt-2">{"收益指标只归因于该策略产生并成交的订单；每 5 秒自动刷新。"}</div>
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
    let cards = [
        ("净收益", money(report, "net_pnl", &currency)),
        ("收益率", percent(report, "return")),
        ("最大回撤", percent(report, "maximum_drawdown_pct")),
        ("胜率", percent(report, "win_rate")),
        ("Sharpe", number(report, "sharpe")),
        ("Sortino", number(report, "sortino")),
    ];
    html! {
        <>
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
                            <tr><td>{"初始资金"}</td><td>{money(report, "initial_capital", &currency)}</td><td>{"总收益"}</td><td>{money(report, "gross_pnl", &currency)}</td></tr>
                            <tr><td>{"佣金"}</td><td>{money(report, "commissions", &currency)}</td><td>{"换手金额"}</td><td>{money(report, "turnover", &currency)}</td></tr>
                            <tr><td>{"已实现交易"}</td><td>{integer(report, "realized_trade_count")}</td><td>{"未平仓证券数"}</td><td>{integer(report, "open_position_count")}</td></tr>
                            <tr><td>{"盈利交易"}</td><td>{integer(report, "winning_trade_count")}</td><td>{"亏损交易"}</td><td>{integer(report, "losing_trade_count")}</td></tr>
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
                            <th>{"时间（本地）"}</th><th>{"账户"}</th><th class="text-end">{"净收益"}</th>
                            <th class="text-end">{"总收益"}</th><th class="text-end">{"佣金"}</th>
                            <th class="text-end">{"换手金额"}</th><th class="text-end">{"交易数"}</th>
                            <th class="text-end">{"盈利"}</th><th class="text-end">{"亏损"}</th><th class="text-end">{"未平仓"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if snapshots.is_empty() {
                                html! { <tr><td colspan="10" class="text-center text-secondary py-4">{"暂无绩效快照"}</td></tr> }
                            } else {
                                snapshots.iter().map(|row| {
                                    let row_currency = text(row, "base_currency");
                                    html! {
                                        <tr>
                                            <td class="text-nowrap">{local_time(row, "observed_at")}</td>
                                            <td>{text(row, "account")}</td>
                                            <td class="text-end">{money(row, "net_pnl", &row_currency)}</td>
                                            <td class="text-end">{money(row, "gross_pnl", &row_currency)}</td>
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

fn percent(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "—".into())
}
