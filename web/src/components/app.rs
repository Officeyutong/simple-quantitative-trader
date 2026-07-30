use gloo_timers::callback::Interval;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{
    component::Button,
    util::{Color, include_inline},
};

use crate::api::{DashboardData, call_method, load_dashboard, load_rpc_endpoint};

use super::{
    MutationRequest, backtest_page::BacktestPage, dashboard_page::DashboardPage,
    error_modal::ErrorModal, execution_cost_page::ExecutionCostPage,
    instruments_page::InstrumentsPage, logs_page::LogsPage,
    moving_average_wizard_page::MovingAverageWizardPage, nav_button::NavButton,
    operations_page::OperationsPage, orders_page::OrdersPage,
    paper_validation_page::PaperValidationPage, performance_page::PerformancePage,
    rpc_tools_page::RpcToolsPage, settings_page::SettingsPage, strategies_page::StrategiesPage,
    strategy_status_page::StrategyStatusPage,
};

const REFRESH_INTERVAL_MS: u32 = 5_000;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Dashboard,
    Instruments,
    Strategies,
    StrategyStatus,
    Performance,
    Backtest,
    ExecutionCosts,
    MovingAverageWizard,
    PaperValidation,
    Orders,
    Operations,
    Logs,
    RpcTools,
    Settings,
}

#[derive(Clone, PartialEq)]
enum LoadState {
    Loading,
    Ready(DashboardData),
    Error(String),
}

fn refresh(state: UseStateHandle<LoadState>, endpoint: String, show_loading: bool) {
    if show_loading {
        state.set(LoadState::Loading);
    }
    spawn_local(async move {
        state.set(match load_dashboard(&endpoint).await {
            Ok(data) => LoadState::Ready(data),
            Err(error) => LoadState::Error(error),
        });
    });
}

#[function_component(App)]
pub fn app() -> Html {
    let page = use_state(|| Page::Dashboard);
    let state = use_state(|| LoadState::Loading);
    let rpc_endpoint = use_state(load_rpc_endpoint);
    let modal_error = use_state(|| None::<String>);

    {
        let state = state.clone();
        let endpoint = (*rpc_endpoint).clone();
        use_effect_with(endpoint.clone(), move |_| {
            refresh(state.clone(), endpoint.clone(), true);
            let interval = Interval::new(REFRESH_INTERVAL_MS, move || {
                refresh(state.clone(), endpoint.clone(), false);
            });
            move || drop(interval)
        });
    }

    let on_mutation = {
        let state = state.clone();
        let endpoint = (*rpc_endpoint).clone();
        let modal_error = modal_error.clone();
        Callback::from(move |request: MutationRequest| {
            let state = state.clone();
            let endpoint = endpoint.clone();
            let modal_error = modal_error.clone();
            spawn_local(async move {
                match call_method(&endpoint, &request.method, request.params).await {
                    Ok(_) => refresh(state, endpoint, false),
                    Err(error) => modal_error.set(Some(format!("{}: {error}", request.method))),
                }
                request.on_complete.emit(());
            });
        })
    };
    let on_refresh = {
        let state = state.clone();
        let endpoint = (*rpc_endpoint).clone();
        Callback::from(move |_| refresh(state.clone(), endpoint.clone(), false))
    };

    let environment = ready_data(&state)
        .and_then(|data| data.system.get("environment"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_uppercase();
    let ibkr_state = ready_data(&state)
        .and_then(|data| data.system.pointer("/ibkr/state"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let active_alerts = ready_data(&state)
        .and_then(|data| data.alerts.get("alerts"))
        .and_then(|value| value.as_array())
        .map_or(0, Vec::len);
    let displayed_error = (*modal_error).clone().or_else(|| match &*state {
        LoadState::Error(error) => Some(format!(
            "无法连接 daemon：{error}\n当前 RPC 地址：{}",
            *rpc_endpoint
        )),
        _ => None,
    });

    html! {
        <>
            {include_inline()}
            <ErrorModal message={displayed_error} on_close={{
                let modal_error = modal_error.clone();
                let state = state.clone();
                let endpoint = (*rpc_endpoint).clone();
                Callback::from(move |_| {
                    modal_error.set(None);
                    if matches!(&*state, LoadState::Error(_)) {
                        refresh(state.clone(), endpoint.clone(), true);
                    }
                })
            }} />
            <header class="navbar navbar-dark bg-dark px-3 shadow-sm">
                <span class="navbar-brand mb-0 h1">{"Quant Trader"}</span>
                <div class="d-flex align-items-center gap-2">
                    <span class={classes!("badge", if environment == "LIVE" { "bg-danger" } else { "bg-success" })}>{environment}</span>
                    <span class="badge bg-secondary">{format!("IBKR: {ibkr_state}")}</span>
                    <span class={classes!("badge", if active_alerts > 0 { "bg-warning text-dark" } else { "bg-success" })}>
                        {format!("告警: {active_alerts}")}
                    </span>
                </div>
            </header>
            <div class="container-fluid"><div class="row">
                <nav class="col-12 col-lg-2 sidebar p-3">
                    <NavButton label="总览" target={Page::Dashboard} page={page.clone()} />
                    <NavButton label="证券搜索" target={Page::Instruments} page={page.clone()} />
                    <NavButton label="策略" target={Page::Strategies} page={page.clone()} />
                    <NavButton label="策略状态" target={Page::StrategyStatus} page={page.clone()} />
                    <NavButton label="策略绩效" target={Page::Performance} page={page.clone()} />
                    <NavButton label="回测" target={Page::Backtest} page={page.clone()} />
                    <NavButton label="交易成本" target={Page::ExecutionCosts} page={page.clone()} />
                    <NavButton label="均线策略向导" target={Page::MovingAverageWizard} page={page.clone()} />
                    <NavButton label="Paper 验证" target={Page::PaperValidation} page={page.clone()} />
                    <NavButton label="订单与成交" target={Page::Orders} page={page.clone()} />
                    <NavButton label="运行维护" target={Page::Operations} page={page.clone()} />
                    <NavButton label="实时日志" target={Page::Logs} page={page.clone()} />
                    <NavButton label="RPC 工具" target={Page::RpcTools} page={page.clone()} />
                    <NavButton label="RPC 设置" target={Page::Settings} page={page.clone()} />
                </nav>
                <main class="col-12 col-lg-10 p-4">
                    <div class="d-flex flex-wrap justify-content-between align-items-center gap-2 mb-4">
                        <div>
                            <h1 class="h3 mb-1">{page_title(*page)}</h1>
                            <div class="small text-secondary">
                                {format!("每 {} 秒自动刷新 · RPC {}", REFRESH_INTERVAL_MS / 1_000, *rpc_endpoint)}
                            </div>
                        </div>
                        <Button
                            style={Color::Primary}
                            outline={true}
                            disabled={matches!(&*state, LoadState::Loading)}
                            onclick={{
                                let state = state.clone();
                                let endpoint = (*rpc_endpoint).clone();
                                Callback::from(move |_| refresh(state.clone(), endpoint.clone(), true))
                            }}
                        >
                            {
                                if matches!(&*state, LoadState::Loading) {
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
                        </Button>
                    </div>
                    {render_page(*page, &state, rpc_endpoint.clone(), on_mutation, on_refresh)}
                </main>
            </div></div>
        </>
    }
}

fn ready_data(state: &LoadState) -> Option<&DashboardData> {
    match state {
        LoadState::Ready(data) => Some(data),
        _ => None,
    }
}

fn page_title(page: Page) -> &'static str {
    match page {
        Page::Dashboard => "运行总览",
        Page::Instruments => "证券搜索",
        Page::Strategies => "策略",
        Page::StrategyStatus => "策略运行状态",
        Page::Performance => "策略绩效",
        Page::Backtest => "策略回测",
        Page::ExecutionCosts => "交易成本",
        Page::MovingAverageWizard => "均线策略向导",
        Page::PaperValidation => "Paper 策略验证",
        Page::Orders => "订单与成交",
        Page::Operations => "运行维护",
        Page::Logs => "Daemon 实时日志",
        Page::RpcTools => "RPC 工具",
        Page::Settings => "RPC 设置",
    }
}

fn render_page(
    page: Page,
    state: &LoadState,
    endpoint: UseStateHandle<String>,
    on_mutation: Callback<MutationRequest>,
    on_refresh: Callback<()>,
) -> Html {
    if page == Page::Settings {
        return html! {
            <SettingsPage endpoint={(*endpoint).clone()} endpoint_handle={endpoint} />
        };
    }
    match state {
        LoadState::Loading => html! {
            <div class="d-flex align-items-center gap-3 py-5">
                <div class="spinner-border text-primary" role="status" />
                <span>{"正在从 daemon 读取数据…"}</span>
            </div>
        },
        LoadState::Error(_) => Html::default(),
        LoadState::Ready(data) => match page {
            Page::Dashboard => html! { <DashboardPage data={data.clone()} /> },
            Page::Instruments => {
                html! { <InstrumentsPage endpoint={(*endpoint).clone()} /> }
            }
            Page::Strategies => html! {
                <StrategiesPage endpoint={(*endpoint).clone()} data={data.clone()} on_mutation={on_mutation} />
            },
            Page::StrategyStatus => html! {
                <StrategyStatusPage endpoint={(*endpoint).clone()} strategies={data.strategies.clone()} />
            },
            Page::Performance => html! {
                <PerformancePage
                    strategies={data.strategies.clone()}
                    endpoint={(*endpoint).clone()}
                />
            },
            Page::Backtest => html! {
                <BacktestPage
                    endpoint={(*endpoint).clone()}
                    strategies={data.strategies.clone()}
                />
            },
            Page::ExecutionCosts => html! {
                <ExecutionCostPage
                    endpoint={(*endpoint).clone()}
                    strategies={data.strategies.clone()}
                />
            },
            Page::MovingAverageWizard => {
                html! {
                    <MovingAverageWizardPage
                        endpoint={(*endpoint).clone()}
                        system={data.system.clone()}
                        on_completed={on_refresh}
                    />
                }
            }
            Page::PaperValidation => {
                html! {
                    <PaperValidationPage
                        endpoint={(*endpoint).clone()}
                        system={data.system.clone()}
                        on_completed={on_refresh}
                    />
                }
            }
            Page::Orders => html! { <OrdersPage endpoint={(*endpoint).clone()} /> },
            Page::Operations => {
                html! { <OperationsPage data={data.clone()} on_mutation={on_mutation} /> }
            }
            Page::Logs => html! { <LogsPage endpoint={(*endpoint).clone()} /> },
            Page::RpcTools => html! { <RpcToolsPage endpoint={(*endpoint).clone()} /> },
            Page::Settings => unreachable!(),
        },
    }
}
