use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;
use yew_bootstrap::{component::Alert, util::Color};

use crate::api::{DashboardData, call_method};

use super::{
    MutationRequest,
    action_button::ActionButton,
    bool_badge::BoolBadge,
    delete_strategy_button::DeleteStrategyButton,
    error_modal::ErrorModal,
    pagination::{Pagination, load_saved_page, save_page},
    rename_strategy_button::RenameStrategyButton,
    value::{
        array, boolean, integer, local_time, number, official_security_name, security_exchange,
        text,
    },
};

#[derive(Properties, PartialEq)]
pub struct StrategiesPageProps {
    pub endpoint: String,
    pub data: DashboardData,
    pub on_mutation: Callback<MutationRequest>,
}

#[function_component(StrategiesPage)]
pub fn strategies_page(props: &StrategiesPageProps) -> Html {
    let strategies = array(&props.data.strategies, "strategies");
    let configs = array(&props.data.execution_configs, "configs");
    let actions = use_state(Vec::<Value>::new);
    let actions_page = use_state(|| load_saved_page("quant-trader.strategy-actions-page"));
    let actions_total_pages = use_state(|| 1_usize);
    let actions_total_items = use_state(|| 0_usize);
    let actions_error = use_state(|| None::<String>);
    let actions_loading = use_state(|| false);
    {
        let endpoint = props.endpoint.clone();
        let page = *actions_page;
        let actions = actions.clone();
        let total_pages = actions_total_pages.clone();
        let total_items = actions_total_items.clone();
        let error = actions_error.clone();
        let loading = actions_loading.clone();
        use_effect_with((endpoint.clone(), page), move |_| {
            load_actions(
                endpoint.clone(),
                page,
                actions.clone(),
                total_pages.clone(),
                total_items.clone(),
                error.clone(),
                loading.clone(),
            );
            let interval = Interval::new(5_000, move || {
                load_actions(
                    endpoint.clone(),
                    page,
                    actions.clone(),
                    total_pages.clone(),
                    total_items.clone(),
                    error.clone(),
                    loading.clone(),
                );
            });
            move || drop(interval)
        });
    }
    html! {
        <>
            <ErrorModal message={(*actions_error).clone()} on_close={{
                let error = actions_error.clone();
                Callback::from(move |_| error.set(None))
            }} />
            <Alert style={Color::Info}>
                {"策略信号和自动执行是两个独立开关。启用执行前请核对账户、证券和目标仓位。"}
            </Alert>
            <section class="mb-4">
                <h2 class="h5">{"策略列表"}</h2>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"名称"}</th><th>{"类型"}</th><th>{"证券（官方名称）"}</th><th>{"所属交易所"}</th><th>{"信号状态"}</th><th>{"执行状态"}</th>
                            <th>{"Strategy ID"}</th><th>{"操作"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if strategies.is_empty() {
                                html! { <tr><td colspan="8" class="text-center text-secondary py-4">{"暂无策略"}</td></tr> }
                            } else {
                                strategies.iter().map(|row| {
                                    let id = text(row, "strategy_id");
                                    let state = text(row, "state");
                                    let name = text(row, "name");
                                    let execution_config = configs.iter().find(|config| {
                                        text(config, "strategy_id") == id
                                    });
                                    let execution_enabled = execution_config
                                        .is_some_and(|config| boolean(config, "enabled"));
                                    let (execution_label, execution_class) =
                                        if execution_enabled {
                                            ("已启用", "bg-success")
                                        } else if execution_config.is_some() {
                                            ("已停用", "bg-secondary")
                                        } else {
                                            ("未配置", "bg-light text-dark")
                                        };
                                    html! {
                                        <tr>
                                            <td class="fw-semibold">{name.clone()}</td>
                                            <td>{text(row, "kind")}</td>
                                            <td>
                                                <div class="fw-semibold">{official_security_name(row)}</div>
                                                <div class="small text-secondary">{text(row, "symbol")}</div>
                                            </td>
                                            <td>{security_exchange(row)}</td>
                                            <td><span class="badge bg-secondary">{state.clone()}</span></td>
                                            <td><span class={classes!("badge", execution_class)}>{execution_label}</span></td>
                                            <td class="strategy-id"><code>{id.clone()}</code></td>
                                            <td><div class="d-flex flex-wrap gap-2">
                                                <RenameStrategyButton
                                                    strategy_id={id.clone()}
                                                    strategy_name={name.clone()}
                                                    on_mutation={props.on_mutation.clone()} />
                                                <ActionButton label="启动信号" class="btn-outline-success"
                                                    disabled={state == "running"}
                                                    method="strategy.start" strategy_id={id.clone()}
                                                    confirm={false} on_mutation={props.on_mutation.clone()} />
                                                <ActionButton label="暂停信号" class="btn-outline-secondary"
                                                    disabled={state != "running"}
                                                    method="strategy.pause" strategy_id={id.clone()}
                                                    confirm={false} on_mutation={props.on_mutation.clone()} />
                                                <ActionButton label="停止策略" class="btn-outline-danger"
                                                    disabled={state == "stopped"}
                                                    method="strategy.stop" strategy_id={id.clone()}
                                                    confirm={true} on_mutation={props.on_mutation.clone()} />
                                                <ActionButton label="启用 Paper 执行" class="btn-outline-primary"
                                                    disabled={execution_enabled}
                                                    method="strategy.execution.enable" strategy_id={id.clone()}
                                                    confirm={true} on_mutation={props.on_mutation.clone()} />
                                                <ActionButton label="关闭自动执行" class="btn-outline-warning"
                                                    disabled={!execution_enabled}
                                                    method="strategy.execution.disable" strategy_id={id.clone()}
                                                    confirm={true} on_mutation={props.on_mutation.clone()} />
                                                <DeleteStrategyButton
                                                    strategy_id={id}
                                                    strategy_name={name}
                                                    disabled={state != "stopped" || execution_enabled}
                                                    on_mutation={props.on_mutation.clone()} />
                                            </div></td>
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
                <h2 class="h5">{"策略执行配置"}</h2>
                <p class="text-secondary">
                    {"切换盘前盘后会重新保存执行配置并关闭自动执行；核对限价模式后需要重新启用 Paper 执行。"}
                </p>
                <div class="card shadow-sm table-responsive">
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"策略 ID"}</th><th>{"账户"}</th><th>{"证券（官方名称）"}</th><th>{"所属交易所"}</th><th>{"Conid"}</th>
                            <th class="text-end">{"多头目标"}</th><th class="text-end">{"空头目标"}</th>
                            <th>{"订单类型"}</th><th>{"盘前盘后"}</th><th>{"Paper Only"}</th><th>{"允许做空"}</th><th>{"启用"}</th>
                            <th>{"更新时间（本地）"}</th><th>{"操作"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if configs.is_empty() {
                                html! { <tr><td colspan="14" class="text-center text-secondary py-4">{"暂无执行配置"}</td></tr> }
                            } else {
                                configs.iter().map(|row| {
                                    let contract = row.get("contract").unwrap_or(&serde_json::Value::Null);
                                    let outside_rth = boolean(row, "outside_rth");
                                    let params = json!({
                                        "strategy_id": text(row, "strategy_id"),
                                        "account": text(row, "account"),
                                        "target_quantity": row.get("target_quantity").cloned().unwrap_or(Value::Null),
                                        "short_target_quantity": row.get("short_target_quantity").cloned().unwrap_or(Value::Null),
                                        "allow_short": boolean(row, "allow_short"),
                                        "outside_rth": !outside_rth,
                                        "order_type": if outside_rth { "market" } else { "limit" },
                                        "paper_only": boolean(row, "paper_only"),
                                        "contract": contract.clone()
                                    });
                                    html! { <tr>
                                        <td class="strategy-id"><code>{text(row, "strategy_id")}</code></td>
                                        <td>{text(row, "account")}</td>
                                        <td>
                                            <div class="fw-semibold">{official_security_name(contract)}</div>
                                            <div class="small text-secondary">{text(contract, "symbol")}</div>
                                        </td>
                                        <td>{security_exchange(contract)}</td>
                                        <td>{integer(contract, "conid")}</td>
                                        <td class="text-end">{number(row, "target_quantity")}</td>
                                        <td class="text-end">{number(row, "short_target_quantity")}</td>
                                        <td>{text(row, "order_type")}</td>
                                        <td><BoolBadge value={boolean(row, "outside_rth")} /></td>
                                        <td><BoolBadge value={boolean(row, "paper_only")} /></td>
                                        <td><BoolBadge value={boolean(row, "allow_short")} /></td>
                                        <td><BoolBadge value={boolean(row, "enabled")} /></td>
                                        <td class="text-nowrap">{local_time(row, "updated_at")}</td>
                                        <td><button class="btn btn-sm btn-outline-primary text-nowrap" onclick={{
                                            let callback = props.on_mutation.clone();
                                            Callback::from(move |_| callback.emit(MutationRequest {
                                                method: "strategy.execution.configure".into(),
                                                params: params.clone(),
                                                on_complete: Callback::noop(),
                                            }))
                                        }}>
                                            {if outside_rth { "关闭盘前盘后" } else { "开启盘前盘后" }}
                                        </button></td>
                                    </tr> }
                                }).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                </div>
            </section>
            <section class="mb-4">
                <h2 class="h5">{"最近执行动作"}</h2>
                <div class="card shadow-sm table-responsive">
                    <Pagination page={*actions_page} total_pages={*actions_total_pages}
                        total_items={*actions_total_items} on_page={{
                            let page = actions_page.clone();
                            Callback::from(move |next| {
                                save_page("quant-trader.strategy-actions-page", next);
                                page.set(next);
                            })
                    }} />
                    <table class="table table-hover align-middle mb-0">
                        <thead><tr>
                            <th>{"时间（本地）"}</th><th>{"策略名称"}</th><th>{"策略 ID"}</th><th>{"证券及交易所"}</th><th>{"信号"}</th>
                            <th class="text-end">{"请求数量"}</th><th>{"状态"}</th>
                            <th class="text-end">{"信号强度(bps)"}</th>
                            <th class="text-end">{"成本门槛(bps)"}</th>
                            <th class="text-end">{"预计往返成本"}</th>
                            <th>{"成本门控结果"}</th>
                            <th>{"Broker Order ID"}</th><th>{"详情"}</th>
                        </tr></thead>
                        <tbody>
                        {
                            if actions.is_empty() {
                                html! { <tr><td colspan="13" class="text-center text-secondary py-4">{"暂无执行动作"}</td></tr> }
                            } else {
                                actions.iter().map(|row| {
                                    let strategy_id = text(row, "strategy_id");
                                    let strategy_name = strategies
                                        .iter()
                                        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
                                        .map(|strategy| text(strategy, "name"))
                                        .unwrap_or_else(|| "—".into());
                                    html! {
                                        <tr>
                                            <td class="text-nowrap">{local_time(row, "created_at")}</td>
                                            <td>{strategy_name}</td>
                                            <td class="strategy-id"><code>{strategy_id}</code></td>
                                            <td>{action_securities(row)}</td>
                                            <td>{text(row, "signal")}</td>
                                            <td class="text-end">{number(row, "requested_quantity")}</td>
                                            <td><span class="badge bg-secondary">{text(row, "state")}</span></td>
                                            <td class="text-end">{number(row, "signal_edge_bps")}</td>
                                            <td class="text-end">{number(row, "required_edge_bps")}</td>
                                            <td class="text-end">{number(row, "estimated_round_trip_cost")}</td>
                                            <td>{cost_gate_result(row)}</td>
                                            <td>{integer(row, "broker_order_id")}</td>
                                            <td>{text(row, "detail")}</td>
                                        </tr>
                                    }
                                }).collect::<Html>()
                            }
                        }
                        </tbody>
                    </table>
                    <Pagination page={*actions_page} total_pages={*actions_total_pages}
                        total_items={*actions_total_items} on_page={{
                            let page = actions_page.clone();
                            Callback::from(move |next| {
                                save_page("quant-trader.strategy-actions-page", next);
                                page.set(next);
                            })
                        }} />
                </div>
                {(*actions_loading).then(|| html! { <div class="small text-secondary mt-2">{"正在刷新当前页…"}</div> }).unwrap_or_default()}
            </section>
        </>
    }
}

fn load_actions(
    endpoint: String,
    page: usize,
    actions: UseStateHandle<Vec<Value>>,
    total_pages: UseStateHandle<usize>,
    total_items: UseStateHandle<usize>,
    error: UseStateHandle<Option<String>>,
    loading: UseStateHandle<bool>,
) {
    loading.set(true);
    spawn_local(async move {
        match call_method(
            &endpoint,
            "strategy.execution.actions",
            json!({"page": page, "page_size": 25}),
        )
        .await
        {
            Ok(value) => {
                actions.set(array(&value, "actions"));
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

fn cost_gate_result(action: &Value) -> Html {
    let result = action
        .get("cost_gate_result")
        .and_then(Value::as_str)
        .unwrap_or("");
    let (label, class, explanation) = match result {
        "passed" => ("通过", "bg-success", "信号收益已达到配置的成本门槛"),
        "blocked" => (
            "已拦截",
            "bg-danger",
            "信号收益、费用模型币种或其他成本条件未通过",
        ),
        "auto_paused" => (
            "自动暂停",
            "bg-warning text-dark",
            "历史佣金与毛利润比例超过配置上限",
        ),
        "execution_disabled" => (
            "执行关闭",
            "bg-secondary",
            "信号产生时该策略的 Paper 自动执行配置未启用",
        ),
        _ => ("未执行", "bg-secondary", "该动作没有进入成本门控"),
    };
    html! {
        <div class="text-nowrap">
            <span class={classes!("badge", class)} title={explanation}>{label}</span>
            {(!result.is_empty()).then(|| html! {
                <div class="small text-secondary mt-1"><code>{result}</code></div>
            }).unwrap_or_default()}
        </div>
    }
}

fn action_securities(action: &serde_json::Value) -> Html {
    let legs = action
        .get("legs")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if legs.is_empty() {
        return html! { "—" };
    }
    html! {
        <>
            {legs.iter().map(|leg| html! {
                <div class="mb-1">
                    <span class="fw-semibold">{official_security_name(leg)}</span>
                    <span class="text-secondary">{format!(" ({}, {})", text(leg, "symbol"), security_exchange(leg))}</span>
                </div>
            }).collect::<Html>()}
        </>
    }
}
