use gloo_timers::callback::Interval;
use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    strategy_chart::StrategyChart,
    value::{
        array, boolean, integer, local_time, number, official_security_name, security_exchange,
        text,
    },
};

#[derive(Properties, PartialEq)]
pub struct StrategyStatusPageProps {
    pub endpoint: String,
    pub strategies: Value,
    pub execution_configs: Value,
}

#[function_component(StrategyStatusPage)]
pub fn strategy_status_page(props: &StrategyStatusPageProps) -> Html {
    let strategies = array(&props.strategies, "strategies");
    let selected_id = use_state(|| {
        strategies
            .first()
            .map(|strategy| text(strategy, "strategy_id"))
            .unwrap_or_default()
    });
    let evaluations = use_state(Vec::<Value>::new);
    let bars = use_state(Vec::<Value>::new);
    let cost_controls = use_state(Vec::<Value>::new);
    let cost_models = use_state(Vec::<Value>::new);
    let risk_controls = use_state(Vec::<Value>::new);
    let calendar_sessions = use_state(Vec::<Value>::new);
    let calendar_status = use_state(|| None::<Value>);
    let error = use_state(|| None::<String>);
    let busy = use_state(|| false);

    {
        let selected_id = selected_id.clone();
        let strategy_ids = strategies
            .iter()
            .map(|strategy| text(strategy, "strategy_id"))
            .collect::<Vec<_>>();
        use_effect_with(strategy_ids, move |strategy_ids| {
            if !strategy_ids.iter().any(|id| id == &*selected_id) {
                selected_id.set(strategy_ids.first().cloned().unwrap_or_default());
            }
            || ()
        });
    }

    {
        let endpoint = props.endpoint.clone();
        let selected_id_value = (*selected_id).clone();
        let strategies = strategies.clone();
        let evaluations = evaluations.clone();
        let bars = bars.clone();
        let cost_controls = cost_controls.clone();
        let cost_models = cost_models.clone();
        let risk_controls = risk_controls.clone();
        let execution_configs = array(&props.execution_configs, "configs");
        let calendar_sessions = calendar_sessions.clone();
        let calendar_status = calendar_status.clone();
        let error = error.clone();
        let busy = busy.clone();
        let current_selection = selected_id.clone();
        use_effect_with((endpoint.clone(), selected_id_value.clone()), move |_| {
            refresh_status(
                endpoint.clone(),
                selected_id_value.clone(),
                strategies.clone(),
                evaluations.clone(),
                bars.clone(),
                cost_controls.clone(),
                cost_models.clone(),
                risk_controls.clone(),
                execution_configs.clone(),
                calendar_sessions.clone(),
                calendar_status.clone(),
                error.clone(),
                busy.clone(),
                current_selection.clone(),
            );
            let interval = Interval::new(5_000, move || {
                refresh_status(
                    endpoint.clone(),
                    selected_id_value.clone(),
                    strategies.clone(),
                    evaluations.clone(),
                    bars.clone(),
                    cost_controls.clone(),
                    cost_models.clone(),
                    risk_controls.clone(),
                    execution_configs.clone(),
                    calendar_sessions.clone(),
                    calendar_status.clone(),
                    error.clone(),
                    busy.clone(),
                    current_selection.clone(),
                );
            });
            move || drop(interval)
        });
    }

    let strategy = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == *selected_id);
    let required_bars = strategy.map(strategy_required_bars).unwrap_or(0);
    let available_bars = bars.len() as u64;
    let bar_timeframe = strategy.map(strategy_timeframe).unwrap_or_default();
    let bar_timeframe_label = match bar_timeframe.as_str() {
        "5s" => "5 秒".to_owned(),
        "1m" => "1 分钟".to_owned(),
        "" => "—".to_owned(),
        value => value.to_owned(),
    };
    let latest = evaluations.first();
    let cost_control = cost_controls
        .iter()
        .find(|control| text(control, "strategy_id") == *selected_id);
    let cost_model = cost_control.and_then(|control| {
        let cost_model_id = text(control, "cost_model_id");
        cost_models
            .iter()
            .find(|model| text(model, "cost_model_id") == cost_model_id)
    });
    let risk_control = risk_controls
        .iter()
        .find(|control| text(control, "strategy_id") == *selected_id);
    let execution_configs = array(&props.execution_configs, "configs");
    let execution_config = execution_configs
        .iter()
        .find(|config| text(config, "strategy_id") == *selected_id);
    let execution_contract =
        execution_config.and_then(|config| config.get("contract").filter(|value| !value.is_null()));
    let outside_rth = execution_config.is_some_and(|config| boolean(config, "outside_rth"));
    let progress = if required_bars == 0 {
        0
    } else {
        (available_bars.saturating_mul(100) / required_bars).min(100)
    };

    html! {
        <>
            <ErrorModal message={(*error).clone()} on_close={{
                let error = error.clone();
                Callback::from(move |_| error.set(None))
            }} />
            <section class="card shadow-sm mb-4"><div class="card-body">
                <div class="d-flex flex-wrap justify-content-between align-items-end gap-3">
                    <div class="flex-grow-1">
                        <label class="form-label" for="status-strategy">{"选择策略"}</label>
                        <select id="status-strategy" class="form-select" value={(*selected_id).clone()} onchange={{
                            let selected_id = selected_id.clone();
                            let evaluations = evaluations.clone();
                            let bars = bars.clone();
                            let calendar_sessions = calendar_sessions.clone();
                            let calendar_status = calendar_status.clone();
                            Callback::from(move |event: Event| {
                                let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                evaluations.set(Vec::new());
                                bars.set(Vec::new());
                                calendar_sessions.set(Vec::new());
                                calendar_status.set(None);
                                selected_id.set(input.value());
                            })
                        }}>
                            {strategies.iter().map(|strategy| {
                                let id = text(strategy, "strategy_id");
                                html! {
                                    <option
                                        key={id.clone()}
                                        value={id.clone()}
                                        selected={id == *selected_id}
                                    >
                                        {text(strategy, "name")}
                                    </option>
                                }
                            }).collect::<Html>()}
                        </select>
                    </div>
                    <span class="text-secondary">{if *busy { "刷新中…" } else { "每 5 秒自动刷新" }}</span>
                </div>
            </div></section>

            {strategy.map(|strategy| html! {
                <>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"运行状态"}</h2>
                        <div class="row g-3">
                            <Status label="完整策略 UUID" value={text(strategy, "strategy_id")} />
                            <Status label="名称" value={text(strategy, "name")} />
                            <Status label="类型" value={text(strategy, "kind")} />
                            <Status label="Bar 周期" value={bar_timeframe_label.clone()} />
                            <Status label="状态" value={text(strategy, "state")} />
                            <Status label="证券（官方名称）" value={official_security_name(strategy)} />
                            <Status label="所属交易所" value={security_exchange(strategy)} />
                            <Status label="Conid" value={integer(strategy, "conid")} />
                            <Status label="最后处理 Bar（本地）" value={local_time(strategy, "last_evaluated_bar")} />
                            <Status label="最近错误" value={text(strategy, "last_error")} />
                            {strategy_catalog_web::render_config(
                                &text(strategy, "kind"),
                                strategy.get("config").unwrap_or(&Value::Null),
                            ).unwrap_or_default()}
                        </div>
                    </div></section>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"交易时间"}</h2>
                        {execution_config.map(|config| {
                            let status = (*calendar_status).clone();
                            let configured = status.as_ref()
                                .and_then(|value| value.get("configured"))
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            let open = status.as_ref()
                                .and_then(|value| value.get("open"))
                                .and_then(Value::as_bool);
                            let current_state = match (configured, open) {
                                (true, Some(true)) => "当前可交易",
                                (true, Some(false)) => "当前休市",
                                _ => "日历尚未配置",
                            };
                            let source = calendar_sessions.first()
                                .map(|session| text(session, "source"))
                                .unwrap_or_else(|| "—".into());
                            html! {
                                <>
                                    <div class="row g-3 mb-3">
                                        <Status label="执行证券" value={execution_contract.map(official_security_name).unwrap_or_else(|| "—".into())} />
                                        <Status label="交易所日历" value={execution_contract.map(security_exchange).unwrap_or_else(|| "—".into())} />
                                        <Status label="检查的交易时段" value={if outside_rth { "扩展交易时段（tradingHours）" } else { "正常交易时段（liquidHours）" }} />
                                        <Status label="当前状态" value={current_state} />
                                        <Status label="盘前盘后" value={if outside_rth { "允许" } else { "不允许" }} />
                                        <Status label="订单类型" value={text(config, "order_type")} />
                                        <Status label="日历来源/时区" value={source} />
                                        <Status label="缓存区间数" value={calendar_sessions.len().to_string()} />
                                    </div>
                                    {
                                        if calendar_sessions.is_empty() {
                                            html! {
                                                <div class="alert alert-warning mb-0">
                                                    {"当前没有该交易所对应类型的缓存时段。自动执行会在下单前尝试从 IBKR 更新；无法确认交易时间时会拒绝订单。"}
                                                </div>
                                            }
                                        } else {
                                            html! {
                                                <div class="table-responsive">
                                                    <table class="table table-sm table-hover align-middle mb-0">
                                                        <thead><tr>
                                                            <th>{"交易日期"}</th>
                                                            <th>{"类型"}</th>
                                                            <th>{"开市（本地）"}</th>
                                                            <th>{"收市（本地）"}</th>
                                                            <th>{"更新时间（本地）"}</th>
                                                        </tr></thead>
                                                        <tbody>
                                                            {calendar_sessions.iter().take(10).map(|session| html! {
                                                                <tr>
                                                                    <td>{text(session, "trading_date")}</td>
                                                                    <td>{if text(session, "session_kind") == "extended" { "扩展" } else { "正常" }}</td>
                                                                    <td class="text-nowrap">{local_time(session, "opens_at")}</td>
                                                                    <td class="text-nowrap">{local_time(session, "closes_at")}</td>
                                                                    <td class="text-nowrap">{local_time(session, "updated_at")}</td>
                                                                </tr>
                                                            }).collect::<Html>()}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            }
                                        }
                                    }
                                </>
                            }
                        }).unwrap_or_else(|| html! {
                            <div class="alert alert-secondary mb-0">
                                {"该策略尚未配置自动执行，因此没有可关联的交易所日历和交易时间。"}
                            </div>
                        })}
                    </div></section>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"成本控制"}</h2>
                        {cost_control.map(|control| html! {
                            <>
                                <div class="row g-3">
                                    <Status label="成本门控" value={if boolean(control, "enabled") { "已启用" } else { "已停用" }.to_owned()} />
                                    <Status label="费用模型" value={text(control, "cost_model_name")} />
                                    <Status label="模型币种" value={cost_model.map(|model| text(model, "currency")).unwrap_or_else(|| "—".into())} />
                                    <Status label="成本安全倍数" value={number(control, "minimum_cost_multiple")} />
                                    <Status label="佣金/毛利润上限" value={format_ratio(control, "maximum_commission_to_gross_profit_ratio")} />
                                    <Status label="门控最少交易数" value={integer(control, "minimum_completed_trades")} />
                                    <Status label="买入费用（固定/股/比例/最低）" value={cost_model.map(|model| fee_summary(model, "buy")).unwrap_or_else(|| "—".into())} />
                                    <Status label="卖出费用（固定/股/比例/最低）" value={cost_model.map(|model| fee_summary(model, "sell")).unwrap_or_else(|| "—".into())} />
                                    <Status label="卖出税费" value={cost_model.map(|model| format!("{} bps", number(model, "sell_tax_bps"))).unwrap_or_else(|| "—".into())} />
                                    <Status label="预计点差" value={cost_model.map(|model| format!("{} bps", number(model, "estimated_spread_bps"))).unwrap_or_else(|| "—".into())} />
                                    <Status label="单边预计滑点" value={cost_model.map(|model| format!("{} bps", number(model, "estimated_slippage_bps"))).unwrap_or_else(|| "—".into())} />
                                </div>
                                {(!boolean(control, "enabled")).then(|| html! {
                                    <div class="alert alert-warning mt-3 mb-0">{"该策略已关联费用模型，但成本门控当前未启用。"}</div>
                                }).unwrap_or_default()}
                            </>
                        }).unwrap_or_else(|| html! {
                            <div class="alert alert-secondary mb-0">{"尚未给该策略配置成本控制；当前不会执行成本门控。"}</div>
                        })}
                    </div></section>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"策略风险控制"}</h2>
                        {risk_control.map(|control| {
                            let statistics = control.get("statistics").unwrap_or(&Value::Null);
                            let currency = text(control, "capital_currency");
                            let capital = control.get("strategy_capital").and_then(Value::as_f64).unwrap_or(0.0);
                            let loss_ratio = control.get("maximum_rolling_24h_realized_net_loss_ratio").and_then(Value::as_f64).unwrap_or(0.0);
                            let loss_limit = capital * loss_ratio;
                            let turnover_ratio = control.get("maximum_rolling_24h_turnover_capital_ratio").and_then(Value::as_f64).unwrap_or(0.0);
                            let turnover_limit = capital * turnover_ratio;
                            let data_complete = boolean(statistics, "data_complete");
                            let threshold_reached = strategy_risk_threshold_reached(control);
                            html! {
                                <>
                                    <div class="row g-3">
                                        <Status label="风险门控" value={if boolean(control, "enabled") { "已启用".to_owned() } else { "已停用".to_owned() }} />
                                        <Status label="基础币种" value={currency.clone()} />
                                        <Status label="策略资本" value={format!("{} {}", number(control, "strategy_capital"), currency)} />
                                        <Status label="最大持仓/资本" value={format_optional_ratio(control, "maximum_position_capital_ratio")} />
                                        <Status label="24h 最大净亏损" value={if loss_limit > 0.0 { format!("{loss_limit:.2} {}（{}）", currency, format_ratio(control, "maximum_rolling_24h_realized_net_loss_ratio")) } else { "关闭".into() }} />
                                        <Status label="最大连续净亏损交易" value={format_optional_count(control, "maximum_consecutive_net_losing_trades")} />
                                        <Status label="24h 最多完成交易" value={format_optional_count(control, "maximum_rolling_24h_completed_trades")} />
                                        <Status label="24h 最大换手" value={if turnover_limit > 0.0 { format!("{turnover_limit:.2} {}（{} 倍资本）", currency, number(control, "maximum_rolling_24h_turnover_capital_ratio")) } else { "关闭".into() }} />
                                        <Status label="统计基线复位时间（本地）" value={local_time(control, "statistics_reset_at")} />
                                        <Status label="最近复位说明" value={text(control, "statistics_reset_note")} />
                                        <Status label="统计完整性" value={if data_complete { "完整".to_owned() } else { "不完整".to_owned() }} />
                                        <Status label="24h 已实现净损益" value={risk_money(statistics, "rolling_24h_realized_net_pnl", &currency)} />
                                        <Status label="24h 换手" value={risk_money(statistics, "rolling_24h_turnover", &currency)} />
                                        <Status label="24h 完成交易" value={number(statistics, "rolling_24h_completed_trades")} />
                                        <Status label="连续净亏损交易" value={number(statistics, "consecutive_net_losing_trades")} />
                                    </div>
                                    <div class="alert alert-info mt-3 mb-0">
                                        {"策略风险门控只阻止开仓、加仓和反向开仓；经当前持仓验证的严格减仓和平仓只旁路这些策略级开仓阈值，全局交易开关、交易控制/紧急停止和交易日历仍然有效。最大持仓限制会在下单时用新鲜行情和汇率计算。"}
                                    </div>
                                    {(!boolean(control, "currency_matches_daemon")).then(|| html! {
                                        <div class="alert alert-danger mt-3 mb-0">
                                            <strong>{"自动执行启用及新的风险增加动作已被阻止："}</strong>
                                            {statistics.get("warning")
                                                .and_then(Value::as_str)
                                                .unwrap_or("策略资本币种缺失或与 daemon 当前 risk.base_currency 不一致。请暂停策略，到“交易成本”页面核对资本金额并重新保存策略风险控制。")}
                                        </div>
                                    }).unwrap_or_default()}
                                    {(!boolean(control, "enabled")).then(|| html! {
                                        <div class="alert alert-warning mt-3 mb-0">{"该策略的风险门控当前未启用。"}</div>
                                    }).unwrap_or_default()}
                                    {(boolean(control, "currency_matches_daemon") && boolean(control, "enabled") && (!data_complete || threshold_reached)).then(|| html! {
                                        <div class="alert alert-danger mt-3 mb-0">
                                            {if !data_complete {
                                                "风险统计不完整，新的风险增加订单会被阻止；严格减仓和平仓不受该策略级阈值限制，但仍须通过全局交易开关、交易控制/紧急停止和交易日历。"
                                            } else {
                                                "当前至少一个滚动风险阈值已经达到，新的风险增加订单会被阻止；严格减仓和平仓不受该策略级阈值限制，但仍须通过全局交易开关、交易控制/紧急停止和交易日历。"
                                            }}
                                        </div>
                                    }).unwrap_or_default()}
                                    {statistics.get("warning").and_then(Value::as_str).map(|warning| html! {
                                        <div class="alert alert-warning mt-3 mb-0">{format!("统计数据警告：{warning}")}</div>
                                    }).filter(|_| boolean(control, "currency_matches_daemon")).unwrap_or_default()}
                                </>
                            }
                        }).unwrap_or_else(|| html! {
                            <div class="alert alert-danger mb-0">
                                {"该策略尚未配置策略级风险控制：自动执行启用及新的风险增加动作已被阻止。请到“交易成本”页面设置并保存策略资本、基础币种、亏损、交易次数及换手限制；严格减仓仍须通过账户级风险和交易控制。"}
                            </div>
                        })}
                    </div></section>
                    <section class="card shadow-sm mb-4"><div class="card-body">
                        <h2 class="h5">{"Bar 准备进度"}</h2>
                        <div class="mb-2">{format!("已有 {} / 需要 {} 根已完成的 {} Bar", available_bars, required_bars, bar_timeframe_label)}</div>
                        <div class="progress" role="progressbar" aria-valuenow={progress.to_string()} aria-valuemin="0" aria-valuemax="100">
                            <div class="progress-bar" style={format!("width: {progress}%")}>{format!("{progress}%")}</div>
                        </div>
                    </div></section>
                </>
            }).unwrap_or_default()}

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"价格与均线图"}</h2>
                <p class="text-secondary">{"K 线、短均线和长均线共享时间轴；可缩放、平移并悬停查看数据。"}</p>
                <StrategyChart
                    bars={(*bars).clone()}
                    evaluations={(*evaluations).clone()}
                    symbol={strategy.map(|value| text(value, "symbol")).unwrap_or_else(|| "—".into())}
                    view_key={strategy.map(|value| text(value, "strategy_id")).unwrap_or_default()}
                />
            </div></section>

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"最新均线计算"}</h2>
                {latest.map(|row| html! {
                    <div class="row g-3">
                        <Status label="Bar 时间（本地）" value={local_time(row, "bar_time")} />
                        <Status label="当前信号" value={text(row, "signal")} />
                        <Status label="当前短均线" value={number(row, "short_value")} />
                        <Status label="当前长均线" value={number(row, "long_value")} />
                        <Status label="上一根短均线" value={number(row, "previous_short_value")} />
                        <Status label="上一根长均线" value={number(row, "previous_long_value")} />
                        <Status label="收盘价" value={row.pointer("/output/bar/close").and_then(Value::as_f64).map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())} />
                        <Status label="均线差" value={row.pointer("/output/gap_percent").and_then(Value::as_f64).map(|v| format!("{v:.4}%")).unwrap_or_else(|| "—".into())} />
                        <Status label="ATR" value={row.pointer("/output/atr").and_then(Value::as_f64).map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())} />
                        <Status label="ATR 占价格" value={row.pointer("/output/atr_percent").and_then(Value::as_f64).map(|v| format!("{v:.4}%")).unwrap_or_else(|| "—".into())} />
                        <Status label="合格方向" value={text_at(row, "/output/qualified_direction")} />
                        <Status label="待确认方向" value={text_at(row, "/output/pending_direction")} />
                        <Status label="确认进度" value={row.pointer("/output/confirmation_progress").and_then(Value::as_u64).map(|v| v.to_string()).unwrap_or_else(|| "—".into())} />
                        <Status label="确认窗口剩余 Bar" value={row.pointer("/output/confirmation_window_remaining").and_then(Value::as_u64).map(|v| v.to_string()).unwrap_or_else(|| "—".into())} />
                        <Status label="信号原因" value={text_at(row, "/output/signal_reason")} />
                        <Status label="计算写入时间（本地）" value={local_time(row, "created_at")} />
                    </div>
                }).unwrap_or_else(|| html! {
                    <div class="text-secondary">{"尚无计算结果。请确认策略已启动、行情订阅有效，并等待 Bar 数量达到要求。"}</div>
                })}
            </div></section>

            <section>
                <h2 class="h5">{"最近计算历史"}</h2>
                <div class="card shadow-sm table-responsive"><table class="table table-hover align-middle mb-0">
                    <thead><tr><th>{"Bar 时间（本地）"}</th><th>{"短均线"}</th><th>{"长均线"}</th><th>{"信号"}</th><th>{"收盘价"}</th><th>{"计算时间（本地）"}</th></tr></thead>
                    <tbody>
                        {if evaluations.is_empty() {
                            html! { <tr><td colspan="6" class="text-center text-secondary py-4">{"暂无计算记录"}</td></tr> }
                        } else {
                            evaluations.iter().map(|row| html! {
                                <tr>
                                    <td>{local_time(row, "bar_time")}</td><td>{number(row, "short_value")}</td>
                                    <td>{number(row, "long_value")}</td><td>{text(row, "signal")}</td>
                                    <td>{row.pointer("/output/bar/close").and_then(Value::as_f64).map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())}</td>
                                    <td>{local_time(row, "created_at")}</td>
                                </tr>
                            }).collect::<Html>()
                        }}
                    </tbody>
                </table></div>
            </section>
        </>
    }
}

fn refresh_status(
    endpoint: String,
    strategy_id: String,
    strategies: Vec<Value>,
    evaluations: UseStateHandle<Vec<Value>>,
    bars: UseStateHandle<Vec<Value>>,
    cost_controls: UseStateHandle<Vec<Value>>,
    cost_models: UseStateHandle<Vec<Value>>,
    risk_controls: UseStateHandle<Vec<Value>>,
    execution_configs: Vec<Value>,
    calendar_sessions: UseStateHandle<Vec<Value>>,
    calendar_status: UseStateHandle<Option<Value>>,
    error: UseStateHandle<Option<String>>,
    busy: UseStateHandle<bool>,
    current_selection: UseStateHandle<String>,
) {
    if strategy_id.is_empty() {
        return;
    }
    let conid = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
        .and_then(|strategy| strategy.get("conid"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let timeframe = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
        .map(strategy_timeframe)
        .unwrap_or_default();
    let required = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == strategy_id)
        .map(strategy_required_bars)
        .unwrap_or(0)
        .max(200);
    let execution_config = execution_configs
        .iter()
        .find(|config| text(config, "strategy_id") == strategy_id);
    let calendar_exchange = execution_config
        .and_then(|config| config.get("contract"))
        .map(security_exchange)
        .filter(|exchange| exchange != "—");
    let outside_rth = execution_config.is_some_and(|config| boolean(config, "outside_rth"));
    busy.set(true);
    spawn_local(async move {
        let requested_strategy_id = strategy_id.clone();
        match call_method(
            &endpoint,
            "strategy.signals",
            json!({"strategy_id": strategy_id, "limit": 100}),
        )
        .await
        {
            Ok(value) if *current_selection == requested_strategy_id => {
                evaluations.set(array(&value, "evaluations"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        match call_method(&endpoint, "execution_cost.control.list", json!({})).await {
            Ok(value) if *current_selection == requested_strategy_id => {
                cost_controls.set(array(&value, "controls"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        match call_method(&endpoint, "execution_cost.model.list", json!({})).await {
            Ok(value) if *current_selection == requested_strategy_id => {
                cost_models.set(array(&value, "models"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        match call_method(&endpoint, "execution_risk.control.list", json!({})).await {
            Ok(value) if *current_selection == requested_strategy_id => {
                risk_controls.set(array(&value, "controls"))
            }
            Ok(_) => return,
            Err(message) => error.set(Some(message)),
        }
        if let Some(exchange) = calendar_exchange {
            match call_method(
                &endpoint,
                "calendar.status",
                json!({"exchange": exchange, "outside_rth": outside_rth}),
            )
            .await
            {
                Ok(value) if *current_selection == requested_strategy_id => {
                    calendar_status.set(Some(value))
                }
                Ok(_) => return,
                Err(message) => error.set(Some(message)),
            }
            match call_method(
                &endpoint,
                "calendar.list",
                json!({"exchange": exchange, "limit": 100}),
            )
            .await
            {
                Ok(value) if *current_selection == requested_strategy_id => {
                    let expected_kind = if outside_rth { "extended" } else { "regular" };
                    calendar_sessions.set(
                        array(&value, "sessions")
                            .into_iter()
                            .filter(|session| text(session, "session_kind") == expected_kind)
                            .collect(),
                    );
                }
                Ok(_) => return,
                Err(message) => error.set(Some(message)),
            }
        } else if *current_selection == requested_strategy_id {
            calendar_status.set(None);
            calendar_sessions.set(Vec::new());
        }
        if conid > 0 && !timeframe.is_empty() {
            match call_method(
                &endpoint,
                "market_data.bars",
                json!({"conid": conid, "timeframe": timeframe, "limit": required.max(1)}),
            )
            .await
            {
                Ok(value) if *current_selection == requested_strategy_id => bars.set(
                    array(&value, "bars")
                        .into_iter()
                        .filter(|bar| bar.get("final").and_then(Value::as_bool).unwrap_or(false))
                        .collect(),
                ),
                Ok(_) => return,
                Err(message) => error.set(Some(message)),
            }
        }
        busy.set(false);
    });
}

fn fee_summary(model: &Value, side: &str) -> String {
    format!(
        "{} / {} / {} bps / {} {}",
        number(model, &format!("{side}_fixed_fee")),
        number(model, &format!("{side}_per_share_fee")),
        number(model, &format!("{side}_rate_bps")),
        number(model, &format!("{side}_min_fee")),
        text(model, "currency")
    )
}

fn format_ratio(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|ratio| format!("{} ({:.2}%)", number(value, key), ratio * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn format_optional_ratio(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|ratio| {
            if ratio <= 0.0 {
                String::from("关闭")
            } else {
                format!("{} ({:.2}%)", number(value, key), ratio * 100.0)
            }
        })
        .unwrap_or_else(|| String::from("—"))
}

fn format_optional_count(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_u64)
        .map(|count| {
            if count == 0 {
                String::from("关闭")
            } else {
                count.to_string()
            }
        })
        .unwrap_or_else(|| String::from("—"))
}

fn risk_money(value: &Value, key: &str, currency: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|amount| format!("{amount:.2} {currency}"))
        .unwrap_or_else(|| String::from("—"))
}

fn strategy_risk_threshold_reached(control: &Value) -> bool {
    let statistics = control.get("statistics").unwrap_or(&Value::Null);
    let capital = control
        .get("strategy_capital")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let loss_ratio = control
        .get("maximum_rolling_24h_realized_net_loss_ratio")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let realized_net_pnl = statistics
        .get("rolling_24h_realized_net_pnl")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let loss_reached =
        capital > 0.0 && loss_ratio > 0.0 && realized_net_pnl <= -(capital * loss_ratio);

    let maximum_consecutive_losses = control
        .get("maximum_consecutive_net_losing_trades")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let consecutive_losses = statistics
        .get("consecutive_net_losing_trades")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let consecutive_losses_reached =
        maximum_consecutive_losses > 0 && consecutive_losses >= maximum_consecutive_losses;

    let maximum_completed_trades = control
        .get("maximum_rolling_24h_completed_trades")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completed_trades = statistics
        .get("rolling_24h_completed_trades")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completed_trades_reached =
        maximum_completed_trades > 0 && completed_trades >= maximum_completed_trades;

    let turnover_ratio = control
        .get("maximum_rolling_24h_turnover_capital_ratio")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let turnover = statistics
        .get("rolling_24h_turnover")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let turnover_reached =
        capital > 0.0 && turnover_ratio > 0.0 && turnover >= capital * turnover_ratio;

    loss_reached || consecutive_losses_reached || completed_trades_reached || turnover_reached
}

fn strategy_timeframe(strategy: &Value) -> String {
    strategy
        .get("bar_timeframe")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn strategy_required_bars(strategy: &Value) -> u64 {
    strategy
        .get("minimum_history")
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn text_at(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or("—")
        .to_owned()
}

#[derive(Properties, PartialEq)]
struct StatusProps {
    label: &'static str,
    value: String,
}

#[function_component(Status)]
fn status(props: &StatusProps) -> Html {
    html! {
        <div class="col-12 col-md-6 col-xl-3">
            <div class="small text-secondary">{props.label}</div>
            <div class="text-break strategy-id">{props.value.clone()}</div>
        </div>
    }
}
