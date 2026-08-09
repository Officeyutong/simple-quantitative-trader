use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    value::{array, boolean, number, text},
};

#[derive(Properties, PartialEq)]
pub struct ExecutionCostPageProps {
    pub endpoint: String,
    pub strategies: Value,
}

#[function_component(ExecutionCostPage)]
pub fn execution_cost_page(props: &ExecutionCostPageProps) -> Html {
    let models = use_state(Vec::<Value>::new);
    let controls = use_state(Vec::<Value>::new);
    let risk_controls = use_state(Vec::<Value>::new);
    let risk_base_currency = use_state(String::new);
    let selected_model_id = use_state(String::new);
    let name = use_state(String::new);
    let currency = use_state(|| "USD".to_owned());
    let fees = use_state(|| ["0"; 11].map(str::to_owned));
    let strategy_id = use_state(String::new);
    let control_model_id = use_state(String::new);
    let multiple = use_state(|| "2".to_owned());
    let ratio = use_state(|| "0.5".to_owned());
    let minimum_trades = use_state(|| "5".to_owned());
    let enabled = use_state(|| true);
    let risk_enabled = use_state(|| true);
    let risk_values = use_state(|| ["100000", "1", "0.02", "3", "10", "10"].map(str::to_owned));
    let risk_reset_note = use_state(String::new);
    let risk_reset_confirmed = use_state(|| false);
    let busy = use_state(|| false);
    let notice = use_state(|| None::<Result<String, String>>);
    let strategies = array(&props.strategies, "strategies");

    let reload = {
        let endpoint = props.endpoint.clone();
        let models = models.clone();
        let controls = controls.clone();
        let risk_controls = risk_controls.clone();
        let risk_base_currency = risk_base_currency.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            load(
                endpoint.clone(),
                models.clone(),
                controls.clone(),
                risk_controls.clone(),
                risk_base_currency.clone(),
                notice.clone(),
            )
        })
    };
    {
        let reload = reload.clone();
        use_effect_with(props.endpoint.clone(), move |_| {
            reload.emit(());
            || ()
        });
    }
    {
        let strategy_options = strategies
            .iter()
            .map(|value| text(value, "strategy_id"))
            .filter(|value| value != "—")
            .collect::<Vec<_>>();
        let strategy_id = strategy_id.clone();
        use_effect_with(strategy_options.clone(), move |_| {
            if !strategy_options.iter().any(|id| id == strategy_id.as_str()) {
                strategy_id.set(strategy_options.first().cloned().unwrap_or_default());
            }
            || ()
        });
    }
    {
        let selected_strategy_id = (*strategy_id).clone();
        let strategies = strategies.clone();
        let models_snapshot = (*models).clone();
        let controls_snapshot = (*controls).clone();
        let control_model_id = control_model_id.clone();
        let multiple = multiple.clone();
        let ratio = ratio.clone();
        let minimum_trades = minimum_trades.clone();
        let enabled = enabled.clone();
        use_effect_with(
            (
                selected_strategy_id.clone(),
                strategies.clone(),
                models_snapshot.clone(),
                controls_snapshot.clone(),
            ),
            move |_| {
                if let Some(control) = controls_snapshot
                    .iter()
                    .find(|control| text(control, "strategy_id") == selected_strategy_id)
                {
                    control_model_id.set(text(control, "cost_model_id"));
                    multiple.set(number(control, "minimum_cost_multiple"));
                    ratio.set(number(control, "maximum_commission_to_gross_profit_ratio"));
                    minimum_trades.set(number(control, "minimum_completed_trades"));
                    enabled.set(boolean(control, "enabled"));
                } else {
                    let strategy_currency = strategies
                        .iter()
                        .find(|strategy| text(strategy, "strategy_id") == selected_strategy_id)
                        .map(|strategy| text(strategy, "currency"));
                    let matching_model = strategy_currency.and_then(|currency| {
                        models_snapshot
                            .iter()
                            .find(|model| text(model, "currency").eq_ignore_ascii_case(&currency))
                    });
                    control_model_id.set(
                        matching_model
                            .map(|model| text(model, "cost_model_id"))
                            .unwrap_or_default(),
                    );
                    multiple.set("2".into());
                    ratio.set("0.5".into());
                    minimum_trades.set("5".into());
                    enabled.set(true);
                }
                || ()
            },
        );
    }
    {
        let selected_strategy_id = (*strategy_id).clone();
        let risk_controls_snapshot = (*risk_controls).clone();
        let risk_enabled = risk_enabled.clone();
        let risk_values = risk_values.clone();
        use_effect_with(
            (selected_strategy_id.clone(), risk_controls_snapshot.clone()),
            move |_| {
                if let Some(control) = risk_controls_snapshot
                    .iter()
                    .find(|control| text(control, "strategy_id") == selected_strategy_id)
                {
                    risk_enabled.set(boolean(control, "enabled"));
                    risk_values.set([
                        number(control, "strategy_capital"),
                        number(control, "maximum_position_capital_ratio"),
                        number(control, "maximum_rolling_24h_realized_net_loss_ratio"),
                        number(control, "maximum_consecutive_net_losing_trades"),
                        number(control, "maximum_rolling_24h_completed_trades"),
                        number(control, "maximum_rolling_24h_turnover_capital_ratio"),
                    ]);
                } else {
                    risk_enabled.set(true);
                    risk_values.set(["100000", "1", "0.02", "3", "10", "10"].map(str::to_owned));
                }
                || ()
            },
        );
    }

    let selected_strategy_currency = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == *strategy_id)
        .map(|strategy| text(strategy, "currency"))
        .unwrap_or_default();
    let selected_strategy_state = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == *strategy_id)
        .map(|strategy| text(strategy, "state"))
        .unwrap_or_default();
    let strategy_is_running = selected_strategy_state == "running";
    let selected_model_currency = models
        .iter()
        .find(|model| text(model, "cost_model_id") == *control_model_id)
        .map(|model| text(model, "currency"))
        .unwrap_or_default();
    let currency_mismatch = !selected_strategy_currency.is_empty()
        && !selected_model_currency.is_empty()
        && !selected_strategy_currency.eq_ignore_ascii_case(&selected_model_currency);
    let selected_risk_control = risk_controls
        .iter()
        .find(|control| text(control, "strategy_id") == *strategy_id);

    let save_model = {
        let endpoint = props.endpoint.clone();
        let selected_model_id = selected_model_id.clone();
        let name = name.clone();
        let currency = currency.clone();
        let fees = fees.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        let reload = reload.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let parsed = fees
                .iter()
                .map(|value| value.parse::<f64>())
                .collect::<Result<Vec<_>, _>>();
            let Ok(values) = parsed else {
                notice.set(Some(Err("所有费用字段必须是数字".into())));
                return;
            };
            let params = json!({
                "cost_model_id": if selected_model_id.is_empty() { Value::Null } else { Value::String((*selected_model_id).clone()) },
                "name": (*name).clone(),
                "currency": (*currency).clone(),
                "buy_fixed_fee": values[0], "buy_per_share_fee": values[1],
                "buy_rate_bps": values[2], "buy_min_fee": values[3],
                "sell_fixed_fee": values[4], "sell_per_share_fee": values[5],
                "sell_rate_bps": values[6], "sell_min_fee": values[7],
                "sell_tax_bps": values[8], "estimated_spread_bps": values[9],
                "estimated_slippage_bps": values[10]
            });
            busy.set(true);
            let endpoint = endpoint.clone();
            let busy = busy.clone();
            let notice = notice.clone();
            let reload = reload.clone();
            spawn_local(async move {
                match call_method(&endpoint, "execution_cost.model.upsert", params).await {
                    Ok(_) => {
                        notice.set(Some(Ok("费用模型已保存".into())));
                        reload.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    let save_control = {
        let endpoint = props.endpoint.clone();
        let strategy_id = strategy_id.clone();
        let control_model_id = control_model_id.clone();
        let multiple = multiple.clone();
        let ratio = ratio.clone();
        let minimum_trades = minimum_trades.clone();
        let enabled = enabled.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        let reload = reload.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let params = json!({
                "strategy_id": (*strategy_id).clone(),
                "enabled": *enabled,
                "cost_model_id": (*control_model_id).clone(),
                "minimum_cost_multiple": multiple.parse::<f64>().unwrap_or(0.0),
                "maximum_commission_to_gross_profit_ratio": ratio.parse::<f64>().unwrap_or(0.0),
                "minimum_completed_trades": minimum_trades.parse::<usize>().unwrap_or(0)
            });
            busy.set(true);
            let endpoint = endpoint.clone();
            let busy = busy.clone();
            let notice = notice.clone();
            let reload = reload.clone();
            spawn_local(async move {
                match call_method(&endpoint, "execution_cost.control.configure", params).await {
                    Ok(_) => {
                        notice.set(Some(Ok("策略成本控制已保存".into())));
                        reload.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    let save_risk_control = {
        let endpoint = props.endpoint.clone();
        let strategy_id = strategy_id.clone();
        let risk_enabled = risk_enabled.clone();
        let risk_values = risk_values.clone();
        let risk_base_currency = risk_base_currency.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        let reload = reload.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let values = (*risk_values).clone();
            let parsed = (
                values[0].parse::<f64>(),
                values[1].parse::<f64>(),
                values[2].parse::<f64>(),
                values[3].parse::<usize>(),
                values[4].parse::<usize>(),
                values[5].parse::<f64>(),
            );
            let (
                Ok(capital),
                Ok(position_ratio),
                Ok(loss_ratio),
                Ok(consecutive_losses),
                Ok(trades),
                Ok(turnover_ratio),
            ) = parsed
            else {
                notice.set(Some(Err(
                    "策略资本和各比例必须是数字，交易数及连续亏损数必须是非负整数".into(),
                )));
                return;
            };
            if !capital.is_finite()
                || capital <= 0.0
                || [position_ratio, loss_ratio, turnover_ratio]
                    .iter()
                    .any(|value| !value.is_finite() || *value < 0.0)
            {
                notice.set(Some(Err(
                    "策略资本必须大于 0；各限制比例必须是大于或等于 0 的有限数字".into(),
                )));
                return;
            }
            let params = json!({
                "strategy_id": (*strategy_id).clone(),
                "enabled": *risk_enabled,
                "strategy_capital": capital,
                "capital_currency": (*risk_base_currency).clone(),
                "maximum_position_capital_ratio": position_ratio,
                "maximum_rolling_24h_realized_net_loss_ratio": loss_ratio,
                "maximum_consecutive_net_losing_trades": consecutive_losses,
                "maximum_rolling_24h_completed_trades": trades,
                "maximum_rolling_24h_turnover_capital_ratio": turnover_ratio,
            });
            busy.set(true);
            let endpoint = endpoint.clone();
            let busy = busy.clone();
            let notice = notice.clone();
            let reload = reload.clone();
            spawn_local(async move {
                match call_method(&endpoint, "execution_risk.control.configure", params).await {
                    Ok(_) => {
                        notice.set(Some(Ok("策略风险控制已保存".into())));
                        reload.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    let reset_risk_statistics = {
        let endpoint = props.endpoint.clone();
        let strategy_id = strategy_id.clone();
        let risk_reset_note = risk_reset_note.clone();
        let risk_reset_confirmed = risk_reset_confirmed.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        let reload = reload.clone();
        Callback::from(move |_| {
            if !*risk_reset_confirmed {
                notice.set(Some(Err("请先勾选复位确认框".into())));
                return;
            }
            let note = risk_reset_note.trim().to_owned();
            if note.is_empty() {
                notice.set(Some(Err("复位说明不能为空".into())));
                return;
            }
            busy.set(true);
            let endpoint = endpoint.clone();
            let strategy_id = (*strategy_id).clone();
            let busy = busy.clone();
            let notice = notice.clone();
            let reload = reload.clone();
            let risk_reset_confirmed = risk_reset_confirmed.clone();
            spawn_local(async move {
                match call_method(
                    &endpoint,
                    "execution_risk.control.reset",
                    json!({"strategy_id": strategy_id, "confirm": true, "note": note}),
                )
                .await
                {
                    Ok(_) => {
                        risk_reset_confirmed.set(false);
                        notice.set(Some(Ok(
                            "风险统计基线已复位；滚动 24 小时指标仍按最近 24 小时计算".into(),
                        )));
                        reload.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    html! {
        <>
            <ErrorModal message={notice.as_ref().and_then(|value| value.as_ref().err()).cloned()}
                on_close={{ let notice = notice.clone(); Callback::from(move |_| notice.set(None)) }} />
            {notice.as_ref().and_then(|value| value.as_ref().ok()).map(|message| html! {
                <div class="alert alert-success" role="status">{message.clone()}</div>
            }).unwrap_or_default()}
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"费用模型"}</h2>
                <p class="text-secondary">{"模型保存在 DuckDB。bps 为万分之一；固定费、比例费和最低费可以同时使用。"}</p>
                <form onsubmit={save_model}>
                    <div class="row g-3">
                        <TextField
                            label="名称"
                            help="费用模型的唯一名称，仅用于识别和选择，例如 france-stock；不参与费用计算。"
                            value={name.clone()}
                        />
                        <TextField
                            label="币种"
                            help="费用金额使用的 ISO 货币代码，例如 EUR、USD、HKD。必须与策略交易证券的合约币种一致，否则成本门控会阻止交易。"
                            value={currency.clone()}
                        />
                        {[
                            ("买入固定费/笔", "每一笔买单固定收取的金额，与股数和成交金额无关。填 0 表示没有固定费。"),
                            ("买入每股费", "买入每股收取的金额。该项费用等于买入数量 × 此值；它不是每笔费用。"),
                            ("买入比例(bps)", "按买入名义金额收取的费率。1 bps = 0.01%，5 bps = 0.05%；费用等于名义金额 × bps ÷ 10000。"),
                            ("买入最低费", "单笔买入佣金的最低金额。系统先合计固定费、每股费和比例费，再与此值取较大者。"),
                            ("卖出固定费/笔", "每一笔卖单固定收取的金额，与股数和成交金额无关。填 0 表示没有固定费。"),
                            ("卖出每股费", "卖出每股收取的金额。该项费用等于卖出数量 × 此值；它不是每笔费用。"),
                            ("卖出比例(bps)", "按卖出名义金额收取的费率。1 bps = 0.01%，5 bps = 0.05%；费用等于名义金额 × bps ÷ 10000。"),
                            ("卖出最低费", "单笔卖出券商佣金的最低金额。系统先将固定费、每股费和比例费合计后与此值取较大者，再在结果之外另加卖出税费。"),
                            ("卖出税费(bps)", "仅在卖出一侧按名义金额估算的税费或监管费，单位 bps。它会与卖出比例费相加。"),
                            ("预计点差(bps)", "预计完整买卖价差占名义金额的比例，单位 bps。往返成本中计算一次完整点差；填 0 表示忽略点差。"),
                            ("单边预计滑点(bps)", "每次买入或卖出相对参考价的不利偏移，单位 bps。往返成本会计算两次，即买入一次、卖出一次。")
                        ].into_iter().enumerate().map(|(index, (label, help))| {
                            let value = fees[index].clone();
                            html! { <div class="col-6 col-lg-3"><HelpLabel label={label} help={help} />
                                <input class="form-control" type="number" min="0" step="any" value={value}
                                    oninput={{ let fees = fees.clone(); Callback::from(move |event: InputEvent| {
                                        let mut next = (*fees).clone();
                                        let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                        next[index] = input.value();
                                        fees.set(next);
                                    }) }} /></div> }
                        }).collect::<Html>()}
                        <div class="col-12"><button class="btn btn-primary" disabled={*busy}>{"保存费用模型"}</button></div>
                    </div>
                </form>
                <div class="table-responsive mt-4"><table class="table table-sm">
                    <thead><tr><th>{"名称"}</th><th>{"币种"}</th><th>{"买固定/每股/比例/最低"}</th>
                        <th>{"卖固定/每股/比例/最低/税"}</th><th>{"点差/滑点"}</th><th>{"操作"}</th></tr></thead>
                    <tbody>{models.iter().map(|model| html! { <tr>
                        <td>{text(model, "name")}</td><td>{text(model, "currency")}</td>
                        <td>{format!("{}/{}/{}/{}", number(model, "buy_fixed_fee"), number(model, "buy_per_share_fee"), number(model, "buy_rate_bps"), number(model, "buy_min_fee"))}</td>
                        <td>{format!("{}/{}/{}/{}/{}", number(model, "sell_fixed_fee"), number(model, "sell_per_share_fee"), number(model, "sell_rate_bps"), number(model, "sell_min_fee"), number(model, "sell_tax_bps"))}</td>
                        <td>{format!("{}/{}", number(model, "estimated_spread_bps"), number(model, "estimated_slippage_bps"))}</td>
                        <td><button class="btn btn-sm btn-outline-primary" type="button" onclick={{
                            let model = model.clone(); let selected_model_id = selected_model_id.clone();
                            let name = name.clone(); let currency = currency.clone(); let fees = fees.clone();
                            Callback::from(move |_| {
                                selected_model_id.set(text(&model, "cost_model_id"));
                                name.set(text(&model, "name")); currency.set(text(&model, "currency"));
                                fees.set([
                                    number(&model, "buy_fixed_fee"), number(&model, "buy_per_share_fee"),
                                    number(&model, "buy_rate_bps"), number(&model, "buy_min_fee"),
                                    number(&model, "sell_fixed_fee"), number(&model, "sell_per_share_fee"),
                                    number(&model, "sell_rate_bps"), number(&model, "sell_min_fee"),
                                    number(&model, "sell_tax_bps"), number(&model, "estimated_spread_bps"),
                                    number(&model, "estimated_slippage_bps")
                                ]);
                            })
                        }}>{"编辑"}</button></td>
                    </tr> }).collect::<Html>()}</tbody>
                </table></div>
            </div></section>

            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"策略成本控制"}</h2>
                <form onsubmit={save_control}><div class="row g-3">
                    <SelectField label="策略" value={strategy_id.clone()} options={strategies.iter().map(|v| (text(v, "strategy_id"), text(v, "name"))).collect::<Vec<_>>()} />
                    <SelectField label="费用模型" value={control_model_id.clone()} options={models.iter().map(|v| (text(v, "cost_model_id"), text(v, "name"))).collect::<Vec<_>>()} />
                    <TextField
                        label="成本安全倍数"
                        help="信号必须覆盖预计往返成本的倍数，最小为 1。例如预计成本为 10 bps、倍数为 2，则信号强度至少需要 20 bps。"
                        value={multiple.clone()}
                    />
                    <TextField
                        label="最大佣金/毛利润"
                        help="累计佣金相对于正毛利润的允许上限。0.5 表示 50%；达到最少交易数且毛利润为正时，若实际比例超过此值，系统会阻止后续开仓或加仓，但不会自动暂停执行配置。毛利润为零或负数时不使用该比例门控。"
                        value={ratio.clone()}
                    />
                    <TextField
                        label="门控最少交易数"
                        help="至少完成这么多笔已实现交易后，才根据实际佣金/毛利润比例阻止后续开仓或加仓，避免样本过少时误判。"
                        value={minimum_trades.clone()}
                    />
                    <div class="col-6 col-lg-3 form-check mt-5"><input class="form-check-input" type="checkbox" checked={*enabled}
                        onchange={{ let enabled = enabled.clone(); Callback::from(move |event: Event| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); enabled.set(input.checked());
                        }) }} /><label class="form-check-label">{"启用成本门控"}</label></div>
                    {
                        currency_mismatch.then(|| html! {
                            <div class="col-12">
                                <div class="alert alert-danger mb-0">
                                    {format!(
                                        "费用模型币种 {} 与策略证券币种 {} 不匹配，请选择相同币种的模型。",
                                        selected_model_currency, selected_strategy_currency
                                    )}
                                </div>
                            </div>
                        }).unwrap_or_default()
                    }
                    {
                        strategy_is_running.then(|| html! {
                            <div class="col-12">
                                <div class="alert alert-warning mb-0" role="alert">
                                    <strong>{"当前无法保存："}</strong>
                                    {"所选策略正在运行。请先到“策略”页面点击“暂停信号”，并等待所有处理中动作完成；保存成本控制后再重新启动策略。"}
                                </div>
                            </div>
                        }).unwrap_or_default()
                    }
                    <div class="col-12"><button class="btn btn-primary"
                        title={strategy_is_running.then_some("策略运行期间不能修改成本控制，请先暂停策略")}
                        disabled={*busy || strategy_id.is_empty() || control_model_id.is_empty() || currency_mismatch || strategy_is_running}>
                        {"保存策略控制"}
                    </button></div>
                </div></form>
                <div class="table-responsive mt-4"><table class="table table-sm"><thead><tr>
                    <th>{"策略"}</th><th>{"模型"}</th><th>{"启用"}</th><th>{"安全倍数"}</th><th>{"佣金比例上限"}</th><th>{"最少交易数"}</th>
                </tr></thead><tbody>{controls.iter().map(|row| html! { <tr>
                    <td>{text(row, "strategy_name")}</td><td>{text(row, "cost_model_name")}</td>
                    <td>{if boolean(row, "enabled") { "已启用" } else { "已停用" }}</td><td>{number(row, "minimum_cost_multiple")}</td>
                    <td>{number(row, "maximum_commission_to_gross_profit_ratio")}</td>
                    <td>{number(row, "minimum_completed_trades")}</td>
                </tr> }).collect::<Html>()}</tbody></table></div>
            </div></section>

            <section class="card shadow-sm"><div class="card-body">
                <h2 class="h5">{"策略风险控制"}</h2>
                <p class="text-secondary mb-2">
                    {"所有阈值都保存在数据库中。触发后只阻止增加风险的开仓或加仓；经当前持仓验证的严格减仓和平仓只旁路本页策略级阈值，全局交易开关、交易控制/紧急停止和交易日历仍然有效。"}
                </p>
                <div class="alert alert-info py-2">
                    {"除策略资本外，任一限制字段填 0 表示关闭该单项限制；关闭总开关则停用整组策略风险门控。"}
                </div>
                <form onsubmit={save_risk_control}><div class="row g-3">
                    <SelectField label="策略" value={strategy_id.clone()} options={strategies.iter().map(|v| (text(v, "strategy_id"), text(v, "name"))).collect::<Vec<_>>()} />
                    <div class="col-6 col-lg-3">
                        <HelpLabel label="基础币种" help="保存时会把该币种与策略资本金额一起写入数据库。它必须匹配 daemon 的全局风险基础币种，避免修改配置后静默改变旧预算含义；不能在此处单独修改。" />
                        <input class="form-control" value={(*risk_base_currency).clone()} readonly=true />
                    </div>
                    {[
                        ("策略资本", "分配给该策略的资本金额，单位为基础币种。它是仓位、亏损和换手比例限制的计算基数；必须大于 0。"),
                        ("最大持仓/资本", "目标持仓名义金额相对于策略资本的上限。1 表示 100%，0 表示关闭该项限制。系统使用新鲜行情和汇率在下单前估算。"),
                        ("24h 最大净亏损/资本", "滚动 24 小时已实现净亏损占策略资本的上限。0.02 表示 2%；佣金计入净损益，0 表示关闭。"),
                        ("最大连续净亏损交易", "完整交易周期在扣除佣金后连续亏损达到该笔数时阻止新开仓。盈利周期会清零连续计数；0 表示关闭。"),
                        ("24h 最多完成交易", "滚动 24 小时内允许完成的交易周期数。达到上限后阻止新开仓，0 表示关闭。"),
                        ("24h 最大换手/资本", "滚动 24 小时成交名义金额相对于策略资本的上限。10 表示最多换手 10 倍策略资本，0 表示关闭。"),
                    ].into_iter().enumerate().map(|(index, (label, help))| {
                        let value = risk_values[index].clone();
                        html! { <div class="col-6 col-lg-3"><HelpLabel label={label} help={help} />
                            <input class="form-control" type="number" min="0" step="any" value={value}
                                oninput={{ let risk_values = risk_values.clone(); Callback::from(move |event: InputEvent| {
                                    let mut next = (*risk_values).clone();
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                    next[index] = input.value();
                                    risk_values.set(next);
                                }) }} /></div> }
                    }).collect::<Html>()}
                    <div class="col-6 col-lg-3 form-check mt-5"><input class="form-check-input" type="checkbox" checked={*risk_enabled}
                        onchange={{ let risk_enabled = risk_enabled.clone(); Callback::from(move |event: Event| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); risk_enabled.set(input.checked());
                        }) }} /><label class="form-check-label">{"启用策略风险门控"}</label></div>
                    {
                        strategy_is_running.then(|| html! {
                            <div class="col-12">
                                <div class="alert alert-warning mb-0" role="alert">
                                    <strong>{"当前无法保存："}</strong>
                                    {"所选策略正在运行。请先到“策略”页面暂停信号，并等待所有处理中动作完成；保存风险控制后再重新启动。已有风险门控仍会继续生效。"}
                                </div>
                            </div>
                        }).unwrap_or_default()
                    }
                    {
                        selected_risk_control
                            .filter(|control| !boolean(control, "currency_matches_daemon"))
                            .map(|control| html! {
                                <div class="col-12">
                                    <div class="alert alert-danger mb-0" role="alert">
                                        <strong>{"自动执行启用及新的风险增加动作已被阻止："}</strong>
                                        {control.get("statistics")
                                            .and_then(|value| value.get("warning"))
                                            .and_then(Value::as_str)
                                            .unwrap_or("策略资本币种缺失或与 daemon 配置不一致。请核对资本金额并点击下方“保存策略风险控制”，为它绑定当前基础币种。")}
                                    </div>
                                </div>
                            })
                            .unwrap_or_default()
                    }
                    <div class="col-12"><button class="btn btn-primary"
                        title={strategy_is_running.then_some("策略运行期间不能修改风险控制，请先暂停策略")}
                        disabled={*busy || strategy_id.is_empty() || risk_base_currency.is_empty() || strategy_is_running}>
                        {"保存策略风险控制"}
                    </button></div>
                </div></form>

                <div class="border rounded p-3 mt-4">
                    <h3 class="h6">{"复位累计统计基线"}</h3>
                    <p class="small text-secondary">
                        {"复位会从当前时间重新累计连续净亏损及累计成本绩效，不会删除成交，也不会清除滚动 24 小时亏损、交易数和换手记录。存在归因持仓、未决订单或处理中动作时会拒绝复位，请先平仓并完成对账。"}
                    </p>
                    <label class="form-label" for="risk-reset-note">{"复位说明（必填）"}</label>
                    <textarea id="risk-reset-note" class="form-control" rows="2"
                        value={(*risk_reset_note).clone()} oninput={{
                            let risk_reset_note = risk_reset_note.clone();
                            Callback::from(move |event: InputEvent| {
                                let input: web_sys::HtmlTextAreaElement = event.target_unchecked_into();
                                risk_reset_note.set(input.value());
                            })
                        }} />
                    <div class="form-check mt-3">
                        <input id="risk-reset-confirm" class="form-check-input" type="checkbox"
                            checked={*risk_reset_confirmed} onchange={{
                                let risk_reset_confirmed = risk_reset_confirmed.clone();
                                Callback::from(move |event: Event| {
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                    risk_reset_confirmed.set(input.checked());
                                })
                            }} />
                        <label class="form-check-label" for="risk-reset-confirm">
                            {"我确认已复核成交、持仓、未决订单及风险统计，并理解复位不会删除历史成交。"}
                        </label>
                    </div>
                    <button class="btn btn-outline-danger mt-3" type="button"
                        disabled={*busy || selected_risk_control.is_none() || risk_reset_note.trim().is_empty() || !*risk_reset_confirmed}
                        onclick={reset_risk_statistics}>
                        {"确认复位统计基线"}
                    </button>
                </div>

                {selected_risk_control.map(|control| {
                    let statistics = control.get("statistics").unwrap_or(&Value::Null);
                    html! {
                        <div class="mt-4">
                            <h3 class="h6">{"所选策略当前风险统计"}</h3>
                            <div class="row g-3">
                                <RiskMetric label="统计完整性" value={if boolean(statistics, "data_complete") { String::from("完整") } else { String::from("不完整") }} />
                                <RiskMetric label="最近复位说明" value={text(control, "statistics_reset_note")} />
                                <RiskMetric label="24h 已实现净损益" value={risk_money(statistics, "rolling_24h_realized_net_pnl", &text(control, "capital_currency"))} />
                                <RiskMetric label="24h 换手" value={risk_money(statistics, "rolling_24h_turnover", &text(control, "capital_currency"))} />
                                <RiskMetric label="24h 完成交易" value={number(statistics, "rolling_24h_completed_trades")} />
                                <RiskMetric label="连续净亏损交易" value={number(statistics, "consecutive_net_losing_trades")} />
                            </div>
                            {statistics.get("warning").and_then(Value::as_str).map(|warning| html! {
                                <div class="alert alert-warning mt-3 mb-0">{format!("统计数据警告：{warning}")}</div>
                            }).unwrap_or_default()}
                        </div>
                    }
                }).unwrap_or_else(|| html! {
                    <div class="alert alert-danger mt-4 mb-0">{"所选策略尚未配置风险控制：自动执行启用及新的风险增加动作已被阻止。请核对策略资本与基础币种并保存后再启用策略。"}</div>
                })}

                <div class="table-responsive mt-4"><table class="table table-sm align-middle"><thead><tr>
                    <th>{"策略"}</th><th>{"启用"}</th><th>{"策略资本"}</th><th>{"持仓/资本"}</th>
                    <th>{"24h 净亏损"}</th><th>{"连续亏损"}</th><th>{"24h 交易"}</th><th>{"24h 换手"}</th>
                </tr></thead><tbody>{risk_controls.iter().map(|row| html! { <tr>
                    <td>{text(row, "strategy_name")}</td>
                    <td>{if boolean(row, "enabled") { "已启用" } else { "已停用" }}</td>
                    <td>{format!("{} {}", number(row, "strategy_capital"), text(row, "capital_currency"))}</td>
                    <td>{optional_ratio(row, "maximum_position_capital_ratio")}</td>
                    <td>{optional_ratio(row, "maximum_rolling_24h_realized_net_loss_ratio")}</td>
                    <td>{optional_count(row, "maximum_consecutive_net_losing_trades")}</td>
                    <td>{optional_count(row, "maximum_rolling_24h_completed_trades")}</td>
                    <td>{optional_ratio(row, "maximum_rolling_24h_turnover_capital_ratio")}</td>
                </tr> }).collect::<Html>()}</tbody></table></div>
            </div></section>
        </>
    }
}

fn load(
    endpoint: String,
    models: UseStateHandle<Vec<Value>>,
    controls: UseStateHandle<Vec<Value>>,
    risk_controls: UseStateHandle<Vec<Value>>,
    risk_base_currency: UseStateHandle<String>,
    notice: UseStateHandle<Option<Result<String, String>>>,
) {
    spawn_local(async move {
        match call_method(&endpoint, "execution_cost.model.list", json!({})).await {
            Ok(value) => models.set(array(&value, "models")),
            Err(error) => notice.set(Some(Err(error))),
        }
        match call_method(&endpoint, "execution_cost.control.list", json!({})).await {
            Ok(value) => controls.set(array(&value, "controls")),
            Err(error) => notice.set(Some(Err(error))),
        }
        match call_method(&endpoint, "execution_risk.control.list", json!({})).await {
            Ok(value) => {
                risk_base_currency.set(text(&value, "base_currency"));
                risk_controls.set(array(&value, "controls"));
            }
            Err(error) => notice.set(Some(Err(error))),
        }
    });
}

fn optional_ratio(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|ratio| {
            if ratio <= 0.0 {
                "关闭".into()
            } else {
                format!("{}（{:.2}%）", number(value, key), ratio * 100.0)
            }
        })
        .unwrap_or_else(|| "—".into())
}

fn optional_count(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_u64)
        .map(|count| {
            if count == 0 {
                "关闭".into()
            } else {
                count.to_string()
            }
        })
        .unwrap_or_else(|| "—".into())
}

fn risk_money(value: &Value, key: &str, currency: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|amount| format!("{amount:.2} {currency}"))
        .unwrap_or_else(|| "—".into())
}

#[derive(Properties, PartialEq)]
struct RiskMetricProps {
    label: &'static str,
    value: String,
}

#[function_component(RiskMetric)]
fn risk_metric(props: &RiskMetricProps) -> Html {
    html! {
        <div class="col-12 col-md-6 col-xl-3">
            <div class="small text-secondary">{props.label}</div>
            <div class="text-break">{props.value.clone()}</div>
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct TextFieldProps {
    label: &'static str,
    help: &'static str,
    value: UseStateHandle<String>,
}
#[function_component(TextField)]
fn text_field(props: &TextFieldProps) -> Html {
    html! { <div class="col-6 col-lg-3"><HelpLabel label={props.label} help={props.help} />
    <input class="form-control" value={(*props.value).clone()} oninput={{
        let value = props.value.clone(); Callback::from(move |event: InputEvent| {
            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
        })
    }} /></div> }
}

#[derive(Properties, PartialEq)]
struct HelpLabelProps {
    label: &'static str,
    help: &'static str,
}

#[function_component(HelpLabel)]
fn help_label(props: &HelpLabelProps) -> Html {
    html! {
        <label class="form-label d-flex align-items-center gap-1">
            <span>{props.label}</span>
            <span
                class="field-help"
                tabindex="0"
                aria-label={format!("{}说明：{}", props.label, props.help)}
            >
                {"?"}
                <span class="field-help-popup" role="tooltip">{props.help}</span>
            </span>
        </label>
    }
}

#[derive(Properties, PartialEq)]
struct SelectFieldProps {
    label: &'static str,
    value: UseStateHandle<String>,
    options: Vec<(String, String)>,
}
#[function_component(SelectField)]
fn select_field(props: &SelectFieldProps) -> Html {
    html! { <div class="col-12 col-lg-4"><label class="form-label">{props.label}</label>
    <select class="form-select" value={(*props.value).clone()} onchange={{
        let value = props.value.clone(); Callback::from(move |event: Event| {
            let input: web_sys::HtmlSelectElement = event.target_unchecked_into(); value.set(input.value());
        })
    }}>
        <option value="" selected={props.value.is_empty()}>{"请选择"}</option>
        {props.options.iter().map(|(id, label)| html! {
            <option
                key={id.clone()}
                value={id.clone()}
                selected={id == props.value.as_str()}
            >
                {label}
            </option>
        }).collect::<Html>()}
    </select></div> }
}
