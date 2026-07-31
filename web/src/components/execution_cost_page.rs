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
    let busy = use_state(|| false);
    let notice = use_state(|| None::<Result<String, String>>);
    let strategies = array(&props.strategies, "strategies");

    let reload = {
        let endpoint = props.endpoint.clone();
        let models = models.clone();
        let controls = controls.clone();
        let notice = notice.clone();
        Callback::from(move |_| {
            load(
                endpoint.clone(),
                models.clone(),
                controls.clone(),
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

    let selected_strategy_currency = strategies
        .iter()
        .find(|strategy| text(strategy, "strategy_id") == *strategy_id)
        .map(|strategy| text(strategy, "currency"))
        .unwrap_or_default();
    let selected_model_currency = models
        .iter()
        .find(|model| text(model, "cost_model_id") == *control_model_id)
        .map(|model| text(model, "currency"))
        .unwrap_or_default();
    let currency_mismatch = !selected_strategy_currency.is_empty()
        && !selected_model_currency.is_empty()
        && !selected_strategy_currency.eq_ignore_ascii_case(&selected_model_currency);

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

    html! {
        <>
            <ErrorModal message={notice.as_ref().and_then(|value| value.as_ref().err()).cloned()}
                on_close={{ let notice = notice.clone(); Callback::from(move |_| notice.set(None)) }} />
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
                            ("卖出最低费", "单笔卖出佣金的最低金额。系统先合计固定费、每股费、比例费和卖出税，再与此值取较大者。"),
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

            <section class="card shadow-sm"><div class="card-body">
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
                        help="累计佣金相对于累计毛利润的允许上限。0.5 表示 50%；达到最少交易数后若实际比例超过此值，系统会自动暂停策略执行。"
                        value={ratio.clone()}
                    />
                    <TextField
                        label="熔断最少交易数"
                        help="至少完成这么多笔已实现交易后，才根据实际佣金/毛利润比例触发自动暂停，避免样本过少时误判。"
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
                    <div class="col-12"><button class="btn btn-primary"
                        disabled={*busy || strategy_id.is_empty() || control_model_id.is_empty() || currency_mismatch}>
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
        </>
    }
}

fn load(
    endpoint: String,
    models: UseStateHandle<Vec<Value>>,
    controls: UseStateHandle<Vec<Value>>,
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
    });
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
