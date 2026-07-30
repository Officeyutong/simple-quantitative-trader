use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    value::{array, number, text},
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
        let model_options = models
            .iter()
            .map(|value| text(value, "cost_model_id"))
            .filter(|value| value != "—")
            .collect::<Vec<_>>();
        let key = (strategy_options.clone(), model_options.clone());
        let strategy_id = strategy_id.clone();
        let control_model_id = control_model_id.clone();
        use_effect_with(key, move |_| {
            if !strategy_options.iter().any(|id| id == strategy_id.as_str()) {
                strategy_id.set(strategy_options.first().cloned().unwrap_or_default());
            }
            if !model_options
                .iter()
                .any(|id| id == control_model_id.as_str())
            {
                control_model_id.set(model_options.first().cloned().unwrap_or_default());
            }
            || ()
        });
    }

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
                        <TextField label="名称" value={name.clone()} />
                        <TextField label="币种" value={currency.clone()} />
                        {[
                            "买入固定费/笔", "买入每股费", "买入比例(bps)", "买入最低费",
                            "卖出固定费/笔", "卖出每股费", "卖出比例(bps)", "卖出最低费",
                            "卖出税费(bps)", "预计点差(bps)", "单边预计滑点(bps)"
                        ].into_iter().enumerate().map(|(index, label)| {
                            let value = fees[index].clone();
                            html! { <div class="col-6 col-lg-3"><label class="form-label">{label}</label>
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
                    <TextField label="成本安全倍数" value={multiple.clone()} />
                    <TextField label="最大佣金/毛利润" value={ratio.clone()} />
                    <TextField label="熔断最少交易数" value={minimum_trades.clone()} />
                    <div class="col-6 col-lg-3 form-check mt-5"><input class="form-check-input" type="checkbox" checked={*enabled}
                        onchange={{ let enabled = enabled.clone(); Callback::from(move |event: Event| {
                            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); enabled.set(input.checked());
                        }) }} /><label class="form-check-label">{"启用成本门控"}</label></div>
                    <div class="col-12"><button class="btn btn-primary" disabled={*busy || strategy_id.is_empty() || control_model_id.is_empty()}>{"保存策略控制"}</button></div>
                </div></form>
                <div class="table-responsive mt-4"><table class="table table-sm"><thead><tr>
                    <th>{"策略"}</th><th>{"模型"}</th><th>{"启用"}</th><th>{"安全倍数"}</th><th>{"佣金比例上限"}</th><th>{"最少交易数"}</th>
                </tr></thead><tbody>{controls.iter().map(|row| html! { <tr>
                    <td>{text(row, "strategy_name")}</td><td>{text(row, "cost_model_name")}</td>
                    <td>{text(row, "enabled")}</td><td>{number(row, "minimum_cost_multiple")}</td>
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
    value: UseStateHandle<String>,
}
#[function_component(TextField)]
fn text_field(props: &TextFieldProps) -> Html {
    html! { <div class="col-6 col-lg-3"><label class="form-label">{props.label}</label>
    <input class="form-control" value={(*props.value).clone()} oninput={{
        let value = props.value.clone(); Callback::from(move |event: InputEvent| {
            let input: web_sys::HtmlInputElement = event.target_unchecked_into(); value.set(input.value());
        })
    }} /></div> }
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
    }}><option value="">{"请选择"}</option>{props.options.iter().map(|(id, label)|
        html! { <option value={id.clone()}>{label}</option> }).collect::<Html>()}</select></div> }
}
