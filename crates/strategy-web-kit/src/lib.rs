use serde_json::{Map, Value};
use strategy_api::{ConfigFieldKind, StrategyMetadata};
use yew::prelude::*;

#[derive(Clone, Copy)]
pub struct StrategyWebRegistration {
    pub metadata: &'static StrategyMetadata,
    pub render_config: fn(&Value) -> Html,
}

pub fn render_config_table(metadata: &'static StrategyMetadata, config: &Value) -> Html {
    html! {
        <div class="col-12">
            <div class="row g-3">
                {metadata.fields.iter().map(|field| {
                    let value = config.get(field.key).map(format_value).unwrap_or_else(|| "—".into());
                    html! {
                        <div class="col-12 col-md-6 col-xl-3">
                            <div class="small text-secondary" title={field.help}>{field.label}</div>
                            <div class="text-break">{value}</div>
                        </div>
                    }
                }).collect::<Html>()}
            </div>
        </div>
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "—".into(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => if *value { "是" } else { "否" }.into(),
        Value::Number(value) => value.to_string(),
        value => value.to_string(),
    }
}

#[derive(Properties, PartialEq)]
pub struct GenericStrategyFormProps {
    pub metadata: &'static StrategyMetadata,
    pub value: Value,
    pub on_change: Callback<Value>,
    #[prop_or(false)]
    pub disabled: bool,
}

/// Schema-driven editor used by strategies that do not need a custom wizard.
#[function_component(GenericStrategyForm)]
pub fn generic_strategy_form(props: &GenericStrategyFormProps) -> Html {
    let object = props.value.as_object().cloned().unwrap_or_default();
    html! {
        <div class="row g-3">
            {props.metadata.fields.iter().map(|field| {
                let key = field.key;
                let id = format!("strategy-config-{key}");
                let current = object.get(key).cloned().unwrap_or(Value::Null);
                let on_change = props.on_change.clone();
                let original = object.clone();
                let control = match field.kind {
                    ConfigFieldKind::Select(options) => html! {
                        <select id={id.clone()} class="form-select" disabled={props.disabled}
                            onchange={Callback::from(move |event: Event| {
                                let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                emit_change(&on_change, &original, key, Value::String(input.value()));
                            })}>
                            {options.iter().map(|option| html! {
                                <option value={*option} selected={current.as_str() == Some(option)}>{*option}</option>
                            }).collect::<Html>()}
                        </select>
                    },
                    kind => {
                        let step = if matches!(kind, ConfigFieldKind::Integer | ConfigFieldKind::Instrument) { "1" } else { "any" };
                        let current = if current.is_null() {
                            String::new()
                        } else {
                            format_value(&current)
                        };
                        html! {
                            <input id={id.clone()} type="number" step={step} class="form-control"
                                disabled={props.disabled} value={current}
                                oninput={Callback::from(move |event: InputEvent| {
                                    let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                                    let value = if matches!(kind, ConfigFieldKind::Integer | ConfigFieldKind::Instrument) {
                                        input.value().parse::<i64>().map(Value::from).unwrap_or(Value::Null)
                                    } else {
                                        input.value().parse::<f64>().map(Value::from).unwrap_or(Value::Null)
                                    };
                                    emit_change(&on_change, &original, key, value);
                                })} />
                        }
                    }
                };
                html! {
                    <div class="col-12 col-md-6">
                        <label class="form-label" for={id} title={field.help}>
                            {field.label}
                            {field.required.then_some(" *").unwrap_or_default()}
                        </label>
                        {control}
                        <div class="form-text">{field.help}</div>
                    </div>
                }
            }).collect::<Html>()}
        </div>
    }
}

fn emit_change(callback: &Callback<Value>, original: &Map<String, Value>, key: &str, value: Value) {
    let mut next = original.clone();
    next.insert(key.to_owned(), value);
    callback.emit(Value::Object(next));
}
