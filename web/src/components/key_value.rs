use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct KeyValueProps {
    pub label: &'static str,
    pub value: String,
}

#[function_component(KeyValue)]
pub fn key_value(props: &KeyValueProps) -> Html {
    html! { <tr><td class="text-secondary w-25">{props.label}</td><td>{props.value.clone()}</td></tr> }
}
