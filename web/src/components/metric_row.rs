use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct MetricRowProps {
    pub label: &'static str,
    pub value: String,
    pub healthy: bool,
}

#[function_component(MetricRow)]
pub fn metric_row(props: &MetricRowProps) -> Html {
    html! {
        <tr>
            <td>{props.label}</td><td>{props.value.clone()}</td>
            <td><span class={classes!("badge", if props.healthy { "bg-success" } else { "bg-warning text-dark" })}>
                {if props.healthy { "正常" } else { "需关注" }}
            </span></td>
        </tr>
    }
}
