use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct BoolBadgeProps {
    pub value: bool,
}

#[function_component(BoolBadge)]
pub fn bool_badge(props: &BoolBadgeProps) -> Html {
    html! {
        <span class={classes!("badge", if props.value { "bg-success" } else { "bg-secondary" })}>
            {if props.value { "是" } else { "否" }}
        </span>
    }
}
