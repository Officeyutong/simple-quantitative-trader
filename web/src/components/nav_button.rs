use yew::prelude::*;

use super::app::Page;

#[derive(Properties, PartialEq)]
pub struct NavButtonProps {
    pub label: &'static str,
    pub target: Page,
    pub page: UseStateHandle<Page>,
}

#[function_component(NavButton)]
pub fn nav_button(props: &NavButtonProps) -> Html {
    let active = *props.page == props.target;
    html! {
        <button
            class={classes!("nav-link", "w-100", "text-start", "mb-2", active.then_some("active"))}
            onclick={{
                let page = props.page.clone();
                let target = props.target;
                Callback::from(move |_| page.set(target))
            }}
        >
            {props.label}
        </button>
    }
}
