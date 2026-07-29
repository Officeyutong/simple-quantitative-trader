use yew::prelude::*;

pub fn load_saved_page(key: &str) -> usize {
    web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(key).ok().flatten())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1)
}

pub fn save_page(key: &str, page: usize) {
    if let Some(storage) =
        web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    {
        let _ = storage.set_item(key, &page.max(1).to_string());
    }
}

#[derive(Properties, PartialEq)]
pub struct PaginationProps {
    pub page: usize,
    pub total_pages: usize,
    pub total_items: usize,
    pub on_page: Callback<usize>,
}

#[function_component(Pagination)]
pub fn pagination(props: &PaginationProps) -> Html {
    let total_pages = props.total_pages.max(1);
    let start = props.page.saturating_sub(2).max(1);
    let end = (start + 4).min(total_pages);
    html! {
        <nav class="d-flex flex-wrap justify-content-between align-items-center gap-2 p-3 border-top" aria-label="分页">
            <span class="small text-secondary">
                {format!("共 {} 条 · 第 {} / {} 页", props.total_items, props.page, total_pages)}
            </span>
            <ul class="pagination pagination-sm mb-0">
                <PageButton label="上一页" target={props.page.saturating_sub(1).max(1)}
                    disabled={props.page <= 1} active={false} on_page={props.on_page.clone()} />
                {(start..=end).map(|page| html! {
                    <PageButton label={page.to_string()} target={page} disabled={false}
                        active={page == props.page} on_page={props.on_page.clone()} />
                }).collect::<Html>()}
                <PageButton label="下一页" target={(props.page + 1).min(total_pages)}
                    disabled={props.page >= total_pages} active={false} on_page={props.on_page.clone()} />
            </ul>
        </nav>
    }
}

#[derive(Properties, PartialEq)]
struct PageButtonProps {
    label: String,
    target: usize,
    disabled: bool,
    active: bool,
    on_page: Callback<usize>,
}

#[function_component(PageButton)]
fn page_button(props: &PageButtonProps) -> Html {
    let target = props.target;
    let on_page = props.on_page.clone();
    html! {
        <li class={classes!("page-item", props.disabled.then_some("disabled"), props.active.then_some("active"))}>
            <button class="page-link" disabled={props.disabled} onclick={move |_| on_page.emit(target)}>
                {props.label.clone()}
            </button>
        </li>
    }
}
