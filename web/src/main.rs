mod api;
mod components;

fn main() {
    yew::Renderer::<components::app::App>::new().render();
}
