use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ErrorModalProps {
    pub message: Option<String>,
    pub on_close: Callback<()>,
}

#[function_component(ErrorModal)]
pub fn error_modal(props: &ErrorModalProps) -> Html {
    let Some(message) = props.message.as_ref() else {
        return Html::default();
    };
    let close = {
        let on_close = props.on_close.clone();
        Callback::from(move |_| on_close.emit(()))
    };

    html! {
        <>
            <div
                class="modal fade show d-block"
                tabindex="-1"
                role="dialog"
                aria-modal="true"
                aria-labelledby="rpc-error-title"
            >
                <div class="modal-dialog modal-dialog-centered">
                    <div class="modal-content shadow">
                        <div class="modal-header bg-danger text-white">
                            <h2 id="rpc-error-title" class="modal-title h5">{"操作失败"}</h2>
                            <button
                                type="button"
                                class="btn-close btn-close-white"
                                aria-label="关闭"
                                onclick={close.clone()}
                            />
                        </div>
                        <div class="modal-body">
                            <p class="mb-0 text-break" style="white-space: pre-wrap;">{message}</p>
                        </div>
                        <div class="modal-footer">
                            <button class="btn btn-secondary" onclick={close.clone()}>{"关闭"}</button>
                        </div>
                    </div>
                </div>
            </div>
            <div class="modal-backdrop fade show" onclick={close} />
        </>
    }
}
