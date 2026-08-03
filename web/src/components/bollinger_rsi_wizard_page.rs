use serde_json::{Value, json};
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::api::call_method;

use super::{
    error_modal::ErrorModal,
    instrument_search::InstrumentSearch,
    value::{integer, official_security_name, security_exchange, text},
};

#[derive(Properties, PartialEq)]
pub struct BollingerRsiWizardPageProps {
    pub endpoint: String,
    pub on_completed: Callback<()>,
}

#[function_component(BollingerRsiWizardPage)]
pub fn bollinger_rsi_wizard_page(props: &BollingerRsiWizardPageProps) -> Html {
    let selected = use_state(|| None::<Value>);
    let name = use_state(|| "bollinger-rsi-strategy".to_owned());
    let timeframe = use_state(|| "1m".to_owned());
    let bollinger_window = use_state(|| "20".to_owned());
    let deviations = use_state(|| "2".to_owned());
    let rsi_window = use_state(|| "14".to_owned());
    let oversold_rsi = use_state(|| "30".to_owned());
    let exit_rsi = use_state(|| "50".to_owned());
    let minimum_bandwidth = use_state(|| "0".to_owned());
    let busy = use_state(|| false);
    let notice = use_state(|| None::<Result<String, String>>);

    let create = {
        let endpoint = props.endpoint.clone();
        let selected = selected.clone();
        let name = name.clone();
        let timeframe = timeframe.clone();
        let bollinger_window = bollinger_window.clone();
        let deviations = deviations.clone();
        let rsi_window = rsi_window.clone();
        let oversold_rsi = oversold_rsi.clone();
        let exit_rsi = exit_rsi.clone();
        let minimum_bandwidth = minimum_bandwidth.clone();
        let busy = busy.clone();
        let notice = notice.clone();
        let on_completed = props.on_completed.clone();
        Callback::from(move |event: SubmitEvent| {
            event.prevent_default();
            let conid = selected
                .as_ref()
                .and_then(|value| value.get("conid"))
                .and_then(Value::as_i64);
            let parsed = (
                bollinger_window.parse::<usize>(),
                deviations.parse::<f64>(),
                rsi_window.parse::<usize>(),
                oversold_rsi.parse::<f64>(),
                exit_rsi.parse::<f64>(),
                minimum_bandwidth.parse::<f64>(),
            );
            let (
                Some(conid),
                (
                    Ok(bollinger_window),
                    Ok(deviations),
                    Ok(rsi_window),
                    Ok(oversold),
                    Ok(exit),
                    Ok(minimum_bandwidth),
                ),
            ) = (conid, parsed)
            else {
                notice.set(Some(Err("请选择证券并填写有效的数值参数".into())));
                return;
            };
            if name.trim().is_empty() {
                notice.set(Some(Err("策略名称不能为空".into())));
                return;
            }
            let params = json!({
                "name": name.trim(),
                "kind": "bollinger_rsi_mean_reversion",
                "config": {
                    "conid": conid,
                    "bar_timeframe": *timeframe,
                    "bollinger_window": bollinger_window,
                    "standard_deviations": deviations,
                    "rsi_window": rsi_window,
                    "oversold_rsi": oversold,
                    "exit_rsi": exit,
                    "minimum_bandwidth_percent": minimum_bandwidth,
                }
            });
            let endpoint = endpoint.clone();
            let busy = busy.clone();
            let notice = notice.clone();
            let on_completed = on_completed.clone();
            busy.set(true);
            spawn_local(async move {
                match call_method(&endpoint, "strategy.create", params).await {
                    Ok(value) => {
                        let strategy_id = value
                            .get("strategy_id")
                            .and_then(Value::as_str)
                            .unwrap_or("未知 ID");
                        notice.set(Some(Ok(format!(
                            "策略已创建：{strategy_id}。下一步请配置执行目标和费用模型，再启动信号。"
                        ))));
                        on_completed.emit(());
                    }
                    Err(error) => notice.set(Some(Err(error))),
                }
                busy.set(false);
            });
        })
    };

    html! {
        <>
            <ErrorModal message={notice.as_ref().and_then(|result| result.as_ref().err().cloned())}
                on_close={{
                    let notice = notice.clone();
                    Callback::from(move |_| notice.set(None))
                }} />
            {notice.as_ref().and_then(|result| result.as_ref().ok()).map(|message| html! {
                <div class="alert alert-success">{message}</div>
            }).unwrap_or_default()}
            <div class="alert alert-info">
                {"这是多头均值回归策略：下轨 + RSI 超卖触发买入，中轨或 RSI 修复触发退出。创建后不会自动启动或下单。"}
            </div>
            <section class="card shadow-sm mb-4"><div class="card-body">
                <h2 class="h5">{"1. 选择证券"}</h2>
                <InstrumentSearch endpoint={props.endpoint.clone()} stock_only={true}
                    on_select={{
                        let selected = selected.clone();
                        Callback::from(move |value| selected.set(Some(value)))
                    }} />
                {selected.as_ref().map(|instrument| html! {
                    <div class="alert alert-light border mt-3 mb-0">
                        <span class="fw-semibold">{official_security_name(instrument)}</span>
                        {format!(" · {} · {} · Conid {}", text(instrument, "symbol"), security_exchange(instrument), integer(instrument, "conid"))}
                    </div>
                }).unwrap_or_default()}
            </div></section>
            <section class="card shadow-sm"><div class="card-body">
                <h2 class="h5">{"2. 策略参数"}</h2>
                <form class="row g-3" onsubmit={create}>
                    <TextField label="策略名称" value={name.clone()} kind="text" help="必须唯一" />
                    <div class="col-12 col-md-6">
                        <label class="form-label">{"Bar 周期"}</label>
                        <select class="form-select" onchange={{
                            let timeframe = timeframe.clone();
                            Callback::from(move |event: Event| {
                                let input: web_sys::HtmlSelectElement = event.target_unchecked_into();
                                timeframe.set(input.value());
                            })}}>
                            <option value="1m" selected={*timeframe == "1m"}>{"1 分钟"}</option>
                            <option value="5s" selected={*timeframe == "5s"}>{"5 秒"}</option>
                        </select>
                        <div class="form-text">{"5 秒更敏感，也更容易受噪声和交易成本影响。"}</div>
                    </div>
                    <TextField label="布林带窗口" value={bollinger_window.clone()} kind="number" help="默认 20，至少 2" />
                    <TextField label="标准差倍数" value={deviations.clone()} kind="number" help="默认 2" />
                    <TextField label="RSI 窗口" value={rsi_window.clone()} kind="number" help="默认 14，至少 2" />
                    <TextField label="超卖 RSI" value={oversold_rsi.clone()} kind="number" help="默认 30" />
                    <TextField label="退出 RSI" value={exit_rsi.clone()} kind="number" help="默认 50，必须大于超卖阈值" />
                    <TextField label="最小带宽（%）" value={minimum_bandwidth.clone()} kind="number" help="0 表示关闭窄幅过滤" />
                    <div class="col-12">
                        <button class="btn btn-primary" type="submit" disabled={*busy || selected.is_none()}>
                            {if *busy { "创建中…" } else { "创建均值回归策略" }}
                        </button>
                    </div>
                </form>
            </div></section>
        </>
    }
}

#[derive(Properties, PartialEq)]
struct TextFieldProps {
    label: &'static str,
    value: UseStateHandle<String>,
    kind: &'static str,
    help: &'static str,
}

#[function_component(TextField)]
fn text_field(props: &TextFieldProps) -> Html {
    html! {
        <div class="col-12 col-md-6">
            <label class="form-label">{props.label}</label>
            <input class="form-control" type={props.kind} step={if props.kind == "number" { "any" } else { "1" }}
                value={(*props.value).clone()} oninput={{
                    let value = props.value.clone();
                    Callback::from(move |event: InputEvent| {
                        let input: web_sys::HtmlInputElement = event.target_unchecked_into();
                        value.set(input.value());
                    })}} />
            <div class="form-text">{props.help}</div>
        </div>
    }
}
