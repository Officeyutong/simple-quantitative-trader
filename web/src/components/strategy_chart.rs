use chrono::{DateTime, Local};
use plotly::{
    Candlestick, Plot, Scatter,
    common::{Line, Mode, Title},
    layout::{Axis, HoverMode, Layout, RangeSlider},
};
use serde_json::Value;
use yew::prelude::*;
#[cfg(target_arch = "wasm32")]
use {
    wasm_bindgen::{JsValue, prelude::wasm_bindgen},
    wasm_bindgen_futures::{js_sys::Object, spawn_local},
};

const CHART_ID: &str = "strategy-price-chart";

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(catch, js_namespace = Plotly, js_name = react)]
    async fn plotly_react(id: &str, plot: &Object) -> Result<JsValue, JsValue>;
}

#[derive(Properties, PartialEq)]
pub struct StrategyChartProps {
    pub bars: Vec<Value>,
    pub evaluations: Vec<Value>,
    pub symbol: String,
    /// Remains stable while one strategy is selected, so Plotly preserves the
    /// user's zoom and pan during data refreshes.
    pub view_key: String,
}

#[function_component(StrategyChart)]
pub fn strategy_chart(props: &StrategyChartProps) -> Html {
    let bars = props.bars.clone();
    let evaluations = props.evaluations.clone();
    let symbol = props.symbol.clone();
    let view_key = props.view_key.clone();

    use_effect_with(
        (bars, evaluations, symbol, view_key),
        move |(bars, evaluations, symbol, view_key)| {
            let mut ordered_bars = bars.clone();
            ordered_bars.sort_by_key(|bar| {
                bar.get("bar_time")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            });
            let times = local_times(&ordered_bars, "bar_time");
            let mut plot = Plot::new();
            if !times.is_empty() {
                plot.add_trace(Box::new(
                    Candlestick::new(
                        times,
                        numbers(&ordered_bars, "open"),
                        numbers(&ordered_bars, "high"),
                        numbers(&ordered_bars, "low"),
                        numbers(&ordered_bars, "close"),
                    )
                    .name(format!("{symbol} K 线"))
                    .show_legend(true),
                ));
            }

            let mut ordered_evaluations = evaluations.clone();
            ordered_evaluations.sort_by_key(|row| {
                row.get("bar_time")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            });
            let evaluation_times = local_times(&ordered_evaluations, "bar_time");
            if !evaluation_times.is_empty() {
                plot.add_trace(
                    Scatter::new(
                        evaluation_times.clone(),
                        numbers(&ordered_evaluations, "short_value"),
                    )
                    .mode(Mode::Lines)
                    .line(Line::new().color("#0d6efd").width(2.0))
                    .name("短均线（Short MA）"),
                );
                plot.add_trace(
                    Scatter::new(
                        evaluation_times,
                        numbers(&ordered_evaluations, "long_value"),
                    )
                    .mode(Mode::Lines)
                    .line(Line::new().color("#fd7e14").width(2.0))
                    .name("长均线（Long MA）"),
                );
            }

            plot.set_layout(
                Layout::new()
                    .title(Title::with_text(format!("{symbol} · K 线与移动平均线")))
                    .height(560)
                    .hover_mode(HoverMode::XUnified)
                    .x_axis(
                        Axis::new()
                            .title(Title::with_text("时间（浏览器本地时区）"))
                            .range_slider(RangeSlider::new().visible(false)),
                    )
                    .y_axis(Axis::new().title(Title::with_text("价格")))
                    .show_legend(true),
            );

            #[cfg(target_arch = "wasm32")]
            spawn_local({
                let view_key = view_key.clone();
                async move {
                    let plot_object = plot.to_js_object();
                    let layout = js_sys::Reflect::get(&plot_object, &JsValue::from_str("layout"))
                        .expect("Plotly layout must exist");
                    js_sys::Reflect::set(
                        &layout,
                        &JsValue::from_str("uirevision"),
                        &JsValue::from_str(&view_key),
                    )
                    .expect("Plotly uirevision must be writable");
                    plotly_react(CHART_ID, &plot_object)
                        .await
                        .expect("Error updating chart");
                }
            });
            || ()
        },
    );

    html! {
        <div id={CHART_ID} class="w-100" style="min-height: 560px;" />
    }
}

fn local_times(rows: &[Value], key: &str) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.get(key)
                .and_then(Value::as_str)
                .map(chart_local_time)
                .unwrap_or_default()
        })
        .collect()
}

fn chart_local_time(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&Local)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_owned())
}

fn numbers(rows: &[Value], key: &str) -> Vec<f64> {
    rows.iter()
        .map(|row| row.get(key).and_then(Value::as_f64).unwrap_or(f64::NAN))
        .collect()
}
