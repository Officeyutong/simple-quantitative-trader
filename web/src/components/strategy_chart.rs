use chrono::{DateTime, Local, NaiveDateTime};
use plotly::{
    Candlestick, Plot, Scatter,
    common::{Line, Mode, TickMode, Title},
    layout::{Axis, AxisType, HoverMode, Layout, Margin, RangeSlider},
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
            let chart_view_key = view_key.clone();
            #[cfg(not(target_arch = "wasm32"))]
            let _ = &chart_view_key;
            let mut ordered_bars = bars.clone();
            ordered_bars.sort_by_key(|bar| {
                bar.get("bar_time")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            });
            let ordered_bars = without_orphan_bar_fragments(ordered_bars);
            let times = local_times(&ordered_bars, "bar_time");
            let (tick_values, tick_text) = chart_axis_ticks(&times, 9);
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
                    .height(600)
                    .margin(Margin::new().left(70).right(190).top(80).bottom(105))
                    .hover_mode(HoverMode::XUnified)
                    .x_axis(
                        Axis::new()
                            .title(Title::with_text("时间（浏览器本地时区）"))
                            // Trading charts should not reserve horizontal
                            // space for nights, weekends, or connectivity
                            // gaps. Each persisted Bar occupies one category.
                            .type_(AxisType::Category)
                            // Category axes otherwise try to render one full
                            // timestamp per Bar. Sample explicit horizontal
                            // ticks so both the date and time remain readable.
                            .tick_mode(TickMode::Array)
                            .tick_values(tick_values)
                            .tick_text(tick_text)
                            .tick_angle(0.0)
                            .auto_margin(true)
                            .range_slider(RangeSlider::new().visible(false)),
                    )
                    .y_axis(Axis::new().title(Title::with_text("价格")))
                    .show_legend(true),
            );

            #[cfg(target_arch = "wasm32")]
            spawn_local({
                async move {
                    let plot_object = plot.to_js_object();
                    let layout = js_sys::Reflect::get(&plot_object, &JsValue::from_str("layout"))
                        .expect("Plotly layout must exist");
                    js_sys::Reflect::set(
                        &layout,
                        &JsValue::from_str("uirevision"),
                        &JsValue::from_str(&chart_view_key),
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
        <div id={CHART_ID} class="w-100" style="min-height: 600px;" />
    }
}

fn chart_axis_ticks(times: &[String], maximum_ticks: usize) -> (Vec<f64>, Vec<String>) {
    if times.is_empty() || maximum_ticks == 0 {
        return (Vec::new(), Vec::new());
    }
    let tick_count = times.len().min(maximum_ticks.max(2));
    let indexes = if tick_count == 1 {
        vec![0]
    } else {
        (0..tick_count)
            .map(|tick| tick * (times.len() - 1) / (tick_count - 1))
            .collect::<Vec<_>>()
    };
    let values = indexes.iter().map(|index| *index as f64).collect();
    let labels = indexes
        .iter()
        .map(|index| chart_tick_label(&times[*index]))
        .collect();
    (values, labels)
}

fn chart_tick_label(value: &str) -> String {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
        .map(|time| time.format("%Y-%m-%d<br>%H:%M:%S").to_string())
        .unwrap_or_else(|_| value.to_owned())
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

/// Old daemon versions could turn the one-off Last snapshot delivered during
/// a reconnect into an isolated Bar. Hide only runs shorter than three Bars
/// when a real contiguous run exists; a genuinely new/short data set remains
/// visible until it has accumulated enough Bars.
fn without_orphan_bar_fragments(rows: Vec<Value>) -> Vec<Value> {
    if rows.len() < 3 {
        return rows;
    }
    let interval_seconds = rows
        .iter()
        .find_map(|row| row.get("timeframe").and_then(Value::as_str))
        .and_then(|timeframe| match timeframe {
            "5s" => Some(5),
            "1m" => Some(60),
            _ => None,
        });
    let Some(interval_seconds) = interval_seconds else {
        return rows;
    };
    let timestamps = rows
        .iter()
        .map(|row| {
            row.get("bar_time")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|time| time.timestamp())
        })
        .collect::<Vec<_>>();
    if timestamps.iter().any(Option::is_none) {
        return rows;
    }

    let mut keep = vec![false; rows.len()];
    let mut run_start = 0;
    for index in 1..=rows.len() {
        let run_ended = index == rows.len()
            || timestamps[index].expect("validated timestamp")
                - timestamps[index - 1].expect("validated timestamp")
                > interval_seconds * 2;
        if run_ended {
            if index - run_start >= 3 {
                keep[run_start..index].fill(true);
            }
            run_start = index;
        }
    }
    if !keep.iter().any(|keep| *keep) {
        return rows;
    }
    rows.into_iter()
        .zip(keep)
        .filter_map(|(row, keep)| keep.then_some(row))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{chart_axis_ticks, without_orphan_bar_fragments};

    fn bar(time: &str) -> serde_json::Value {
        serde_json::json!({"bar_time": time, "timeframe": "5s"})
    }

    #[test]
    fn orphan_reconnect_snapshots_are_hidden_when_contiguous_bars_exist() {
        let bars = vec![
            bar("2026-08-07T15:44:40Z"),
            bar("2026-08-07T15:44:45Z"),
            bar("2026-08-07T15:44:50Z"),
            bar("2026-08-08T11:39:45Z"),
            bar("2026-08-08T12:27:10Z"),
        ];
        let filtered = without_orphan_bar_fragments(bars);
        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered[2]["bar_time"], "2026-08-07T15:44:50Z");
    }

    #[test]
    fn a_short_new_data_set_remains_visible() {
        let bars = vec![bar("2026-08-08T13:00:00Z"), bar("2026-08-08T13:00:05Z")];
        assert_eq!(without_orphan_bar_fragments(bars).len(), 2);
    }

    #[test]
    fn chart_axis_ticks_keep_dates_readable_without_labeling_every_bar() {
        let times = (0..100)
            .map(|second| format!("2026-08-07T21:30:{:02}", second % 60))
            .collect::<Vec<_>>();
        let (values, labels) = chart_axis_ticks(&times, 9);
        assert_eq!(values.len(), 9);
        assert_eq!(labels.len(), 9);
        assert_eq!(values.first(), Some(&0.0));
        assert_eq!(values.last(), Some(&99.0));
        assert!(labels[0].contains("2026-08-07<br>21:30:00"));
    }
}
