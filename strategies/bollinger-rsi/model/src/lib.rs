use serde::{Deserialize, Serialize};
use strategy_api::{ConfigField, ConfigFieldKind, StrategyCapabilities, StrategyMetadata};

pub const KIND: &str = "bollinger_rsi_mean_reversion";

fn default_bar_timeframe() -> String {
    "1m".into()
}

fn default_bollinger_window() -> usize {
    20
}

fn default_standard_deviations() -> f64 {
    2.0
}

fn default_rsi_window() -> usize {
    14
}

fn default_oversold_rsi() -> f64 {
    30.0
}

fn default_exit_rsi() -> f64 {
    50.0
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BollingerRsiConfig {
    pub conid: i32,
    #[serde(default = "default_bar_timeframe")]
    pub bar_timeframe: String,
    #[serde(default = "default_bollinger_window")]
    pub bollinger_window: usize,
    #[serde(default = "default_standard_deviations")]
    pub standard_deviations: f64,
    #[serde(default = "default_rsi_window")]
    pub rsi_window: usize,
    #[serde(default = "default_oversold_rsi")]
    pub oversold_rsi: f64,
    #[serde(default = "default_exit_rsi")]
    pub exit_rsi: f64,
    #[serde(default)]
    pub minimum_bandwidth_percent: f64,
}

const FIELDS: &[ConfigField] = &[
    ConfigField {
        key: "conid",
        label: "证券",
        help: "IBKR 合约 Conid",
        kind: ConfigFieldKind::Instrument,
        required: true,
    },
    ConfigField {
        key: "bar_timeframe",
        label: "Bar 周期",
        help: "实时和回测共同使用的 Bar 周期",
        kind: ConfigFieldKind::Select(&["1m", "5s"]),
        required: true,
    },
    ConfigField {
        key: "bollinger_window",
        label: "布林带窗口",
        help: "计算中轨和总体标准差使用的已完成 Bar 数量",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "standard_deviations",
        label: "标准差倍数",
        help: "上下轨距离中轨的总体标准差倍数，常用值为 2",
        kind: ConfigFieldKind::Number,
        required: true,
    },
    ConfigField {
        key: "rsi_window",
        label: "RSI 窗口",
        help: "滚动 RSI 统计涨跌幅使用的相邻 Bar 数量",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "oversold_rsi",
        label: "超卖 RSI",
        help: "价格跌破下轨且 RSI 不高于该值时产生买入候选",
        kind: ConfigFieldKind::Number,
        required: true,
    },
    ConfigField {
        key: "exit_rsi",
        label: "退出 RSI",
        help: "价格回到中轨或 RSI 上穿该值时产生卖出信号",
        kind: ConfigFieldKind::Number,
        required: true,
    },
    ConfigField {
        key: "minimum_bandwidth_percent",
        label: "最小带宽",
        help: "布林带宽度占中轨的最低百分比，用于过滤窄幅震荡；0 表示关闭",
        kind: ConfigFieldKind::Percentage,
        required: true,
    },
];

pub static METADATA: StrategyMetadata = StrategyMetadata {
    kind: KIND,
    display_name: "布林带 + RSI 均值回归",
    description: "价格跌破布林带下轨且 RSI 超卖时买入，回归中轨或 RSI 修复时退出",
    fields: FIELDS,
    capabilities: StrategyCapabilities {
        live: true,
        backtest: true,
        supports_short_targets: false,
        timeframes: &["1m", "5s"],
    },
};
