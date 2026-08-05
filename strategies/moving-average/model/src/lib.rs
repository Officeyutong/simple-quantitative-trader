use serde::{Deserialize, Serialize};
use strategy_api::{ConfigField, ConfigFieldKind, StrategyCapabilities, StrategyMetadata};

pub const KIND: &str = "moving_average_cross";
pub const FIVE_SECOND_KIND: &str = "moving_average_cross_5s";
pub const V2_KIND: &str = "moving_average_cross_v2";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MovingAverageCrossConfig {
    pub conid: i32,
    pub short_window: usize,
    pub long_window: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MovingAverageType {
    Sma,
    Ema,
}

fn default_average_type() -> MovingAverageType {
    MovingAverageType::Ema
}

fn default_bar_timeframe() -> String {
    "1m".into()
}

fn default_confirmation_bars() -> usize {
    2
}

fn default_confirmation_window_bars() -> usize {
    12
}

fn default_atr_window() -> usize {
    14
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MovingAverageCrossV2Config {
    pub conid: i32,
    pub short_window: usize,
    pub long_window: usize,
    #[serde(default = "default_bar_timeframe")]
    pub bar_timeframe: String,
    #[serde(default = "default_average_type")]
    pub average_type: MovingAverageType,
    #[serde(default)]
    pub min_gap_percent: f64,
    #[serde(default = "default_confirmation_bars")]
    pub confirmation_bars: usize,
    #[serde(default = "default_confirmation_window_bars")]
    pub confirmation_window_bars: usize,
    #[serde(default)]
    pub cooldown_bars: usize,
    #[serde(default = "default_atr_window")]
    pub atr_window: usize,
    #[serde(default)]
    pub min_atr_percent: f64,
    #[serde(default)]
    pub trend_window: usize,
}

const BASIC_FIELDS: &[ConfigField] = &[
    ConfigField {
        key: "conid",
        label: "证券",
        help: "IBKR 合约 Conid",
        kind: ConfigFieldKind::Instrument,
        required: true,
    },
    ConfigField {
        key: "short_window",
        label: "短期窗口",
        help: "短期移动平均使用的已完成 Bar 数量",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "long_window",
        label: "长期窗口",
        help: "长期移动平均使用的已完成 Bar 数量，必须大于短期窗口",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
];

const V2_FIELDS: &[ConfigField] = &[
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
        key: "average_type",
        label: "均线算法",
        help: "SMA 为简单移动平均，EMA 为指数移动平均",
        kind: ConfigFieldKind::Select(&["sma", "ema"]),
        required: true,
    },
    ConfigField {
        key: "short_window",
        label: "短期窗口",
        help: "短期均线窗口",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "long_window",
        label: "长期窗口",
        help: "长期均线窗口",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "min_gap_percent",
        label: "最小均线差",
        help: "过滤均线差过小的信号",
        kind: ConfigFieldKind::Percentage,
        required: true,
    },
    ConfigField {
        key: "confirmation_bars",
        label: "确认 Bar",
        help: "方向连续满足条件后才发出信号",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "confirmation_window_bars",
        label: "交叉确认窗口",
        help: "交叉后允许等待过滤条件达标的最大 Bar 数，必须不少于确认 Bar 数",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "cooldown_bars",
        label: "冷却 Bar",
        help: "两次信号之间的最短冷却期",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "atr_window",
        label: "ATR 窗口",
        help: "波动率过滤使用的 ATR 窗口",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
    ConfigField {
        key: "min_atr_percent",
        label: "最小 ATR",
        help: "ATR 占价格的最低百分比",
        kind: ConfigFieldKind::Percentage,
        required: true,
    },
    ConfigField {
        key: "trend_window",
        label: "趋势窗口",
        help: "零表示关闭长期趋势过滤",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
];

const BASIC_CAPABILITIES: StrategyCapabilities = StrategyCapabilities {
    live: true,
    backtest: true,
    supports_short_targets: true,
    timeframes: &["1m"],
};

pub static METADATA: StrategyMetadata = StrategyMetadata {
    kind: KIND,
    display_name: "移动平均交叉",
    description: "短期均线上穿或下穿长期均线时产生方向信号",
    fields: BASIC_FIELDS,
    capabilities: BASIC_CAPABILITIES,
};

pub static FIVE_SECOND_METADATA: StrategyMetadata = StrategyMetadata {
    kind: FIVE_SECOND_KIND,
    display_name: "5 秒移动平均交叉",
    description: "使用已完成 5 秒 Bar 的移动平均交叉策略",
    fields: BASIC_FIELDS,
    capabilities: StrategyCapabilities {
        timeframes: &["5s"],
        ..BASIC_CAPABILITIES
    },
};

pub static V2_METADATA: StrategyMetadata = StrategyMetadata {
    kind: V2_KIND,
    display_name: "移动平均交叉 V2",
    description: "带确认、冷却、ATR 和趋势过滤的抗噪移动平均策略",
    fields: V2_FIELDS,
    capabilities: StrategyCapabilities {
        timeframes: &["1m", "5s"],
        ..BASIC_CAPABILITIES
    },
};
