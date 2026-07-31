use serde::{Deserialize, Serialize};
use strategy_api::{ConfigField, ConfigFieldKind, StrategyCapabilities, StrategyMetadata};

pub const KIND: &str = "close_threshold";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CloseThresholdConfig {
    pub conid: i32,
    pub buy_below: f64,
    pub sell_above: f64,
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
        key: "buy_below",
        label: "买入阈值",
        help: "收盘价低于该值时产生买入信号",
        kind: ConfigFieldKind::Number,
        required: true,
    },
    ConfigField {
        key: "sell_above",
        label: "卖出阈值",
        help: "收盘价高于该值时产生卖出信号",
        kind: ConfigFieldKind::Number,
        required: true,
    },
];

pub static METADATA: StrategyMetadata = StrategyMetadata {
    kind: KIND,
    display_name: "收盘价阈值",
    description: "根据收盘价与买卖阈值的关系产生方向信号",
    fields: FIELDS,
    capabilities: StrategyCapabilities {
        live: true,
        backtest: true,
        supports_short_targets: true,
        timeframes: &["1m"],
    },
};
