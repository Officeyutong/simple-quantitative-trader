use serde::{Deserialize, Serialize};
use strategy_api::{ConfigField, ConfigFieldKind, StrategyCapabilities, StrategyMetadata};

pub const KIND: &str = "paper_round_trip";

fn default_phase_bars() -> u32 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PaperRoundTripConfig {
    pub conid: i32,
    #[serde(default = "default_phase_bars")]
    pub phase_bars: u32,
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
        key: "phase_bars",
        label: "阶段 Bar 数",
        help: "每隔指定 Bar 数在买入和卖出阶段之间切换",
        kind: ConfigFieldKind::Integer,
        required: true,
    },
];

pub static METADATA: StrategyMetadata = StrategyMetadata {
    kind: KIND,
    display_name: "Paper 往返验证",
    description: "用于验证信号、订单、成交和绩效链路的确定性 Paper 策略",
    fields: FIELDS,
    capabilities: StrategyCapabilities {
        live: true,
        backtest: true,
        supports_short_targets: false,
        timeframes: &["1m"],
    },
};
