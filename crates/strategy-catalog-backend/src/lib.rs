use serde_json::Value;
use strategy_api::{BackendStrategyRegistration, ConfigFieldKind, Strategy, StrategyMetadata};

pub use bollinger_rsi_engine::BollingerRsiMeanReversion;
pub use bollinger_rsi_model::BollingerRsiConfig;
pub use close_threshold_engine::CloseThreshold;
pub use close_threshold_model::CloseThresholdConfig;
pub use moving_average_engine::{
    FiveSecondMovingAverageCross, MovingAverageCross, MovingAverageCrossV2,
};
pub use moving_average_model::{
    MovingAverageCrossConfig, MovingAverageCrossV2Config, MovingAverageType,
};
pub use paper_round_trip_engine::PaperRoundTrip;
pub use paper_round_trip_model::PaperRoundTripConfig;

pub static REGISTRATIONS: &[BackendStrategyRegistration] = &[
    moving_average_engine::REGISTRATIONS[0],
    moving_average_engine::REGISTRATIONS[1],
    moving_average_engine::REGISTRATIONS[2],
    close_threshold_engine::REGISTRATION,
    bollinger_rsi_engine::REGISTRATION,
    paper_round_trip_engine::REGISTRATION,
];

pub fn build(kind: &str, config: Value) -> Result<Box<dyn Strategy>, String> {
    let registration = REGISTRATIONS
        .iter()
        .find(|registration| registration.metadata.kind == kind)
        .ok_or_else(|| format!("unknown strategy kind: {kind}"))?;
    (registration.factory)(config)
}

pub fn registered_kinds() -> &'static [&'static str] {
    &[
        moving_average_model::KIND,
        moving_average_model::FIVE_SECOND_KIND,
        moving_average_model::V2_KIND,
        close_threshold_model::KIND,
        bollinger_rsi_model::KIND,
        paper_round_trip_model::KIND,
    ]
}

pub fn metadata(kind: &str) -> Option<&'static StrategyMetadata> {
    REGISTRATIONS
        .iter()
        .find(|registration| registration.metadata.kind == kind)
        .map(|registration| registration.metadata)
}

pub fn metadata_json() -> Vec<Value> {
    REGISTRATIONS
        .iter()
        .map(|registration| {
            let metadata = registration.metadata;
            serde_json::json!({
                "kind": metadata.kind,
                "display_name": metadata.display_name,
                "description": metadata.description,
                "capabilities": {
                    "live": metadata.capabilities.live,
                    "backtest": metadata.capabilities.backtest,
                    "supports_short_targets": metadata.capabilities.supports_short_targets,
                    "timeframes": metadata.capabilities.timeframes,
                },
                "fields": metadata.fields.iter().map(|field| {
                    let (field_type, options) = match field.kind {
                        ConfigFieldKind::Instrument => ("instrument", Vec::new()),
                        ConfigFieldKind::Integer => ("integer", Vec::new()),
                        ConfigFieldKind::Number => ("number", Vec::new()),
                        ConfigFieldKind::Percentage => ("percentage", Vec::new()),
                        ConfigFieldKind::Select(options) => ("select", options.to_vec()),
                    };
                    serde_json::json!({
                        "key": field.key,
                        "label": field.label,
                        "help": field.help,
                        "required": field.required,
                        "field_type": field_type,
                        "options": options,
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_kind_builds_from_its_model_config() {
        for (kind, config) in [
            (
                moving_average_model::KIND,
                serde_json::json!({"conid": 1, "short_window": 2, "long_window": 3}),
            ),
            (
                moving_average_model::FIVE_SECOND_KIND,
                serde_json::json!({"conid": 1, "short_window": 2, "long_window": 3}),
            ),
            (
                moving_average_model::V2_KIND,
                serde_json::json!({"conid": 1, "short_window": 2, "long_window": 3}),
            ),
            (
                close_threshold_model::KIND,
                serde_json::json!({"conid": 1, "buy_below": 10.0, "sell_above": 20.0}),
            ),
            (bollinger_rsi_model::KIND, serde_json::json!({"conid": 1})),
            (
                paper_round_trip_model::KIND,
                serde_json::json!({"conid": 1, "phase_bars": 1}),
            ),
        ] {
            assert_eq!(build(kind, config).unwrap().kind(), kind);
        }
    }
}
