use serde_json::Value;
use strategy_api::StrategyMetadata;
use strategy_web_kit::StrategyWebRegistration;
use yew::Html;

pub static REGISTRATIONS: &[StrategyWebRegistration] = &[
    moving_average_web::REGISTRATIONS[0],
    moving_average_web::REGISTRATIONS[1],
    moving_average_web::REGISTRATIONS[2],
    close_threshold_web::REGISTRATION,
    paper_round_trip_web::REGISTRATION,
];

pub fn registration(kind: &str) -> Option<&'static StrategyWebRegistration> {
    REGISTRATIONS
        .iter()
        .find(|registration| registration.metadata.kind == kind)
}

pub fn metadata(kind: &str) -> Option<&'static StrategyMetadata> {
    registration(kind).map(|registration| registration.metadata)
}

pub fn render_config(kind: &str, config: &Value) -> Option<Html> {
    registration(kind).map(|registration| (registration.render_config)(config))
}
