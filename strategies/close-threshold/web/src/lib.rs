use serde_json::Value;
use strategy_web_kit::{StrategyWebRegistration, render_config_table};

fn render(config: &Value) -> yew::Html {
    render_config_table(&close_threshold_model::METADATA, config)
}

pub static REGISTRATION: StrategyWebRegistration = StrategyWebRegistration {
    metadata: &close_threshold_model::METADATA,
    render_config: render,
};
