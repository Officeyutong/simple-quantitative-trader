use serde_json::Value;
use strategy_web_kit::{StrategyWebRegistration, render_config_table};

fn render_basic(config: &Value) -> yew::Html {
    render_config_table(&moving_average_model::METADATA, config)
}

fn render_five_second(config: &Value) -> yew::Html {
    render_config_table(&moving_average_model::FIVE_SECOND_METADATA, config)
}

fn render_v2(config: &Value) -> yew::Html {
    render_config_table(&moving_average_model::V2_METADATA, config)
}

pub static REGISTRATIONS: &[StrategyWebRegistration] = &[
    StrategyWebRegistration {
        metadata: &moving_average_model::METADATA,
        render_config: render_basic,
    },
    StrategyWebRegistration {
        metadata: &moving_average_model::FIVE_SECOND_METADATA,
        render_config: render_five_second,
    },
    StrategyWebRegistration {
        metadata: &moving_average_model::V2_METADATA,
        render_config: render_v2,
    },
];
