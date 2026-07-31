mod action_button;
pub mod app;
mod backtest_data_panel;
mod backtest_page;
mod bool_badge;
mod calendar_page;
mod cancel_order_button;
mod dashboard_page;
mod delete_strategy_button;
mod error_modal;
mod execution_cost_page;
mod instrument_search;
mod instruments_page;
mod key_value;
mod logs_page;
mod metric_row;
mod moving_average_wizard_page;
mod nav_button;
mod operations_page;
mod orders_page;
mod pagination;
mod paper_validation_page;
mod performance_page;
mod positions_table;
mod rename_strategy_button;
mod rpc_tools_page;
mod settings_page;
mod strategies_page;
mod strategy_chart;
mod strategy_status_page;
mod value;

use serde_json::Value;
use yew::Callback;

#[derive(Clone, PartialEq)]
pub struct MutationRequest {
    pub method: String,
    pub params: Value,
    pub on_complete: Callback<()>,
}
