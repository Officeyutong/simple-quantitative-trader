use yew::prelude::*;
use yew_bootstrap::component::card::Card;

use crate::api::DashboardData;

use super::{
    positions_table::PositionsTable,
    value::{array, nested_text, text},
};

#[derive(Properties, PartialEq)]
pub struct DashboardPageProps {
    pub data: DashboardData,
}

#[function_component(DashboardPage)]
pub fn dashboard_page(props: &DashboardPageProps) -> Html {
    let data = &props.data;
    let cards = [
        ("Daemon", text(&data.system, "state")),
        ("IBKR", nested_text(&data.system, "/ibkr/state")),
        ("对账", nested_text(&data.system, "/reconciliation/state")),
        (
            "持仓数",
            array(&data.positions, "positions").len().to_string(),
        ),
    ];
    html! {
        <>
            <div class="row g-3 mb-4">
                {cards.into_iter().map(|(title, value)| html! {
                    <div class="col-12 col-sm-6 col-xl-3">
                        <Card body={true} class="h-100 shadow-sm">
                            <div class="text-secondary small">{title}</div>
                            <div class="fs-4 fw-semibold mt-1">{value}</div>
                        </Card>
                    </div>
                }).collect::<Html>()}
            </div>
            <PositionsTable positions={data.positions.clone()} />
        </>
    }
}
