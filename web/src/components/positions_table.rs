use serde_json::Value;
use yew::prelude::*;

use super::value::{
    array, integer, local_time, number, official_security_name, security_exchange, text,
};

#[derive(Properties, PartialEq)]
pub struct PositionsTableProps {
    pub positions: Value,
}

#[function_component(PositionsTable)]
pub fn positions_table(props: &PositionsTableProps) -> Html {
    let rows = array(&props.positions, "positions");
    html! {
        <section class="mb-4">
            <h2 class="h5">{"账户持仓"}</h2>
            <div class="card shadow-sm table-responsive">
                <table class="table table-hover align-middle mb-0">
                    <thead><tr>
                        <th>{"账户"}</th><th>{"证券（官方名称）"}</th><th>{"Conid"}</th><th>{"类型"}</th>
                        <th class="text-end">{"数量"}</th><th class="text-end">{"平均成本"}</th>
                        <th>{"币种"}</th><th>{"交易所"}</th><th>{"更新时间（本地）"}</th>
                    </tr></thead>
                    <tbody>
                        {
                            if rows.is_empty() {
                                html! { <tr><td colspan="9" class="text-center text-secondary py-4">{"暂无持仓"}</td></tr> }
                            } else {
                                rows.iter().map(|row| html! {
                                    <tr>
                                        <td>{text(row, "account")}</td>
                                        <td>
                                            <div class="fw-semibold">{official_security_name(row)}</div>
                                            <div class="small text-secondary">{text(row, "symbol")}</div>
                                        </td>
                                        <td>{integer(row, "conid")}</td>
                                        <td>{text(row, "security_type")}</td>
                                        <td class="text-end">{number(row, "quantity")}</td>
                                        <td class="text-end">{number(row, "average_cost")}</td>
                                        <td>{text(row, "currency")}</td>
                                        <td>{security_exchange(row)}</td>
                                        <td class="text-nowrap">{local_time(row, "observed_at")}</td>
                                    </tr>
                                }).collect::<Html>()
                            }
                        }
                    </tbody>
                </table>
            </div>
        </section>
    }
}
