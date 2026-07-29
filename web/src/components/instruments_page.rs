use yew::prelude::*;

use super::instrument_search::InstrumentSearch;

#[derive(Properties, PartialEq)]
pub struct InstrumentsPageProps {
    pub endpoint: String,
}

#[function_component(InstrumentsPage)]
pub fn instruments_page(props: &InstrumentsPageProps) -> Html {
    html! {
        <section class="card shadow-sm">
            <div class="card-body">
                <p class="text-secondary">
                    {"搜索 IBKR 合约并查看官方名称、Conid、币种和交易所详情。搜索结果会保存到本地证券元数据，供持仓、策略、订单和成交页面关联显示。"}
                </p>
                <InstrumentSearch endpoint={props.endpoint.clone()} />
            </div>
        </section>
    }
}
