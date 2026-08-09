use serde::Serialize;

use crate::{config::RiskConfig, ibkr::BrokerOrderRequest};

#[derive(Debug, Serialize)]
pub struct Decision {
    pub allowed: bool,
    pub reason_code: &'static str,
    pub detail: String,
    pub estimated_notional: f64,
}

pub fn evaluate(
    config: &RiskConfig,
    request: &BrokerOrderRequest,
    estimated_price: Option<f64>,
    fx_rate_to_base: Option<f64>,
    require_trading_enabled: bool,
) -> Decision {
    let price = request.limit_price.or(estimated_price).unwrap_or(0.0);
    let raw_notional = request.quantity * price;
    let notional = raw_notional * fx_rate_to_base.unwrap_or(0.0);
    if require_trading_enabled && !config.trading_enabled {
        return reject(
            "TRADING_DISABLED",
            "trading is disabled by configuration",
            notional,
        );
    }
    if request.quantity <= 0.0 || !request.quantity.is_finite() {
        return reject(
            "INVALID_QUANTITY",
            "quantity must be finite and positive",
            notional,
        );
    }
    if request.quantity > config.max_order_quantity {
        return reject(
            "MAX_ORDER_QUANTITY",
            format!(
                "quantity {} exceeds maximum {}",
                request.quantity, config.max_order_quantity
            ),
            notional,
        );
    }
    if price <= 0.0 || !price.is_finite() {
        return reject(
            "PRICE_REQUIRED",
            "a positive limit_price or estimated_price is required for risk evaluation",
            notional,
        );
    }
    if fx_rate_to_base.is_none_or(|rate| !rate.is_finite() || rate <= 0.0) {
        return reject(
            "FX_RATE_UNAVAILABLE",
            format!(
                "a fresh {} to {} FX conversion rate is required",
                request.contract.currency, config.base_currency
            ),
            raw_notional,
        );
    }
    if notional > config.max_order_notional {
        return reject(
            "MAX_ORDER_NOTIONAL",
            format!(
                "estimated notional {} exceeds maximum {}",
                notional, config.max_order_notional
            ),
            notional,
        );
    }
    if request.contract.security_type != "STK" {
        return reject(
            "SECURITY_TYPE_NOT_ALLOWED",
            "the MVP only permits STK contracts",
            notional,
        );
    }
    Decision {
        allowed: true,
        reason_code: "ALLOWED",
        detail: "all configured pre-trade checks passed".into(),
        estimated_notional: notional,
    }
}

/// Strictly position-reducing orders have already been proven by the
/// authoritative position snapshot.  They must remain executable when an
/// opening-risk input (price, FX, account PnL, or a configured exposure cap)
/// is unavailable; otherwise a safety control can trap an existing position.
pub fn allow_position_reduction(
    config: &RiskConfig,
    request: &BrokerOrderRequest,
    estimated_price: Option<f64>,
    fx_rate_to_base: Option<f64>,
    require_trading_enabled: bool,
) -> Decision {
    let price = request.limit_price.or(estimated_price).unwrap_or(0.0);
    let rate = fx_rate_to_base.unwrap_or(0.0);
    if require_trading_enabled && !config.trading_enabled {
        return reject(
            "TRADING_DISABLED",
            "trading is disabled by configuration",
            request.quantity * price * rate,
        );
    }
    Decision {
        allowed: true,
        reason_code: "CLOSE_ONLY_ALLOWED",
        detail: "strictly position-reducing order bypassed opening-risk inputs".into(),
        estimated_notional: request.quantity * price * rate,
    }
}

fn reject(reason_code: &'static str, detail: impl Into<String>, notional: f64) -> Decision {
    Decision {
        allowed: false,
        reason_code,
        detail: detail.into(),
        estimated_notional: notional,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ibkr::ContractCandidate;

    #[test]
    fn rejects_order_above_notional_limit() {
        let request = BrokerOrderRequest {
            contract: ContractCandidate {
                conid: 1,
                symbol: "TEST".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: String::new(),
                local_symbol: "TEST".into(),
                description: String::new(),
                derivative_security_types: vec![],
            },
            side: "buy".into(),
            quantity: 100.0,
            order_type: "limit".into(),
            limit_price: Some(101.0),
            outside_rth: false,
        };
        let decision = evaluate(&RiskConfig::default(), &request, None, Some(1.0), false);
        assert!(!decision.allowed);
        assert_eq!(decision.reason_code, "MAX_ORDER_NOTIONAL");
    }

    #[test]
    fn position_reduction_remains_allowed_without_price_or_fx() {
        let request = BrokerOrderRequest {
            contract: ContractCandidate {
                conid: 1,
                symbol: "TEST".into(),
                security_type: "STK".into(),
                currency: "USD".into(),
                exchange: "SMART".into(),
                primary_exchange: String::new(),
                local_symbol: "TEST".into(),
                description: String::new(),
                derivative_security_types: vec![],
            },
            side: "sell".into(),
            quantity: 100.0,
            order_type: "market".into(),
            limit_price: None,
            outside_rth: false,
        };

        let decision =
            allow_position_reduction(&RiskConfig::default(), &request, None, None, false);
        assert!(decision.allowed);
        assert_eq!(decision.reason_code, "CLOSE_ONLY_ALLOWED");

        let mut disabled = RiskConfig::default();
        disabled.trading_enabled = false;
        let decision = allow_position_reduction(&disabled, &request, None, None, true);
        assert!(!decision.allowed);
        assert_eq!(decision.reason_code, "TRADING_DISABLED");
    }
}
