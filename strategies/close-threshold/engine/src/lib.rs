use close_threshold_model::{CloseThresholdConfig, KIND};
use serde_json::{Value, json};
use strategy_api::{
    BackendStrategyRegistration, Strategy, StrategyBar, StrategyOutput, StrategySignal,
};

pub struct CloseThreshold {
    config: CloseThresholdConfig,
}

impl CloseThreshold {
    pub fn new(config: CloseThresholdConfig) -> Result<Self, String> {
        if config.conid <= 0
            || !config.buy_below.is_finite()
            || !config.sell_above.is_finite()
            || config.buy_below <= 0.0
            || config.sell_above <= config.buy_below
        {
            return Err("close_threshold requires conid > 0 and 0 < buy_below < sell_above".into());
        }
        Ok(Self { config })
    }
}

impl Strategy for CloseThreshold {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn conid(&self) -> i32 {
        self.config.conid
    }

    fn minimum_history(&self) -> usize {
        1
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        let bar = bars
            .last()
            .ok_or_else(|| "close_threshold requires one finalized bar".to_string())?;
        let signal = if bar.close < self.config.buy_below {
            StrategySignal::Buy
        } else if bar.close > self.config.sell_above {
            StrategySignal::Sell
        } else {
            StrategySignal::Hold
        };
        Ok(StrategyOutput {
            signal,
            indicator_a: bar.close,
            indicator_b: self.config.buy_below,
            previous_indicator_a: bar.close,
            previous_indicator_b: self.config.sell_above,
            details: json!({
                "close": bar.close,
                "buy_below": self.config.buy_below,
                "sell_above": self.config.sell_above
            }),
        })
    }
}

fn build(config: Value) -> Result<Box<dyn Strategy>, String> {
    let config = serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(CloseThreshold::new(config)?))
}

pub static REGISTRATION: BackendStrategyRegistration = BackendStrategyRegistration {
    metadata: &close_threshold_model::METADATA,
    factory: build,
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bar(close: f64) -> StrategyBar {
        StrategyBar {
            time: Utc.timestamp_opt(0, 0).unwrap(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
        }
    }

    #[test]
    fn emits_buy_hold_and_sell_at_configured_thresholds() {
        let strategy = CloseThreshold::new(CloseThresholdConfig {
            conid: 1,
            buy_below: 10.0,
            sell_above: 20.0,
        })
        .unwrap();
        assert_eq!(
            strategy.evaluate(&[bar(9.0)]).unwrap().signal,
            StrategySignal::Buy
        );
        assert_eq!(
            strategy.evaluate(&[bar(15.0)]).unwrap().signal,
            StrategySignal::Hold
        );
        assert_eq!(
            strategy.evaluate(&[bar(21.0)]).unwrap().signal,
            StrategySignal::Sell
        );
    }
}
