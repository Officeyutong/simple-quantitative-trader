use paper_round_trip_model::{KIND, PaperRoundTripConfig};
use serde_json::{Value, json};
use strategy_api::{
    BackendStrategyRegistration, Strategy, StrategyBar, StrategyOutput, StrategySignal,
};

pub struct PaperRoundTrip {
    config: PaperRoundTripConfig,
}

impl PaperRoundTrip {
    pub fn new(config: PaperRoundTripConfig) -> Result<Self, String> {
        if config.conid <= 0 || config.phase_bars == 0 || config.phase_bars > 1_440 {
            return Err("paper_round_trip requires conid > 0 and 1 <= phase_bars <= 1440".into());
        }
        Ok(Self { config })
    }
}

impl Strategy for PaperRoundTrip {
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
            .ok_or_else(|| "paper_round_trip requires one finalized bar".to_string())?;
        let minute = bar.time.timestamp().div_euclid(60);
        let phase = minute.div_euclid(i64::from(self.config.phase_bars));
        let signal = if phase.rem_euclid(2) == 0 {
            StrategySignal::Buy
        } else {
            StrategySignal::Sell
        };
        Ok(StrategyOutput {
            signal,
            indicator_a: phase as f64,
            indicator_b: self.config.phase_bars.into(),
            previous_indicator_a: (phase - 1) as f64,
            previous_indicator_b: self.config.phase_bars.into(),
            details: json!({
                "purpose": "paper_web_validation",
                "phase_bars": self.config.phase_bars,
                "phase": phase,
                "bar_time": bar.time,
                "close": bar.close,
                "signal": signal.as_str()
            }),
        })
    }
}

fn build(config: Value) -> Result<Box<dyn Strategy>, String> {
    let config = serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(PaperRoundTrip::new(config)?))
}

pub static REGISTRATION: BackendStrategyRegistration = BackendStrategyRegistration {
    metadata: &paper_round_trip_model::METADATA,
    factory: build,
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bar(minute: i64) -> StrategyBar {
        StrategyBar {
            time: Utc.timestamp_opt(minute * 60, 0).unwrap(),
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 1.0,
        }
    }

    #[test]
    fn alternates_at_absolute_utc_phase_boundaries() {
        let strategy = PaperRoundTrip::new(PaperRoundTripConfig {
            conid: 1,
            phase_bars: 2,
        })
        .unwrap();
        assert_eq!(
            strategy.evaluate(&[bar(0)]).unwrap().signal,
            StrategySignal::Buy
        );
        assert_eq!(
            strategy.evaluate(&[bar(2)]).unwrap().signal,
            StrategySignal::Sell
        );
    }
}
