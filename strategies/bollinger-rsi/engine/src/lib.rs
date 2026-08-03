use bollinger_rsi_model::{BollingerRsiConfig, KIND};
use serde_json::{Value, json};
use strategy_api::{
    BackendStrategyRegistration, Strategy, StrategyBar, StrategyOutput, StrategySignal,
};

pub struct BollingerRsiMeanReversion {
    config: BollingerRsiConfig,
}

#[derive(Clone, Copy, Debug)]
struct BollingerBands {
    middle: f64,
    upper: f64,
    lower: f64,
    bandwidth_percent: f64,
}

impl BollingerRsiMeanReversion {
    pub fn new(config: BollingerRsiConfig) -> Result<Self, String> {
        if config.conid <= 0
            || !matches!(config.bar_timeframe.as_str(), "1m" | "5s")
            || !(2..=10_000).contains(&config.bollinger_window)
            || !(2..=10_000).contains(&config.rsi_window)
            || !config.standard_deviations.is_finite()
            || config.standard_deviations <= 0.0
            || config.standard_deviations > 100.0
            || !config.oversold_rsi.is_finite()
            || !config.exit_rsi.is_finite()
            || config.oversold_rsi < 0.0
            || config.oversold_rsi >= config.exit_rsi
            || config.exit_rsi > 100.0
            || !config.minimum_bandwidth_percent.is_finite()
            || !(0.0..=100.0).contains(&config.minimum_bandwidth_percent)
        {
            return Err(
                "bollinger_rsi_mean_reversion requires conid > 0, timeframe 1m or 5s, \
                 windows in 2..=10000, 0 < standard_deviations <= 100, \
                 0 <= oversold_rsi < exit_rsi <= 100, and minimum bandwidth in 0..=100"
                    .into(),
            );
        }
        Ok(Self { config })
    }

    fn required_history(&self) -> usize {
        (self.config.bollinger_window + 1).max(self.config.rsi_window + 2)
    }
}

impl Strategy for BollingerRsiMeanReversion {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn conid(&self) -> i32 {
        self.config.conid
    }

    fn minimum_history(&self) -> usize {
        self.required_history()
    }

    fn bar_timeframe(&self) -> &'static str {
        match self.config.bar_timeframe.as_str() {
            "5s" => "5s",
            _ => "1m",
        }
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        let required = self.required_history();
        if bars.len() < required {
            return Err(format!(
                "bollinger_rsi_mean_reversion requires {required} finalized Bars"
            ));
        }
        // Match the platform's live contract exactly even when a caller passes
        // an expanding history: all calculations use only the documented tail.
        let bars = &bars[bars.len() - required..];
        let current_index = bars.len() - 1;
        let previous_index = current_index - 1;
        let current_bands = bollinger_bands(
            &bars[current_index + 1 - self.config.bollinger_window..=current_index],
            self.config.standard_deviations,
        )?;
        let previous_bands = bollinger_bands(
            &bars[previous_index + 1 - self.config.bollinger_window..=previous_index],
            self.config.standard_deviations,
        )?;
        let current_rsi =
            rolling_rsi(&bars[current_index - self.config.rsi_window..=current_index])?;
        let previous_rsi =
            rolling_rsi(&bars[previous_index - self.config.rsi_window..=previous_index])?;
        let current_close = bars[current_index].close;
        let previous_close = bars[previous_index].close;
        let entry_now = current_close < current_bands.lower
            && current_rsi <= self.config.oversold_rsi
            && current_bands.bandwidth_percent >= self.config.minimum_bandwidth_percent;
        let entry_previous = previous_close < previous_bands.lower
            && previous_rsi <= self.config.oversold_rsi
            && previous_bands.bandwidth_percent >= self.config.minimum_bandwidth_percent;
        let price_exit_now = current_close >= current_bands.middle;
        let price_exit_previous = previous_close >= previous_bands.middle;
        let rsi_exit_now = current_rsi >= self.config.exit_rsi;
        let rsi_exit_previous = previous_rsi >= self.config.exit_rsi;
        let signal = if entry_now && !entry_previous {
            StrategySignal::Buy
        } else if (price_exit_now && !price_exit_previous) || (rsi_exit_now && !rsi_exit_previous) {
            StrategySignal::Sell
        } else {
            StrategySignal::Hold
        };
        let signal_reason = match signal {
            StrategySignal::Buy => "lower_band_and_rsi_entry",
            StrategySignal::Sell if price_exit_now && rsi_exit_now => "middle_band_and_rsi_exit",
            StrategySignal::Sell if price_exit_now => "middle_band_exit",
            StrategySignal::Sell => "rsi_exit",
            StrategySignal::Hold
                if current_bands.bandwidth_percent < self.config.minimum_bandwidth_percent =>
            {
                "bandwidth_below_minimum"
            }
            StrategySignal::Hold if entry_now => "entry_condition_already_active",
            StrategySignal::Hold if current_close < current_bands.middle => {
                "waiting_for_mean_reversion"
            }
            StrategySignal::Hold => "no_new_threshold_cross",
        };
        Ok(StrategyOutput {
            signal,
            // The generic cost gate interprets this distance as the plausible
            // mean-reversion edge. Full bands and RSI remain in details.
            indicator_a: current_close,
            indicator_b: current_bands.middle,
            previous_indicator_a: previous_close,
            previous_indicator_b: previous_bands.middle,
            details: json!({
                "signal_reason": signal_reason,
                "timeframe": self.bar_timeframe(),
                "close": current_close,
                "previous_close": previous_close,
                "middle_band": current_bands.middle,
                "upper_band": current_bands.upper,
                "lower_band": current_bands.lower,
                "previous_middle_band": previous_bands.middle,
                "previous_upper_band": previous_bands.upper,
                "previous_lower_band": previous_bands.lower,
                "bandwidth_percent": current_bands.bandwidth_percent,
                "previous_bandwidth_percent": previous_bands.bandwidth_percent,
                "rsi": current_rsi,
                "previous_rsi": previous_rsi,
                "entry_condition": entry_now,
                "price_exit_condition": price_exit_now,
                "rsi_exit_condition": rsi_exit_now,
                "config": self.config,
            }),
        })
    }
}

fn bollinger_bands(bars: &[StrategyBar], deviations: f64) -> Result<BollingerBands, String> {
    if bars.is_empty() || bars.iter().any(|bar| !bar.close.is_finite()) {
        return Err("Bollinger Bands require finite closing prices".into());
    }
    let middle = bars.iter().map(|bar| bar.close).sum::<f64>() / bars.len() as f64;
    let variance = bars
        .iter()
        .map(|bar| (bar.close - middle).powi(2))
        .sum::<f64>()
        / bars.len() as f64;
    let width = deviations * variance.sqrt();
    let bandwidth_percent = if middle.abs() > f64::EPSILON {
        2.0 * width / middle.abs() * 100.0
    } else {
        0.0
    };
    Ok(BollingerBands {
        middle,
        upper: middle + width,
        lower: middle - width,
        bandwidth_percent,
    })
}

/// Rolling (Cutler) RSI. Equal gains and losses with no price movement map to
/// 50 instead of producing NaN, making flat markets deterministic.
fn rolling_rsi(bars: &[StrategyBar]) -> Result<f64, String> {
    if bars.len() < 2 || bars.iter().any(|bar| !bar.close.is_finite()) {
        return Err("RSI requires at least two finite closing prices".into());
    }
    let mut gains = 0.0;
    let mut losses = 0.0;
    for pair in bars.windows(2) {
        let change = pair[1].close - pair[0].close;
        if change > 0.0 {
            gains += change;
        } else {
            losses -= change;
        }
    }
    if losses <= f64::EPSILON {
        return Ok(if gains <= f64::EPSILON { 50.0 } else { 100.0 });
    }
    if gains <= f64::EPSILON {
        return Ok(0.0);
    }
    let relative_strength = gains / losses;
    Ok(100.0 - 100.0 / (1.0 + relative_strength))
}

fn build(config: Value) -> Result<Box<dyn Strategy>, String> {
    let config = serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(BollingerRsiMeanReversion::new(config)?))
}

pub static REGISTRATION: BackendStrategyRegistration = BackendStrategyRegistration {
    metadata: &bollinger_rsi_model::METADATA,
    factory: build,
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn strategy(minimum_bandwidth_percent: f64) -> BollingerRsiMeanReversion {
        BollingerRsiMeanReversion::new(BollingerRsiConfig {
            conid: 1,
            bar_timeframe: "1m".into(),
            bollinger_window: 5,
            standard_deviations: 1.0,
            rsi_window: 3,
            oversold_rsi: 30.0,
            exit_rsi: 50.0,
            minimum_bandwidth_percent,
        })
        .unwrap()
    }

    fn bars(closes: &[f64]) -> Vec<StrategyBar> {
        closes
            .iter()
            .enumerate()
            .map(|(index, close)| StrategyBar {
                time: Utc.timestamp_opt(index as i64 * 60, 0).unwrap(),
                open: *close,
                high: *close,
                low: *close,
                close: *close,
                volume: 1.0,
            })
            .collect()
    }

    #[test]
    fn emits_entry_only_when_lower_band_and_rsi_conditions_become_active() {
        let strategy = strategy(0.0);
        let output = strategy
            .evaluate(&bars(&[10.0, 10.0, 10.0, 10.0, 10.0, 8.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Buy);
        assert_eq!(output.details["signal_reason"], "lower_band_and_rsi_entry");
        assert_eq!(output.details["rsi"], 0.0);

        let persistent = strategy
            .evaluate(&bars(&[10.0, 10.0, 10.0, 8.0, 7.0, 6.0]))
            .unwrap();
        assert_eq!(persistent.signal, StrategySignal::Hold);
        assert_eq!(
            persistent.details["signal_reason"],
            "entry_condition_already_active"
        );
    }

    #[test]
    fn exits_on_a_new_middle_band_or_rsi_recovery() {
        let output = strategy(0.0)
            .evaluate(&bars(&[10.0, 10.0, 10.0, 8.0, 9.0, 10.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Sell);
        assert_eq!(output.details["signal_reason"], "middle_band_and_rsi_exit");
        assert_eq!(output.details["rsi"], 50.0);
    }

    #[test]
    fn minimum_bandwidth_filters_entries_but_never_exit_crosses() {
        let output = strategy(100.0)
            .evaluate(&bars(&[10.0, 10.0, 10.0, 10.0, 10.0, 8.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Hold);
        assert_eq!(output.details["signal_reason"], "bandwidth_below_minimum");

        let exit = strategy(100.0)
            .evaluate(&bars(&[10.0, 10.0, 10.0, 8.0, 9.0, 10.0]))
            .unwrap();
        assert_eq!(exit.signal, StrategySignal::Sell);
    }

    #[test]
    fn rejects_invalid_parameters_and_uses_requested_timeframe() {
        let mut config = strategy(0.0).config;
        config.oversold_rsi = 60.0;
        config.exit_rsi = 50.0;
        assert!(BollingerRsiMeanReversion::new(config).is_err());

        let mut config = strategy(0.0).config;
        config.bar_timeframe = "5s".into();
        assert_eq!(
            BollingerRsiMeanReversion::new(config)
                .unwrap()
                .bar_timeframe(),
            "5s"
        );
    }
}
