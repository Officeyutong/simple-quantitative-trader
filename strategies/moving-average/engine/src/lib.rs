use moving_average_model::{
    FIVE_SECOND_KIND, KIND, MovingAverageCrossConfig, MovingAverageCrossV2Config,
    MovingAverageType, V2_KIND,
};
use serde_json::{Value, json};
use strategy_api::{
    BackendStrategyRegistration, Strategy, StrategyBar, StrategyOutput, StrategySignal,
};

pub struct MovingAverageCross {
    config: MovingAverageCrossConfig,
}

impl MovingAverageCross {
    pub fn new(config: MovingAverageCrossConfig) -> Result<Self, String> {
        if config.conid <= 0
            || config.short_window == 0
            || config.long_window <= config.short_window
            || config.long_window > 10_000
        {
            return Err(
                "strategy requires conid > 0 and 0 < short_window < long_window <= 10000".into(),
            );
        }
        Ok(Self { config })
    }
}

impl Strategy for MovingAverageCross {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn conid(&self) -> i32 {
        self.config.conid
    }

    fn minimum_history(&self) -> usize {
        self.config.long_window + 1
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        if bars.len() < self.minimum_history() {
            return Err(format!(
                "{} requires at least {} finalized bars, found {}",
                self.kind(),
                self.minimum_history(),
                bars.len()
            ));
        }
        let average = |start: usize, end: usize| {
            bars[start..end].iter().map(|bar| bar.close).sum::<f64>() / (end - start) as f64
        };
        let end = bars.len();
        let current_short = average(end - self.config.short_window, end);
        let current_long = average(end - self.config.long_window, end);
        let previous_short = average(end - self.config.short_window - 1, end - 1);
        let previous_long = average(end - self.config.long_window - 1, end - 1);
        let signal = if previous_short <= previous_long && current_short > current_long {
            StrategySignal::Buy
        } else if previous_short >= previous_long && current_short < current_long {
            StrategySignal::Sell
        } else {
            StrategySignal::Hold
        };
        let current_bar = bars.last().expect("minimum history validated");
        Ok(StrategyOutput {
            signal,
            indicator_a: current_short,
            indicator_b: current_long,
            previous_indicator_a: previous_short,
            previous_indicator_b: previous_long,
            details: json!({
                "timeframe": "1m",
                "short_window": self.config.short_window,
                "long_window": self.config.long_window,
                "short_average": current_short,
                "long_average": current_long,
                "previous_short_average": previous_short,
                "previous_long_average": previous_long,
                "bar": {
                    "time": current_bar.time,
                    "open": current_bar.open,
                    "high": current_bar.high,
                    "low": current_bar.low,
                    "close": current_bar.close,
                    "volume": current_bar.volume
                }
            }),
        })
    }
}

pub struct FiveSecondMovingAverageCross {
    inner: MovingAverageCross,
}

impl FiveSecondMovingAverageCross {
    pub fn new(config: MovingAverageCrossConfig) -> Result<Self, String> {
        Ok(Self {
            inner: MovingAverageCross::new(config)?,
        })
    }
}

impl Strategy for FiveSecondMovingAverageCross {
    fn kind(&self) -> &'static str {
        FIVE_SECOND_KIND
    }

    fn conid(&self) -> i32 {
        self.inner.conid()
    }

    fn minimum_history(&self) -> usize {
        self.inner.minimum_history()
    }

    fn bar_timeframe(&self) -> &'static str {
        "5s"
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        let mut output = self.inner.evaluate(bars)?;
        output.details["timeframe"] = Value::String("5s".into());
        Ok(output)
    }
}

pub struct MovingAverageCrossV2 {
    config: MovingAverageCrossV2Config,
}

impl MovingAverageCrossV2 {
    pub fn new(config: MovingAverageCrossV2Config) -> Result<Self, String> {
        if config.conid <= 0
            || config.short_window == 0
            || config.long_window <= config.short_window
            || config.long_window > 10_000
            || !matches!(config.bar_timeframe.as_str(), "1m" | "5s")
            || !config.min_gap_percent.is_finite()
            || !(0.0..=100.0).contains(&config.min_gap_percent)
            || config.confirmation_bars == 0
            || config.confirmation_bars > 1_000
            || config.cooldown_bars > 10_000
            || config.atr_window == 0
            || config.atr_window > 10_000
            || !config.min_atr_percent.is_finite()
            || !(0.0..=100.0).contains(&config.min_atr_percent)
            || config.trend_window > 10_000
        {
            return Err(
                "moving_average_cross_v2 requires conid > 0, timeframe 1m or 5s, \
                 0 < short_window < long_window <= 10000, confirmation_bars 1..=1000, \
                 cooldown_bars <= 10000, atr_window 1..=10000, trend_window <= 10000, \
                 and percentage filters between 0 and 100"
                    .into(),
            );
        }
        Ok(Self { config })
    }

    fn base_history(&self) -> usize {
        self.config
            .long_window
            .max(self.config.atr_window + 1)
            .max(self.config.trend_window)
    }

    fn indicators(&self, bars: &[StrategyBar], end: usize) -> V2Indicators {
        let closes: Vec<f64> = bars[..end].iter().map(|bar| bar.close).collect();
        let short = moving_average(
            &closes[end - self.config.short_window..end],
            self.config.average_type,
        );
        let long = moving_average(
            &closes[end - self.config.long_window..end],
            self.config.average_type,
        );
        let close = closes[end - 1];
        let gap_percent = if close > 0.0 {
            (short - long).abs() / close * 100.0
        } else {
            0.0
        };
        let atr = average_true_range(&bars[..end], self.config.atr_window);
        let atr_percent = if close > 0.0 {
            atr / close * 100.0
        } else {
            0.0
        };
        let trend_average = (self.config.trend_window > 0).then(|| {
            moving_average(
                &closes[end - self.config.trend_window..end],
                self.config.average_type,
            )
        });
        let filters_pass = gap_percent >= self.config.min_gap_percent
            && atr_percent >= self.config.min_atr_percent;
        let direction = if filters_pass
            && short > long
            && trend_average.is_none_or(|trend| close >= trend)
        {
            1
        } else if filters_pass && short < long && trend_average.is_none_or(|trend| close <= trend) {
            -1
        } else {
            0
        };
        V2Indicators {
            short,
            long,
            gap_percent,
            atr,
            atr_percent,
            trend_average,
            direction,
        }
    }
}

struct V2Indicators {
    short: f64,
    long: f64,
    gap_percent: f64,
    atr: f64,
    atr_percent: f64,
    trend_average: Option<f64>,
    direction: i8,
}

impl Strategy for MovingAverageCrossV2 {
    fn kind(&self) -> &'static str {
        V2_KIND
    }

    fn conid(&self) -> i32 {
        self.config.conid
    }

    fn minimum_history(&self) -> usize {
        self.base_history() + self.config.confirmation_bars + self.config.cooldown_bars
    }

    fn bar_timeframe(&self) -> &'static str {
        match self.config.bar_timeframe.as_str() {
            "5s" => "5s",
            _ => "1m",
        }
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        if bars.len() < self.minimum_history() {
            return Err(format!(
                "{} requires at least {} finalized bars, found {}",
                self.kind(),
                self.minimum_history(),
                bars.len()
            ));
        }
        let bars = &bars[bars.len() - self.minimum_history()..];
        let mut streak_direction = 0;
        let mut streak = 0usize;
        let mut previous_emission = None;
        let mut current_emission = None;
        for end in self.base_history()..=bars.len() {
            let direction = self.indicators(bars, end).direction;
            if direction == 0 {
                streak_direction = 0;
                streak = 0;
            } else if direction == streak_direction {
                streak += 1;
            } else {
                streak_direction = direction;
                streak = 1;
            }
            if streak == self.config.confirmation_bars {
                if end == bars.len() {
                    current_emission = Some(direction);
                } else {
                    previous_emission = Some(end);
                }
            }
        }

        let current = self.indicators(bars, bars.len());
        let previous = self.indicators(bars, bars.len() - 1);
        let cooling_down = current_emission.is_some()
            && previous_emission
                .is_some_and(|end| bars.len().saturating_sub(end) <= self.config.cooldown_bars);
        let signal = match (current_emission, cooling_down) {
            (Some(1), false) => StrategySignal::Buy,
            (Some(-1), false) => StrategySignal::Sell,
            _ => StrategySignal::Hold,
        };
        let reason = if cooling_down {
            "cooldown"
        } else if current_emission.is_some() {
            "confirmed_cross"
        } else if current.gap_percent < self.config.min_gap_percent {
            "gap_below_threshold"
        } else if current.atr_percent < self.config.min_atr_percent {
            "atr_below_threshold"
        } else if current.direction == 0 && self.config.trend_window > 0 {
            "trend_filter"
        } else {
            "waiting_for_confirmation_or_new_cross"
        };
        let current_bar = bars.last().expect("minimum history validated");
        Ok(StrategyOutput {
            signal,
            indicator_a: current.short,
            indicator_b: current.long,
            previous_indicator_a: previous.short,
            previous_indicator_b: previous.long,
            details: json!({
                "version": 2,
                "timeframe": self.bar_timeframe(),
                "average_type": self.config.average_type,
                "short_window": self.config.short_window,
                "long_window": self.config.long_window,
                "min_gap_percent": self.config.min_gap_percent,
                "confirmation_bars": self.config.confirmation_bars,
                "cooldown_bars": self.config.cooldown_bars,
                "atr_window": self.config.atr_window,
                "min_atr_percent": self.config.min_atr_percent,
                "trend_window": self.config.trend_window,
                "short_average": current.short,
                "long_average": current.long,
                "gap_percent": current.gap_percent,
                "atr": current.atr,
                "atr_percent": current.atr_percent,
                "trend_average": current.trend_average,
                "qualified_direction": match current.direction { 1 => "buy", -1 => "sell", _ => "none" },
                "signal_reason": reason,
                "bar": {
                    "time": current_bar.time,
                    "open": current_bar.open,
                    "high": current_bar.high,
                    "low": current_bar.low,
                    "close": current_bar.close,
                    "volume": current_bar.volume
                }
            }),
        })
    }
}

fn moving_average(values: &[f64], average_type: MovingAverageType) -> f64 {
    match average_type {
        MovingAverageType::Sma => values.iter().sum::<f64>() / values.len() as f64,
        MovingAverageType::Ema => {
            let alpha = 2.0 / (values.len() as f64 + 1.0);
            values[1..]
                .iter()
                .fold(values[0], |ema, value| alpha * value + (1.0 - alpha) * ema)
        }
    }
}

fn average_true_range(bars: &[StrategyBar], window: usize) -> f64 {
    let start = bars.len() - window;
    bars[start..]
        .iter()
        .enumerate()
        .map(|(offset, bar)| {
            let index = start + offset;
            let previous_close = bars[index - 1].close;
            (bar.high - bar.low)
                .max((bar.high - previous_close).abs())
                .max((bar.low - previous_close).abs())
        })
        .sum::<f64>()
        / window as f64
}

fn build_basic(config: Value) -> Result<Box<dyn Strategy>, String> {
    let config = serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(MovingAverageCross::new(config)?))
}

fn build_five_second(config: Value) -> Result<Box<dyn Strategy>, String> {
    let config = serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(FiveSecondMovingAverageCross::new(config)?))
}

fn build_v2(config: Value) -> Result<Box<dyn Strategy>, String> {
    let config = serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(MovingAverageCrossV2::new(config)?))
}

pub static REGISTRATIONS: &[BackendStrategyRegistration] = &[
    BackendStrategyRegistration {
        metadata: &moving_average_model::METADATA,
        factory: build_basic,
    },
    BackendStrategyRegistration {
        metadata: &moving_average_model::FIVE_SECOND_METADATA,
        factory: build_five_second,
    },
    BackendStrategyRegistration {
        metadata: &moving_average_model::V2_METADATA,
        factory: build_v2,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bars(closes: &[f64]) -> Vec<StrategyBar> {
        closes
            .iter()
            .enumerate()
            .map(|(index, close)| StrategyBar {
                time: Utc.timestamp_opt(index as i64 * 60, 0).unwrap(),
                open: *close,
                high: *close + 0.2,
                low: *close - 0.2,
                close: *close,
                volume: 1.0,
            })
            .collect()
    }

    #[test]
    fn basic_cross_is_preserved() {
        let strategy = MovingAverageCross::new(MovingAverageCrossConfig {
            conid: 1,
            short_window: 2,
            long_window: 3,
        })
        .unwrap();
        assert_eq!(
            strategy
                .evaluate(&bars(&[3.0, 2.0, 1.0, 4.0]))
                .unwrap()
                .signal,
            StrategySignal::Buy
        );
    }

    #[test]
    fn v2_rejects_an_unsupported_timeframe() {
        let error = MovingAverageCrossV2::new(MovingAverageCrossV2Config {
            conid: 1,
            short_window: 2,
            long_window: 3,
            bar_timeframe: "15m".into(),
            average_type: MovingAverageType::Ema,
            min_gap_percent: 0.0,
            confirmation_bars: 1,
            cooldown_bars: 0,
            atr_window: 2,
            min_atr_percent: 0.0,
            trend_window: 0,
        })
        .err()
        .expect("unsupported timeframe must fail");
        assert!(error.contains("timeframe 1m or 5s"));
    }

    #[test]
    fn v2_waits_for_confirmation_and_supports_five_second_bars() {
        let strategy = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "bar_timeframe": "5s",
            "average_type": "sma",
            "confirmation_bars": 2,
            "cooldown_bars": 0,
            "atr_window": 1
        }))
        .unwrap();
        let output = strategy
            .evaluate(&bars(&[3.0, 2.0, 1.0, 4.0, 5.0]))
            .unwrap();
        assert_eq!(strategy.bar_timeframe(), "5s");
        assert_eq!(output.signal, StrategySignal::Buy);
        assert_eq!(output.details["signal_reason"], "confirmed_cross");
    }

    #[test]
    fn v2_explains_gap_filter_and_cooldown_holds() {
        let gap_filtered = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "average_type": "sma",
            "min_gap_percent": 100.0,
            "confirmation_bars": 1,
            "cooldown_bars": 0,
            "atr_window": 1
        }))
        .unwrap();
        let output = gap_filtered.evaluate(&bars(&[3.0, 2.0, 1.0, 4.0])).unwrap();
        assert_eq!(output.signal, StrategySignal::Hold);
        assert_eq!(output.details["signal_reason"], "gap_below_threshold");

        let cooling_down = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "average_type": "sma",
            "confirmation_bars": 1,
            "cooldown_bars": 3,
            "atr_window": 1
        }))
        .unwrap();
        let output = cooling_down
            .evaluate(&bars(&[3.0, 2.0, 1.0, 4.0, 1.0, 0.0, 3.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Hold);
        assert_eq!(output.details["signal_reason"], "cooldown");
    }

    #[test]
    fn five_second_variant_uses_distinct_timeframe() {
        let strategy = FiveSecondMovingAverageCross::new(MovingAverageCrossConfig {
            conid: 1,
            short_window: 2,
            long_window: 3,
        })
        .unwrap();
        assert_eq!(strategy.bar_timeframe(), "5s");
    }
}
