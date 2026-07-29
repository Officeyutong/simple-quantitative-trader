use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// A finalized market bar passed to both live strategies and backtests.
#[derive(Clone, Debug)]
pub struct StrategyBar {
    pub time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategySignal {
    Buy,
    Sell,
    Hold,
}

impl StrategySignal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
            Self::Hold => "hold",
        }
    }
}

/// The auditable result of evaluating one finalized bar.
#[derive(Clone, Debug)]
pub struct StrategyOutput {
    pub signal: StrategySignal,
    /// Two optional scalar indicators retained for efficient SQL analysis.
    pub indicator_a: f64,
    pub indicator_b: f64,
    pub previous_indicator_a: f64,
    pub previous_indicator_b: f64,
    /// Strategy-specific diagnostics persisted as JSON.
    pub details: Value,
}

/// Implement this trait to add a strategy in Rust.
///
/// Implementations must be deterministic and side-effect free. They receive only
/// finalized bars in chronological order. Broker calls, storage writes and order
/// submission deliberately remain outside the strategy.
pub trait Strategy: Send + Sync {
    fn kind(&self) -> &'static str;
    fn conid(&self) -> i32;
    fn minimum_history(&self) -> usize;
    fn bar_timeframe(&self) -> &'static str {
        "1m"
    }
    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MovingAverageCrossConfig {
    pub conid: i32,
    pub short_window: usize,
    pub long_window: usize,
}

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
        "moving_average_cross"
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
        let closes: Vec<f64> = bars.iter().map(|bar| bar.close).collect();
        let average = |slice: &[f64]| slice.iter().sum::<f64>() / slice.len() as f64;
        let current_short = average(&closes[closes.len() - self.config.short_window..]);
        let current_long = average(&closes[closes.len() - self.config.long_window..]);
        let previous_short =
            average(&closes[closes.len() - self.config.short_window - 1..closes.len() - 1]);
        let previous_long =
            average(&closes[closes.len() - self.config.long_window - 1..closes.len() - 1]);
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

/// Five-second moving-average crossover strategy.
///
/// Its signal rules and configuration intentionally match the existing
/// minute strategy; only the finalized input Bar source differs.
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
        "moving_average_cross_5s"
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

/// A deliberately small second strategy that also serves as an extension example.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CloseThresholdConfig {
    pub conid: i32,
    pub buy_below: f64,
    pub sell_above: f64,
}

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
        "close_threshold"
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

/// A deterministic paper-trading strategy used to validate the complete
/// signal-to-fill and Web performance pipeline.
///
/// It alternates between buy and sell phases based on finalized bar time. The
/// execution configuration should normally map Buy to a small long target and
/// Sell to a zero target, producing round trips without opening a short.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PaperRoundTripConfig {
    pub conid: i32,
    #[serde(default = "default_phase_bars")]
    pub phase_bars: u32,
}

fn default_phase_bars() -> u32 {
    1
}

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
        "paper_round_trip"
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

/// Compile-time strategy registry.
///
/// To add a strategy, implement [`Strategy`] and add one factory arm here. The
/// daemon, JSON-RPC strategy runner and backtest engine will then share it.
pub fn build(kind: &str, config: Value) -> Result<Box<dyn Strategy>, String> {
    match kind {
        "moving_average_cross" => {
            let config: MovingAverageCrossConfig =
                serde_json::from_value(config).map_err(|error| error.to_string())?;
            Ok(Box::new(MovingAverageCross::new(config)?))
        }
        "moving_average_cross_5s" => {
            let config: MovingAverageCrossConfig =
                serde_json::from_value(config).map_err(|error| error.to_string())?;
            Ok(Box::new(FiveSecondMovingAverageCross::new(config)?))
        }
        "close_threshold" => {
            let config: CloseThresholdConfig =
                serde_json::from_value(config).map_err(|error| error.to_string())?;
            Ok(Box::new(CloseThreshold::new(config)?))
        }
        "paper_round_trip" => {
            let config: PaperRoundTripConfig =
                serde_json::from_value(config).map_err(|error| error.to_string())?;
            Ok(Box::new(PaperRoundTrip::new(config)?))
        }
        _ => Err(format!("unknown strategy kind: {kind}")),
    }
}

pub fn registered_kinds() -> &'static [&'static str] {
    &[
        "moving_average_cross",
        "moving_average_cross_5s",
        "close_threshold",
        "paper_round_trip",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_strategy_generates_a_cross_signal() {
        let strategy = build(
            "moving_average_cross",
            json!({"conid": 756733, "short_window": 2, "long_window": 3}),
        )
        .unwrap();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars: Vec<_> = [3.0, 2.0, 1.0, 4.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| StrategyBar {
                time: start + chrono::Duration::minutes(index as i64),
                open: close,
                high: close,
                low: close,
                close,
                volume: 1.0,
            })
            .collect();
        assert_eq!(
            strategy.evaluate(&bars).unwrap().signal,
            StrategySignal::Buy
        );
    }

    #[test]
    fn five_second_strategy_uses_the_same_cross_rule_with_a_distinct_bar_source() {
        let strategy = build(
            "moving_average_cross_5s",
            json!({"conid": 756733, "short_window": 2, "long_window": 3}),
        )
        .unwrap();
        assert_eq!(strategy.kind(), "moving_average_cross_5s");
        assert_eq!(strategy.bar_timeframe(), "5s");
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let bars: Vec<_> = [3.0, 2.0, 1.0, 4.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| StrategyBar {
                time: start + chrono::Duration::seconds(index as i64 * 5),
                open: close,
                high: close,
                low: close,
                close,
                volume: 1.0,
            })
            .collect();
        let output = strategy.evaluate(&bars).unwrap();
        assert_eq!(output.signal, StrategySignal::Buy);
        assert_eq!(output.details["timeframe"], "5s");
    }

    #[test]
    fn second_registered_strategy_is_built_from_json() {
        let strategy = build(
            "close_threshold",
            json!({"conid": 756733, "buy_below": 100.0, "sell_above": 200.0}),
        )
        .unwrap();
        let bar = StrategyBar {
            time: Utc::now(),
            open: 90.0,
            high: 95.0,
            low: 85.0,
            close: 90.0,
            volume: 10.0,
        };
        assert_eq!(
            strategy.evaluate(&[bar]).unwrap().signal,
            StrategySignal::Buy
        );
    }

    #[test]
    fn paper_round_trip_alternates_on_finalized_bar_phases() {
        let strategy = build(
            "paper_round_trip",
            json!({"conid": 12087792, "phase_bars": 1}),
        )
        .unwrap();
        let bar = |minute: i64| StrategyBar {
            time: DateTime::from_timestamp(minute * 60, 0).unwrap(),
            open: 1.1,
            high: 1.1,
            low: 1.1,
            close: 1.1,
            volume: 1.0,
        };
        assert_eq!(
            strategy.evaluate(&[bar(100)]).unwrap().signal,
            StrategySignal::Buy
        );
        assert_eq!(
            strategy.evaluate(&[bar(101)]).unwrap().signal,
            StrategySignal::Sell
        );
    }
}
