use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

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

#[derive(Clone, Debug)]
pub struct StrategyOutput {
    pub signal: StrategySignal,
    pub indicator_a: f64,
    pub indicator_b: f64,
    pub previous_indicator_a: f64,
    pub previous_indicator_b: f64,
    pub details: Value,
}

#[derive(Clone, Debug)]
pub struct StrategyTransition {
    pub output: StrategyOutput,
    pub next_state: Value,
}

/// Deterministic strategy algorithm shared by live evaluation and backtests.
///
/// Broker access, storage, cost controls, risk and order submission deliberately
/// remain in the platform.
pub trait Strategy: Send + Sync {
    fn kind(&self) -> &'static str;
    fn conid(&self) -> i32;
    fn minimum_history(&self) -> usize;
    fn bar_timeframe(&self) -> &'static str {
        "1m"
    }
    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String>;

    /// Version of the persisted runtime-state schema owned by this strategy.
    ///
    /// Incrementing this requires an explicit state migration before the
    /// strategy can resume. The platform fails closed on a version mismatch.
    fn state_version(&self) -> u32 {
        1
    }

    fn initial_state(&self) -> Value {
        serde_json::json!({})
    }

    /// Evaluate one finalized Bar and return the state to commit atomically
    /// with the evaluation. Stateless strategies inherit this implementation.
    fn evaluate_with_state(
        &self,
        bars: &[StrategyBar],
        state: &Value,
    ) -> Result<StrategyTransition, String> {
        Ok(StrategyTransition {
            output: self.evaluate(bars)?,
            next_state: state.clone(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigFieldKind {
    Instrument,
    Integer,
    Number,
    Percentage,
    Select(&'static [&'static str]),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ConfigField {
    pub key: &'static str,
    pub label: &'static str,
    pub help: &'static str,
    pub kind: ConfigFieldKind,
    pub required: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StrategyCapabilities {
    pub live: bool,
    pub backtest: bool,
    pub supports_short_targets: bool,
    pub timeframes: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StrategyMetadata {
    pub kind: &'static str,
    pub display_name: &'static str,
    pub description: &'static str,
    pub fields: &'static [ConfigField],
    pub capabilities: StrategyCapabilities,
}

#[derive(Clone, Copy)]
pub struct BackendStrategyRegistration {
    pub metadata: &'static StrategyMetadata,
    pub factory: fn(Value) -> Result<Box<dyn Strategy>, String>,
}
