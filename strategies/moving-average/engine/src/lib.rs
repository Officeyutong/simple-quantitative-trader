#![recursion_limit = "256"]

use chrono::{DateTime, Utc};
use moving_average_model::{
    FIVE_SECOND_KIND, KIND, MovingAverageCrossConfig, MovingAverageCrossV2Config,
    MovingAverageType, V2_KIND,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use strategy_api::{
    BackendStrategyRegistration, Strategy, StrategyBar, StrategyOutput, StrategySignal,
    StrategyTransition,
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

const V2_STATE_VERSION: u32 = 3;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MovingAverageCrossV2State {
    last_bar_time: Option<DateTime<Utc>>,
    ema_history_started_at: Option<DateTime<Utc>>,
    short_average: Option<f64>,
    long_average: Option<f64>,
    trend_average: Option<f64>,
    previous_raw_direction: i8,
    pending: Option<PendingCross>,
    cooldown_remaining: usize,
    last_published_direction: i8,
    last_published_bar_time: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct PendingCross {
    direction: i8,
    age_bars: usize,
    qualified_bars: usize,
}

struct V2Step {
    previous: V2Indicators,
    current: V2Indicators,
    emission: Option<V2Emission>,
    reason: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V2TargetIntent {
    Directional,
    FlattenOnly,
}

impl V2TargetIntent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Directional => "directional",
            Self::FlattenOnly => "flatten_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct V2Emission {
    direction: i8,
    target_intent: V2TargetIntent,
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
            || config.confirmation_window_bars < config.confirmation_bars
            || config.confirmation_window_bars > 10_000
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
                 confirmation_window_bars between confirmation_bars and 10000, \
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

    fn indicators_from_averages(
        &self,
        bars: &[StrategyBar],
        end: usize,
        short: f64,
        long: f64,
        trend_average: Option<f64>,
    ) -> V2Indicators {
        let close = bars[end - 1].close;
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
        let filters_pass = gap_percent >= self.config.min_gap_percent
            && atr_percent >= self.config.min_atr_percent;
        let raw_direction = if short > long {
            1
        } else if short < long {
            -1
        } else {
            0
        };
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
            raw_direction,
            direction,
        }
    }

    fn bootstrap(
        &self,
        bars: &[StrategyBar],
    ) -> Result<(MovingAverageCrossV2State, V2Indicators, usize), String> {
        let end = self.base_history();
        let closes = bars[..end].iter().map(|bar| bar.close).collect::<Vec<_>>();
        let short = moving_average_from_history(
            &closes,
            self.config.short_window,
            self.config.average_type,
        );
        let long =
            moving_average_from_history(&closes, self.config.long_window, self.config.average_type);
        let trend_average = (self.config.trend_window > 0).then(|| {
            moving_average_from_history(&closes, self.config.trend_window, self.config.average_type)
        });
        let indicators = self.indicators_from_averages(bars, end, short, long, trend_average);
        let state = MovingAverageCrossV2State {
            last_bar_time: Some(bars[end - 1].time),
            ema_history_started_at: Some(bars[0].time),
            short_average: Some(short),
            long_average: Some(long),
            trend_average,
            previous_raw_direction: indicators.raw_direction,
            ..MovingAverageCrossV2State::default()
        };
        Ok((state, indicators, end))
    }

    fn advance_indicators(
        &self,
        bars: &[StrategyBar],
        end: usize,
        state: &MovingAverageCrossV2State,
    ) -> Result<V2Indicators, String> {
        let close = bars[end - 1].close;
        let previous_short = state
            .short_average
            .ok_or_else(|| "moving_average_cross_v2 state is missing short_average".to_owned())?;
        let previous_long = state
            .long_average
            .ok_or_else(|| "moving_average_cross_v2 state is missing long_average".to_owned())?;
        let short = match self.config.average_type {
            MovingAverageType::Sma => simple_moving_average(
                &bars[end - self.config.short_window..end]
                    .iter()
                    .map(|bar| bar.close)
                    .collect::<Vec<_>>(),
            ),
            MovingAverageType::Ema => update_ema(previous_short, close, self.config.short_window),
        };
        let long = match self.config.average_type {
            MovingAverageType::Sma => simple_moving_average(
                &bars[end - self.config.long_window..end]
                    .iter()
                    .map(|bar| bar.close)
                    .collect::<Vec<_>>(),
            ),
            MovingAverageType::Ema => update_ema(previous_long, close, self.config.long_window),
        };
        let trend_average = if self.config.trend_window == 0 {
            None
        } else {
            Some(match self.config.average_type {
                MovingAverageType::Sma => simple_moving_average(
                    &bars[end - self.config.trend_window..end]
                        .iter()
                        .map(|bar| bar.close)
                        .collect::<Vec<_>>(),
                ),
                MovingAverageType::Ema => update_ema(
                    state.trend_average.ok_or_else(|| {
                        "moving_average_cross_v2 state is missing trend_average".to_owned()
                    })?,
                    close,
                    self.config.trend_window,
                ),
            })
        };
        Ok(self.indicators_from_averages(bars, end, short, long, trend_average))
    }

    fn decode_state(&self, persisted_state: &Value) -> Result<MovingAverageCrossV2State, String> {
        if persisted_state
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
        {
            return Ok(MovingAverageCrossV2State::default());
        }
        serde_json::from_value(persisted_state.clone())
            .map_err(|error| format!("invalid moving_average_cross_v2 state: {error}"))
    }

    fn validate_state(
        &self,
        state: &MovingAverageCrossV2State,
        latest_bar_time: DateTime<Utc>,
    ) -> Result<(), String> {
        if !matches!(state.previous_raw_direction, -1..=1)
            || !matches!(state.last_published_direction, -1..=1)
        {
            return Err("moving_average_cross_v2 state contains an invalid direction".into());
        }
        if let Some(pending) = state.pending
            && (!matches!(pending.direction, -1 | 1)
                || pending.age_bars >= self.config.confirmation_window_bars
                || pending.qualified_bars >= self.config.confirmation_bars
                || pending.qualified_bars > pending.age_bars
                || pending.direction != state.previous_raw_direction)
        {
            return Err("moving_average_cross_v2 state contains an invalid pending cross".into());
        }
        if state.cooldown_remaining > self.config.cooldown_bars {
            return Err("moving_average_cross_v2 state contains an invalid cooldown".into());
        }
        if let Some(last_bar_time) = state.last_bar_time {
            if last_bar_time > latest_bar_time {
                return Err(
                    "moving_average_cross_v2 state is ahead of the latest supplied Bar".into(),
                );
            }
            let ema_history_started_at = state.ema_history_started_at.ok_or_else(|| {
                "moving_average_cross_v2 initialized state is missing ema_history_started_at"
                    .to_owned()
            })?;
            if ema_history_started_at > last_bar_time {
                return Err(
                    "moving_average_cross_v2 state has EMA history after its latest Bar".into(),
                );
            }
            let averages_are_valid = state.short_average.is_some_and(f64::is_finite)
                && state.long_average.is_some_and(f64::is_finite)
                && if self.config.trend_window == 0 {
                    state.trend_average.is_none()
                } else {
                    state.trend_average.is_some_and(f64::is_finite)
                };
            if !averages_are_valid {
                return Err("moving_average_cross_v2 state contains invalid averages".into());
            }
            if state.last_published_direction != 0 && state.last_published_bar_time.is_none() {
                return Err(
                    "moving_average_cross_v2 published direction is missing its Bar time".into(),
                );
            }
            if state
                .last_published_bar_time
                .is_some_and(|published_at| published_at > last_bar_time)
            {
                return Err(
                    "moving_average_cross_v2 published target is ahead of its latest Bar".into(),
                );
            }
        } else {
            let contains_runtime_data = state.ema_history_started_at.is_some()
                || state.short_average.is_some()
                || state.long_average.is_some()
                || state.trend_average.is_some()
                || state.previous_raw_direction != 0
                || state.pending.is_some()
                || state.cooldown_remaining != 0
                || state.last_published_direction != 0
                || state.last_published_bar_time.is_some();
            if contains_runtime_data {
                return Err(
                    "moving_average_cross_v2 uninitialized state contains runtime data".into(),
                );
            }
        }
        Ok(())
    }

    fn advance_signal_state(
        &self,
        state: &mut MovingAverageCrossV2State,
        current: &V2Indicators,
    ) -> (Option<V2Emission>, &'static str) {
        let cooling_down = state.cooldown_remaining > 0;
        state.cooldown_remaining = state.cooldown_remaining.saturating_sub(1);

        // Keep the most recent *non-zero* raw direction.  An exact equality is
        // a neutral point on the crossing path, not a new regime: +1 -> 0 ->
        // -1 must still be recognised as a bearish cross (and vice versa).
        let crossed = current.raw_direction != 0
            && state.previous_raw_direction != 0
            && state.previous_raw_direction == -current.raw_direction;
        if crossed {
            state.pending = Some(PendingCross {
                direction: current.raw_direction,
                age_bars: 0,
                qualified_bars: 0,
            });
        }
        if current.raw_direction != 0 {
            state.previous_raw_direction = current.raw_direction;
        }

        // Entry filters reduce churn, but must never turn into an exit lock.
        // An opposite raw regime immediately publishes only a flat target. It
        // deliberately does not publish the opposite directional target: that
        // new entry remains pending and must qualify normally on later Bars.
        if state.last_published_direction != 0
            && current.raw_direction == -state.last_published_direction
        {
            if state
                .pending
                .is_none_or(|candidate| candidate.direction != current.raw_direction)
            {
                state.pending = Some(PendingCross {
                    direction: current.raw_direction,
                    age_bars: 0,
                    qualified_bars: 0,
                });
            }
            return (
                Some(V2Emission {
                    direction: current.raw_direction,
                    target_intent: V2TargetIntent::FlattenOnly,
                }),
                "protective_direction_change",
            );
        }

        let mut expired = false;
        let mut confirmed = None;
        let mut cooldown_blocked = false;
        if let Some(mut candidate) = state.pending {
            if current.raw_direction != candidate.direction {
                state.pending = None;
            } else {
                candidate.age_bars = candidate.age_bars.saturating_add(1);
                candidate.qualified_bars = if current.direction == candidate.direction {
                    candidate.qualified_bars.saturating_add(1)
                } else {
                    0
                };
                if candidate.qualified_bars >= self.config.confirmation_bars {
                    if cooling_down {
                        // A candidate that qualified during cooldown is kept
                        // alive, but must qualify again on a later Bar. This
                        // prevents cooldown from permanently consuming the
                        // only crossing that can open the new direction.
                        candidate.age_bars = self.config.confirmation_bars - 1;
                        candidate.qualified_bars = self.config.confirmation_bars - 1;
                        state.pending = Some(candidate);
                        cooldown_blocked = true;
                    } else {
                        confirmed = Some(candidate.direction);
                        state.pending = None;
                    }
                } else if candidate.age_bars >= self.config.confirmation_window_bars {
                    expired = true;
                    state.pending = None;
                } else {
                    state.pending = Some(candidate);
                }
            }
        }
        if let Some(direction) = confirmed {
            if direction == state.last_published_direction {
                return (None, "already_published_direction");
            }
            return (
                Some(V2Emission {
                    direction,
                    target_intent: V2TargetIntent::Directional,
                }),
                "confirmed_cross",
            );
        }
        let reason = if cooldown_blocked {
            "cooldown"
        } else if state.pending.is_some() && current.gap_percent < self.config.min_gap_percent {
            "gap_below_threshold"
        } else if state.pending.is_some() && current.atr_percent < self.config.min_atr_percent {
            "atr_below_threshold"
        } else if state.pending.is_some() && current.direction == 0 && self.config.trend_window > 0
        {
            "trend_filter"
        } else if state.pending.is_some() {
            "waiting_for_confirmation"
        } else if expired {
            "confirmation_window_expired"
        } else {
            "waiting_for_new_cross"
        };
        (None, reason)
    }

    fn record_published_emission(
        &self,
        state: &mut MovingAverageCrossV2State,
        emission: V2Emission,
        bar_time: DateTime<Utc>,
    ) {
        state.last_published_direction = match emission.target_intent {
            V2TargetIntent::Directional => emission.direction,
            V2TargetIntent::FlattenOnly => 0,
        };
        state.last_published_bar_time = Some(bar_time);
        state.cooldown_remaining = self.config.cooldown_bars;
    }

    fn transition(
        &self,
        bars: &[StrategyBar],
        persisted_state: &Value,
    ) -> Result<StrategyTransition, String> {
        if bars.len() < self.minimum_history() {
            return Err(format!(
                "{} requires at least {} finalized bars, found {}",
                self.kind(),
                self.minimum_history(),
                bars.len()
            ));
        }
        let bars = &bars[bars.len() - self.minimum_history()..];
        if bars.windows(2).any(|pair| pair[0].time >= pair[1].time) {
            return Err("moving_average_cross_v2 requires Bars in strictly ascending order".into());
        }
        if bars.iter().any(|bar| {
            ![bar.open, bar.high, bar.low, bar.close, bar.volume]
                .into_iter()
                .all(f64::is_finite)
        }) {
            return Err("moving_average_cross_v2 requires finite Bar values".into());
        }

        let latest_bar_time = bars.last().expect("minimum history validated").time;
        let decoded = self.decode_state(persisted_state)?;
        self.validate_state(&decoded, latest_bar_time)?;
        let previous_state_bar_time = decoded.last_bar_time;
        let previous_published_direction = decoded.last_published_direction;
        let previous_published_bar_time = decoded.last_published_bar_time;

        let resume_index = decoded
            .last_bar_time
            .and_then(|last_bar_time| bars.iter().position(|bar| bar.time == last_bar_time));
        if resume_index == Some(bars.len() - 1) {
            return Err(
                "moving_average_cross_v2 state already includes the latest supplied Bar".into(),
            );
        }

        // The persisted recursive averages belong to `index`. Resumption is
        // safe as soon as the first unseen Bar has enough retained history for
        // SMA/ATR/trend calculations. With more than one unseen Bar the state
        // Bar itself can therefore sit one position before base_history.
        let can_resume = resume_index.is_some_and(|index| index + 2 >= self.base_history());
        let state_reinitialized = decoded.last_bar_time.is_some() && !can_resume;
        let (mut state, mut previous, start_index) = if can_resume {
            let index = resume_index.expect("can_resume implies an index");
            let previous = (index + 1 >= self.base_history()).then(|| {
                self.indicators_from_averages(
                    bars,
                    index + 1,
                    decoded.short_average.expect("validated state"),
                    decoded.long_average.expect("validated state"),
                    decoded.trend_average,
                )
            });
            (decoded, previous, index + 1)
        } else {
            let (mut state, previous, start_index) = self.bootstrap(bars)?;
            // Recursive indicators can be re-seeded when the persisted Bar is
            // no longer retained, but the externally published target cannot
            // be inferred from historical indicator replay. Preserve it so a
            // still-opposite latest regime can publish a protective flatten.
            state.last_published_direction = previous_published_direction;
            state.last_published_bar_time = previous_published_bar_time;
            (state, Some(previous), start_index)
        };

        let processed_bar_count = bars.len() - start_index;
        let mut suppressed_catch_up_signals = 0usize;
        let mut suppressed_protective_flattens = 0usize;
        let mut last_step = None;
        for index in start_index..bars.len() {
            let end = index + 1;
            let current = self.advance_indicators(bars, end, &state)?;
            let (emission, reason) = self.advance_signal_state(&mut state, &current);
            state.short_average = Some(current.short);
            state.long_average = Some(current.long);
            state.trend_average = current.trend_average;
            state.last_bar_time = Some(bars[index].time);
            if index + 1 < bars.len() && emission.is_some() {
                suppressed_catch_up_signals += 1;
                if emission
                    .is_some_and(|emission| emission.target_intent == V2TargetIntent::FlattenOnly)
                {
                    suppressed_protective_flattens += 1;
                }
            }
            last_step = Some(V2Step {
                // `None` is possible only for the first of at least two
                // catch-up Bars. It can never become the externally visible
                // final step, and using the current value keeps the temporary
                // diagnostic snapshot finite.
                previous: previous.unwrap_or(current),
                current,
                emission: (index + 1 == bars.len()).then_some(emission).flatten(),
                reason,
            });
            previous = Some(current);
        }
        let mut last_step = last_step.ok_or_else(|| {
            "moving_average_cross_v2 did not receive a new finalized Bar".to_owned()
        })?;
        if last_step
            .emission
            .is_some_and(|emission| emission.target_intent == V2TargetIntent::FlattenOnly)
            && (state_reinitialized || suppressed_protective_flattens > 0)
        {
            last_step.reason = "protective_catch_up";
        }
        // Re-seeding recursive averages after the persisted Bar has fallen
        // outside the retained window is sufficient to detect that an
        // already-published position now needs protection, but it is not
        // reliable evidence for increasing risk.  A directional cross seen
        // only in the reconstructed window must therefore wait for a later,
        // continuous cross instead of opening a position immediately.
        if state_reinitialized
            && last_step
                .emission
                .is_some_and(|emission| emission.target_intent == V2TargetIntent::Directional)
        {
            state.pending = None;
            last_step.emission = None;
            last_step.reason = "state_reinitialized_entry_suppressed";
        }
        if let Some(emission) = last_step.emission {
            self.record_published_emission(&mut state, emission, latest_bar_time);
        }
        let signal = match last_step.emission.map(|emission| emission.direction) {
            Some(1) => StrategySignal::Buy,
            Some(-1) => StrategySignal::Sell,
            _ => StrategySignal::Hold,
        };
        let target_intent = last_step
            .emission
            .map_or("none", |emission| emission.target_intent.as_str());
        let pending_direction = state.pending.map(|candidate| match candidate.direction {
            1 => "buy",
            -1 => "sell",
            _ => "none",
        });
        let confirmation_progress = state.pending.map(|candidate| candidate.qualified_bars);
        let confirmation_window_remaining = state.pending.map(|candidate| {
            self.config
                .confirmation_window_bars
                .saturating_sub(candidate.age_bars)
        });
        let last_published_direction = match state.last_published_direction {
            1 => "buy",
            -1 => "sell",
            _ => "none",
        };
        let current_bar = bars.last().expect("minimum history validated");
        let next_state = serde_json::to_value(&state).map_err(|error| {
            format!("failed to serialize moving_average_cross_v2 state: {error}")
        })?;
        Ok(StrategyTransition {
            output: StrategyOutput {
                signal,
                indicator_a: last_step.current.short,
                indicator_b: last_step.current.long,
                previous_indicator_a: last_step.previous.short,
                previous_indicator_b: last_step.previous.long,
                details: json!({
                    "version": 3,
                    "runtime_state_version": V2_STATE_VERSION,
                    "timeframe": self.bar_timeframe(),
                    "average_type": self.config.average_type,
                    "short_window": self.config.short_window,
                    "long_window": self.config.long_window,
                    "min_gap_percent": self.config.min_gap_percent,
                    "confirmation_bars": self.config.confirmation_bars,
                    "confirmation_window_bars": self.config.confirmation_window_bars,
                    "cooldown_bars": self.config.cooldown_bars,
                    "atr_window": self.config.atr_window,
                    "min_atr_percent": self.config.min_atr_percent,
                    "trend_window": self.config.trend_window,
                    "short_average": last_step.current.short,
                    "long_average": last_step.current.long,
                    "previous_short_average": last_step.previous.short,
                    "previous_long_average": last_step.previous.long,
                    "gap_percent": last_step.current.gap_percent,
                    "atr": last_step.current.atr,
                    "atr_percent": last_step.current.atr_percent,
                    "trend_average": last_step.current.trend_average,
                    "qualified_direction": match last_step.current.direction { 1 => "buy", -1 => "sell", _ => "none" },
                    "pending_direction": pending_direction,
                    "confirmation_progress": confirmation_progress,
                    "confirmation_window_remaining": confirmation_window_remaining,
                    "cooldown_remaining": state.cooldown_remaining,
                    "last_published_direction": last_published_direction,
                    "last_published_bar_time": state.last_published_bar_time,
                    // Compatibility aliases for existing diagnostics. These
                    // now mean the last externally published target, never a
                    // signal suppressed during catch-up.
                    "last_emitted_direction": last_published_direction,
                    "last_emitted_bar_time": state.last_published_bar_time,
                    "ema_history_started_at": state.ema_history_started_at,
                    "previous_state_bar_time": previous_state_bar_time,
                    "processed_bar_count": processed_bar_count,
                    "catch_up_bar_count": processed_bar_count.saturating_sub(1),
                    "catch_up_signals_suppressed": suppressed_catch_up_signals,
                    "state_reinitialized": state_reinitialized,
                    "signal_reason": last_step.reason,
                    "target_intent": target_intent,
                    "bar": {
                        "time": current_bar.time,
                        "open": current_bar.open,
                        "high": current_bar.high,
                        "low": current_bar.low,
                        "close": current_bar.close,
                        "volume": current_bar.volume
                    }
                }),
            },
            next_state,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct V2Indicators {
    short: f64,
    long: f64,
    gap_percent: f64,
    atr: f64,
    atr_percent: f64,
    trend_average: Option<f64>,
    raw_direction: i8,
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
        self.base_history() + self.config.confirmation_window_bars + self.config.cooldown_bars
    }

    fn bar_timeframe(&self) -> &'static str {
        match self.config.bar_timeframe.as_str() {
            "5s" => "5s",
            _ => "1m",
        }
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        Ok(self.transition(bars, &self.initial_state())?.output)
    }

    fn state_version(&self) -> u32 {
        V2_STATE_VERSION
    }

    fn initial_state(&self) -> Value {
        serde_json::to_value(MovingAverageCrossV2State::default())
            .expect("moving_average_cross_v2 initial state is serializable")
    }

    fn evaluate_with_state(
        &self,
        bars: &[StrategyBar],
        state: &Value,
    ) -> Result<StrategyTransition, String> {
        self.transition(bars, state)
    }
}

fn moving_average_from_history(
    values: &[f64],
    window: usize,
    average_type: MovingAverageType,
) -> f64 {
    debug_assert!(values.len() >= window);
    match average_type {
        MovingAverageType::Sma => simple_moving_average(&values[values.len() - window..]),
        MovingAverageType::Ema => {
            let seed = simple_moving_average(&values[..window]);
            values[window..]
                .iter()
                .fold(seed, |ema, value| update_ema(ema, *value, window))
        }
    }
}

fn simple_moving_average(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn update_ema(previous: f64, value: f64, window: usize) -> f64 {
    let alpha = 2.0 / (window as f64 + 1.0);
    alpha * value + (1.0 - alpha) * previous
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

    fn v2_strategy(
        short_window: usize,
        long_window: usize,
        average_type: MovingAverageType,
        confirmation_bars: usize,
        confirmation_window_bars: usize,
        cooldown_bars: usize,
    ) -> MovingAverageCrossV2 {
        MovingAverageCrossV2::new(MovingAverageCrossV2Config {
            conid: 1,
            short_window,
            long_window,
            bar_timeframe: "1m".into(),
            average_type,
            min_gap_percent: 0.0,
            confirmation_bars,
            confirmation_window_bars,
            cooldown_bars,
            atr_window: 1,
            min_atr_percent: 0.0,
            trend_window: 0,
        })
        .unwrap()
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-10,
            "expected {expected}, found {actual}"
        );
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
            confirmation_window_bars: 1,
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
            "confirmation_window_bars": 2,
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
    fn v2_suppressed_catch_up_entries_do_not_create_false_protective_targets() {
        let gap_filtered = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "average_type": "sma",
            "min_gap_percent": 100.0,
            "confirmation_bars": 1,
            "confirmation_window_bars": 2,
            "cooldown_bars": 0,
            "atr_window": 1
        }))
        .unwrap();
        let output = gap_filtered
            .evaluate(&bars(&[3.0, 3.0, 2.0, 1.0, 4.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Hold);
        assert_eq!(output.details["signal_reason"], "gap_below_threshold");

        let cooling_down = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "average_type": "sma",
            "confirmation_bars": 1,
            "confirmation_window_bars": 1,
            "cooldown_bars": 3,
            "atr_window": 1
        }))
        .unwrap();
        let output = cooling_down
            .evaluate(&bars(&[3.0, 2.0, 1.0, 4.0, 1.0, 0.0, 3.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Buy);
        assert_eq!(output.details["signal_reason"], "confirmed_cross");
        assert_eq!(output.details["target_intent"], "directional");
    }

    #[test]
    fn v2_allows_filters_to_qualify_after_the_cross_within_the_window() {
        let strategy = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "average_type": "sma",
            "min_gap_percent": 10.0,
            "confirmation_bars": 1,
            "confirmation_window_bars": 2,
            "cooldown_bars": 0,
            "atr_window": 1
        }))
        .unwrap();
        let output = strategy
            .evaluate(&bars(&[3.0, 2.0, 1.0, 4.0, 8.0]))
            .unwrap();
        assert_eq!(output.signal, StrategySignal::Buy);
        assert_eq!(output.details["signal_reason"], "confirmed_cross");
    }

    #[test]
    fn v2_expires_a_cross_that_never_qualifies_inside_the_window() {
        let strategy = build_v2(json!({
            "conid": 1,
            "short_window": 2,
            "long_window": 3,
            "average_type": "sma",
            "min_gap_percent": 10.0,
            "confirmation_bars": 1,
            "confirmation_window_bars": 1,
            "cooldown_bars": 0,
            "atr_window": 1
        }))
        .unwrap();
        let output = strategy.evaluate(&bars(&[3.0, 2.0, 1.0, 4.0])).unwrap();
        assert_eq!(output.signal, StrategySignal::Hold);
        assert_eq!(
            output.details["signal_reason"],
            "confirmation_window_expired"
        );
    }

    #[test]
    fn v2_ema_is_seeded_once_and_continues_from_persisted_state() {
        let strategy = v2_strategy(2, 3, MovingAverageType::Ema, 1, 1, 0);
        assert_eq!(strategy.state_version(), 3);

        let all_bars = bars(&[1.0, 10.0, 1.0, 10.0, 1.0]);
        let first = strategy
            .evaluate_with_state(&all_bars[..4], &strategy.initial_state())
            .unwrap();
        assert_close(first.output.indicator_a, 7.5);
        assert_close(first.output.indicator_b, 7.0);

        // The live and backtest runners both retain exactly minimum_history
        // Bars. The old implementation re-seeded on this shifted slice and
        // produced 3.5; a continuous two-period EMA produces 19/6 instead.
        let second = strategy
            .evaluate_with_state(&all_bars[1..], &first.next_state)
            .unwrap();
        assert_close(second.output.indicator_a, 19.0 / 6.0);
        assert_close(second.output.indicator_b, 4.0);
        assert_eq!(second.output.details["processed_bar_count"], 1);
        assert_eq!(second.output.details["state_reinitialized"], false);
    }

    #[test]
    fn v2_pending_confirmation_survives_state_serialization() {
        let strategy = v2_strategy(2, 3, MovingAverageType::Sma, 2, 3, 0);
        let all_bars = bars(&[5.0, 4.0, 3.0, 2.0, 1.0, 10.0, 11.0]);
        let first = strategy
            .evaluate_with_state(&all_bars[..6], &strategy.initial_state())
            .unwrap();
        assert_eq!(first.output.signal, StrategySignal::Hold);
        assert_eq!(first.output.details["pending_direction"], "buy");
        assert_eq!(first.output.details["confirmation_progress"], 1);

        // JSON encode/decode is the same boundary used by
        // strategy_runtime_states when the daemon restarts.
        let persisted =
            serde_json::from_str::<Value>(&serde_json::to_string(&first.next_state).unwrap())
                .unwrap();
        let second = strategy
            .evaluate_with_state(&all_bars[1..], &persisted)
            .unwrap();
        assert_eq!(second.output.signal, StrategySignal::Buy);
        assert_eq!(second.output.details["signal_reason"], "confirmed_cross");
        assert_eq!(
            second.output.details["previous_state_bar_time"],
            serde_json::to_value(all_bars[5].time).unwrap()
        );
    }

    #[test]
    fn v2_cooldown_never_consumes_an_opposite_protective_direction() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 2);
        let all_bars = bars(&[5.0, 4.0, 3.0, 2.0, 1.0, 10.0, 0.1]);
        let first = strategy
            .evaluate_with_state(&all_bars[..6], &strategy.initial_state())
            .unwrap();
        assert_eq!(first.output.signal, StrategySignal::Buy);
        assert_eq!(first.output.details["cooldown_remaining"], 2);

        let second = strategy
            .evaluate_with_state(&all_bars[1..], &first.next_state)
            .unwrap();
        assert_eq!(second.output.signal, StrategySignal::Sell);
        assert_eq!(
            second.output.details["signal_reason"],
            "protective_direction_change"
        );
        assert_eq!(second.output.details["target_intent"], "flatten_only");
        assert_eq!(second.output.details["last_published_direction"], "none");
        assert_eq!(second.output.details["pending_direction"], "sell");
        assert_eq!(second.output.details["cooldown_remaining"], 2);
    }

    #[test]
    fn v2_detects_a_cross_that_passes_through_exact_equality() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let all_bars = bars(&[1.0, 1.0, 2.0, 1.5, 0.0]);
        let equal = strategy
            .evaluate_with_state(&all_bars[..4], &strategy.initial_state())
            .unwrap();
        assert_eq!(equal.output.signal, StrategySignal::Hold);
        assert_close(equal.output.indicator_a, equal.output.indicator_b);

        let bearish = strategy
            .evaluate_with_state(&all_bars[1..], &equal.next_state)
            .unwrap();
        assert_eq!(bearish.output.signal, StrategySignal::Sell);
        assert_eq!(bearish.output.details["signal_reason"], "confirmed_cross");
    }

    #[test]
    fn v2_entry_filters_cannot_block_the_opposite_protective_direction() {
        let strategy = MovingAverageCrossV2::new(MovingAverageCrossV2Config {
            conid: 1,
            short_window: 1,
            long_window: 3,
            bar_timeframe: "1m".into(),
            average_type: MovingAverageType::Sma,
            min_gap_percent: 50.0,
            confirmation_bars: 1,
            confirmation_window_bars: 2,
            cooldown_bars: 0,
            atr_window: 1,
            min_atr_percent: 0.0,
            trend_window: 0,
        })
        .unwrap();
        let all_bars = bars(&[3.0, 2.0, 1.0, 1.0, 10.0, 4.0, 5.0, 1.0]);
        let entered = strategy
            .evaluate_with_state(&all_bars[..5], &strategy.initial_state())
            .unwrap();
        assert_eq!(entered.output.signal, StrategySignal::Buy);

        let exit = strategy
            .evaluate_with_state(&all_bars[1..6], &entered.next_state)
            .unwrap();
        assert!(exit.output.details["gap_percent"].as_f64().unwrap() < 50.0);
        assert_eq!(exit.output.details["qualified_direction"], "none");
        assert_eq!(exit.output.signal, StrategySignal::Sell);
        assert_eq!(
            exit.output.details["signal_reason"],
            "protective_direction_change"
        );
        assert_eq!(exit.output.details["target_intent"], "flatten_only");
        assert_eq!(exit.output.details["last_published_direction"], "none");

        let filtered = strategy
            .evaluate_with_state(&all_bars[2..7], &exit.next_state)
            .unwrap();
        assert_eq!(filtered.output.signal, StrategySignal::Hold);
        assert_eq!(
            filtered.output.details["signal_reason"],
            "gap_below_threshold"
        );
        assert_eq!(filtered.output.details["target_intent"], "none");

        let reentered = strategy
            .evaluate_with_state(&all_bars[3..8], &filtered.next_state)
            .unwrap();
        assert_eq!(reentered.output.signal, StrategySignal::Sell);
        assert_eq!(reentered.output.details["signal_reason"], "confirmed_cross");
        assert_eq!(reentered.output.details["target_intent"], "directional");
        assert_eq!(reentered.output.details["last_published_direction"], "sell");
    }

    #[test]
    fn v2_catch_up_advances_state_without_emitting_an_old_signal() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let all_bars = bars(&[5.0, 4.0, 3.0, 2.0, 10.0, 11.0]);
        let first = strategy
            .evaluate_with_state(&all_bars[..4], &strategy.initial_state())
            .unwrap();
        assert_eq!(first.output.signal, StrategySignal::Hold);

        // Bar 4 contains the buy cross, but Bar 5 is the only currently
        // actionable Bar. Catch-up must consume the old cross without
        // presenting it to the order runner as a fresh signal.
        let caught_up = strategy
            .evaluate_with_state(&all_bars[2..], &first.next_state)
            .unwrap();
        assert_eq!(caught_up.output.signal, StrategySignal::Hold);
        assert_eq!(caught_up.output.details["processed_bar_count"], 2);
        assert_eq!(caught_up.output.details["catch_up_bar_count"], 1);
        assert_eq!(caught_up.output.details["catch_up_signals_suppressed"], 1);
        assert_eq!(caught_up.output.details["target_intent"], "none");
        assert_eq!(caught_up.output.details["last_published_direction"], "none");
        assert_eq!(
            caught_up.output.details["last_published_bar_time"],
            Value::Null
        );
    }

    #[test]
    fn v2_catch_up_republishes_a_still_current_protective_direction() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let all_bars = bars(&[3.0, 2.0, 1.0, 10.0, 0.0, 0.0]);
        let entered = strategy
            .evaluate_with_state(&all_bars[..4], &strategy.initial_state())
            .unwrap();
        assert_eq!(entered.output.signal, StrategySignal::Buy);

        let caught_up = strategy
            .evaluate_with_state(&all_bars[2..], &entered.next_state)
            .unwrap();
        assert_eq!(caught_up.output.signal, StrategySignal::Sell);
        assert_eq!(
            caught_up.output.details["signal_reason"],
            "protective_catch_up"
        );
        assert_eq!(caught_up.output.details["target_intent"], "flatten_only");
        assert_eq!(caught_up.output.details["catch_up_signals_suppressed"], 1);
        assert_eq!(caught_up.output.details["last_published_direction"], "none");
        assert_eq!(
            caught_up.output.details["last_published_bar_time"],
            serde_json::to_value(all_bars[5].time).unwrap()
        );
    }

    #[test]
    fn v2_reverse_entry_requalifies_after_protective_flatten_and_cooldown() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 2);
        let all_bars = bars(&[3.0, 2.0, 1.0, 1.0, 1.0, 10.0, 3.0, 2.0, 1.0, 0.0]);

        let entered = strategy
            .evaluate_with_state(&all_bars[..6], &strategy.initial_state())
            .unwrap();
        assert_eq!(entered.output.signal, StrategySignal::Buy);
        assert_eq!(entered.output.details["target_intent"], "directional");

        let flattened = strategy
            .evaluate_with_state(&all_bars[1..7], &entered.next_state)
            .unwrap();
        assert_eq!(flattened.output.signal, StrategySignal::Sell);
        assert_eq!(flattened.output.details["target_intent"], "flatten_only");
        assert_eq!(flattened.output.details["last_published_direction"], "none");

        let cooldown_one = strategy
            .evaluate_with_state(&all_bars[2..8], &flattened.next_state)
            .unwrap();
        assert_eq!(cooldown_one.output.signal, StrategySignal::Hold);
        assert_eq!(cooldown_one.output.details["signal_reason"], "cooldown");
        assert_eq!(cooldown_one.output.details["pending_direction"], "sell");

        let cooldown_two = strategy
            .evaluate_with_state(&all_bars[3..9], &cooldown_one.next_state)
            .unwrap();
        assert_eq!(cooldown_two.output.signal, StrategySignal::Hold);
        assert_eq!(cooldown_two.output.details["signal_reason"], "cooldown");

        let reentered = strategy
            .evaluate_with_state(&all_bars[4..10], &cooldown_two.next_state)
            .unwrap();
        assert_eq!(reentered.output.signal, StrategySignal::Sell);
        assert_eq!(reentered.output.details["target_intent"], "directional");
        assert_eq!(reentered.output.details["last_published_direction"], "sell");
    }

    #[test]
    fn v2_catch_up_does_not_flatten_when_latest_regime_matches_published_target() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let all_bars = bars(&[3.0, 2.0, 1.0, 10.0, 0.0, 10.0]);
        let entered = strategy
            .evaluate_with_state(&all_bars[..4], &strategy.initial_state())
            .unwrap();
        assert_eq!(entered.output.signal, StrategySignal::Buy);

        let caught_up = strategy
            .evaluate_with_state(&all_bars[2..], &entered.next_state)
            .unwrap();
        assert_eq!(caught_up.output.signal, StrategySignal::Hold);
        assert_eq!(caught_up.output.details["target_intent"], "none");
        assert_eq!(
            caught_up.output.details["signal_reason"],
            "already_published_direction"
        );
        assert_eq!(caught_up.output.details["last_published_direction"], "buy");
        assert_eq!(caught_up.output.details["catch_up_signals_suppressed"], 1);
    }

    #[test]
    fn v2_reinitialization_with_one_new_bar_still_publishes_required_flatten() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let all_bars = bars(&[3.0, 2.0, 1.0, 10.0, 10.0, 10.0, 10.0, 0.0]);
        let entered = strategy
            .evaluate_with_state(&all_bars[..4], &strategy.initial_state())
            .unwrap();
        assert_eq!(entered.output.signal, StrategySignal::Buy);

        let reinitialized = strategy
            .evaluate_with_state(&all_bars[4..], &entered.next_state)
            .unwrap();
        assert_eq!(reinitialized.output.details["state_reinitialized"], true);
        assert_eq!(reinitialized.output.details["processed_bar_count"], 1);
        assert_eq!(reinitialized.output.signal, StrategySignal::Sell);
        assert_eq!(
            reinitialized.output.details["target_intent"],
            "flatten_only"
        );
        assert_eq!(
            reinitialized.output.details["signal_reason"],
            "protective_catch_up"
        );
    }

    #[test]
    fn v2_reinitialization_cannot_open_a_new_directional_position() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let old_bars = bars(&[10.0, 9.0, 8.0, 7.0]);
        let initialized = strategy
            .evaluate_with_state(&old_bars, &strategy.initial_state())
            .unwrap();
        assert_eq!(initialized.output.signal, StrategySignal::Hold);

        // The persisted Bar is no longer present.  The reconstructed window
        // happens to contain a bullish cross, but that discontinuous evidence
        // is not allowed to increase exposure.
        let mut rebuilt_bars = bars(&[3.0, 2.0, 1.0, 10.0]);
        for bar in &mut rebuilt_bars {
            bar.time += chrono::Duration::hours(1);
        }
        let rebuilt = strategy
            .evaluate_with_state(&rebuilt_bars, &initialized.next_state)
            .unwrap();
        assert_eq!(rebuilt.output.details["state_reinitialized"], true);
        assert_eq!(rebuilt.output.signal, StrategySignal::Hold);
        assert_eq!(rebuilt.output.details["target_intent"], "none");
        assert_eq!(
            rebuilt.output.details["signal_reason"],
            "state_reinitialized_entry_suppressed"
        );
        assert_eq!(rebuilt.output.details["last_published_direction"], "none");
    }

    #[test]
    fn v2_accepts_empty_state_but_rejects_partial_or_inconsistent_initialized_state() {
        let strategy = v2_strategy(1, 3, MovingAverageType::Sma, 1, 1, 0);
        let all_bars = bars(&[3.0, 2.0, 1.0, 1.0, 1.0]);
        let initialized = strategy
            .evaluate_with_state(&all_bars[..4], &json!({}))
            .unwrap();

        let mut partial = initialized.next_state.clone();
        partial
            .as_object_mut()
            .unwrap()
            .remove("ema_history_started_at");
        let error = strategy
            .evaluate_with_state(&all_bars[1..], &partial)
            .err()
            .expect("partial initialized state must fail");
        assert!(error.contains("ema_history_started_at"));

        let mut inconsistent = initialized.next_state;
        inconsistent
            .as_object_mut()
            .unwrap()
            .insert("last_published_direction".into(), Value::Number(1.into()));
        let error = strategy
            .evaluate_with_state(&all_bars[1..], &inconsistent)
            .err()
            .expect("inconsistent initialized state must fail");
        assert!(error.contains("published direction is missing its Bar time"));
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
