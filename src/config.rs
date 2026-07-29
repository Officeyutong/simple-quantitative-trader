use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub app: AppConfig,
    pub rpc: RpcConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub ibkr: IbkrConfig,
    pub risk: RiskConfig,
    pub monitoring: MonitoringConfig,
    pub web: WebConfig,
    /// Safety-relevant notices collected while loading. They are logged by the
    /// daemon after telemetry is initialised; emitting them here would be lost
    /// because `Config::load` runs before the tracing subscriber exists.
    #[serde(skip)]
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub environment: Environment,
    pub data_dir: PathBuf,
    pub timezone: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Development,
    Paper,
    Live,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RpcConfig {
    pub http_listen: SocketAddr,
    pub allowed_web_origin: String,
    pub max_request_bytes: usize,
    pub max_concurrent_requests: usize,
    pub request_timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub duckdb_path: PathBuf,
    pub lake_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub backup_dir: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IbkrConfig {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
    pub account: Option<String>,
    pub connect_on_start: bool,
    pub request_timeout_seconds: u64,
    pub reconnect_max_seconds: u64,
    pub readonly: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    pub base_currency: String,
    pub max_fx_rate_age_seconds: u64,
    pub trading_enabled: bool,
    pub max_order_notional: f64,
    pub max_order_quantity: f64,
    pub max_market_data_age_seconds: u64,
    pub max_account_data_age_seconds: u64,
    pub max_position_quantity: f64,
    pub max_gross_exposure: f64,
    pub max_net_exposure: f64,
    pub max_open_orders: usize,
    pub max_orders_per_minute: usize,
    pub max_daily_loss: f64,
    pub max_price_deviation_bps: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub performance_snapshot_seconds: u64,
    pub performance_initial_capital: f64,
    pub alert_on_delayed_market_data: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    pub enabled: bool,
    pub listen: SocketAddr,
    pub static_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            app: AppConfig::default(),
            rpc: RpcConfig::default(),
            storage: StorageConfig::default(),
            logging: LoggingConfig::default(),
            ibkr: IbkrConfig::default(),
            risk: RiskConfig::default(),
            monitoring: MonitoringConfig::default(),
            web: WebConfig::default(),
            warnings: Vec::new(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            environment: Environment::Development,
            data_dir: PathBuf::from("./data"),
            timezone: "UTC".into(),
        }
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            http_listen: SocketAddr::from(([127, 0, 0, 1], 8787)),
            allowed_web_origin: "http://127.0.0.1:8080".into(),
            max_request_bytes: 1024 * 1024,
            max_concurrent_requests: 32,
            request_timeout_seconds: 30,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            duckdb_path: PathBuf::from("state.duckdb"),
            lake_dir: PathBuf::from("lake"),
            staging_dir: PathBuf::from("staging"),
            backup_dir: PathBuf::from("backups"),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: LogFormat::Pretty,
        }
    }
}

impl Default for IbkrConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 4002,
            client_id: 17,
            account: None,
            connect_on_start: false,
            request_timeout_seconds: 10,
            reconnect_max_seconds: 60,
            readonly: true,
        }
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            base_currency: "USD".into(),
            max_fx_rate_age_seconds: 3_600,
            trading_enabled: false,
            max_order_notional: 10_000.0,
            max_order_quantity: 1_000.0,
            max_market_data_age_seconds: 30,
            max_account_data_age_seconds: 120,
            max_position_quantity: 1_000.0,
            max_gross_exposure: 100_000.0,
            max_net_exposure: 100_000.0,
            max_open_orders: 20,
            max_orders_per_minute: 10,
            max_daily_loss: 1_000.0,
            max_price_deviation_bps: 500.0,
        }
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
            static_dir: PathBuf::from("../web/dist"),
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 30,
            performance_snapshot_seconds: 300,
            performance_initial_capital: 100_000.0,
            alert_on_delayed_market_data: true,
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut config = match path {
            Some(path) => {
                let source = fs::read_to_string(path).map_err(|error| {
                    AppError::Config(format!("cannot read {}: {error}", path.display()))
                })?;
                toml::from_str(&source).map_err(|error| {
                    AppError::Config(format!("cannot parse {}: {error}", path.display()))
                })?
            }
            None => Config::default(),
        };
        config.apply_environment_overrides()?;
        config.resolve_paths(path.and_then(Path::parent));
        config.validate()?;
        if config.app.environment == Environment::Live && config.risk.trading_enabled {
            config
                .warnings
                .push("live trading is enabled by configuration".into());
        }
        if config.rpc.allowed_web_origin == "*" {
            config.warnings.push(
                "rpc.allowed_web_origin is '*': every website may attempt to call the trading RPC"
                    .into(),
            );
        }
        Ok(config)
    }

    fn apply_environment_overrides(&mut self) -> Result<()> {
        if let Ok(value) = env::var("QUANT__APP__DATA_DIR") {
            self.app.data_dir = value.into();
        }
        if let Ok(value) = env::var("QUANT__RPC__HTTP_LISTEN") {
            self.rpc.http_listen = value.parse().map_err(|_| {
                AppError::Config(
                    "QUANT__RPC__HTTP_LISTEN must be an IP socket address such as \
                     127.0.0.1:8787"
                        .into(),
                )
            })?;
        }
        if let Ok(value) = env::var("QUANT__WEB__LISTEN") {
            self.web.listen = value.parse().map_err(|_| {
                AppError::Config(
                    "QUANT__WEB__LISTEN must be an IP socket address such as \
                     127.0.0.1:8080"
                        .into(),
                )
            })?;
        }
        if let Ok(value) = env::var("QUANT__LOGGING__LEVEL") {
            self.logging.level = value;
        }
        if let Ok(value) = env::var("QUANT__RISK__TRADING_ENABLED") {
            self.risk.trading_enabled = value.parse().map_err(|_| {
                AppError::Config("QUANT__RISK__TRADING_ENABLED must be true or false".into())
            })?;
            self.warnings.push(format!(
                "risk.trading_enabled was overridden to {} by the \
                 QUANT__RISK__TRADING_ENABLED environment variable",
                self.risk.trading_enabled
            ));
        }
        Ok(())
    }

    fn resolve_paths(&mut self, config_dir: Option<&Path>) {
        if self.web.static_dir.is_relative() {
            let base = config_dir.unwrap_or_else(|| Path::new("."));
            self.web.static_dir = base.join(std::mem::take(&mut self.web.static_dir));
        }
        if self.app.data_dir.is_relative() {
            let base = config_dir.unwrap_or_else(|| Path::new("."));
            self.app.data_dir = base.join(&self.app.data_dir);
        }
        self.storage.duckdb_path = resolve_under(
            &self.app.data_dir,
            std::mem::take(&mut self.storage.duckdb_path),
        );
        self.storage.lake_dir = resolve_under(
            &self.app.data_dir,
            std::mem::take(&mut self.storage.lake_dir),
        );
        self.storage.staging_dir = resolve_under(
            &self.app.data_dir,
            std::mem::take(&mut self.storage.staging_dir),
        );
        self.storage.backup_dir = resolve_under(
            &self.app.data_dir,
            std::mem::take(&mut self.storage.backup_dir),
        );
    }

    fn validate(&self) -> Result<()> {
        if self.app.timezone != "UTC" {
            return Err(AppError::Config(
                "app.timezone must be UTC; local time is presentation-only".into(),
            ));
        }
        if self.rpc.max_request_bytes == 0 || self.rpc.max_concurrent_requests == 0 {
            return Err(AppError::Config(
                "RPC size and concurrency limits must be greater than zero".into(),
            ));
        }
        let origin = self.rpc.allowed_web_origin.as_str();
        if origin != "*" {
            let origin_authority = origin
                .strip_prefix("http://")
                .or_else(|| origin.strip_prefix("https://"));
            if origin_authority.is_none_or(|authority| {
                authority.is_empty()
                    || authority.contains('/')
                    || authority.contains('*')
                    || authority.chars().any(char::is_whitespace)
            }) {
                return Err(AppError::Config(
                    "rpc.allowed_web_origin must be '*' or one explicit HTTP(S) origin without a path"
                        .into(),
                ));
            }
        }
        if self.web.enabled && self.web.listen == self.rpc.http_listen {
            return Err(AppError::Config(
                "web.listen and rpc.http_listen must use different addresses".into(),
            ));
        }
        if self.ibkr.request_timeout_seconds == 0 || self.ibkr.reconnect_max_seconds == 0 {
            return Err(AppError::Config(
                "IBKR timeout and reconnect limits must be greater than zero".into(),
            ));
        }
        if self.risk.base_currency.trim().len() != 3
            || self.risk.max_fx_rate_age_seconds == 0
            || self.monitoring.interval_seconds == 0
            || self.monitoring.performance_snapshot_seconds == 0
            || !self.monitoring.performance_initial_capital.is_finite()
            || self.monitoring.performance_initial_capital <= 0.0
        {
            return Err(AppError::Config(
                "risk.base_currency must be a three-letter code and monitoring/FX intervals must be greater than zero".into(),
            ));
        }
        if self.risk.max_order_notional <= 0.0 || self.risk.max_order_quantity <= 0.0 {
            return Err(AppError::Config(
                "risk order limits must be greater than zero".into(),
            ));
        }
        if self.risk.max_market_data_age_seconds == 0 || self.risk.max_account_data_age_seconds == 0
        {
            return Err(AppError::Config(
                "risk market/account data age limits must be greater than zero".into(),
            ));
        }
        if ![
            self.risk.max_position_quantity,
            self.risk.max_gross_exposure,
            self.risk.max_net_exposure,
            self.risk.max_daily_loss,
            self.risk.max_price_deviation_bps,
        ]
        .iter()
        .all(|value| value.is_finite() && *value > 0.0)
            || self.risk.max_open_orders == 0
            || self.risk.max_orders_per_minute == 0
        {
            return Err(AppError::Config(
                "portfolio risk limits must be finite and greater than zero".into(),
            ));
        }
        Ok(())
    }

    pub fn lock_path(&self) -> PathBuf {
        self.app.data_dir.join("run/quant.lock")
    }
}

fn resolve_under(data_dir: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        data_dir.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_storage_paths_are_resolved_under_data_dir() {
        let mut config = Config::default();
        config.resolve_paths(None);
        assert_eq!(
            config.storage.duckdb_path,
            PathBuf::from("./data/state.duckdb")
        );
        assert_eq!(
            config.rpc.http_listen,
            SocketAddr::from(([127, 0, 0, 1], 8787))
        );
    }

    #[test]
    fn non_utc_storage_is_rejected() {
        let mut config = Config::default();
        config.app.timezone = "Asia/Shanghai".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn wildcard_web_origin_is_accepted_but_partial_globs_are_rejected() {
        let mut config = Config::default();
        config.rpc.allowed_web_origin = "*".into();
        assert!(config.validate().is_ok());

        config.rpc.allowed_web_origin = "https://*.example.com".into();
        assert!(config.validate().is_err());
    }
}
