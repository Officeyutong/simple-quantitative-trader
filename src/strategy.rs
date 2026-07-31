//! Compatibility facade for the extracted strategy crates.
//!
//! Platform code continues to import `crate::strategy`, while implementations,
//! strongly typed configuration and registration now live in independent
//! workspace crates.

pub use strategy_api::{Strategy, StrategyBar, StrategySignal};
pub use strategy_catalog_backend::{build, metadata_json, registered_kinds};
