//! # Zipline-Rust
//!
//! A Rust implementation of the Zipline algorithmic trading backtesting library.
//!
//! Zipline-Rust is an event-driven backtesting system that allows you to test
//! trading algorithms against historical market data.
//!
//! ## Example
//!
//! ```rust,no_run
//! use zipline_rust::prelude::*;
//!
//! struct MyStrategy {
//!     // Strategy state
//! }
//!
//! impl Algorithm for MyStrategy {
//!     fn initialize(&mut self, context: &mut Context) {
//!         // One-time setup
//!     }
//!
//!     fn handle_data(&mut self, context: &mut Context, data: &BarData) {
//!         // Called for each bar of data
//!     }
//! }
//! ```

pub mod algorithm;
pub mod asset;
pub mod calendar;
pub mod data;
pub mod engine;
pub mod error;
pub mod execution;
pub mod finance;
pub mod order;
pub mod performance;
pub mod pipeline;
pub mod types;

pub mod prelude {
    //! Commonly used types and traits
    pub use crate::algorithm::{Algorithm, Context};
    pub use crate::asset::{Asset, AssetType};
    pub use crate::data::BarData;
    pub use crate::engine::SimulationEngine;
    pub use crate::error::{Result, ZiplineError};
    pub use crate::finance::{Portfolio, Position};
    pub use crate::order::{Order, OrderSide, OrderType};
    pub use crate::types::*;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lib_compile() {
        // Smoke test to ensure library compiles
    }
}
