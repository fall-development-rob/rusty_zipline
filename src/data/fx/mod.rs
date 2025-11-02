//! Foreign Exchange (FX) rate system
//!
//! Provides currency conversion and FX rate management for multi-currency portfolios.
//!
//! # Components
//!
//! - **base**: Core traits and types (Currency, FXRateReader)
//! - **in_memory**: Fast in-memory rate storage for backtesting
//! - **hdf5**: Efficient HDF5-based rate storage for large datasets
//! - **exploding**: Testing stub that panics on FX usage
//! - **utils**: Utilities for currency conversion and analysis
//!
//! # Example
//!
//! ```rust
//! use rusty_zipline::data::fx::{InMemoryFXRateReader, Currency, FXRateReader};
//! use chrono::Utc;
//!
//! let mut reader = InMemoryFXRateReader::new();
//! let dt = Utc::now();
//!
//! // Add rate: 1 EUR = 1.20 USD
//! reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20);
//!
//! // Get rate
//! let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
//! assert_eq!(rate, 1.20);
//!
//! // Convert amount
//! let usd_amount = rusty_zipline::data::fx::convert_amount(
//!     &reader,
//!     100.0,
//!     Currency::EUR,
//!     Currency::USD,
//!     dt
//! ).unwrap();
//! assert_eq!(usd_amount, 120.0);
//! ```

pub mod base;
pub mod exploding;
pub mod hdf5;
pub mod in_memory;
pub mod utils;

pub use base::{Currency, FXRateReader};
pub use exploding::ExplodingFXRateReader;
pub use hdf5::HDF5FXRateReader;
pub use in_memory::InMemoryFXRateReader;
pub use utils::{convert_amount, convert_amounts, portfolio_value, CurrencyPair};
