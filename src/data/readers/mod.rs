//! Bar data readers
//!
//! This module provides implementations for reading bar data from various storage formats.
//! The primary focus is on reading Zipline-compatible bcolz bundles.

pub mod bcolz_daily;
pub mod bcolz_minute;
pub mod bcolz_utils;

pub use bcolz_daily::BcolzDailyBarReader;
pub use bcolz_minute::BcolzMinuteBarReader;
pub use bcolz_utils::{BcolzMetadata, BcolzChunk, read_bcolz_attrs};
