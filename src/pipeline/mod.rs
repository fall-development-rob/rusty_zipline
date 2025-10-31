//! Pipeline system for factor-based strategy development

pub mod factors;

pub use factors::{
    AverageTrueRange, BollingerBands, ExponentialMovingAverage, HistoricalVolatility, Momentum,
    SimpleMovingAverage, MACD, RSI, VWAP,
};

// Placeholder for future pipeline engine
// TODO: Implement Pipeline, Factor trait, Classifier, Filter, etc.
