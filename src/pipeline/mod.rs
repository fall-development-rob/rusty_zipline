//! Pipeline system for factor-based strategy development

pub mod composite;
pub mod engine;
pub mod factors;

pub use composite::{
    AddFactors, DivideFactors, MultiplyFactors, RankFactor, SubtractFactors, TopNFilter,
    ZScoreFactor,
};
pub use engine::{
    Classifier, DataProvider, Factor, Filter, FactorOutput, OHLCVBar, Pipeline, PipelineContext,
    PipelineOutput,
};
pub use factors::{
    AverageTrueRange, BollingerBands, ExponentialMovingAverage, HistoricalVolatility, Momentum,
    SimpleMovingAverage, MACD, RSI, VWAP,
};
