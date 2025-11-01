//! Pipeline system for factor-based strategy development

pub mod classifiers; // NEW: Asset categorization
pub mod composite;
pub mod engine;
pub mod factors;
pub mod filters; // NEW: Asset screening

pub use classifiers::{Classifier as PipelineClassifier, Everything, Quantiles, Relabel};
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
pub use filters::{
    AllPresent, ArrayPredicate, Filter as PipelineFilter, MaximumFilter, NullFilter,
    NotNullFilter, PercentileFilter, SingleAsset, StaticAssets, StaticSids,
};
