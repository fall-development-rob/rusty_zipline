//! Pipeline system for factor-based strategy development

pub mod classifiers; // Asset categorization
pub mod composite;
pub mod domain; // NEW: P1 - Asset universe definitions
pub mod engine;
pub mod factors;
pub mod factors_fundamental; // NEW: Fundamental analysis factors
pub mod factors_returns; // NEW: Returns-based factors
pub mod factors_statistical; // NEW: Statistical factors
pub mod factors_technical; // NEW: Advanced technical indicators
pub mod factors_volume; // NEW: Volume-based indicators
pub mod filters; // Asset screening
pub mod graph; // NEW: P1 - Computational dependency graph
pub mod term; // NEW: P1 - Pipeline computation terms

pub use classifiers::{Classifier as PipelineClassifier, Everything, Quantiles, Relabel};
pub use composite::{
    AddFactors, DivideFactors, MultiplyFactors, RankFactor, SubtractFactors, TopNFilter,
    ZScoreFactor,
};
pub use engine::{
    Classifier, DataProvider, Factor, Filter, FactorOutput, OHLCVBar, Pipeline, PipelineContext,
    PipelineOutput,
};

// Basic technical factors
pub use factors::{
    AverageTrueRange, BollingerBands, ExponentialMovingAverage, HistoricalVolatility, Momentum,
    SimpleMovingAverage, MACD, RSI, VWAP,
};

// Returns factors
pub use factors_returns::{
    CumulativeReturns, DailyReturns, LogReturns, MaxDrawdown, PercentChange, Returns,
};

// Advanced technical indicators
pub use factors_technical::{Aroon, ADX, CCI, StochasticOscillator, WilliamsR};

// Volume indicators
pub use factors_volume::{
    AccumulationDistribution, AverageDollarVolume, ChaikinMoneyFlow, MoneyFlowIndex,
    OnBalanceVolume, VolumeWeightedMA,
};

// Statistical factors
pub use factors_statistical::{Alpha, Beta, Correlation, SharpeRatio, SortinoRatio};

// Fundamental factors
pub use factors_fundamental::{
    CurrentRatio, DebtToEquity, DividendYield, EarningsYield, EVToEBITDA, PayoutRatio, PBRatio,
    PERatio, PSRatio, QuickRatio, ROA, ROE, ROIC,
};

// Filters
pub use filters::{
    AllPresent, ArrayPredicate, Filter as PipelineFilter, MaximumFilter, NullFilter,
    NotNullFilter, PercentileFilter, SingleAsset, StaticAssets, StaticSids,
};

// Pipeline Core (P1 Task 4)
pub use domain::{
    Domain, DomainId, EquityUniverse, FilteredDomain, IntersectionDomain, StaticDomain,
    UnionDomain,
};
pub use graph::Graph;
pub use term::{
    BaseTerm, BinOp, BinaryOpTerm, DType, NDim, Term, TermId, UnaryOp, UnaryOpTerm,
};
