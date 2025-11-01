//! Pipeline execution engine for factor-based strategies

use crate::asset::Asset;
use crate::error::{Result, ZiplineError};
use crate::types::Timestamp;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use std::any::Any;
use std::sync::Arc;

/// Factor computation result for a single asset
pub type FactorValue = f64;

/// Factor output for all assets at a timestamp
pub type FactorOutput = HashMap<u64, FactorValue>; // asset_id -> value

/// Base trait for all pipeline factors
pub trait Factor: Send + Sync {
    /// Compute factor values for all assets at given timestamp
    fn compute(&self, timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput>;

    /// Get factor name
    fn name(&self) -> &str;

    /// Get dependencies (other factors this factor depends on)
    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    /// Clone as trait object
    fn clone_box(&self) -> Box<dyn Factor>;
}

/// Pipeline context holds data and computed factors
pub struct PipelineContext {
    /// Asset universe
    assets: Vec<Asset>,
    /// Historical data access
    data_provider: Arc<dyn DataProvider>,
    /// Cached factor results
    cache: HashMap<String, FactorOutput>,
    /// Current timestamp
    timestamp: DateTime<Utc>,
}

impl PipelineContext {
    /// Create new pipeline context
    pub fn new(
        assets: Vec<Asset>,
        data_provider: Arc<dyn DataProvider>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            assets,
            data_provider,
            cache: HashMap::new(),
            timestamp,
        }
    }

    /// Get assets in universe
    pub fn assets(&self) -> &[Asset] {
        &self.assets
    }

    /// Get data provider
    pub fn data_provider(&self) -> &Arc<dyn DataProvider> {
        &self.data_provider
    }

    /// Get cached factor result
    pub fn get_cached(&self, factor_name: &str) -> Option<&FactorOutput> {
        self.cache.get(factor_name)
    }

    /// Cache factor result
    pub fn cache_result(&mut self, factor_name: String, output: FactorOutput) {
        self.cache.insert(factor_name, output);
    }

    /// Get current timestamp
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

/// Data provider trait for pipeline
pub trait DataProvider: Send + Sync {
    /// Get historical prices for asset
    fn get_prices(&self, asset_id: u64, lookback: usize) -> Result<Vec<f64>>;

    /// Get historical volumes for asset
    fn get_volumes(&self, asset_id: u64, lookback: usize) -> Result<Vec<f64>>;

    /// Get OHLCV data for asset
    fn get_ohlcv(&self, asset_id: u64, lookback: usize) -> Result<Vec<OHLCVBar>>;

    /// Get latest price for asset
    fn get_latest_price(&self, asset_id: u64) -> Result<f64>;
}

/// OHLCV bar for pipeline data access
#[derive(Debug, Clone)]
pub struct OHLCVBar {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Filter trait for boolean conditions
pub trait Filter: Send + Sync {
    /// Evaluate filter for all assets
    fn evaluate(&self, timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<HashMap<u64, bool>>;

    /// Get filter name
    fn name(&self) -> &str;

    /// Clone as trait object
    fn clone_box(&self) -> Box<dyn Filter>;
}

/// Classifier trait for categorical values
pub trait Classifier: Send + Sync {
    /// Classify all assets into categories
    fn classify(&self, timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<HashMap<u64, String>>;

    /// Get classifier name
    fn name(&self) -> &str;

    /// Clone as trait object
    fn clone_box(&self) -> Box<dyn Classifier>;
}

/// Pipeline definition
pub struct Pipeline {
    /// Registered factors
    factors: HashMap<String, Box<dyn Factor>>,
    /// Registered filters
    filters: HashMap<String, Box<dyn Filter>>,
    /// Registered classifiers
    classifiers: HashMap<String, Box<dyn Classifier>>,
    /// Asset universe
    universe: Vec<Asset>,
    /// Dependency graph (topologically sorted factor names)
    execution_order: Vec<String>,
}

impl Pipeline {
    /// Create new empty pipeline
    pub fn new() -> Self {
        Self {
            factors: HashMap::new(),
            filters: HashMap::new(),
            classifiers: HashMap::new(),
            universe: Vec::new(),
            execution_order: Vec::new(),
        }
    }

    /// Add a factor to the pipeline
    pub fn add_factor(&mut self, name: String, factor: Box<dyn Factor>) -> &mut Self {
        self.factors.insert(name, factor);
        self.rebuild_execution_order();
        self
    }

    /// Add a filter to the pipeline
    pub fn add_filter(&mut self, name: String, filter: Box<dyn Filter>) -> &mut Self {
        self.filters.insert(name, filter);
        self
    }

    /// Add a classifier to the pipeline
    pub fn add_classifier(&mut self, name: String, classifier: Box<dyn Classifier>) -> &mut Self {
        self.classifiers.insert(name, classifier);
        self
    }

    /// Set the asset universe
    pub fn set_universe(&mut self, assets: Vec<Asset>) -> &mut Self {
        self.universe = assets;
        self
    }

    /// Rebuild execution order based on dependencies
    fn rebuild_execution_order(&mut self) {
        // Simple topological sort
        let mut order = Vec::new();
        let mut visited = HashMap::new();

        for name in self.factors.keys() {
            self.visit_factor(name, &mut order, &mut visited);
        }

        self.execution_order = order;
    }

    /// Visit factor for topological sort
    fn visit_factor(&self, name: &str, order: &mut Vec<String>, visited: &mut HashMap<String, bool>) {
        if visited.contains_key(name) {
            return;
        }

        visited.insert(name.to_string(), true);

        if let Some(factor) = self.factors.get(name) {
            for dep in factor.dependencies() {
                self.visit_factor(&dep, order, visited);
            }
        }

        order.push(name.to_string());
    }

    /// Run the pipeline for a given timestamp
    pub fn run(
        &self,
        timestamp: DateTime<Utc>,
        data_provider: Arc<dyn DataProvider>,
    ) -> Result<PipelineOutput> {
        let mut context = PipelineContext::new(self.universe.clone(), data_provider, timestamp);

        // Execute factors in dependency order
        let mut factor_results = HashMap::new();
        for factor_name in &self.execution_order {
            if let Some(factor) = self.factors.get(factor_name) {
                let output = factor.compute(timestamp, &context)?;
                context.cache_result(factor_name.clone(), output.clone());
                factor_results.insert(factor_name.clone(), output);
            }
        }

        // Execute filters
        let mut filter_results = HashMap::new();
        for (filter_name, filter) in &self.filters {
            let output = filter.evaluate(timestamp, &context)?;
            filter_results.insert(filter_name.clone(), output);
        }

        // Execute classifiers
        let mut classifier_results = HashMap::new();
        for (classifier_name, classifier) in &self.classifiers {
            let output = classifier.classify(timestamp, &context)?;
            classifier_results.insert(classifier_name.clone(), output);
        }

        Ok(PipelineOutput {
            timestamp,
            factors: factor_results,
            filters: filter_results,
            classifiers: classifier_results,
        })
    }

    /// Get factor by name
    pub fn get_factor(&self, name: &str) -> Option<&Box<dyn Factor>> {
        self.factors.get(name)
    }

    /// Get filter by name
    pub fn get_filter(&self, name: &str) -> Option<&Box<dyn Filter>> {
        self.filters.get(name)
    }

    /// Get number of factors
    pub fn factor_count(&self) -> usize {
        self.factors.len()
    }

    /// Get number of filters
    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Pipeline execution output
#[derive(Debug, Clone)]
pub struct PipelineOutput {
    /// Timestamp of execution
    pub timestamp: DateTime<Utc>,
    /// Factor results (factor_name -> asset_id -> value)
    pub factors: HashMap<String, FactorOutput>,
    /// Filter results (filter_name -> asset_id -> bool)
    pub filters: HashMap<String, HashMap<u64, bool>>,
    /// Classifier results (classifier_name -> asset_id -> category)
    pub classifiers: HashMap<String, HashMap<u64, String>>,
}

impl PipelineOutput {
    /// Get factor value for an asset
    pub fn get_factor_value(&self, factor_name: &str, asset_id: u64) -> Option<f64> {
        self.factors
            .get(factor_name)
            .and_then(|output| output.get(&asset_id))
            .copied()
    }

    /// Get filter result for an asset
    pub fn get_filter_result(&self, filter_name: &str, asset_id: u64) -> Option<bool> {
        self.filters
            .get(filter_name)
            .and_then(|output| output.get(&asset_id))
            .copied()
    }

    /// Get classifier result for an asset
    pub fn get_classifier_result(&self, filter_name: &str, asset_id: u64) -> Option<&str> {
        self.classifiers
            .get(filter_name)
            .and_then(|output| output.get(&asset_id))
            .map(|s| s.as_str())
    }

    /// Get all assets passing a filter
    pub fn get_filtered_assets(&self, filter_name: &str) -> Vec<u64> {
        self.filters
            .get(filter_name)
            .map(|output| {
                output
                    .iter()
                    .filter(|(_, &passed)| passed)
                    .map(|(&asset_id, _)| asset_id)
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDataProvider;

    impl DataProvider for MockDataProvider {
        fn get_prices(&self, _asset_id: u64, lookback: usize) -> Result<Vec<f64>> {
            Ok(vec![100.0; lookback])
        }

        fn get_volumes(&self, _asset_id: u64, lookback: usize) -> Result<Vec<f64>> {
            Ok(vec![1000.0; lookback])
        }

        fn get_ohlcv(&self, _asset_id: u64, lookback: usize) -> Result<Vec<OHLCVBar>> {
            Ok((0..lookback)
                .map(|_| OHLCVBar {
                    timestamp: Utc::now(),
                    open: 100.0,
                    high: 105.0,
                    low: 95.0,
                    close: 102.0,
                    volume: 1000.0,
                })
                .collect())
        }

        fn get_latest_price(&self, _asset_id: u64) -> Result<f64> {
            Ok(100.0)
        }
    }

    #[derive(Clone)]
    struct ConstantFactor {
        name: String,
        value: f64,
    }

    impl Factor for ConstantFactor {
        fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
            let mut output = HashMap::new();
            for asset in context.assets() {
                output.insert(asset.id, self.value);
            }
            Ok(output)
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn clone_box(&self) -> Box<dyn Factor> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_pipeline_creation() {
        let pipeline = Pipeline::new();
        assert_eq!(pipeline.factor_count(), 0);
        assert_eq!(pipeline.filter_count(), 0);
    }

    #[test]
    fn test_add_factor() {
        let mut pipeline = Pipeline::new();
        let factor = Box::new(ConstantFactor {
            name: "test".to_string(),
            value: 42.0,
        });

        pipeline.add_factor("test".to_string(), factor);
        assert_eq!(pipeline.factor_count(), 1);
        assert!(pipeline.get_factor("test").is_some());
    }

    #[test]
    fn test_pipeline_execution() {
        let mut pipeline = Pipeline::new();
        let factor = Box::new(ConstantFactor {
            name: "test".to_string(),
            value: 42.0,
        });

        let asset = Asset::equity(1, "TEST".to_string(), "TEST".to_string());
        pipeline.add_factor("test".to_string(), factor);
        pipeline.set_universe(vec![asset]);

        let data_provider = Arc::new(MockDataProvider);
        let output = pipeline.run(Utc::now(), data_provider).unwrap();

        assert_eq!(output.get_factor_value("test", 1), Some(42.0));
    }

    #[test]
    fn test_pipeline_output() {
        let output = PipelineOutput {
            timestamp: Utc::now(),
            factors: HashMap::new(),
            filters: HashMap::new(),
            classifiers: HashMap::new(),
        };

        assert!(output.get_factor_value("test", 1).is_none());
        assert!(output.get_filter_result("test", 1).is_none());
    }
}
