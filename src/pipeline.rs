//! Pipeline system for data processing and factor computation

use crate::asset::Asset;
use crate::error::Result;
use crate::types::Timestamp;
use hashbrown::HashMap;

/// Pipeline factor trait
pub trait Factor: Send + Sync {
    /// Compute factor values for all assets at a given timestamp
    fn compute(&self, timestamp: Timestamp, universe: &[Asset]) -> Result<HashMap<u64, f64>>;

    /// Get factor name
    fn name(&self) -> &str;
}

/// Simple moving average factor
pub struct SimpleMovingAverage {
    name: String,
    window: usize,
    // In a real implementation, this would access historical data
}

impl SimpleMovingAverage {
    pub fn new(window: usize) -> Self {
        Self {
            name: format!("SMA_{}", window),
            window,
        }
    }
}

impl Factor for SimpleMovingAverage {
    fn compute(&self, _timestamp: Timestamp, _universe: &[Asset]) -> Result<HashMap<u64, f64>> {
        // Placeholder implementation
        // In reality, this would:
        // 1. Access historical price data for each asset
        // 2. Calculate SMA for the window
        // 3. Return results
        Ok(HashMap::new())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Pipeline for computing multiple factors
pub struct Pipeline {
    factors: Vec<Box<dyn Factor>>,
    filters: Vec<Box<dyn Filter>>,
}

impl Pipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self {
            factors: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Add a factor to the pipeline
    pub fn add_factor(&mut self, factor: Box<dyn Factor>) {
        self.factors.push(factor);
    }

    /// Add a filter to the pipeline
    pub fn add_filter(&mut self, filter: Box<dyn Filter>) {
        self.filters.push(filter);
    }

    /// Run the pipeline for a given timestamp
    pub fn run(&self, timestamp: Timestamp, universe: &[Asset]) -> Result<PipelineOutput> {
        // Apply filters to universe
        let mut filtered_universe = universe.to_vec();
        for filter in &self.filters {
            filtered_universe = filter.apply(timestamp, &filtered_universe)?;
        }

        // Compute all factors
        let mut factor_data: HashMap<String, HashMap<u64, f64>> = HashMap::new();

        for factor in &self.factors {
            let values = factor.compute(timestamp, &filtered_universe)?;
            factor_data.insert(factor.name().to_string(), values);
        }

        Ok(PipelineOutput {
            timestamp,
            universe: filtered_universe,
            factor_data,
        })
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter trait for selecting assets
pub trait Filter: Send + Sync {
    /// Apply filter to universe
    fn apply(&self, timestamp: Timestamp, universe: &[Asset]) -> Result<Vec<Asset>>;
}

/// Pipeline output containing computed factor values
pub struct PipelineOutput {
    pub timestamp: Timestamp,
    pub universe: Vec<Asset>,
    pub factor_data: HashMap<String, HashMap<u64, f64>>,
}

impl PipelineOutput {
    /// Get factor value for an asset
    pub fn get_factor(&self, factor_name: &str, asset_id: u64) -> Option<f64> {
        self.factor_data
            .get(factor_name)
            .and_then(|values| values.get(&asset_id).copied())
    }

    /// Get all factor values for an asset
    pub fn get_asset_factors(&self, asset_id: u64) -> HashMap<String, f64> {
        let mut result = HashMap::new();

        for (factor_name, values) in &self.factor_data {
            if let Some(value) = values.get(&asset_id) {
                result.insert(factor_name.clone(), *value);
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use chrono::Utc;

    #[test]
    fn test_pipeline_creation() {
        let mut pipeline = Pipeline::new();
        pipeline.add_factor(Box::new(SimpleMovingAverage::new(20)));

        assert_eq!(pipeline.factors.len(), 1);
    }

    #[test]
    fn test_pipeline_run() {
        let pipeline = Pipeline::new();
        let universe = vec![Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string())];

        let output = pipeline.run(Utc::now(), &universe).unwrap();
        assert_eq!(output.universe.len(), 1);
    }
}
