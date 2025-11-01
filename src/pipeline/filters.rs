//! Pipeline Filters - Asset screening and filtering
//!
//! Filters are Pipeline computations that produce boolean outputs,
//! used to screen universes of assets.

use crate::error::{Result, ZiplineError};
use std::collections::HashMap;

/// Filter trait - produces boolean output for asset screening
pub trait Filter: Send + Sync {
    /// Compute filter for each asset
    /// Returns HashMap<asset_id, bool> where true = passes filter
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>>;

    /// Get filter name for debugging
    fn name(&self) -> &str {
        "Filter"
    }
}

/// PercentileFilter - Filter assets by percentile range
///
/// Keep only assets whose values fall within specified percentile range.
/// Example: PercentileFilter(0.1, 0.9) keeps middle 80% of assets
pub struct PercentileFilter {
    min_percentile: f64,
    max_percentile: f64,
}

impl PercentileFilter {
    pub fn new(min_percentile: f64, max_percentile: f64) -> Result<Self> {
        if min_percentile < 0.0 || min_percentile > 1.0 {
            return Err(ZiplineError::PipelineError(
                "min_percentile must be between 0 and 1".to_string(),
            ));
        }
        if max_percentile < 0.0 || max_percentile > 1.0 {
            return Err(ZiplineError::PipelineError(
                "max_percentile must be between 0 and 1".to_string(),
            ));
        }
        if min_percentile >= max_percentile {
            return Err(ZiplineError::PipelineError(
                "min_percentile must be less than max_percentile".to_string(),
            ));
        }

        Ok(Self {
            min_percentile,
            max_percentile,
        })
    }
}

impl Filter for PercentileFilter {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        // Get latest values for all assets
        let mut values: Vec<(u64, f64)> = data
            .iter()
            .filter_map(|(asset_id, vals)| vals.last().map(|&v| (*asset_id, v)))
            .collect();

        if values.is_empty() {
            return Ok(result);
        }

        // Sort by value
        values.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate percentile indices
        let min_idx = ((values.len() - 1) as f64 * self.min_percentile) as usize;
        let max_idx = ((values.len() - 1) as f64 * self.max_percentile) as usize;

        let min_value = values[min_idx].1;
        let max_value = values[max_idx].1;

        // Mark assets in range
        for (asset_id, value) in values {
            result.insert(asset_id, value >= min_value && value <= max_value);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "PercentileFilter"
    }
}

/// NullFilter - Filter for null/NaN values
///
/// Passes assets that have null/NaN values
pub struct NullFilter;

impl Filter for NullFilter {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            let has_null = values.last().map(|v| v.is_nan()).unwrap_or(true);
            result.insert(*asset_id, has_null);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "NullFilter"
    }
}

/// NotNullFilter - Filter for non-null values
///
/// Passes assets that have valid (non-null/NaN) values
pub struct NotNullFilter;

impl Filter for NotNullFilter {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            let has_value = values.last().map(|v| !v.is_nan()).unwrap_or(false);
            result.insert(*asset_id, has_value);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "NotNullFilter"
    }
}

/// AllPresent - Require all values in window to be present (no NaN)
///
/// Filters assets that have complete data over the lookback window
pub struct AllPresent {
    window_length: usize,
}

impl AllPresent {
    pub fn new(window_length: usize) -> Self {
        Self { window_length }
    }
}

impl Filter for AllPresent {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            let lookback_start = values.len().saturating_sub(self.window_length);
            let window = &values[lookback_start..];

            let all_present = window.iter().all(|v| !v.is_nan());
            result.insert(*asset_id, all_present);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "AllPresent"
    }
}

/// SingleAsset - Filter for a single specific asset
///
/// Passes only the specified asset
pub struct SingleAsset {
    asset_id: u64,
}

impl SingleAsset {
    pub fn new(asset_id: u64) -> Self {
        Self { asset_id }
    }
}

impl Filter for SingleAsset {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for asset_id in data.keys() {
            result.insert(*asset_id, *asset_id == self.asset_id);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "SingleAsset"
    }
}

/// StaticSids - Filter for a static set of SIDs
///
/// Passes only assets in the provided list
pub struct StaticSids {
    sids: Vec<u64>,
}

impl StaticSids {
    pub fn new(sids: Vec<u64>) -> Self {
        Self { sids }
    }
}

impl Filter for StaticSids {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for asset_id in data.keys() {
            result.insert(*asset_id, self.sids.contains(asset_id));
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "StaticSids"
    }
}

/// StaticAssets - Alias for StaticSids
pub type StaticAssets = StaticSids;

/// MaximumFilter - Keep top N assets by value
///
/// Filters to keep only the N assets with highest values
pub struct MaximumFilter {
    n: usize,
}

impl MaximumFilter {
    pub fn new(n: usize) -> Self {
        Self { n }
    }
}

impl Filter for MaximumFilter {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        // Get latest values for all assets
        let mut values: Vec<(u64, f64)> = data
            .iter()
            .filter_map(|(asset_id, vals)| vals.last().map(|&v| (*asset_id, v)))
            .collect();

        if values.is_empty() {
            return Ok(result);
        }

        // Sort by value descending
        values.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top N
        let top_n_assets: std::collections::HashSet<u64> = values
            .iter()
            .take(self.n)
            .map(|(asset_id, _)| *asset_id)
            .collect();

        // Mark assets
        for asset_id in data.keys() {
            result.insert(*asset_id, top_n_assets.contains(asset_id));
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "MaximumFilter"
    }
}

/// ArrayPredicate - Custom filter with user-provided function
///
/// Allows arbitrary filtering logic via closures
pub struct ArrayPredicate<F>
where
    F: Fn(&[f64]) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> ArrayPredicate<F>
where
    F: Fn(&[f64]) -> bool + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> Filter for ArrayPredicate<F>
where
    F: Fn(&[f64]) -> bool + Send + Sync,
{
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            let passes = (self.predicate)(values);
            result.insert(*asset_id, passes);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "ArrayPredicate"
    }
}

/// Latest - Filter based on latest value
///
/// Filters based on the most recent value meeting a condition
pub struct Latest<F>
where
    F: Fn(f64) -> bool + Send + Sync,
{
    condition: F,
}

impl<F> Latest<F>
where
    F: Fn(f64) -> bool + Send + Sync,
{
    pub fn new(condition: F) -> Self {
        Self { condition }
    }
}

impl<F> Filter for Latest<F>
where
    F: Fn(f64) -> bool + Send + Sync,
{
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            let passes = values
                .last()
                .map(|&v| (self.condition)(v))
                .unwrap_or(false);
            result.insert(*asset_id, passes);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "Latest"
    }
}

/// CustomFilter - User-defined filter with custom compute logic
///
/// Base for creating custom filters with arbitrary logic
pub trait CustomFilter: Send + Sync {
    fn compute_custom(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>>;
}

impl<T: CustomFilter> Filter for T {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, bool>> {
        self.compute_custom(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_data() -> HashMap<u64, Vec<f64>> {
        let mut data = HashMap::new();
        data.insert(1, vec![100.0, 105.0, 110.0]);
        data.insert(2, vec![50.0, 55.0, 60.0]);
        data.insert(3, vec![200.0, 195.0, 190.0]);
        data.insert(4, vec![75.0, 80.0, 85.0]);
        data.insert(5, vec![150.0, 155.0, 160.0]);
        data
    }

    #[test]
    fn test_percentile_filter() {
        let filter = PercentileFilter::new(0.2, 0.8).unwrap();
        let data = create_test_data();
        let result = filter.compute(&data).unwrap();

        // Should keep middle 60% (assets 2, 4, 1)
        assert_eq!(result.get(&2), Some(&true)); // 60 is in middle range
        assert_eq!(result.get(&4), Some(&true)); // 85 is in middle range
        assert_eq!(result.get(&1), Some(&true)); // 110 is in middle range
    }

    #[test]
    fn test_percentile_filter_validation() {
        assert!(PercentileFilter::new(-0.1, 0.5).is_err());
        assert!(PercentileFilter::new(0.5, 1.5).is_err());
        assert!(PercentileFilter::new(0.8, 0.2).is_err());
    }

    #[test]
    fn test_null_filter() {
        let filter = NullFilter;
        let mut data = HashMap::new();
        data.insert(1, vec![100.0, f64::NAN]);
        data.insert(2, vec![50.0, 55.0]);

        let result = filter.compute(&data).unwrap();
        assert_eq!(result.get(&1), Some(&true)); // Has NaN
        assert_eq!(result.get(&2), Some(&false)); // No NaN
    }

    #[test]
    fn test_not_null_filter() {
        let filter = NotNullFilter;
        let mut data = HashMap::new();
        data.insert(1, vec![100.0, f64::NAN]);
        data.insert(2, vec![50.0, 55.0]);

        let result = filter.compute(&data).unwrap();
        assert_eq!(result.get(&1), Some(&false)); // Has NaN
        assert_eq!(result.get(&2), Some(&true)); // No NaN
    }

    #[test]
    fn test_all_present() {
        let filter = AllPresent::new(3);
        let mut data = HashMap::new();
        data.insert(1, vec![100.0, 105.0, 110.0]); // All present
        data.insert(2, vec![50.0, f64::NAN, 60.0]); // Has NaN

        let result = filter.compute(&data).unwrap();
        assert_eq!(result.get(&1), Some(&true)); // All present
        assert_eq!(result.get(&2), Some(&false)); // Has NaN
    }

    #[test]
    fn test_single_asset() {
        let filter = SingleAsset::new(3);
        let data = create_test_data();
        let result = filter.compute(&data).unwrap();

        assert_eq!(result.get(&1), Some(&false));
        assert_eq!(result.get(&2), Some(&false));
        assert_eq!(result.get(&3), Some(&true));
        assert_eq!(result.get(&4), Some(&false));
        assert_eq!(result.get(&5), Some(&false));
    }

    #[test]
    fn test_static_sids() {
        let filter = StaticSids::new(vec![1, 3, 5]);
        let data = create_test_data();
        let result = filter.compute(&data).unwrap();

        assert_eq!(result.get(&1), Some(&true));
        assert_eq!(result.get(&2), Some(&false));
        assert_eq!(result.get(&3), Some(&true));
        assert_eq!(result.get(&4), Some(&false));
        assert_eq!(result.get(&5), Some(&true));
    }

    #[test]
    fn test_maximum_filter() {
        let filter = MaximumFilter::new(2);
        let data = create_test_data();
        let result = filter.compute(&data).unwrap();

        // Top 2 should be assets 3 (190) and 5 (160)
        assert_eq!(result.get(&3), Some(&true));
        assert_eq!(result.get(&5), Some(&true));
        assert_eq!(result.get(&1), Some(&false));
        assert_eq!(result.get(&2), Some(&false));
        assert_eq!(result.get(&4), Some(&false));
    }

    #[test]
    fn test_array_predicate() {
        let filter = ArrayPredicate::new(|values| {
            values.last().map(|&v| v > 100.0).unwrap_or(false)
        });
        let data = create_test_data();
        let result = filter.compute(&data).unwrap();

        assert_eq!(result.get(&1), Some(&true)); // 110 > 100
        assert_eq!(result.get(&2), Some(&false)); // 60 < 100
        assert_eq!(result.get(&3), Some(&true)); // 190 > 100
        assert_eq!(result.get(&4), Some(&false)); // 85 < 100
        assert_eq!(result.get(&5), Some(&true)); // 160 > 100
    }

    #[test]
    fn test_latest() {
        let filter = Latest::new(|v| v > 100.0);
        let data = create_test_data();
        let result = filter.compute(&data).unwrap();

        assert_eq!(result.get(&1), Some(&true)); // 110 > 100
        assert_eq!(result.get(&2), Some(&false)); // 60 < 100
        assert_eq!(result.get(&3), Some(&true)); // 190 > 100
    }

    #[test]
    fn test_filter_name() {
        let filter = PercentileFilter::new(0.1, 0.9).unwrap();
        assert_eq!(filter.name(), "PercentileFilter");

        let filter = NullFilter;
        assert_eq!(filter.name(), "NullFilter");
    }
}
