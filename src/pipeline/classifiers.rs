//! Pipeline Classifiers - Asset categorization and labeling
//!
//! Classifiers are Pipeline computations that produce categorical outputs,
//! used to group or label assets.

use crate::error::{Result, ZiplineError};
use std::collections::HashMap;

/// Classifier trait - produces categorical labels for assets
pub trait Classifier: Send + Sync {
    /// Compute classifier for each asset
    /// Returns HashMap<asset_id, label> where label is category/group
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>>;

    /// Get classifier name for debugging
    fn name(&self) -> &str {
        "Classifier"
    }

    /// Get number of categories (if known)
    fn num_categories(&self) -> Option<usize> {
        None
    }
}

/// Quantiles - Classify assets into quantile buckets
///
/// Divides assets into N equal-sized groups based on their values.
/// Example: Quantiles(5) creates quintiles (5 groups)
pub struct Quantiles {
    bins: usize,
    mask: Option<Vec<u64>>, // Optional: only classify these assets
}

impl Quantiles {
    pub fn new(bins: usize) -> Result<Self> {
        if bins < 2 {
            return Err(ZiplineError::PipelineError(
                "bins must be at least 2".to_string(),
            ));
        }

        Ok(Self { bins, mask: None })
    }

    pub fn with_mask(bins: usize, mask: Vec<u64>) -> Result<Self> {
        if bins < 2 {
            return Err(ZiplineError::PipelineError(
                "bins must be at least 2".to_string(),
            ));
        }

        Ok(Self {
            bins,
            mask: Some(mask),
        })
    }
}

impl Classifier for Quantiles {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>> {
        let mut result = HashMap::new();

        // Get latest values for assets (filtered by mask if provided)
        let mut values: Vec<(u64, f64)> = data
            .iter()
            .filter(|(asset_id, _)| {
                if let Some(ref mask) = self.mask {
                    mask.contains(asset_id)
                } else {
                    true
                }
            })
            .filter_map(|(asset_id, vals)| vals.last().map(|&v| (*asset_id, v)))
            .filter(|(_, v)| !v.is_nan())
            .collect();

        if values.is_empty() {
            return Ok(result);
        }

        // Sort by value
        values.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Assign quantile labels (0-indexed)
        let assets_per_bin = (values.len() + self.bins - 1) / self.bins; // Round up

        for (idx, (asset_id, _)) in values.iter().enumerate() {
            let quantile = (idx / assets_per_bin).min(self.bins - 1) as i64;
            result.insert(*asset_id, quantile);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "Quantiles"
    }

    fn num_categories(&self) -> Option<usize> {
        Some(self.bins)
    }
}

/// Relabel - Remap classifier labels to new values
///
/// Transforms one set of labels to another using a mapping.
/// Example: Relabel({0: 10, 1: 20}) maps label 0→10, 1→20
pub struct Relabel {
    mapping: HashMap<i64, i64>,
    missing_value: i64, // Label for unmapped values
}

impl Relabel {
    pub fn new(mapping: HashMap<i64, i64>) -> Self {
        Self {
            mapping,
            missing_value: -1,
        }
    }

    pub fn with_missing_value(mapping: HashMap<i64, i64>, missing_value: i64) -> Self {
        Self {
            mapping,
            missing_value,
        }
    }

    /// Apply relabeling to existing classifier results
    pub fn relabel(&self, labels: &HashMap<u64, i64>) -> HashMap<u64, i64> {
        labels
            .iter()
            .map(|(asset_id, label)| {
                let new_label = *self.mapping.get(label).unwrap_or(&self.missing_value);
                (*asset_id, new_label)
            })
            .collect()
    }
}

impl Classifier for Relabel {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>> {
        // For Relabel to work as a standalone classifier, it needs input labels
        // This is a simplified version that assumes data values are labels
        let labels: HashMap<u64, i64> = data
            .iter()
            .filter_map(|(asset_id, vals)| vals.last().map(|&v| (*asset_id, v as i64)))
            .collect();

        Ok(self.relabel(&labels))
    }

    fn name(&self) -> &str {
        "Relabel"
    }
}

/// Everything - Put all assets in a single group
///
/// Assigns the same label (0) to all assets.
/// Useful for global computations across all assets.
pub struct Everything;

impl Classifier for Everything {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>> {
        let mut result = HashMap::new();

        for asset_id in data.keys() {
            result.insert(*asset_id, 0);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "Everything"
    }

    fn num_categories(&self) -> Option<usize> {
        Some(1)
    }
}

/// CustomClassifier - User-defined classifier with custom logic
///
/// Base for creating custom classifiers with arbitrary categorization logic
pub trait CustomClassifierLogic: Send + Sync {
    fn compute_custom(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>>;
    fn custom_name(&self) -> &str {
        "CustomClassifier"
    }
}

pub struct CustomClassifier<T: CustomClassifierLogic> {
    logic: T,
}

impl<T: CustomClassifierLogic> CustomClassifier<T> {
    pub fn new(logic: T) -> Self {
        Self { logic }
    }
}

impl<T: CustomClassifierLogic> Classifier for CustomClassifier<T> {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>> {
        self.logic.compute_custom(data)
    }

    fn name(&self) -> &str {
        self.logic.custom_name()
    }
}

/// Latest - Classify based on latest value
///
/// Applies a categorization function to the most recent value
pub struct Latest<F>
where
    F: Fn(f64) -> i64 + Send + Sync,
{
    classifier_fn: F,
}

impl<F> Latest<F>
where
    F: Fn(f64) -> i64 + Send + Sync,
{
    pub fn new(classifier_fn: F) -> Self {
        Self { classifier_fn }
    }
}

impl<F> Classifier for Latest<F>
where
    F: Fn(f64) -> i64 + Send + Sync,
{
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            if let Some(&latest_value) = values.last() {
                if !latest_value.is_nan() {
                    let label = (self.classifier_fn)(latest_value);
                    result.insert(*asset_id, label);
                }
            }
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "Latest"
    }
}

/// SimpleClassifier - Classify based on simple threshold rules
///
/// Categorizes assets into groups based on value ranges
pub struct SimpleClassifier {
    thresholds: Vec<f64>, // Sorted thresholds defining boundaries
}

impl SimpleClassifier {
    pub fn new(mut thresholds: Vec<f64>) -> Self {
        thresholds.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Self { thresholds }
    }

    fn classify_value(&self, value: f64) -> i64 {
        for (idx, &threshold) in self.thresholds.iter().enumerate() {
            if value < threshold {
                return idx as i64;
            }
        }
        self.thresholds.len() as i64
    }
}

impl Classifier for SimpleClassifier {
    fn compute(&self, data: &HashMap<u64, Vec<f64>>) -> Result<HashMap<u64, i64>> {
        let mut result = HashMap::new();

        for (asset_id, values) in data {
            if let Some(&latest_value) = values.last() {
                if !latest_value.is_nan() {
                    let label = self.classify_value(latest_value);
                    result.insert(*asset_id, label);
                }
            }
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "SimpleClassifier"
    }

    fn num_categories(&self) -> Option<usize> {
        Some(self.thresholds.len() + 1)
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
    fn test_quantiles() {
        let classifier = Quantiles::new(3).unwrap(); // Tertiles
        let data = create_test_data();
        let result = classifier.compute(&data).unwrap();

        // Should have 3 quantiles (0, 1, 2)
        assert!(result.values().all(|&v| v >= 0 && v < 3));

        // Check that we have all assets classified
        assert_eq!(result.len(), 5);

        // Verify quantile ordering (lowest should be 0, highest should be 2)
        assert_eq!(result.get(&2), Some(&0)); // 60 is lowest
        assert_eq!(result.get(&3), Some(&2)); // 190 is highest
    }

    #[test]
    fn test_quantiles_validation() {
        assert!(Quantiles::new(0).is_err());
        assert!(Quantiles::new(1).is_err());
        assert!(Quantiles::new(2).is_ok());
    }

    #[test]
    fn test_quantiles_with_mask() {
        let classifier = Quantiles::with_mask(2, vec![1, 2, 3]).unwrap();
        let data = create_test_data();
        let result = classifier.compute(&data).unwrap();

        // Should only classify assets 1, 2, 3
        assert_eq!(result.len(), 3);
        assert!(result.contains_key(&1));
        assert!(result.contains_key(&2));
        assert!(result.contains_key(&3));
        assert!(!result.contains_key(&4));
        assert!(!result.contains_key(&5));
    }

    #[test]
    fn test_relabel() {
        let mut mapping = HashMap::new();
        mapping.insert(0, 10);
        mapping.insert(1, 20);
        mapping.insert(2, 30);

        let relabel = Relabel::new(mapping);

        let mut input_labels = HashMap::new();
        input_labels.insert(1, 0);
        input_labels.insert(2, 1);
        input_labels.insert(3, 2);
        input_labels.insert(4, 99); // Unmapped

        let result = relabel.relabel(&input_labels);

        assert_eq!(result.get(&1), Some(&10));
        assert_eq!(result.get(&2), Some(&20));
        assert_eq!(result.get(&3), Some(&30));
        assert_eq!(result.get(&4), Some(&-1)); // Missing value
    }

    #[test]
    fn test_relabel_custom_missing_value() {
        let mut mapping = HashMap::new();
        mapping.insert(0, 100);

        let relabel = Relabel::with_missing_value(mapping, 999);

        let mut input_labels = HashMap::new();
        input_labels.insert(1, 0);
        input_labels.insert(2, 5); // Unmapped

        let result = relabel.relabel(&input_labels);

        assert_eq!(result.get(&1), Some(&100));
        assert_eq!(result.get(&2), Some(&999)); // Custom missing value
    }

    #[test]
    fn test_everything() {
        let classifier = Everything;
        let data = create_test_data();
        let result = classifier.compute(&data).unwrap();

        // All assets should be in group 0
        assert_eq!(result.len(), 5);
        for label in result.values() {
            assert_eq!(*label, 0);
        }

        assert_eq!(classifier.num_categories(), Some(1));
    }

    #[test]
    fn test_latest() {
        let classifier = Latest::new(|value| if value > 100.0 { 1 } else { 0 });
        let data = create_test_data();
        let result = classifier.compute(&data).unwrap();

        assert_eq!(result.get(&1), Some(&1)); // 110 > 100
        assert_eq!(result.get(&2), Some(&0)); // 60 < 100
        assert_eq!(result.get(&3), Some(&1)); // 190 > 100
        assert_eq!(result.get(&4), Some(&0)); // 85 < 100
        assert_eq!(result.get(&5), Some(&1)); // 160 > 100
    }

    #[test]
    fn test_simple_classifier() {
        let classifier = SimpleClassifier::new(vec![50.0, 100.0, 150.0]);
        let data = create_test_data();
        let result = classifier.compute(&data).unwrap();

        // 4 categories: <50, 50-100, 100-150, >150
        assert_eq!(classifier.num_categories(), Some(4));

        assert_eq!(result.get(&2), Some(&1)); // 60 in [50, 100)
        assert_eq!(result.get(&4), Some(&1)); // 85 in [50, 100)
        assert_eq!(result.get(&1), Some(&2)); // 110 in [100, 150)
        assert_eq!(result.get(&5), Some(&3)); // 160 in [150, inf)
        assert_eq!(result.get(&3), Some(&3)); // 190 in [150, inf)
    }

    #[test]
    fn test_classifier_name() {
        let classifier = Quantiles::new(5).unwrap();
        assert_eq!(classifier.name(), "Quantiles");

        let classifier = Everything;
        assert_eq!(classifier.name(), "Everything");

        let classifier = Latest::new(|v| (v / 50.0) as i64);
        assert_eq!(classifier.name(), "Latest");
    }

    #[test]
    fn test_quantiles_empty_data() {
        let classifier = Quantiles::new(3).unwrap();
        let data = HashMap::new();
        let result = classifier.compute(&data).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_quantiles_with_nan() {
        let classifier = Quantiles::new(2).unwrap();
        let mut data = HashMap::new();
        data.insert(1, vec![100.0]);
        data.insert(2, vec![f64::NAN]);
        data.insert(3, vec![50.0]);

        let result = classifier.compute(&data).unwrap();

        // NaN should be filtered out
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&1));
        assert!(result.contains_key(&3));
        assert!(!result.contains_key(&2));
    }
}
