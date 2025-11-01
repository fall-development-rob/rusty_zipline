//! Composite factors - combining and transforming factors

use super::engine::{Factor, FactorOutput, PipelineContext};
use crate::error::Result;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;

/// Add two factors
#[derive(Clone)]
pub struct AddFactors {
    name: String,
    factor_a: String,
    factor_b: String,
}

impl AddFactors {
    pub fn new(factor_a: String, factor_b: String) -> Self {
        let name = format!("({} + {})", factor_a, factor_b);
        Self {
            name,
            factor_a,
            factor_b,
        }
    }
}

impl Factor for AddFactors {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let a_values = context.get_cached(&self.factor_a).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_a))
        })?;

        let b_values = context.get_cached(&self.factor_b).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_b))
        })?;

        let mut output = HashMap::new();
        for (&asset_id, &a_val) in a_values {
            if let Some(&b_val) = b_values.get(&asset_id) {
                output.insert(asset_id, a_val + b_val);
            }
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.factor_a.clone(), self.factor_b.clone()]
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

/// Subtract two factors
#[derive(Clone)]
pub struct SubtractFactors {
    name: String,
    factor_a: String,
    factor_b: String,
}

impl SubtractFactors {
    pub fn new(factor_a: String, factor_b: String) -> Self {
        let name = format!("({} - {})", factor_a, factor_b);
        Self {
            name,
            factor_a,
            factor_b,
        }
    }
}

impl Factor for SubtractFactors {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let a_values = context.get_cached(&self.factor_a).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_a))
        })?;

        let b_values = context.get_cached(&self.factor_b).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_b))
        })?;

        let mut output = HashMap::new();
        for (&asset_id, &a_val) in a_values {
            if let Some(&b_val) = b_values.get(&asset_id) {
                output.insert(asset_id, a_val - b_val);
            }
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.factor_a.clone(), self.factor_b.clone()]
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

/// Multiply two factors
#[derive(Clone)]
pub struct MultiplyFactors {
    name: String,
    factor_a: String,
    factor_b: String,
}

impl MultiplyFactors {
    pub fn new(factor_a: String, factor_b: String) -> Self {
        let name = format!("({} * {})", factor_a, factor_b);
        Self {
            name,
            factor_a,
            factor_b,
        }
    }
}

impl Factor for MultiplyFactors {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let a_values = context.get_cached(&self.factor_a).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_a))
        })?;

        let b_values = context.get_cached(&self.factor_b).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_b))
        })?;

        let mut output = HashMap::new();
        for (&asset_id, &a_val) in a_values {
            if let Some(&b_val) = b_values.get(&asset_id) {
                output.insert(asset_id, a_val * b_val);
            }
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.factor_a.clone(), self.factor_b.clone()]
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

/// Divide two factors
#[derive(Clone)]
pub struct DivideFactors {
    name: String,
    factor_a: String,
    factor_b: String,
}

impl DivideFactors {
    pub fn new(factor_a: String, factor_b: String) -> Self {
        let name = format!("({} / {})", factor_a, factor_b);
        Self {
            name,
            factor_a,
            factor_b,
        }
    }
}

impl Factor for DivideFactors {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let a_values = context.get_cached(&self.factor_a).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_a))
        })?;

        let b_values = context.get_cached(&self.factor_b).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor_b))
        })?;

        let mut output = HashMap::new();
        for (&asset_id, &a_val) in a_values {
            if let Some(&b_val) = b_values.get(&asset_id) {
                if b_val.abs() > f64::EPSILON {
                    output.insert(asset_id, a_val / b_val);
                }
            }
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.factor_a.clone(), self.factor_b.clone()]
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

/// Rank factor values across assets
#[derive(Clone)]
pub struct RankFactor {
    name: String,
    factor: String,
    ascending: bool,
}

impl RankFactor {
    pub fn new(factor: String, ascending: bool) -> Self {
        let name = format!("rank({})", factor);
        Self {
            name,
            factor,
            ascending,
        }
    }
}

impl Factor for RankFactor {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let values = context.get_cached(&self.factor).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor))
        })?;

        // Sort assets by value
        let mut sorted: Vec<(u64, f64)> = values.iter().map(|(&id, &val)| (id, val)).collect();

        if self.ascending {
            sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        }

        // Assign ranks
        let mut output = HashMap::new();
        for (rank, (asset_id, _)) in sorted.iter().enumerate() {
            output.insert(*asset_id, rank as f64);
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.factor.clone()]
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

/// Z-score normalization
#[derive(Clone)]
pub struct ZScoreFactor {
    name: String,
    factor: String,
}

impl ZScoreFactor {
    pub fn new(factor: String) -> Self {
        let name = format!("zscore({})", factor);
        Self { name, factor }
    }
}

impl Factor for ZScoreFactor {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let values = context.get_cached(&self.factor).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor))
        })?;

        // Calculate mean and std dev
        let vals: Vec<f64> = values.values().copied().collect();
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        let variance = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
        let std_dev = variance.sqrt();

        // Calculate z-scores
        let mut output = HashMap::new();
        for (&asset_id, &val) in values {
            let z_score = if std_dev > f64::EPSILON {
                (val - mean) / std_dev
            } else {
                0.0
            };
            output.insert(asset_id, z_score);
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.factor.clone()]
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

/// Top N filter based on factor values
#[derive(Clone)]
pub struct TopNFilter {
    name: String,
    factor: String,
    n: usize,
}

impl TopNFilter {
    pub fn new(factor: String, n: usize) -> Self {
        let name = format!("top_{}({})", n, factor);
        Self { name, factor, n }
    }
}

impl super::engine::Filter for TopNFilter {
    fn evaluate(
        &self,
        _timestamp: DateTime<Utc>,
        context: &PipelineContext,
    ) -> Result<HashMap<u64, bool>> {
        let values = context.get_cached(&self.factor).ok_or_else(|| {
            crate::error::ZiplineError::PipelineError(format!("Factor {} not found", self.factor))
        })?;

        // Sort by value descending
        let mut sorted: Vec<(u64, f64)> = values.iter().map(|(&id, &val)| (id, val)).collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top N
        let top_n: std::collections::HashSet<u64> = sorted.iter().take(self.n).map(|(id, _)| *id).collect();

        // Create output
        let mut output = HashMap::new();
        for &asset_id in values.keys() {
            output.insert(asset_id, top_n.contains(&asset_id));
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn super::engine::Filter> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_factors() {
        let factor = AddFactors::new("a".to_string(), "b".to_string());
        assert_eq!(factor.name(), "(a + b)");
        assert_eq!(factor.dependencies(), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_rank_factor() {
        let factor = RankFactor::new("test".to_string(), true);
        assert_eq!(factor.name(), "rank(test)");
        assert_eq!(factor.dependencies(), vec!["test".to_string()]);
    }

    #[test]
    fn test_zscore_factor() {
        let factor = ZScoreFactor::new("test".to_string());
        assert_eq!(factor.name(), "zscore(test)");
    }
}
