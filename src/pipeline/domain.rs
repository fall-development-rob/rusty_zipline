//! Pipeline Domain - Asset universe definitions
//!
//! Domains define the set of assets that are in scope for a pipeline.

use crate::asset::Asset;
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

/// Domain identifier
pub type DomainId = u64;

/// Trait for defining asset universes
pub trait Domain: Send + Sync + fmt::Debug {
    /// Unique identifier for this domain
    fn id(&self) -> DomainId;

    /// Human-readable name
    fn name(&self) -> &str;

    /// Get all assets in this domain at a specific date
    fn assets_at(&self, dt: DateTime<Utc>) -> Result<Vec<Asset>>;

    /// Check if an asset is in this domain at a specific date
    fn contains(&self, asset: &Asset, dt: DateTime<Utc>) -> bool {
        self.assets_at(dt)
            .ok()
            .map(|assets| assets.iter().any(|a| a.id == asset.id))
            .unwrap_or(false)
    }

    /// Start date for this domain
    fn start_date(&self) -> Option<DateTime<Utc>> {
        None
    }

    /// End date for this domain
    fn end_date(&self) -> Option<DateTime<Utc>> {
        None
    }

    /// Country code for this domain (e.g., "US", "GB")
    fn country_code(&self) -> Option<&str> {
        None
    }

    /// Clone as Arc
    fn clone_arc(&self) -> Arc<dyn Domain>;
}

/// Universe of all available assets
#[derive(Debug, Clone)]
pub struct EquityUniverse {
    id: DomainId,
    name: String,
    country_code: Option<String>,
    assets: Vec<Asset>,
}

impl EquityUniverse {
    pub fn new(
        id: DomainId,
        name: impl Into<String>,
        country_code: Option<String>,
        assets: Vec<Asset>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            country_code,
            assets,
        }
    }

    /// Create US equity universe
    pub fn us(id: DomainId, assets: Vec<Asset>) -> Self {
        Self::new(id, "US_EQUITIES", Some("US".to_string()), assets)
    }

    /// Create generic equity universe
    pub fn generic(id: DomainId, assets: Vec<Asset>) -> Self {
        Self::new(id, "EQUITIES", None, assets)
    }
}

impl Domain for EquityUniverse {
    fn id(&self) -> DomainId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn assets_at(&self, _dt: DateTime<Utc>) -> Result<Vec<Asset>> {
        Ok(self.assets.clone())
    }

    fn country_code(&self) -> Option<&str> {
        self.country_code.as_deref()
    }

    fn clone_arc(&self) -> Arc<dyn Domain> {
        Arc::new(self.clone())
    }
}

/// Static domain with fixed asset list
#[derive(Debug, Clone)]
pub struct StaticDomain {
    id: DomainId,
    name: String,
    assets: Vec<Asset>,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
}

impl StaticDomain {
    pub fn new(
        id: DomainId,
        name: impl Into<String>,
        assets: Vec<Asset>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            assets,
            start_date: None,
            end_date: None,
        }
    }

    pub fn with_dates(
        mut self,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Self {
        self.start_date = start;
        self.end_date = end;
        self
    }
}

impl Domain for StaticDomain {
    fn id(&self) -> DomainId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn assets_at(&self, dt: DateTime<Utc>) -> Result<Vec<Asset>> {
        // Check date bounds
        if let Some(start) = self.start_date {
            if dt < start {
                return Ok(Vec::new());
            }
        }
        if let Some(end) = self.end_date {
            if dt > end {
                return Ok(Vec::new());
            }
        }
        Ok(self.assets.clone())
    }

    fn start_date(&self) -> Option<DateTime<Utc>> {
        self.start_date
    }

    fn end_date(&self) -> Option<DateTime<Utc>> {
        self.end_date
    }

    fn clone_arc(&self) -> Arc<dyn Domain> {
        Arc::new(self.clone())
    }
}

/// Domain that filters another domain based on criteria
#[derive(Debug, Clone)]
pub struct FilteredDomain {
    id: DomainId,
    name: String,
    parent: Arc<dyn Domain>,
    allowed_asset_ids: HashSet<u64>,
}

impl FilteredDomain {
    pub fn new(
        id: DomainId,
        name: impl Into<String>,
        parent: Arc<dyn Domain>,
        allowed_asset_ids: HashSet<u64>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            parent,
            allowed_asset_ids,
        }
    }
}

impl Domain for FilteredDomain {
    fn id(&self) -> DomainId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn assets_at(&self, dt: DateTime<Utc>) -> Result<Vec<Asset>> {
        let parent_assets = self.parent.assets_at(dt)?;
        Ok(parent_assets
            .into_iter()
            .filter(|a| self.allowed_asset_ids.contains(&a.id))
            .collect())
    }

    fn start_date(&self) -> Option<DateTime<Utc>> {
        self.parent.start_date()
    }

    fn end_date(&self) -> Option<DateTime<Utc>> {
        self.parent.end_date()
    }

    fn country_code(&self) -> Option<&str> {
        self.parent.country_code()
    }

    fn clone_arc(&self) -> Arc<dyn Domain> {
        Arc::new(self.clone())
    }
}

/// Domain representing the intersection of multiple domains
#[derive(Debug, Clone)]
pub struct IntersectionDomain {
    id: DomainId,
    name: String,
    domains: Vec<Arc<dyn Domain>>,
}

impl IntersectionDomain {
    pub fn new(
        id: DomainId,
        name: impl Into<String>,
        domains: Vec<Arc<dyn Domain>>,
    ) -> Result<Self> {
        if domains.is_empty() {
            return Err(ZiplineError::InvalidConfiguration(
                "IntersectionDomain requires at least one domain".to_string(),
            ));
        }
        Ok(Self {
            id,
            name: name.into(),
            domains,
        })
    }
}

impl Domain for IntersectionDomain {
    fn id(&self) -> DomainId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn assets_at(&self, dt: DateTime<Utc>) -> Result<Vec<Asset>> {
        if self.domains.is_empty() {
            return Ok(Vec::new());
        }

        // Get assets from first domain
        let mut result_set: HashSet<u64> = self.domains[0]
            .assets_at(dt)?
            .iter()
            .map(|a| a.id)
            .collect();

        // Intersect with each subsequent domain
        for domain in &self.domains[1..] {
            let domain_assets: HashSet<u64> =
                domain.assets_at(dt)?.iter().map(|a| a.id).collect();
            result_set.retain(|id| domain_assets.contains(id));
        }

        // Convert back to Asset vec
        let all_assets = self.domains[0].assets_at(dt)?;
        Ok(all_assets
            .into_iter()
            .filter(|a| result_set.contains(&a.id))
            .collect())
    }

    fn clone_arc(&self) -> Arc<dyn Domain> {
        Arc::new(self.clone())
    }
}

/// Domain representing the union of multiple domains
#[derive(Debug, Clone)]
pub struct UnionDomain {
    id: DomainId,
    name: String,
    domains: Vec<Arc<dyn Domain>>,
}

impl UnionDomain {
    pub fn new(
        id: DomainId,
        name: impl Into<String>,
        domains: Vec<Arc<dyn Domain>>,
    ) -> Result<Self> {
        if domains.is_empty() {
            return Err(ZiplineError::InvalidConfiguration(
                "UnionDomain requires at least one domain".to_string(),
            ));
        }
        Ok(Self {
            id,
            name: name.into(),
            domains,
        })
    }
}

impl Domain for UnionDomain {
    fn id(&self) -> DomainId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn assets_at(&self, dt: DateTime<Utc>) -> Result<Vec<Asset>> {
        let mut result_set: HashSet<u64> = HashSet::new();
        let mut all_assets: Vec<Asset> = Vec::new();

        for domain in &self.domains {
            for asset in domain.assets_at(dt)? {
                if result_set.insert(asset.id) {
                    all_assets.push(asset);
                }
            }
        }

        Ok(all_assets)
    }

    fn clone_arc(&self) -> Arc<dyn Domain> {
        Arc::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_asset(id: u64, symbol: &str) -> Asset {
        Asset {
            id,
            symbol: symbol.to_string(),
            exchange: "NYSE".to_string(),
            asset_type: crate::asset::AssetType::Equity,
        }
    }

    #[test]
    fn test_equity_universe() {
        let assets = vec![
            create_test_asset(1, "AAPL"),
            create_test_asset(2, "GOOGL"),
            create_test_asset(3, "MSFT"),
        ];

        let domain = EquityUniverse::us(1, assets.clone());
        assert_eq!(domain.id(), 1);
        assert_eq!(domain.name(), "US_EQUITIES");
        assert_eq!(domain.country_code(), Some("US"));

        let dt = Utc::now();
        let result = domain.assets_at(dt).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_static_domain() {
        let assets = vec![create_test_asset(1, "AAPL"), create_test_asset(2, "GOOGL")];

        let domain = StaticDomain::new(1, "TECH_STOCKS", assets);

        let dt = Utc::now();
        let result = domain.assets_at(dt).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_static_domain_with_dates() {
        let assets = vec![create_test_asset(1, "AAPL")];
        let start = Utc::now();
        let end = start + chrono::Duration::days(365);

        let domain =
            StaticDomain::new(1, "TIME_BOUND", assets).with_dates(Some(start), Some(end));

        // Before start date
        let before_start = start - chrono::Duration::days(1);
        assert_eq!(domain.assets_at(before_start).unwrap().len(), 0);

        // Within range
        let within = start + chrono::Duration::days(30);
        assert_eq!(domain.assets_at(within).unwrap().len(), 1);

        // After end date
        let after_end = end + chrono::Duration::days(1);
        assert_eq!(domain.assets_at(after_end).unwrap().len(), 0);
    }

    #[test]
    fn test_filtered_domain() {
        let assets = vec![
            create_test_asset(1, "AAPL"),
            create_test_asset(2, "GOOGL"),
            create_test_asset(3, "MSFT"),
        ];
        let parent = Arc::new(EquityUniverse::generic(1, assets));

        let mut allowed = HashSet::new();
        allowed.insert(1);
        allowed.insert(3);

        let filtered = FilteredDomain::new(2, "FILTERED", parent, allowed);

        let dt = Utc::now();
        let result = filtered.assets_at(dt).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|a| a.symbol == "AAPL"));
        assert!(result.iter().any(|a| a.symbol == "MSFT"));
        assert!(!result.iter().any(|a| a.symbol == "GOOGL"));
    }

    #[test]
    fn test_intersection_domain() {
        let assets1 = vec![
            create_test_asset(1, "AAPL"),
            create_test_asset(2, "GOOGL"),
            create_test_asset(3, "MSFT"),
        ];
        let assets2 = vec![create_test_asset(2, "GOOGL"), create_test_asset(3, "MSFT")];

        let domain1 = Arc::new(StaticDomain::new(1, "D1", assets1));
        let domain2 = Arc::new(StaticDomain::new(2, "D2", assets2));

        let intersection =
            IntersectionDomain::new(3, "INTERSECTION", vec![domain1, domain2]).unwrap();

        let dt = Utc::now();
        let result = intersection.assets_at(dt).unwrap();
        assert_eq!(result.len(), 2); // GOOGL and MSFT
    }

    #[test]
    fn test_union_domain() {
        let assets1 = vec![create_test_asset(1, "AAPL"), create_test_asset(2, "GOOGL")];
        let assets2 = vec![create_test_asset(3, "MSFT"), create_test_asset(4, "AMZN")];

        let domain1 = Arc::new(StaticDomain::new(1, "D1", assets1));
        let domain2 = Arc::new(StaticDomain::new(2, "D2", assets2));

        let union = UnionDomain::new(3, "UNION", vec![domain1, domain2]).unwrap();

        let dt = Utc::now();
        let result = union.assets_at(dt).unwrap();
        assert_eq!(result.len(), 4); // All assets
    }

    #[test]
    fn test_domain_contains() {
        let assets = vec![create_test_asset(1, "AAPL"), create_test_asset(2, "GOOGL")];
        let domain = StaticDomain::new(1, "TEST", assets);

        let dt = Utc::now();
        let aapl = create_test_asset(1, "AAPL");
        let msft = create_test_asset(3, "MSFT");

        assert!(domain.contains(&aapl, dt));
        assert!(!domain.contains(&msft, dt));
    }
}
