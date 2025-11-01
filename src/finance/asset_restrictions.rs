//! Asset restrictions - control which assets can be traded
//!
//! Restrictions provide a way to prevent trading of certain assets
//! based on various criteria (historical, regulatory, etc.)

use crate::asset::Asset;
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use std::collections::HashSet;

/// Restriction reason enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestrictionReason {
    /// Asset is on a restricted list
    RestrictedList,
    /// Regulatory restriction
    Regulatory,
    /// Historical restriction (e.g., for backtesting accuracy)
    Historical,
    /// Insufficient liquidity
    Liquidity,
    /// Corporate action in progress
    CorporateAction,
    /// Custom restriction
    Custom,
}

impl RestrictionReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::RestrictedList => "restricted_list",
            Self::Regulatory => "regulatory",
            Self::Historical => "historical",
            Self::Liquidity => "liquidity",
            Self::CorporateAction => "corporate_action",
            Self::Custom => "custom",
        }
    }
}

/// Restrictions trait - defines which assets can be traded
pub trait Restrictions: Send + Sync {
    /// Check if an asset is restricted for trading at a given time
    ///
    /// Returns Ok(()) if asset can be traded, Err if restricted
    fn is_restricted(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<()>;

    /// Get restriction name
    fn name(&self) -> &str {
        "Restrictions"
    }
}

/// NoRestrictions - Allow all assets to be traded
pub struct NoRestrictions;

impl Restrictions for NoRestrictions {
    fn is_restricted(&self, _asset: &Asset, _dt: DateTime<Utc>) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "NoRestrictions"
    }
}

/// StaticRestrictions - Fixed set of restricted assets
///
/// Maintains a static list of asset IDs that cannot be traded
pub struct StaticRestrictions {
    /// Set of restricted asset IDs
    restricted_sids: HashSet<u64>,
    /// Reason for restriction
    reason: RestrictionReason,
}

impl StaticRestrictions {
    /// Create new static restrictions
    pub fn new(restricted_sids: HashSet<u64>, reason: RestrictionReason) -> Self {
        Self {
            restricted_sids,
            reason,
        }
    }

    /// Create from vector of SIDs
    pub fn from_sids(sids: Vec<u64>, reason: RestrictionReason) -> Self {
        Self::new(sids.into_iter().collect(), reason)
    }

    /// Add a restricted asset
    pub fn add_restriction(&mut self, sid: u64) {
        self.restricted_sids.insert(sid);
    }

    /// Remove a restriction
    pub fn remove_restriction(&mut self, sid: u64) {
        self.restricted_sids.remove(&sid);
    }

    /// Check if asset is in restricted set
    pub fn contains(&self, sid: u64) -> bool {
        self.restricted_sids.contains(&sid)
    }

    /// Get number of restricted assets
    pub fn count(&self) -> usize {
        self.restricted_sids.len()
    }
}

impl Restrictions for StaticRestrictions {
    fn is_restricted(&self, asset: &Asset, _dt: DateTime<Utc>) -> Result<()> {
        if self.restricted_sids.contains(&asset.id) {
            return Err(ZiplineError::RestrictedAsset {
                asset_id: asset.id,
                reason: self.reason.as_str().to_string(),
            });
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "StaticRestrictions"
    }
}

/// HistoricalRestrictions - Time-based restrictions
///
/// Restricts assets based on their start/end dates to ensure
/// backtesting accuracy (don't trade before asset existed)
pub struct HistoricalRestrictions {
    /// Allow trading of delisted assets
    allow_delisted: bool,
}

impl HistoricalRestrictions {
    pub fn new() -> Self {
        Self {
            allow_delisted: false,
        }
    }

    pub fn allow_delisted(mut self) -> Self {
        self.allow_delisted = true;
        self
    }
}

impl Default for HistoricalRestrictions {
    fn default() -> Self {
        Self::new()
    }
}

impl Restrictions for HistoricalRestrictions {
    fn is_restricted(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<()> {
        // Check if asset exists yet
        if dt < asset.start_date {
            return Err(ZiplineError::TradingBeforeStart {
                asset_id: asset.id,
                attempted: dt,
                start_date: asset.start_date,
            });
        }

        // Check if asset has been delisted
        if !self.allow_delisted {
            if let Some(end_date) = asset.end_date {
                if dt >= end_date {
                    return Err(ZiplineError::RestrictedAsset {
                        asset_id: asset.id,
                        reason: "delisted".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "HistoricalRestrictions"
    }
}

/// SecurityListRestrictions - Dynamic restriction list
///
/// Allows restricting assets based on membership in security lists
/// (e.g., universe definitions, sector filters)
pub struct SecurityListRestrictions {
    /// Allowed security IDs (if empty, all are allowed except those in deny_list)
    allow_list: Option<HashSet<u64>>,
    /// Denied security IDs
    deny_list: HashSet<u64>,
}

impl SecurityListRestrictions {
    pub fn new() -> Self {
        Self {
            allow_list: None,
            deny_list: HashSet::new(),
        }
    }

    /// Set explicit allow list (only these assets can trade)
    pub fn with_allow_list(mut self, sids: Vec<u64>) -> Self {
        self.allow_list = Some(sids.into_iter().collect());
        self
    }

    /// Add to deny list (these assets cannot trade)
    pub fn with_deny_list(mut self, sids: Vec<u64>) -> Self {
        self.deny_list = sids.into_iter().collect();
        self
    }

    /// Add denied asset
    pub fn deny(&mut self, sid: u64) {
        self.deny_list.insert(sid);
    }

    /// Remove from deny list
    pub fn allow(&mut self, sid: u64) {
        self.deny_list.remove(&sid);
    }
}

impl Default for SecurityListRestrictions {
    fn default() -> Self {
        Self::new()
    }
}

impl Restrictions for SecurityListRestrictions {
    fn is_restricted(&self, asset: &Asset, _dt: DateTime<Utc>) -> Result<()> {
        // Check deny list first
        if self.deny_list.contains(&asset.id) {
            return Err(ZiplineError::RestrictedAsset {
                asset_id: asset.id,
                reason: "deny_list".to_string(),
            });
        }

        // Check allow list if present
        if let Some(ref allow_list) = self.allow_list {
            if !allow_list.contains(&asset.id) {
                return Err(ZiplineError::RestrictedAsset {
                    asset_id: asset.id,
                    reason: "not_in_allow_list".to_string(),
                });
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "SecurityListRestrictions"
    }
}

/// CompositeRestrictions - Combine multiple restriction policies
///
/// Applies multiple restrictions in sequence (asset must pass all)
pub struct CompositeRestrictions {
    restrictions: Vec<Box<dyn Restrictions>>,
}

impl CompositeRestrictions {
    pub fn new() -> Self {
        Self {
            restrictions: Vec::new(),
        }
    }

    /// Add a restriction policy
    pub fn add(mut self, restriction: Box<dyn Restrictions>) -> Self {
        self.restrictions.push(restriction);
        self
    }

    /// Get number of restriction policies
    pub fn count(&self) -> usize {
        self.restrictions.len()
    }
}

impl Default for CompositeRestrictions {
    fn default() -> Self {
        Self::new()
    }
}

impl Restrictions for CompositeRestrictions {
    fn is_restricted(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<()> {
        for restriction in &self.restrictions {
            restriction.is_restricted(asset, dt)?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "CompositeRestrictions"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_restrictions() {
        let restrictions = NoRestrictions;
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string());

        assert!(restrictions.is_restricted(&asset, Utc::now()).is_ok());
        assert_eq!(restrictions.name(), "NoRestrictions");
    }

    #[test]
    fn test_static_restrictions() {
        let mut restrictions =
            StaticRestrictions::from_sids(vec![1, 2, 3], RestrictionReason::RestrictedList);

        let restricted_asset = Asset::equity(1, "BAD".to_string(), "NYSE".to_string());
        let allowed_asset = Asset::equity(100, "GOOD".to_string(), "NYSE".to_string());

        assert!(restrictions
            .is_restricted(&restricted_asset, Utc::now())
            .is_err());
        assert!(restrictions
            .is_restricted(&allowed_asset, Utc::now())
            .is_ok());
        assert_eq!(restrictions.count(), 3);

        // Test add/remove
        restrictions.add_restriction(100);
        assert!(restrictions
            .is_restricted(&allowed_asset, Utc::now())
            .is_err());

        restrictions.remove_restriction(100);
        assert!(restrictions
            .is_restricted(&allowed_asset, Utc::now())
            .is_ok());
    }

    #[test]
    fn test_historical_restrictions() {
        let restrictions = HistoricalRestrictions::new();

        let mut asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string());
        asset.start_date = Utc::now();

        let before_start = asset.start_date - chrono::Duration::days(1);
        let after_start = asset.start_date + chrono::Duration::days(1);

        assert!(restrictions.is_restricted(&asset, before_start).is_err());
        assert!(restrictions.is_restricted(&asset, after_start).is_ok());
    }

    #[test]
    fn test_security_list_restrictions() {
        let restrictions = SecurityListRestrictions::new()
            .with_allow_list(vec![1, 2, 3])
            .with_deny_list(vec![2]);

        let allowed = Asset::equity(1, "ALLOWED".to_string(), "NYSE".to_string());
        let denied = Asset::equity(2, "DENIED".to_string(), "NYSE".to_string());
        let not_in_list = Asset::equity(100, "OTHER".to_string(), "NYSE".to_string());

        assert!(restrictions.is_restricted(&allowed, Utc::now()).is_ok());
        assert!(restrictions.is_restricted(&denied, Utc::now()).is_err());
        assert!(restrictions.is_restricted(&not_in_list, Utc::now()).is_err());
    }

    #[test]
    fn test_composite_restrictions() {
        let mut composite = CompositeRestrictions::new();

        composite = composite.add(Box::new(HistoricalRestrictions::new()));
        composite = composite.add(Box::new(StaticRestrictions::from_sids(
            vec![99],
            RestrictionReason::Regulatory,
        )));

        assert_eq!(composite.count(), 2);

        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string());
        let restricted = Asset::equity(99, "RESTRICTED".to_string(), "NYSE".to_string());

        assert!(composite.is_restricted(&asset, Utc::now()).is_ok());
        assert!(composite.is_restricted(&restricted, Utc::now()).is_err());
    }

    #[test]
    fn test_restriction_reason() {
        assert_eq!(
            RestrictionReason::RestrictedList.as_str(),
            "restricted_list"
        );
        assert_eq!(RestrictionReason::Regulatory.as_str(), "regulatory");
        assert_eq!(RestrictionReason::Historical.as_str(), "historical");
    }
}
