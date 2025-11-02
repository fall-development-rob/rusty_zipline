//! Asset database management with SQLite

use crate::asset::{Asset, AssetType};
use crate::error::{Result, ZiplineError};
use chrono::NaiveDate;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// Extended asset metadata for database storage
#[derive(Debug, Clone)]
pub struct AssetMetadata {
    pub id: u64,
    pub symbol: String,
    pub exchange: String,
    pub asset_type: AssetType,
    pub name: Option<String>,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub first_traded: Option<NaiveDate>,
    pub auto_close_date: Option<NaiveDate>,
    pub tick_size: Option<f64>,
}

impl AssetMetadata {
    /// Convert to simplified Asset struct
    pub fn to_asset(&self) -> Asset {
        // Use start_date from metadata, or default to a sensible fallback
        let start_date = self.start_date.unwrap_or_else(|| NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
        let mut asset = Asset::new(self.id, self.symbol.clone(), self.exchange.clone(), self.asset_type, start_date);
        if let Some(name) = &self.name {
            asset = asset.with_name(name.clone());
        }
        asset
    }
}

/// Asset database with SQLite backend
pub struct AssetDB {
    conn: Connection,
}

impl AssetDB {
    /// Create or open database at path
    pub fn new(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .map_err(|e| ZiplineError::DataError(format!("Failed to open database: {}", e)))?;

        let mut db = Self { conn };
        db.create_tables()?;
        Ok(db)
    }

    /// Create in-memory database (for testing)
    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| ZiplineError::DataError(format!("Failed to create in-memory database: {}", e)))?;

        let mut db = Self { conn };
        db.create_tables()?;
        Ok(db)
    }

    /// Create database tables
    pub fn create_tables(&mut self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                asset_type INTEGER NOT NULL,
                name TEXT,
                start_date TEXT,
                end_date TEXT,
                first_traded TEXT,
                auto_close_date TEXT,
                tick_size REAL
            )",
            [],
        ).map_err(|e| ZiplineError::DataError(format!("Failed to create assets table: {}", e)))?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol ON assets(symbol)",
            [],
        ).map_err(|e| ZiplineError::DataError(format!("Failed to create symbol index: {}", e)))?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exchange ON assets(exchange)",
            [],
        ).map_err(|e| ZiplineError::DataError(format!("Failed to create exchange index: {}", e)))?;

        Ok(())
    }

    /// Insert a new asset
    pub fn insert_asset(&mut self, asset: &AssetMetadata) -> Result<u64> {
        let asset_type_int = asset.asset_type as i32;

        self.conn.execute(
            "INSERT INTO assets (id, symbol, exchange, asset_type, name, start_date, end_date, first_traded, auto_close_date, tick_size)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                asset.id as i64,
                &asset.symbol,
                &asset.exchange,
                asset_type_int,
                &asset.name,
                asset.start_date.map(|d| d.to_string()),
                asset.end_date.map(|d| d.to_string()),
                asset.first_traded.map(|d| d.to_string()),
                asset.auto_close_date.map(|d| d.to_string()),
                asset.tick_size,
            ],
        ).map_err(|e| ZiplineError::DataError(format!("Failed to insert asset: {}", e)))?;

        Ok(asset.id)
    }

    /// Get asset by ID
    pub fn get_asset(&self, asset_id: u64) -> Result<Option<AssetMetadata>> {
        let result = self.conn.query_row(
            "SELECT id, symbol, exchange, asset_type, name, start_date, end_date, first_traded, auto_close_date, tick_size
             FROM assets WHERE id = ?1",
            params![asset_id as i64],
            |row| {
                Ok(AssetMetadata {
                    id: row.get::<_, i64>(0)? as u64,
                    symbol: row.get(1)?,
                    exchange: row.get(2)?,
                    asset_type: Self::int_to_asset_type(row.get(3)?),
                    name: row.get(4)?,
                    start_date: row.get::<_, Option<String>>(5)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                    end_date: row.get::<_, Option<String>>(6)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                    first_traded: row.get::<_, Option<String>>(7)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                    auto_close_date: row.get::<_, Option<String>>(8)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                    tick_size: row.get(9)?,
                })
            },
        ).optional()
        .map_err(|e| ZiplineError::DataError(format!("Failed to get asset: {}", e)))?;

        Ok(result)
    }

    /// Find assets by symbol
    pub fn find_by_symbol(&self, symbol: &str, as_of: Option<NaiveDate>) -> Result<Vec<AssetMetadata>> {
        let query = if let Some(date) = as_of {
            format!(
                "SELECT id, symbol, exchange, asset_type, name, start_date, end_date, first_traded, auto_close_date, tick_size
                 FROM assets WHERE symbol = ?1 AND (start_date IS NULL OR start_date <= '{}') AND (end_date IS NULL OR end_date >= '{}')",
                date, date
            )
        } else {
            "SELECT id, symbol, exchange, asset_type, name, start_date, end_date, first_traded, auto_close_date, tick_size
             FROM assets WHERE symbol = ?1".to_string()
        };

        let mut stmt = self.conn.prepare(&query)
            .map_err(|e| ZiplineError::DataError(format!("Failed to prepare query: {}", e)))?;

        let assets = stmt.query_map(params![symbol], |row| {
            Ok(AssetMetadata {
                id: row.get::<_, i64>(0)? as u64,
                symbol: row.get(1)?,
                exchange: row.get(2)?,
                asset_type: Self::int_to_asset_type(row.get(3)?),
                name: row.get(4)?,
                start_date: row.get::<_, Option<String>>(5)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                end_date: row.get::<_, Option<String>>(6)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                first_traded: row.get::<_, Option<String>>(7)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                auto_close_date: row.get::<_, Option<String>>(8)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                tick_size: row.get(9)?,
            })
        })
        .map_err(|e| ZiplineError::DataError(format!("Failed to query assets: {}", e)))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ZiplineError::DataError(format!("Failed to collect assets: {}", e)))?;

        Ok(assets)
    }

    /// Get all assets
    pub fn get_all_assets(&self) -> Result<Vec<AssetMetadata>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, symbol, exchange, asset_type, name, start_date, end_date, first_traded, auto_close_date, tick_size FROM assets"
        ).map_err(|e| ZiplineError::DataError(format!("Failed to prepare query: {}", e)))?;

        let assets = stmt.query_map([], |row| {
            Ok(AssetMetadata {
                id: row.get::<_, i64>(0)? as u64,
                symbol: row.get(1)?,
                exchange: row.get(2)?,
                asset_type: Self::int_to_asset_type(row.get(3)?),
                name: row.get(4)?,
                start_date: row.get::<_, Option<String>>(5)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                end_date: row.get::<_, Option<String>>(6)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                first_traded: row.get::<_, Option<String>>(7)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                auto_close_date: row.get::<_, Option<String>>(8)?.and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                tick_size: row.get(9)?,
            })
        })
        .map_err(|e| ZiplineError::DataError(format!("Failed to query assets: {}", e)))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ZiplineError::DataError(format!("Failed to collect assets: {}", e)))?;

        Ok(assets)
    }

    /// Update existing asset
    pub fn update_asset(&mut self, asset: &AssetMetadata) -> Result<()> {
        let asset_type_int = asset.asset_type as i32;

        self.conn.execute(
            "UPDATE assets SET symbol = ?2, exchange = ?3, asset_type = ?4, name = ?5,
             start_date = ?6, end_date = ?7, first_traded = ?8, auto_close_date = ?9, tick_size = ?10
             WHERE id = ?1",
            params![
                asset.id as i64,
                &asset.symbol,
                &asset.exchange,
                asset_type_int,
                &asset.name,
                asset.start_date.map(|d| d.to_string()),
                asset.end_date.map(|d| d.to_string()),
                asset.first_traded.map(|d| d.to_string()),
                asset.auto_close_date.map(|d| d.to_string()),
                asset.tick_size,
            ],
        ).map_err(|e| ZiplineError::DataError(format!("Failed to update asset: {}", e)))?;

        Ok(())
    }

    /// Convert integer to AssetType
    fn int_to_asset_type(value: i32) -> AssetType {
        match value {
            0 => AssetType::Equity,
            1 => AssetType::Future,
            2 => AssetType::Option,
            3 => AssetType::Forex,
            4 => AssetType::Crypto,
            _ => AssetType::Equity, // Default
        }
    }

    /// Get asset count
    pub fn count(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row("SELECT COUNT(*) FROM assets", [], |row| row.get(0))
            .map_err(|e| ZiplineError::DataError(format!("Failed to count assets: {}", e)))?;
        Ok(count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_asset_db_creation() {
        let db = AssetDB::new_in_memory().unwrap();
        assert_eq!(db.count().unwrap(), 0);
    }

    #[test]
    fn test_insert_and_get_asset() {
        let mut db = AssetDB::new_in_memory().unwrap();

        let asset = AssetMetadata {
            id: 1,
            symbol: "AAPL".to_string(),
            exchange: "NASDAQ".to_string(),
            asset_type: AssetType::Equity,
            name: Some("Apple Inc.".to_string()),
            start_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
            end_date: None,
            first_traded: Some(NaiveDate::from_ymd_opt(1980, 12, 12).unwrap()),
            auto_close_date: None,
            tick_size: Some(0.01),
        };

        db.insert_asset(&asset).unwrap();

        let retrieved = db.get_asset(1).unwrap().unwrap();
        assert_eq!(retrieved.symbol, "AAPL");
        assert_eq!(retrieved.exchange, "NASDAQ");
    }

    #[test]
    fn test_find_by_symbol() {
        let mut db = AssetDB::new_in_memory().unwrap();

        let asset1 = AssetMetadata {
            id: 1,
            symbol: "AAPL".to_string(),
            exchange: "NASDAQ".to_string(),
            asset_type: AssetType::Equity,
            name: Some("Apple Inc.".to_string()),
            start_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
            end_date: None,
            first_traded: None,
            auto_close_date: None,
            tick_size: Some(0.01),
        };

        db.insert_asset(&asset1).unwrap();

        let found = db.find_by_symbol("AAPL", None).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, 1);
    }

    #[test]
    fn test_as_of_date_filtering() {
        let mut db = AssetDB::new_in_memory().unwrap();

        let asset = AssetMetadata {
            id: 1,
            symbol: "TEST".to_string(),
            exchange: "NYSE".to_string(),
            asset_type: AssetType::Equity,
            name: None,
            start_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
            end_date: Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
            first_traded: None,
            auto_close_date: None,
            tick_size: None,
        };

        db.insert_asset(&asset).unwrap();

        // Should find it when querying within the date range
        let found = db.find_by_symbol("TEST", Some(NaiveDate::from_ymd_opt(2021, 6, 1).unwrap())).unwrap();
        assert_eq!(found.len(), 1);

        // Should not find it before start_date
        let not_found = db.find_by_symbol("TEST", Some(NaiveDate::from_ymd_opt(2019, 1, 1).unwrap())).unwrap();
        assert_eq!(not_found.len(), 0);

        // Should not find it after end_date
        let not_found2 = db.find_by_symbol("TEST", Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap())).unwrap();
        assert_eq!(not_found2.len(), 0);
    }
}
