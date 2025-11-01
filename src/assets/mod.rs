//! Asset management and metadata

pub mod asset_db;
pub mod asset_finder; // NEW: Symbol lookup and asset retrieval

pub use asset_db::{AssetDB, AssetMetadata};
pub use asset_finder::{AssetFinder, SymbolEntry};
