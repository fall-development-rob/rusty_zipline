//! Bcolz format utilities
//!
//! Utilities for reading and decompressing bcolz compressed columnar storage format.
//! Bcolz is used by Python Zipline for efficient storage of time series data.

use crate::error::{Result, ZiplineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;

/// Bcolz table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BcolzMetadata {
    /// Number of rows in the table
    pub nrows: usize,
    /// Column names
    pub columns: Vec<String>,
    /// Data types for each column
    pub dtypes: HashMap<String, String>,
    /// First row index
    pub first_row: Option<usize>,
    /// Last row index
    pub last_row: Option<usize>,
    /// Chunk size
    pub chunksize: Option<usize>,
    /// Compression codec
    pub codec: Option<String>,
    /// Compression level
    pub clevel: Option<u8>,
    /// Shuffle setting
    pub shuffle: Option<u8>,
}

impl BcolzMetadata {
    /// Read metadata from bcolz directory
    pub fn from_path(path: &Path) -> Result<Self> {
        let meta_path = path.join("meta");

        if !meta_path.exists() {
            return Err(ZiplineError::InvalidData(format!(
                "No meta directory found at {:?}",
                path
            )));
        }

        // Read column names from directory structure
        let mut columns = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();

            // Bcolz column data files are named like "column_name"
            // Skip meta directory and special files
            if name_str != "meta" && !name_str.starts_with('.') && !name_str.starts_with("__") {
                // Check if there are chunk files for this column
                let column_base = name_str.split('.').next().unwrap_or(&name_str);
                if !columns.contains(&column_base.to_string()) {
                    columns.push(column_base.to_string());
                }
            }
        }

        // Read attributes if available
        let attrs = read_bcolz_attrs(path)?;

        // Estimate nrows from first column
        let nrows = if !columns.is_empty() {
            estimate_rows(path, &columns[0])?
        } else {
            0
        };

        Ok(Self {
            nrows,
            columns,
            dtypes: HashMap::new(),
            first_row: attrs.get("first_row").and_then(|v| v.parse().ok()),
            last_row: attrs.get("last_row").and_then(|v| v.parse().ok()),
            chunksize: attrs.get("chunksize").and_then(|v| v.parse().ok()),
            codec: attrs.get("codec").map(|s| s.to_string()),
            clevel: attrs.get("clevel").and_then(|v| v.parse().ok()),
            shuffle: attrs.get("shuffle").and_then(|v| v.parse().ok()),
        })
    }
}

/// Bcolz data chunk
#[derive(Debug, Clone)]
pub struct BcolzChunk {
    /// Chunk data (decompressed)
    pub data: Vec<u8>,
    /// Number of elements in chunk
    pub nelements: usize,
    /// Element size in bytes
    pub element_size: usize,
}

impl BcolzChunk {
    /// Create a new chunk
    pub fn new(data: Vec<u8>, element_size: usize) -> Self {
        let nelements = data.len() / element_size;
        Self {
            data,
            nelements,
            element_size,
        }
    }

    /// Get element at index as f64 (assumes data is stored as float64)
    pub fn get_f64(&self, idx: usize) -> Result<f64> {
        if idx >= self.nelements {
            return Err(ZiplineError::IndexOutOfBounds(idx, self.nelements));
        }

        let offset = idx * self.element_size;
        if offset + 8 > self.data.len() {
            return Err(ZiplineError::InvalidData(
                "Insufficient data for f64".to_string(),
            ));
        }

        let bytes: [u8; 8] = self.data[offset..offset + 8]
            .try_into()
            .map_err(|_| ZiplineError::InvalidData("Failed to extract f64 bytes".to_string()))?;

        Ok(f64::from_le_bytes(bytes))
    }

    /// Get element at index as i64 (for dates/timestamps)
    pub fn get_i64(&self, idx: usize) -> Result<i64> {
        if idx >= self.nelements {
            return Err(ZiplineError::IndexOutOfBounds(idx, self.nelements));
        }

        let offset = idx * self.element_size;
        if offset + 8 > self.data.len() {
            return Err(ZiplineError::InvalidData(
                "Insufficient data for i64".to_string(),
            ));
        }

        let bytes: [u8; 8] = self.data[offset..offset + 8]
            .try_into()
            .map_err(|_| ZiplineError::InvalidData("Failed to extract i64 bytes".to_string()))?;

        Ok(i64::from_le_bytes(bytes))
    }
}

/// Read bcolz table attributes
pub fn read_bcolz_attrs(path: &Path) -> Result<HashMap<String, String>> {
    let attrs_path = path.join("meta").join("attrs");

    if !attrs_path.exists() {
        // Attrs file may not exist for simple tables
        return Ok(HashMap::new());
    }

    let contents = fs::read_to_string(&attrs_path)?;

    // Parse attrs - it's typically JSON-like format
    let attrs: HashMap<String, serde_json::Value> = serde_json::from_str(&contents)
        .unwrap_or_default();

    let mut result = HashMap::new();
    for (key, value) in attrs {
        if let Some(s) = value.as_str() {
            result.insert(key, s.to_string());
        } else if let Some(n) = value.as_i64() {
            result.insert(key, n.to_string());
        } else if let Some(n) = value.as_u64() {
            result.insert(key, n.to_string());
        }
    }

    Ok(result)
}

/// Read a bcolz column
pub fn read_bcolz_column(path: &Path, column_name: &str) -> Result<Vec<BcolzChunk>> {
    let mut chunks = Vec::new();
    let mut chunk_idx = 0;

    loop {
        // Bcolz chunk files are named: column_name.chunk_idx (e.g., "open.00000", "high.00001")
        let chunk_file = path.join(format!("{}.{:05}", column_name, chunk_idx));

        if !chunk_file.exists() {
            break;
        }

        // Read raw chunk data
        let mut file = fs::File::open(&chunk_file)?;
        let mut compressed_data = Vec::new();
        file.read_to_end(&mut compressed_data)?;

        // For now, assume uncompressed or handle basic formats
        // In production, this would use blosc decompression
        let chunk = if is_compressed(&compressed_data) {
            decompress_chunk(&compressed_data)?
        } else {
            BcolzChunk::new(compressed_data, 8) // Assuming 8-byte elements (f64/i64)
        };

        chunks.push(chunk);
        chunk_idx += 1;
    }

    if chunks.is_empty() {
        return Err(ZiplineError::DataNotFound(format!(
            "No chunks found for column {} at {:?}",
            column_name, path
        )));
    }

    Ok(chunks)
}

/// Read an entire column into a Vec<f64>
pub fn read_column_f64(path: &Path, column_name: &str) -> Result<Vec<f64>> {
    let chunks = read_bcolz_column(path, column_name)?;
    let mut values = Vec::new();

    for chunk in chunks {
        for i in 0..chunk.nelements {
            values.push(chunk.get_f64(i)?);
        }
    }

    Ok(values)
}

/// Read an entire column into a Vec<i64>
pub fn read_column_i64(path: &Path, column_name: &str) -> Result<Vec<i64>> {
    let chunks = read_bcolz_column(path, column_name)?;
    let mut values = Vec::new();

    for chunk in chunks {
        for i in 0..chunk.nelements {
            values.push(chunk.get_i64(i)?);
        }
    }

    Ok(values)
}

/// Check if data is compressed (basic heuristic)
fn is_compressed(data: &[u8]) -> bool {
    // Blosc format starts with specific magic bytes
    // Blosc magic: 0x02, 0x01 (version 2)
    if data.len() < 16 {
        return false;
    }

    // Check for blosc header
    data[0] == 0x02 && (data[1] == 0x01 || data[1] == 0x02)
}

/// Decompress a bcolz chunk
fn decompress_chunk(compressed_data: &[u8]) -> Result<BcolzChunk> {
    // NOTE: This is a placeholder. In production, you would use:
    // - blosc-rs crate for Rust blosc bindings
    // - Or use pyo3 to call Python blosc
    // - Or implement blosc decompression

    // For now, we'll assume the data needs blosc decompression
    // but return an error since we haven't implemented it yet

    #[cfg(feature = "python-blosc")]
    {
        decompress_with_python(compressed_data)
    }

    #[cfg(not(feature = "python-blosc"))]
    {
        // Fallback: try to parse header and return raw data
        if compressed_data.len() < 16 {
            return Err(ZiplineError::UnsupportedFeature(
                "Blosc decompression not available - data too short".to_string(),
            ));
        }

        // Blosc header structure (16 bytes):
        // - version (1 byte)
        // - versionlz (1 byte)
        // - flags (1 byte)
        // - typesize (1 byte)
        // - nbytes (4 bytes) - uncompressed size
        // - blocksize (4 bytes)
        // - cbytes (4 bytes) - compressed size

        let nbytes = u32::from_le_bytes([
            compressed_data[4],
            compressed_data[5],
            compressed_data[6],
            compressed_data[7],
        ]) as usize;

        let typesize = compressed_data[3] as usize;

        // For testing/simple cases, assume uncompressed data follows header
        if compressed_data.len() >= 16 + nbytes {
            let data = compressed_data[16..16 + nbytes].to_vec();
            Ok(BcolzChunk::new(data, typesize.max(8)))
        } else {
            Err(ZiplineError::UnsupportedFeature(
                "Blosc decompression not available - enable 'python-blosc' feature or use conversion tool".to_string(),
            ))
        }
    }
}

#[cfg(feature = "python-blosc")]
fn decompress_with_python(compressed_data: &[u8]) -> Result<BcolzChunk> {
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;

    Python::with_gil(|py| {
        let blosc = py.import("blosc")?;
        let decompress = blosc.getattr("decompress")?;

        let py_bytes = PyBytes::new(py, compressed_data);
        let decompressed: &PyBytes = decompress.call1((py_bytes,))?.extract()?;

        let data = decompressed.as_bytes().to_vec();
        Ok(BcolzChunk::new(data, 8))
    })
    .map_err(|e: PyErr| ZiplineError::DataError(format!("Python blosc error: {}", e)))
}

/// Estimate number of rows from column files
fn estimate_rows(path: &Path, column_name: &str) -> Result<usize> {
    let chunks = read_bcolz_column(path, column_name)?;
    Ok(chunks.iter().map(|c| c.nelements).sum())
}

/// Find all asset directories in a bcolz bundle
pub fn find_asset_sids(bundle_path: &Path) -> Result<Vec<u64>> {
    let mut sids = Vec::new();

    if !bundle_path.exists() {
        return Err(ZiplineError::InvalidData(format!(
            "Bundle path does not exist: {:?}",
            bundle_path
        )));
    }

    for entry in fs::read_dir(bundle_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            if let Some(name) = path.file_name() {
                if let Some(name_str) = name.to_str() {
                    // Asset directories are typically numeric (sid)
                    if let Ok(sid) = name_str.parse::<u64>() {
                        sids.push(sid);
                    }
                }
            }
        }
    }

    sids.sort();
    Ok(sids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_bcolz_chunk_f64() {
        let mut data = Vec::new();
        for i in 0..10 {
            data.extend_from_slice(&(i as f64).to_le_bytes());
        }

        let chunk = BcolzChunk::new(data, 8);
        assert_eq!(chunk.nelements, 10);
        assert_eq!(chunk.get_f64(0).unwrap(), 0.0);
        assert_eq!(chunk.get_f64(5).unwrap(), 5.0);
        assert_eq!(chunk.get_f64(9).unwrap(), 9.0);
    }

    #[test]
    fn test_bcolz_chunk_i64() {
        let mut data = Vec::new();
        for i in 0..10 {
            data.extend_from_slice(&(i as i64).to_le_bytes());
        }

        let chunk = BcolzChunk::new(data, 8);
        assert_eq!(chunk.nelements, 10);
        assert_eq!(chunk.get_i64(0).unwrap(), 0);
        assert_eq!(chunk.get_i64(5).unwrap(), 5);
        assert_eq!(chunk.get_i64(9).unwrap(), 9);
    }

    #[test]
    fn test_read_bcolz_attrs() {
        let temp_dir = TempDir::new().unwrap();
        let meta_dir = temp_dir.path().join("meta");
        fs::create_dir(&meta_dir).unwrap();

        let attrs_path = meta_dir.join("attrs");
        let attrs_json = r#"{"first_row": 0, "last_row": 100, "chunksize": 1000}"#;
        fs::write(&attrs_path, attrs_json).unwrap();

        let attrs = read_bcolz_attrs(temp_dir.path()).unwrap();
        assert_eq!(attrs.get("first_row"), Some(&"0".to_string()));
        assert_eq!(attrs.get("last_row"), Some(&"100".to_string()));
    }

    #[test]
    fn test_find_asset_sids() {
        let temp_dir = TempDir::new().unwrap();

        // Create some asset directories
        fs::create_dir(temp_dir.path().join("1")).unwrap();
        fs::create_dir(temp_dir.path().join("42")).unwrap();
        fs::create_dir(temp_dir.path().join("100")).unwrap();
        fs::create_dir(temp_dir.path().join("not_a_sid")).unwrap();

        let sids = find_asset_sids(temp_dir.path()).unwrap();
        assert_eq!(sids, vec![1, 42, 100]);
    }
}
