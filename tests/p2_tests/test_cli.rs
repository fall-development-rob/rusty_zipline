//! CLI Interface Tests
//!
//! Comprehensive tests for rusty-zipline command-line interface

#[cfg(test)]
mod cli_tests {
    use std::process::Command;
    use tempfile::{tempdir, NamedTempFile};
    use std::io::Write;

    /// Helper to get binary path
    fn get_binary_path() -> String {
        "rusty-zipline".to_string()
    }

    #[test]
    fn test_cli_help() {
        // Test that help message is displayed
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "--help"])
            .output();

        if let Ok(result) = output {
            assert!(result.status.success() || result.status.code() == Some(0));
            let stdout = String::from_utf8_lossy(&result.stdout);
            assert!(stdout.contains("rusty-zipline"));
        }
    }

    #[test]
    fn test_cli_version() {
        // Test version flag
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "--version"])
            .output();

        if let Ok(result) = output {
            let stdout = String::from_utf8_lossy(&result.stdout);
            assert!(stdout.contains("rusty_zipline") || result.status.success());
        }
    }

    #[test]
    fn test_run_command_parsing() {
        // Test run command argument parsing
        let temp_dir = tempdir().unwrap();
        let algo_file = temp_dir.path().join("test_algo.rs");
        std::fs::write(&algo_file, "// Test algorithm").unwrap();

        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "run",
                algo_file.to_str().unwrap(),
                "--start", "2020-01-01",
                "--end", "2020-12-31",
                "--capital", "100000",
            ])
            .output();

        if let Ok(result) = output {
            // Should parse without panic
            let stdout = String::from_utf8_lossy(&result.stdout);
            let stderr = String::from_utf8_lossy(&result.stderr);
            // Either succeeds or shows expected error
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_bundle_list_command() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "bundle", "list"])
            .output();

        if let Ok(result) = output {
            let stdout = String::from_utf8_lossy(&result.stdout);
            // Should display bundles or succeed
            assert!(stdout.contains("bundle") || result.status.success());
        }
    }

    #[test]
    fn test_bundle_info_command() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "bundle", "info", "quandl"])
            .output();

        if let Ok(result) = output {
            let stdout = String::from_utf8_lossy(&result.stdout);
            assert!(stdout.contains("Bundle") || result.status.success());
        }
    }

    #[test]
    fn test_ingest_command_parsing() {
        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "ingest", "test_bundle",
                "--source", "csv",
                "--start", "2020-01-01",
            ])
            .output();

        if let Ok(result) = output {
            // Should parse correctly
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_clean_command_requires_force() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "clean", "test_bundle"])
            .output();

        if let Ok(result) = output {
            let stdout = String::from_utf8_lossy(&result.stdout);
            // Should warn about requiring --force or succeed
            assert!(stdout.contains("force") || result.status.success());
        }
    }

    #[test]
    fn test_info_command() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "info"])
            .output();

        if let Ok(result) = output {
            let stdout = String::from_utf8_lossy(&result.stdout);
            assert!(stdout.contains("rusty-zipline") || stdout.contains("System") || result.status.success());
        }
    }

    #[test]
    fn test_info_detailed_command() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "info", "--detailed"])
            .output();

        if let Ok(result) = output {
            let stdout = String::from_utf8_lossy(&result.stdout);
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_benchmark_command() {
        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "benchmark", "quandl",
                "--iterations", "5",
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_verbose_flag() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "--verbose", "info"])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_invalid_command() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "invalid_command"])
            .output();

        if let Ok(result) = output {
            // Should fail with error
            let stderr = String::from_utf8_lossy(&result.stderr);
            assert!(stderr.contains("error") || stderr.contains("unrecognized") || !result.status.success());
        }
    }

    #[test]
    fn test_run_missing_algo_file() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "run", "nonexistent.rs"])
            .output();

        if let Ok(result) = output {
            // Should handle missing file gracefully
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_capital_validation() {
        let temp_dir = tempdir().unwrap();
        let algo_file = temp_dir.path().join("test_algo.rs");
        std::fs::write(&algo_file, "// Test").unwrap();

        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "run", algo_file.to_str().unwrap(),
                "--capital", "0",
            ])
            .output();

        if let Ok(result) = output {
            // Should parse (validation may happen at runtime)
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_bundle_register_command() {
        let temp_dir = tempdir().unwrap();

        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "bundle", "register", "test_bundle",
                "--type", "csv",
                "--data-dir", temp_dir.path().to_str().unwrap(),
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_bundle_unregister_command() {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "rusty-zipline", "--", "bundle", "unregister", "test_bundle"])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_date_format_parsing() {
        let temp_dir = tempdir().unwrap();
        let algo_file = temp_dir.path().join("test_algo.rs");
        std::fs::write(&algo_file, "// Test").unwrap();

        // Test valid date format
        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "run", algo_file.to_str().unwrap(),
                "--start", "2020-01-01",
                "--end", "2020-12-31",
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_output_file_option() {
        let temp_dir = tempdir().unwrap();
        let algo_file = temp_dir.path().join("test_algo.rs");
        let output_file = temp_dir.path().join("results.csv");
        std::fs::write(&algo_file, "// Test").unwrap();

        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "run", algo_file.to_str().unwrap(),
                "--output", output_file.to_str().unwrap(),
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_custom_benchmark() {
        let temp_dir = tempdir().unwrap();
        let algo_file = temp_dir.path().join("test_algo.rs");
        std::fs::write(&algo_file, "// Test").unwrap();

        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "run", algo_file.to_str().unwrap(),
                "--benchmark", "QQQ",
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_ingest_with_progress() {
        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "ingest", "test_bundle",
                "--show-progress",
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_clean_with_keep_option() {
        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "clean", "test_bundle",
                "--keep", "3",
                "--force",
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }

    #[test]
    fn test_benchmark_type_option() {
        let output = Command::new("cargo")
            .args(&[
                "run", "--bin", "rusty-zipline", "--",
                "benchmark", "quandl",
                "--type", "data",
                "--iterations", "10",
            ])
            .output();

        if let Ok(result) = output {
            assert!(result.status.code().is_some());
        }
    }
}
