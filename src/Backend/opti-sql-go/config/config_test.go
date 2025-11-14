package config

import (
	"os"
	"path/filepath"
	"testing"
)

// resetConfig resets the singleton to defaults between tests
func resetConfig() {
	configInstance = &Config{
		Server: serverConfig{
			Port:             8080,
			Host:             "localhost",
			Timeout:          30,
			MaxRequestSizeMB: 15,
		},
		Batch: batchConfig{
			Size:                 8192,
			EnableParallelRead:   true,
			MaxMemoryBeforeSpill: 2147483648,
			MaxFileSizeMB:        500,
		},
		Query: queryConfig{
			EnableCache:               true,
			CacheTTLSeconds:           600,
			EnableConcurrentExecution: true,
			MaxConcurrentQueries:      2,
		},
		Metrics: metricsConfig{
			EnableMetrics:      true,
			MetricsPort:        9999,
			MetricsHost:        "localhost",
			ExportIntervalSecs: 60,
			EnableQueryStats:   true,
			EnableMemoryStats:  true,
		},
	}
}

// TestGetConfig tests the singleton pattern
func TestGetConfig(t *testing.T) {
	resetConfig()

	config1 := GetConfig()
	config2 := GetConfig()

	// Should return the same instance
	if config1 != config2 {
		t.Error("GetConfig should return the same singleton instance")
	}

	// Verify default values
	if config1.Server.Port != 8080 {
		t.Errorf("Expected default port 8080, got %d", config1.Server.Port)
	}
	if config1.Server.Host != "localhost" {
		t.Errorf("Expected default host 'localhost', got %s", config1.Server.Host)
	}
}

// TestDecodeInvalidExtension tests file extension validation
func TestDecodeInvalidExtension(t *testing.T) {
	resetConfig()

	tests := []struct {
		name     string
		filename string
	}{
		{"JSON extension", "config.json"},
		{"TXT extension", "config.txt"},
		{"No extension", "config"},
		{"Wrong extension", "config.xml"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Decode(tt.filename)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.filename)
			}
			expectedMsg := "file must be a .yaml or .yml file"
			if err.Error() != expectedMsg {
				t.Errorf("Expected error '%s', got '%s'", expectedMsg, err.Error())
			}
		})
	}
}

// TestDecodeMissingFile tests handling of non-existent files
func TestDecodeMissingFile(t *testing.T) {
	resetConfig()

	err := Decode("nonexistent.yaml")
	if err == nil {
		t.Error("Expected error for missing file, got nil")
	}
}

// TestDecodeInvalidYAML tests handling of malformed YAML
func TestDecodeInvalidYAML(t *testing.T) {
	resetConfig()

	invalidPath := filepath.Join("testdata", "invalid.yaml")
	err := Decode(invalidPath)
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}

// TestDecodeEmptyConfig tests that empty config preserves all defaults
func TestDecodeEmptyConfig(t *testing.T) {
	resetConfig()

	emptyPath := filepath.Join("testdata", "empty_config.yaml")
	err := Decode(emptyPath)
	if err == nil {
		t.Fatalf("This operation should have failed due to EOF: %v", err)
	}

	config := GetConfig()

	// Verify all defaults are preserved
	if config.Server.Port != 8080 {
		t.Errorf("Expected port 8080, got %d", config.Server.Port)
	}
	if config.Server.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got %s", config.Server.Host)
	}
	if config.Server.Timeout != 30 {
		t.Errorf("Expected timeout 30, got %d", config.Server.Timeout)
	}
	if config.Server.MaxRequestSizeMB != 15 {
		t.Errorf("Expected max request size 15, got %d", config.Server.MaxRequestSizeMB)
	}

	if config.Batch.Size != 8192 {
		t.Errorf("Expected batch size 8192, got %d", config.Batch.Size)
	}
	if !config.Batch.EnableParallelRead {
		t.Error("Expected enable parallel read true")
	}
	if config.Batch.MaxMemoryBeforeSpill != 2147483648 {
		t.Errorf("Expected max memory 2147483648, got %d", config.Batch.MaxMemoryBeforeSpill)
	}
	if config.Batch.MaxFileSizeMB != 500 {
		t.Errorf("Expected max file size 500, got %d", config.Batch.MaxFileSizeMB)
	}

	if !config.Query.EnableCache {
		t.Error("Expected enable cache true")
	}
	if config.Query.CacheTTLSeconds != 600 {
		t.Errorf("Expected cache TTL 600, got %d", config.Query.CacheTTLSeconds)
	}
	if !config.Query.EnableConcurrentExecution {
		t.Error("Expected enable concurrent execution true")
	}
	if config.Query.MaxConcurrentQueries != 2 {
		t.Errorf("Expected max concurrent queries 2, got %d", config.Query.MaxConcurrentQueries)
	}

	if !config.Metrics.EnableMetrics {
		t.Error("Expected enable metrics true")
	}
	if config.Metrics.MetricsPort != 9999 {
		t.Errorf("Expected metrics port 9999, got %d", config.Metrics.MetricsPort)
	}
	if config.Metrics.MetricsHost != "localhost" {
		t.Errorf("Expected metrics host 'localhost', got %s", config.Metrics.MetricsHost)
	}
	if config.Metrics.ExportIntervalSecs != 60 {
		t.Errorf("Expected export interval 60, got %d", config.Metrics.ExportIntervalSecs)
	}
	if !config.Metrics.EnableQueryStats {
		t.Error("Expected enable query stats true")
	}
	if !config.Metrics.EnableMemoryStats {
		t.Error("Expected enable memory stats true")
	}
}

// TestDecodeFullOverride tests that all values can be overridden
func TestDecodeFullOverride(t *testing.T) {
	resetConfig()

	fullPath := filepath.Join("testdata", "full_override.yaml")
	err := Decode(fullPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	config := GetConfig()

	// Verify all values are overridden
	if config.Server.Port != 9090 {
		t.Errorf("Expected port 9090, got %d", config.Server.Port)
	}
	if config.Server.Host != "0.0.0.0" {
		t.Errorf("Expected host '0.0.0.0', got %s", config.Server.Host)
	}
	if config.Server.Timeout != 60 {
		t.Errorf("Expected timeout 60, got %d", config.Server.Timeout)
	}
	if config.Server.MaxRequestSizeMB != 25 {
		t.Errorf("Expected max request size 25, got %d", config.Server.MaxRequestSizeMB)
	}

	if config.Batch.Size != 16384 {
		t.Errorf("Expected batch size 16384, got %d", config.Batch.Size)
	}
	if config.Batch.EnableParallelRead {
		t.Error("Expected enable parallel read false")
	}
	if config.Batch.MaxMemoryBeforeSpill != 4294967296 {
		t.Errorf("Expected max memory 4294967296, got %d", config.Batch.MaxMemoryBeforeSpill)
	}
	if config.Batch.MaxFileSizeMB != 1000 {
		t.Errorf("Expected max file size 1000, got %d", config.Batch.MaxFileSizeMB)
	}

	if config.Query.EnableCache {
		t.Error("Expected enable cache false")
	}
	if config.Query.CacheTTLSeconds != 1200 {
		t.Errorf("Expected cache TTL 1200, got %d", config.Query.CacheTTLSeconds)
	}
	if config.Query.EnableConcurrentExecution {
		t.Error("Expected enable concurrent execution false")
	}
	if config.Query.MaxConcurrentQueries != 4 {
		t.Errorf("Expected max concurrent queries 4, got %d", config.Query.MaxConcurrentQueries)
	}

	if config.Metrics.EnableMetrics {
		t.Error("Expected enable metrics false")
	}
	if config.Metrics.MetricsPort != 8888 {
		t.Errorf("Expected metrics port 8888, got %d", config.Metrics.MetricsPort)
	}
	if config.Metrics.MetricsHost != "127.0.0.1" {
		t.Errorf("Expected metrics host '127.0.0.1', got %s", config.Metrics.MetricsHost)
	}
	if config.Metrics.ExportIntervalSecs != 120 {
		t.Errorf("Expected export interval 120, got %d", config.Metrics.ExportIntervalSecs)
	}
	if config.Metrics.EnableQueryStats {
		t.Error("Expected enable query stats false")
	}
	if config.Metrics.EnableMemoryStats {
		t.Error("Expected enable memory stats false")
	}
}

// TestMergeConfigServerPartial tests partial server config merge
func TestMergeConfigServerPartial(t *testing.T) {
	resetConfig()

	partialPath := filepath.Join("testdata", "partial_server.yaml")
	err := Decode(partialPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	config := GetConfig()

	// Verify overridden values
	if config.Server.Port != 3000 {
		t.Errorf("Expected port 3000, got %d", config.Server.Port)
	}
	if config.Server.Host != "192.168.1.1" {
		t.Errorf("Expected host '192.168.1.1', got %s", config.Server.Host)
	}

	// Verify preserved defaults
	if config.Server.Timeout != 30 {
		t.Errorf("Expected timeout 30 (default), got %d", config.Server.Timeout)
	}
	if config.Server.MaxRequestSizeMB != 15 {
		t.Errorf("Expected max request size 15 (default), got %d", config.Server.MaxRequestSizeMB)
	}

	// Verify other sections untouched
	if config.Batch.Size != 8192 {
		t.Errorf("Expected batch size 8192 (default), got %d", config.Batch.Size)
	}
}

// TestMergeConfigBatchPartial tests partial batch config merge
func TestMergeConfigBatchPartial(t *testing.T) {
	resetConfig()

	partialPath := filepath.Join("testdata", "partial_batch.yaml")
	err := Decode(partialPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	config := GetConfig()

	// Verify overridden values
	if config.Batch.Size != 4096 {
		t.Errorf("Expected batch size 4096, got %d", config.Batch.Size)
	}
	if config.Batch.EnableParallelRead {
		t.Error("Expected enable parallel read false (overridden)")
	}

	// Verify preserved defaults
	if config.Batch.MaxMemoryBeforeSpill != 2147483648 {
		t.Errorf("Expected max memory 2147483648 (default), got %d", config.Batch.MaxMemoryBeforeSpill)
	}
	if config.Batch.MaxFileSizeMB != 500 {
		t.Errorf("Expected max file size 500 (default), got %d", config.Batch.MaxFileSizeMB)
	}

	// Verify other sections untouched
	if config.Server.Port != 8080 {
		t.Errorf("Expected port 8080 (default), got %d", config.Server.Port)
	}
}

// TestMergeConfigQueryPartial tests partial query config merge
func TestMergeConfigQueryPartial(t *testing.T) {
	resetConfig()

	partialPath := filepath.Join("testdata", "partial_query.yaml")
	err := Decode(partialPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	config := GetConfig()

	// Verify overridden values
	if config.Query.EnableCache {
		t.Error("Expected enable cache false (overridden)")
	}
	if config.Query.MaxConcurrentQueries != 8 {
		t.Errorf("Expected max concurrent queries 8, got %d", config.Query.MaxConcurrentQueries)
	}

	// Verify preserved defaults
	if config.Query.CacheTTLSeconds != 600 {
		t.Errorf("Expected cache TTL 600 (default), got %d", config.Query.CacheTTLSeconds)
	}
	if !config.Query.EnableConcurrentExecution {
		t.Error("Expected enable concurrent execution true (default)")
	}

	// Verify other sections untouched
	if config.Server.Port != 8080 {
		t.Errorf("Expected port 8080 (default), got %d", config.Server.Port)
	}
}

// TestMergeConfigMetricsPartial tests partial metrics config merge
func TestMergeConfigMetricsPartial(t *testing.T) {
	resetConfig()

	partialPath := filepath.Join("testdata", "partial_metrics.yaml")
	err := Decode(partialPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	config := GetConfig()

	// Verify overridden values
	if config.Metrics.EnableMetrics {
		t.Error("Expected enable metrics false (overridden)")
	}
	if config.Metrics.MetricsPort != 7777 {
		t.Errorf("Expected metrics port 7777, got %d", config.Metrics.MetricsPort)
	}
	if config.Metrics.ExportIntervalSecs != 30 {
		t.Errorf("Expected export interval 30, got %d", config.Metrics.ExportIntervalSecs)
	}

	// Verify preserved defaults
	if config.Metrics.MetricsHost != "localhost" {
		t.Errorf("Expected metrics host 'localhost' (default), got %s", config.Metrics.MetricsHost)
	}
	if !config.Metrics.EnableQueryStats {
		t.Error("Expected enable query stats true (default)")
	}
	if !config.Metrics.EnableMemoryStats {
		t.Error("Expected enable memory stats true (default)")
	}

	// Verify other sections untouched
	if config.Server.Port != 8080 {
		t.Errorf("Expected port 8080 (default), got %d", config.Server.Port)
	}
}

// TestMergeConfigMixedPartial tests a realistic mixed partial config
func TestMergeConfigMixedPartial(t *testing.T) {
	resetConfig()

	mixedPath := filepath.Join("testdata", "mixed_partial.yaml")
	err := Decode(mixedPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	config := GetConfig()

	// Server: only timeout should be overridden
	if config.Server.Port != 8080 {
		t.Errorf("Expected port 8080 (default), got %d", config.Server.Port)
	}
	if config.Server.Host != "localhost" {
		t.Errorf("Expected host 'localhost' (default), got %s", config.Server.Host)
	}
	if config.Server.Timeout != 45 {
		t.Errorf("Expected timeout 45 (overridden), got %d", config.Server.Timeout)
	}
	if config.Server.MaxRequestSizeMB != 15 {
		t.Errorf("Expected max request size 15 (default), got %d", config.Server.MaxRequestSizeMB)
	}

	// Batch: only max memory should be overridden
	if config.Batch.Size != 8192 {
		t.Errorf("Expected batch size 8192 (default), got %d", config.Batch.Size)
	}
	if !config.Batch.EnableParallelRead {
		t.Error("Expected enable parallel read true (default)")
	}
	if config.Batch.MaxMemoryBeforeSpill != 3221225472 {
		t.Errorf("Expected max memory 3221225472 (overridden), got %d", config.Batch.MaxMemoryBeforeSpill)
	}
	if config.Batch.MaxFileSizeMB != 500 {
		t.Errorf("Expected max file size 500 (default), got %d", config.Batch.MaxFileSizeMB)
	}

	// Query: cache TTL and concurrent execution should be overridden
	if !config.Query.EnableCache {
		t.Error("Expected enable cache true (default)")
	}
	if config.Query.CacheTTLSeconds != 900 {
		t.Errorf("Expected cache TTL 900 (overridden), got %d", config.Query.CacheTTLSeconds)
	}
	if config.Query.EnableConcurrentExecution {
		t.Error("Expected enable concurrent execution false (overridden)")
	}
	if config.Query.MaxConcurrentQueries != 2 {
		t.Errorf("Expected max concurrent queries 2 (default), got %d", config.Query.MaxConcurrentQueries)
	}

	// Metrics: host and query stats should be overridden
	if !config.Metrics.EnableMetrics {
		t.Error("Expected enable metrics true (default)")
	}
	if config.Metrics.MetricsPort != 9999 {
		t.Errorf("Expected metrics port 9999 (default), got %d", config.Metrics.MetricsPort)
	}
	if config.Metrics.MetricsHost != "metrics.example.com" {
		t.Errorf("Expected metrics host 'metrics.example.com' (overridden), got %s", config.Metrics.MetricsHost)
	}
	if config.Metrics.ExportIntervalSecs != 60 {
		t.Errorf("Expected export interval 60 (default), got %d", config.Metrics.ExportIntervalSecs)
	}
	if config.Metrics.EnableQueryStats {
		t.Error("Expected enable query stats false (overridden)")
	}
	if !config.Metrics.EnableMemoryStats {
		t.Error("Expected enable memory stats true (default)")
	}
}

// TestMergeConfigAllServerFields tests every server field individually
func TestMergeConfigAllServerFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		checkFn func(*Config) error
	}{
		{
			name: "Port only",
			yaml: "server:\n  port: 5000\n",
			checkFn: func(c *Config) error {
				if c.Server.Port != 5000 {
					t.Errorf("Expected port 5000, got %d", c.Server.Port)
				}
				if c.Server.Host != "localhost" {
					t.Errorf("Expected host 'localhost', got %s", c.Server.Host)
				}
				return nil
			},
		},
		{
			name: "Host only",
			yaml: "server:\n  host: \"example.com\"\n",
			checkFn: func(c *Config) error {
				if c.Server.Port != 8080 {
					t.Errorf("Expected port 8080, got %d", c.Server.Port)
				}
				if c.Server.Host != "example.com" {
					t.Errorf("Expected host 'example.com', got %s", c.Server.Host)
				}
				return nil
			},
		},
		{
			name: "Timeout only",
			yaml: "server:\n  timeout: 120\n",
			checkFn: func(c *Config) error {
				if c.Server.Timeout != 120 {
					t.Errorf("Expected timeout 120, got %d", c.Server.Timeout)
				}
				if c.Server.Port != 8080 {
					t.Errorf("Expected port 8080, got %d", c.Server.Port)
				}
				return nil
			},
		},
		{
			name: "MaxRequestSizeMB only",
			yaml: "server:\n  max_request_size_mb: 50\n",
			checkFn: func(c *Config) error {
				if c.Server.MaxRequestSizeMB != 50 {
					t.Errorf("Expected max request size 50, got %d", c.Server.MaxRequestSizeMB)
				}
				if c.Server.Port != 8080 {
					t.Errorf("Expected port 8080, got %d", c.Server.Port)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			// Create temporary file
			tmpFile, err := os.CreateTemp("", "test_*.yaml")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			if _, err := tmpFile.WriteString(tt.yaml); err != nil {
				t.Fatalf("Failed to write temp file: %v", err)
			}
			if err := tmpFile.Close(); err != nil {
				t.Fatalf("Failed to close temp file: %v", err)
			}

			if err := Decode(tmpFile.Name()); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if err := tt.checkFn(GetConfig()); err != nil {
				t.Errorf("Check function failed: %v", err)
			}
			if err := os.Remove(tmpFile.Name()); err != nil {
				t.Fatalf("Failed to remove temp file: %v", err)
			}
		})
	}
}

// TestMergeConfigAllBatchFields tests every batch field individually
func TestMergeConfigAllBatchFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		checkFn func(*Config) error
	}{
		{
			name: "Size only",
			yaml: "batch:\n  size: 1024\n",
			checkFn: func(c *Config) error {
				if c.Batch.Size != 1024 {
					t.Errorf("Expected size 1024, got %d", c.Batch.Size)
				}
				if !c.Batch.EnableParallelRead {
					t.Error("Expected enable parallel read true")
				}
				return nil
			},
		},
		{
			name: "EnableParallelRead only",
			yaml: "batch:\n  enable_parallel_read: false\n",
			checkFn: func(c *Config) error {
				if c.Batch.EnableParallelRead {
					t.Error("Expected enable parallel read false")
				}
				if c.Batch.Size != 8192 {
					t.Errorf("Expected size 8192, got %d", c.Batch.Size)
				}
				return nil
			},
		},
		{
			name: "MaxMemoryBeforeSplill only",
			yaml: "batch:\n  max_memory_before_spill: 1073741824\n",
			checkFn: func(c *Config) error {
				if c.Batch.MaxMemoryBeforeSpill != 1073741824 {
					t.Errorf("Expected max memory 1073741824, got %d", c.Batch.MaxMemoryBeforeSpill)
				}
				if c.Batch.Size != 8192 {
					t.Errorf("Expected size 8192, got %d", c.Batch.Size)
				}
				return nil
			},
		},
		{
			name: "MaxFileSizeMB only",
			yaml: "batch:\n  max_file_size_mb: 250\n",
			checkFn: func(c *Config) error {
				if c.Batch.MaxFileSizeMB != 250 {
					t.Errorf("Expected max file size 250, got %d", c.Batch.MaxFileSizeMB)
				}
				if c.Batch.Size != 8192 {
					t.Errorf("Expected size 8192, got %d", c.Batch.Size)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			tmpFile, err := os.CreateTemp("", "test_*.yaml")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			if _, err := tmpFile.WriteString(tt.yaml); err != nil {
				t.Fatalf("Failed to write temp file: %v", err)
			}
			if err := tmpFile.Close(); err != nil {
				t.Fatalf("Failed to close temp file: %v", err)
			}

			if err := Decode(tmpFile.Name()); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if err := tt.checkFn(GetConfig()); err != nil {
				t.Errorf("Check function failed: %v", err)
			}

			if err := os.Remove(tmpFile.Name()); err != nil {
				t.Fatalf("Failed to remove temp file: %v", err)
			}
		})
	}
}

// TestMergeConfigAllQueryFields tests every query field individually
func TestMergeConfigAllQueryFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		checkFn func(*Config) error
	}{
		{
			name: "EnableCache only",
			yaml: "query:\n  enable_cache: false\n",
			checkFn: func(c *Config) error {
				if c.Query.EnableCache {
					t.Error("Expected enable cache false")
				}
				if c.Query.CacheTTLSeconds != 600 {
					t.Errorf("Expected cache TTL 600, got %d", c.Query.CacheTTLSeconds)
				}
				return nil
			},
		},
		{
			name: "CacheTTLSeconds only",
			yaml: "query:\n  cache_ttl_seconds: 300\n",
			checkFn: func(c *Config) error {
				if c.Query.CacheTTLSeconds != 300 {
					t.Errorf("Expected cache TTL 300, got %d", c.Query.CacheTTLSeconds)
				}
				if !c.Query.EnableCache {
					t.Error("Expected enable cache true")
				}
				return nil
			},
		},
		{
			name: "EnableConcurrentExecution only",
			yaml: "query:\n  enable_concurrent_execution: false\n",
			checkFn: func(c *Config) error {
				if c.Query.EnableConcurrentExecution {
					t.Error("Expected enable concurrent execution false")
				}
				if c.Query.MaxConcurrentQueries != 2 {
					t.Errorf("Expected max concurrent queries 2, got %d", c.Query.MaxConcurrentQueries)
				}
				return nil
			},
		},
		{
			name: "MaxConcurrentQueries only",
			yaml: "query:\n  max_concurrent_queries: 10\n",
			checkFn: func(c *Config) error {
				if c.Query.MaxConcurrentQueries != 10 {
					t.Errorf("Expected max concurrent queries 10, got %d", c.Query.MaxConcurrentQueries)
				}
				if !c.Query.EnableConcurrentExecution {
					t.Error("Expected enable concurrent execution true")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			tmpFile, err := os.CreateTemp("", "test_*.yaml")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			if _, err := tmpFile.WriteString(tt.yaml); err != nil {
				t.Fatalf("Failed to write temp file: %v", err)
			}
			if err := tmpFile.Close(); err != nil {
				t.Fatalf("Failed to close temp file: %v", err)
			}

			if err := Decode(tmpFile.Name()); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if err := tt.checkFn(GetConfig()); err != nil {
				t.Errorf("Check function failed: %v", err)
			}
			if err := os.Remove(tmpFile.Name()); err != nil {
				t.Fatalf("Failed to remove temp file: %v", err)
			}
		})
	}
}

// TestMergeConfigAllMetricsFields tests every metrics field individually
func TestMergeConfigAllMetricsFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		checkFn func(*Config) error
	}{
		{
			name: "EnableMetrics only",
			yaml: "metrics:\n  enable_metrics: false\n",
			checkFn: func(c *Config) error {
				if c.Metrics.EnableMetrics {
					t.Error("Expected enable metrics false")
				}
				if c.Metrics.MetricsPort != 9999 {
					t.Errorf("Expected metrics port 9999, got %d", c.Metrics.MetricsPort)
				}
				return nil
			},
		},
		{
			name: "MetricsPort only",
			yaml: "metrics:\n  metrics_port: 5555\n",
			checkFn: func(c *Config) error {
				if c.Metrics.MetricsPort != 5555 {
					t.Errorf("Expected metrics port 5555, got %d", c.Metrics.MetricsPort)
				}
				if !c.Metrics.EnableMetrics {
					t.Error("Expected enable metrics true")
				}
				return nil
			},
		},
		{
			name: "MetricsHost only",
			yaml: "metrics:\n  metrics_host: \"0.0.0.0\"\n",
			checkFn: func(c *Config) error {
				if c.Metrics.MetricsHost != "0.0.0.0" {
					t.Errorf("Expected metrics host '0.0.0.0', got %s", c.Metrics.MetricsHost)
				}
				if c.Metrics.MetricsPort != 9999 {
					t.Errorf("Expected metrics port 9999, got %d", c.Metrics.MetricsPort)
				}
				return nil
			},
		},
		{
			name: "ExportIntervalSecs only",
			yaml: "metrics:\n  export_interval_secs: 15\n",
			checkFn: func(c *Config) error {
				if c.Metrics.ExportIntervalSecs != 15 {
					t.Errorf("Expected export interval 15, got %d", c.Metrics.ExportIntervalSecs)
				}
				if c.Metrics.MetricsPort != 9999 {
					t.Errorf("Expected metrics port 9999, got %d", c.Metrics.MetricsPort)
				}
				return nil
			},
		},
		{
			name: "EnableQueryStats only",
			yaml: "metrics:\n  enable_query_stats: false\n",
			checkFn: func(c *Config) error {
				if c.Metrics.EnableQueryStats {
					t.Error("Expected enable query stats false")
				}
				if !c.Metrics.EnableMemoryStats {
					t.Error("Expected enable memory stats true")
				}
				return nil
			},
		},
		{
			name: "EnableMemoryStats only",
			yaml: "metrics:\n  enable_memory_stats: false\n",
			checkFn: func(c *Config) error {
				if c.Metrics.EnableMemoryStats {
					t.Error("Expected enable memory stats false")
				}
				if !c.Metrics.EnableQueryStats {
					t.Error("Expected enable query stats true")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			tmpFile, err := os.CreateTemp("", "test_*.yaml")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			if _, err := tmpFile.WriteString(tt.yaml); err != nil {
				t.Fatalf("Failed to write temp file: %v", err)
			}
			if err := tmpFile.Close(); err != nil {
				t.Fatalf("Failed to close temp file: %v", err)
			}

			if err := Decode(tmpFile.Name()); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if err := tt.checkFn(GetConfig()); err != nil {
				t.Errorf("Check function failed: %v", err)
			}
			if err := os.Remove(tmpFile.Name()); err != nil {
				t.Fatalf("Failed to remove temp file: %v", err)
			}
		})
	}
}
