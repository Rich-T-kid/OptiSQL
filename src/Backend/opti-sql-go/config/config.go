package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	kiloByte = 1024
	megaByte = 1024 * kiloByte
	gigaByte = 1024 * megaByte
)

type Config struct {
	Server  serverConfig  `yaml:"server"`
	Batch   batchConfig   `yaml:"batch"`
	Query   queryConfig   `yaml:"query"`
	Metrics metricsConfig `yaml:"metrics"`
}
type serverConfig struct {
	Port             int    `yaml:"port"`
	Host             string `yaml:"host"`
	Timeout          int    `yaml:"timeout"`
	MaxRequestSizeMB uint64 `yaml:"max_request_size_mb"` // max size of a file upload. passed in by grpc request
}
type batchConfig struct {
	Size                 int    `yaml:"size"`
	EnableParallelRead   bool   `yaml:"enable_parallel_read"`
	MaxMemoryBeforeSpill uint64 `yaml:"max_memory_before_spill"`
	MaxFileSizeMB        int    `yaml:"max_file_size_mb"` // max size of a single file
	ShouldDowndload      bool   `yaml:"should_download"`
	MaxDownloadSizeMB    int    `yaml:"max_download_size_mb"` // max size to download from external sources like S3
}
type queryConfig struct {
	// should results be cached, server side? if so how long
	EnableCache     bool `yaml:"enable_cache"`
	CacheTTLSeconds int  `yaml:"cache_ttl_seconds"`
	// run queries concurrently? if so what the max before blocking
	EnableConcurrentExecution bool `yaml:"enable_concurrent_execution"`
	MaxConcurrentQueries      int  `yaml:"max_concurrent_queries"` // blocks after this many concurrent queries until one finishes
}
type metricsConfig struct {
	EnableMetrics      bool   `yaml:"enable_metrics"`
	MetricsPort        int    `yaml:"metrics_port"`
	MetricsHost        string `yaml:"metrics_host"`
	ExportIntervalSecs int    `yaml:"export_interval_secs"`
	// what queries have beeen sent
	EnableQueryStats bool `yaml:"enable_query_stats"`
	// memory usage over time
	EnableMemoryStats bool `yaml:"enable_memory_stats"`
}

var configInstance *Config = &Config{
	Server: serverConfig{
		Port:             8080,
		Host:             "localhost",
		Timeout:          30,
		MaxRequestSizeMB: 15,
	},
	Batch: batchConfig{
		Size:                 1024 * 8, // rows per bathch
		EnableParallelRead:   true,
		MaxMemoryBeforeSpill: uint64(gigaByte) * 2, // 2GB
		MaxFileSizeMB:        500,                  // 500MB
		// should we download files from external sources like S3
		// if so whats the max size to download, if its greater than dont download the file locally
		ShouldDowndload:   true,
		MaxDownloadSizeMB: 10, // 10MB
	},
	Query: queryConfig{
		EnableCache:               true,
		CacheTTLSeconds:           600, // 10 minutes
		EnableConcurrentExecution: true,
		MaxConcurrentQueries:      2, // 2 concurrent queries
	},
	Metrics: metricsConfig{
		EnableMetrics:      true,
		MetricsPort:        9999,
		MetricsHost:        "localhost",
		ExportIntervalSecs: 60, // 1 minute
		EnableQueryStats:   true,
		EnableMemoryStats:  true,
	},
}

func GetConfig() *Config {
	return configInstance
}

// overwrite global instance with loaded config
func Decode(filePath string) error {
	suffix := strings.Split(filePath, ".")[len(strings.Split(filePath, "."))-1]
	if suffix != "yaml" && suffix != "yml" {
		return errors.New("file must be a .yaml or .yml file")
	}
	r, err := os.Open(filePath)
	if err != nil {
		return err
	}
	config := make(map[string]interface{})
	decoder := yaml.NewDecoder(r)
	if err := decoder.Decode(config); err != nil {
		return fmt.Errorf("failed to decode config: %w", err)
	}
	mergeConfig(configInstance, config)
	return nil
}
func mergeConfig(dst *Config, src map[string]interface{}) {
	// =============================
	// SERVER
	// =============================
	if server, ok := src["server"].(map[string]interface{}); ok {
		if v, ok := server["port"].(int); ok {
			dst.Server.Port = v
		}
		if v, ok := server["host"].(string); ok {
			dst.Server.Host = v
		}
		if v, ok := server["timeout"].(int); ok {
			dst.Server.Timeout = v
		}
		if v, ok := server["max_request_size_mb"].(int); ok {
			dst.Server.MaxRequestSizeMB = uint64(v)
		}
	}

	// =============================
	// BATCH
	// =============================
	if batch, ok := src["batch"].(map[string]interface{}); ok {
		if v, ok := batch["size"].(int); ok {
			dst.Batch.Size = v
		}
		if v, ok := batch["enable_parallel_read"].(bool); ok {
			dst.Batch.EnableParallelRead = v
		}
		if v, ok := batch["max_memory_before_spill"].(int); ok {
			dst.Batch.MaxMemoryBeforeSpill = uint64(v)
		}
		if v, ok := batch["max_file_size_mb"].(int); ok {
			dst.Batch.MaxFileSizeMB = v
		}
		if v, ok := batch["should_download"].(bool); ok {
			dst.Batch.ShouldDowndload = v
		}
		if v, ok := batch["max_download_size_mb"].(int); ok {
			dst.Batch.MaxDownloadSizeMB = v
		}
	}

	// =============================
	// QUERY
	// =============================
	if query, ok := src["query"].(map[string]interface{}); ok {
		if v, ok := query["enable_cache"].(bool); ok {
			dst.Query.EnableCache = v
		}
		if v, ok := query["cache_ttl_seconds"].(int); ok {
			dst.Query.CacheTTLSeconds = v
		}
		if v, ok := query["enable_concurrent_execution"].(bool); ok {
			dst.Query.EnableConcurrentExecution = v
		}
		if v, ok := query["max_concurrent_queries"].(int); ok {
			dst.Query.MaxConcurrentQueries = v
		}
	}

	// =============================
	// METRICS
	// =============================
	if metrics, ok := src["metrics"].(map[string]interface{}); ok {
		if v, ok := metrics["enable_metrics"].(bool); ok {
			dst.Metrics.EnableMetrics = v
		}
		if v, ok := metrics["metrics_port"].(int); ok {
			dst.Metrics.MetricsPort = v
		}
		if v, ok := metrics["metrics_host"].(string); ok {
			dst.Metrics.MetricsHost = v
		}
		if v, ok := metrics["export_interval_secs"].(int); ok {
			dst.Metrics.ExportIntervalSecs = v
		}
		if v, ok := metrics["enable_query_stats"].(bool); ok {
			dst.Metrics.EnableQueryStats = v
		}
		if v, ok := metrics["enable_memory_stats"].(bool); ok {
			dst.Metrics.EnableMemoryStats = v
		}
	}
}
