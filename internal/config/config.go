// Package config loads scheduler configuration from environment
// variables with sensible defaults and strict validation.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default values for every supported environment variable.
const (
	DefaultHTTPAddr        = ":8080"
	DefaultDatabaseURL     = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"
	DefaultRedisAddr       = ""
	DefaultWorkerCount     = 3
	DefaultMinWorkerCount  = 1
	DefaultMaxWorkerCount  = 64
	DefaultPollInterval    = 500 * time.Millisecond
	DefaultJobTimeout      = 10 * time.Second
	DefaultStaleTimeout    = 60 * time.Second
	DefaultBackoffBase     = 2 * time.Second
	DefaultBackoffMax      = 5 * time.Minute
	DefaultMaxPayloadBytes = 1 << 20  // 1 MiB
	HardMaxPayloadBytes    = 10 << 20 // 10 MiB
	DefaultRateLimit       = 100
	DefaultRateWindow      = time.Second
	DefaultJobTypes        = "http,noop"
	DefaultAutoMigrate     = true
	DefaultLogLevel        = "info"
	DefaultShutdownTimeout = 15 * time.Second
	DefaultWorkerID        = ""
)

// Config holds the full runtime configuration of the scheduler.
type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	RedisAddr       string
	WorkerCount     int
	PollInterval    time.Duration
	JobTimeout      time.Duration
	StaleTimeout    time.Duration
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	MaxPayloadBytes int
	RateLimit       int
	RateWindow      time.Duration
	JobTypes        []string
	AutoMigrate     bool
	LogLevel        string
	ShutdownTimeout time.Duration
	WorkerIDPrefix  string
}

// Load builds a Config from environment variables, falling back to
// defaults and returning descriptive errors for malformed values.
func Load() (*Config, error) {
	cfg := &Config{}

	var err error
	if cfg.HTTPAddr, err = str("HTTP_ADDR", DefaultHTTPAddr); err != nil {
		return nil, err
	}
	if cfg.DatabaseURL, err = str("DATABASE_URL", DefaultDatabaseURL); err != nil {
		return nil, err
	}
	if cfg.RedisAddr, err = str("REDIS_ADDR", DefaultRedisAddr); err != nil {
		return nil, err
	}
	if cfg.WorkerCount, err = intEnv("WORKER_COUNT", DefaultWorkerCount); err != nil {
		return nil, err
	}
	if cfg.PollInterval, err = duration("POLL_INTERVAL", DefaultPollInterval); err != nil {
		return nil, err
	}
	if cfg.JobTimeout, err = duration("JOB_TIMEOUT", DefaultJobTimeout); err != nil {
		return nil, err
	}
	if cfg.StaleTimeout, err = duration("STALE_TIMEOUT", DefaultStaleTimeout); err != nil {
		return nil, err
	}
	if cfg.BackoffBase, err = duration("BACKOFF_BASE", DefaultBackoffBase); err != nil {
		return nil, err
	}
	if cfg.BackoffMax, err = duration("BACKOFF_MAX", DefaultBackoffMax); err != nil {
		return nil, err
	}
	if cfg.MaxPayloadBytes, err = intEnv("MAX_PAYLOAD_BYTES", DefaultMaxPayloadBytes); err != nil {
		return nil, err
	}
	if cfg.RateLimit, err = intEnv("RATE_LIMIT", DefaultRateLimit); err != nil {
		return nil, err
	}
	if cfg.RateWindow, err = duration("RATE_WINDOW", DefaultRateWindow); err != nil {
		return nil, err
	}
	autoMigrate, err := str("AUTO_MIGRATE", strconv.FormatBool(DefaultAutoMigrate))
	if err != nil {
		return nil, err
	}
	if cfg.AutoMigrate, err = parseBool(autoMigrate); err != nil {
		return nil, err
	}
	if cfg.LogLevel, err = str("LOG_LEVEL", DefaultLogLevel); err != nil {
		return nil, err
	}
	if cfg.ShutdownTimeout, err = duration("SHUTDOWN_TIMEOUT", DefaultShutdownTimeout); err != nil {
		return nil, err
	}
	if cfg.WorkerIDPrefix, err = str("WORKER_ID", DefaultWorkerID); err != nil {
		return nil, err
	}

	jobTypes, err := str("JOB_TYPES", DefaultJobTypes)
	if err != nil {
		return nil, err
	}
	cfg.JobTypes = parseJobTypes(jobTypes)

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// validate enforces all cross-variable invariants.
func (c *Config) validate() error {
	if c.WorkerCount < DefaultMinWorkerCount || c.WorkerCount > DefaultMaxWorkerCount {
		return fmt.Errorf("WORKER_COUNT must be between %d and %d, got %d",
			DefaultMinWorkerCount, DefaultMaxWorkerCount, c.WorkerCount)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive, got %s", c.PollInterval)
	}
	if c.JobTimeout <= 0 {
		return fmt.Errorf("JOB_TIMEOUT must be positive, got %s", c.JobTimeout)
	}
	if c.StaleTimeout <= c.JobTimeout {
		return fmt.Errorf("STALE_TIMEOUT (%s) must be greater than JOB_TIMEOUT (%s)",
			c.StaleTimeout, c.JobTimeout)
	}
	if c.BackoffBase <= 0 {
		return fmt.Errorf("BACKOFF_BASE must be positive, got %s", c.BackoffBase)
	}
	if c.BackoffMax < c.BackoffBase {
		return fmt.Errorf("BACKOFF_MAX (%s) must not be less than BACKOFF_BASE (%s)",
			c.BackoffMax, c.BackoffBase)
	}
	if c.MaxPayloadBytes <= 0 {
		return fmt.Errorf("MAX_PAYLOAD_BYTES must be positive, got %d", c.MaxPayloadBytes)
	}
	if c.MaxPayloadBytes > HardMaxPayloadBytes {
		return fmt.Errorf("MAX_PAYLOAD_BYTES may not exceed %d bytes (10 MiB), got %d",
			HardMaxPayloadBytes, c.MaxPayloadBytes)
	}
	if c.RateLimit <= 0 {
		return fmt.Errorf("RATE_LIMIT must be positive, got %d", c.RateLimit)
	}
	if c.RateWindow <= 0 {
		return fmt.Errorf("RATE_WINDOW must be positive, got %s", c.RateWindow)
	}
	if len(c.JobTypes) == 0 {
		return fmt.Errorf("JOB_TYPES must contain at least one type")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be positive, got %s", c.ShutdownTimeout)
	}
	if _, err := LogLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}

// parseJobTypes splits the comma-separated JOB_TYPES value and trims
// whitespace; empty entries are dropped.
func parseJobTypes(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func str(name, def string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	return v, nil
}

func intEnv(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: must be an integer, got %q", name, v)
	}
	return n, nil
}

func duration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: must be a valid duration (e.g. 500ms, 10s), got %q", name, v)
	}
	return d, nil
}

func parseBool(v string) (bool, error) {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("AUTO_MIGRATE: must be a boolean (true/false), got %q", v)
	}
	return b, nil
}

// LogLevel maps a level name to slog.Level, validating the input.
func LogLevel(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("LOG_LEVEL must be one of debug/info/warn/error, got %q", name)
	}
}
