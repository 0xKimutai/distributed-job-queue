package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for both the API server and workers.
// We read from environment variables — no config files, no flags for now.
// This is the 12-factor app pattern: configuration lives in the environment.
type Config struct {
	// Database
	DatabaseURL string

	// API server
	APIPort string

	// Worker
	WorkerID          string        // unique identifier for this worker instance
	WorkerPollInterval time.Duration // how often a worker polls for new jobs
	WorkerLeaseDuration time.Duration // how long a claimed job lease lasts
	WorkerHeartbeatInterval time.Duration // how often the worker renews its lease

	// Migrations
	MigrationsPath string
}

// Load reads configuration from environment variables and returns a Config.
// It fails fast: if a required variable is missing, we exit immediately.
// Silent misconfiguration is harder to debug than a loud startup failure.
func Load() (*Config, error) {
	// Load .env file if present. In production this file won't exist and
	// godotenv.Load silently returns nil — env vars come from the platform.
	// In local dev it populates the process environment from the file,
	// so plain `go run ./cmd/api/` works without manually sourcing anything.
	_ = godotenv.Load()
	cfg := &Config{
		DatabaseURL:             requireEnv("DATABASE_URL"),
		APIPort:                 getEnvOrDefault("API_PORT", "8080"),
		WorkerID:                getEnvOrDefault("WORKER_ID", generateWorkerID()),
		WorkerPollInterval:      getDurationOrDefault("WORKER_POLL_INTERVAL", 2*time.Second),
		WorkerLeaseDuration:     getDurationOrDefault("WORKER_LEASE_DURATION", 30*time.Second),
		WorkerHeartbeatInterval: getDurationOrDefault("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		MigrationsPath:          getEnvOrDefault("MIGRATIONS_PATH", "migrations"),
	}

	// Enforce the 3:1 ratio we discussed: heartbeat must fire well before lease expires.
	// If someone misconfigures this, jobs will be falsely reclaimed while still running.
	if cfg.WorkerHeartbeatInterval >= cfg.WorkerLeaseDuration/2 {
		return nil, fmt.Errorf(
			"WORKER_HEARTBEAT_INTERVAL (%s) must be less than half of WORKER_LEASE_DURATION (%s)",
			cfg.WorkerHeartbeatInterval, cfg.WorkerLeaseDuration,
		)
	}

	return cfg, nil
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		// Crash loudly at startup rather than silently using a zero value.
		panic(fmt.Sprintf("required environment variable %q is not set", key))
	}
	return v
}

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getDurationOrDefault(key string, defaultVal time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	// Accept either a Go duration string ("10s", "2m") or plain seconds as integer.
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	// Bad value — fail loudly.
	panic(fmt.Sprintf("environment variable %q has invalid duration value %q", key, v))
}

// generateWorkerID produces a default worker ID from the hostname + PID.
// In production you'd use a UUID or the container/pod name.
func generateWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
