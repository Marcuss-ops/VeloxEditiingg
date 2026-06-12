package config

import (
	"os"
	"testing"
)

func TestFromEnv_Defaults(t *testing.T) {
	// Clear relevant env to test defaults
	envVars := []string{
		"VELOX_MASTER_PORT", "VELOX_STUDIO_PORT", "VELOX_STATIC_DIR",
		"VELOX_REDIS_HOST", "VELOX_REDIS_PORT", "VELOX_REDIS_DB", "VELOX_REDIS_PASSWORD",
		"VELOX_ALLOWED_WORKERS", "VELOX_FORCE_SINGLE_WORKER", "VELOX_MAX_JOB_ATTEMPTS",
		"VELOX_ALLOWLIST_ALLOW_REGISTERED",
		"VELOX_MASTER_SERVER_URL", "VELOX_REMOTE_WORKER_URL", "VELOX_REMOTE_SCRIPT_BACKEND", "VELOX_SCRIPT_BACKEND",
	}
	for _, k := range envVars {
		os.Unsetenv(k)
	}
	defer func() {
		for _, k := range envVars {
			os.Unsetenv(k)
		}
	}()

	c := FromEnv()
	if c.MasterPort != 8000 {
		t.Errorf("MasterPort want 8000, got %d", c.MasterPort)
	}
	if c.RedisHost != "localhost" || c.RedisPort != "6379" {
		t.Errorf("Redis default: got %s:%s", c.RedisHost, c.RedisPort)
	}
	if c.MaxJobAttempts != 3 {
		t.Errorf("MaxJobAttempts want 3, got %d", c.MaxJobAttempts)
	}
	if c.AllowlistAllowRegistered {
		t.Error("AllowlistAllowRegistered should default false")
	}
}

func TestFromEnv_Overrides(t *testing.T) {
	os.Setenv("VELOX_MASTER_PORT", "9000")
	os.Setenv("VELOX_REDIS_HOST", "redis.example.com")
	os.Setenv("VELOX_MAX_JOB_ATTEMPTS", "5")
	os.Setenv("VELOX_ALLOWLIST_ALLOW_REGISTERED", "true")
	defer func() {
		os.Unsetenv("VELOX_MASTER_PORT")
		os.Unsetenv("VELOX_REDIS_HOST")
		os.Unsetenv("VELOX_MAX_JOB_ATTEMPTS")
		os.Unsetenv("VELOX_ALLOWLIST_ALLOW_REGISTERED")
	}()

	c := FromEnv()
	if c.MasterPort != 9000 {
		t.Errorf("MasterPort want 9000, got %d", c.MasterPort)
	}
	if c.RedisHost != "redis.example.com" {
		t.Errorf("RedisHost want redis.example.com, got %s", c.RedisHost)
	}
	if c.MaxJobAttempts != 5 {
		t.Errorf("MaxJobAttempts want 5, got %d", c.MaxJobAttempts)
	}
	if !c.AllowlistAllowRegistered {
		t.Error("AllowlistAllowRegistered should be true")
	}
}
