package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// RuntimeConfigSource records the provenance of a raw setting during loading.
type RuntimeConfigSource string

const (
	SourceDefault RuntimeConfigSource = "default"
	SourceEnv     RuntimeConfigSource = "env"
	SourceFile    RuntimeConfigSource = "file"
)

// RawConfig is the short-lived, unvalidated input snapshot used only while
// loading Config. It is deliberately not embedded in Config and must not be
// passed to runtime components. The snapshot preserves presence separately
// from value so source tracking can distinguish an unset key from an empty
// environment value.
type RawConfig struct {
	values  map[string]string
	present map[string]struct{}
	sources map[string]RuntimeConfigSource
}

// NewRawConfig creates a raw snapshot from explicit values. It is primarily
// useful for deterministic loader tests; production bootstrap uses
// RawConfigFromEnv so the process environment is captured exactly once.
func NewRawConfig(values map[string]string) RawConfig {
	copyValues := make(map[string]string, len(values))
	present := make(map[string]struct{}, len(values))
	for key, value := range values {
		copyValues[key] = value
		present[key] = struct{}{}
	}
	sources := make(map[string]RuntimeConfigSource, len(values))
	for key := range values {
		sources[key] = SourceEnv
	}
	return RawConfig{values: copyValues, present: present, sources: sources}
}

// RawConfigFromEnv captures os.Environ once at the configuration boundary.
func RawConfigFromEnv() RawConfig {
	return rawConfigFromEnvironment(nil)
}

// RawConfigFromEnvFile loads the optional env file, preserves shell-variable
// precedence, and captures the resulting environment exactly once. The file
// key set is attached to this immutable snapshot rather than stored in global
// mutable state, so concurrent/test loads cannot change the provenance of an
// already-created Config.
func RawConfigFromEnvFile(path string) (RawConfig, error) {
	fileKeys, err := loadEnvFile(path)
	if err != nil {
		return RawConfig{}, err
	}
	return rawConfigFromEnvironment(fileKeys), nil
}

func rawConfigFromEnvironment(fileKeys map[string]struct{}) RawConfig {
	values := make(map[string]string)
	present := make(map[string]struct{})
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		values[key] = value
		present[key] = struct{}{}
	}
	sources := make(map[string]RuntimeConfigSource, len(values))
	for key := range values {
		sources[key] = SourceEnv
		if _, fromFile := fileKeys[key]; fromFile {
			sources[key] = SourceFile
		}
	}
	return RawConfig{values: values, present: present, sources: sources}
}

func (r RawConfig) Get(key string) string { return r.values[key] }

func (r RawConfig) Lookup(key string) (string, bool) {
	_, ok := r.present[key]
	return r.values[key], ok
}

func (r RawConfig) Source(key string) RuntimeConfigSource {
	if _, ok := r.Lookup(key); !ok {
		return SourceDefault
	}
	if source, ok := r.sources[key]; ok {
		return source
	}
	return SourceEnv
}

func (r RawConfig) Float(key string, fallback, min float64) float64 {
	raw, ok := r.Lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || value < min {
		return fallback
	}
	return value
}

func (r RawConfig) Int(key string, fallback, min int) int {
	raw, ok := r.Lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < min {
		return fallback
	}
	return value
}

func (r RawConfig) Bool(key string, fallback bool) bool {
	raw, ok := r.Lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "t", "yes", "y":
		return true
	case "0", "false", "f", "no", "n":
		return false
	default:
		return fallback
	}
}

func (r RawConfig) Duration(key string, fallback time.Duration) time.Duration {
	raw, ok := r.Lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
