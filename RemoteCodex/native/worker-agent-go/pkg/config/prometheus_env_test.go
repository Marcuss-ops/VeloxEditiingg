package config

import "testing"

func TestPrometheusPortDefaultsAndEnvOverride(t *testing.T) {
	t.Setenv(EnvPrometheusPort, "")
	cfg := &WorkerConfig{}
	cfg.applyDefaults()
	if cfg.PrometheusPort != 9090 {
		t.Fatalf("default PrometheusPort=%d, want 9090", cfg.PrometheusPort)
	}

	t.Setenv(EnvPrometheusPort, "9191")
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.PrometheusPort != 9191 {
		t.Fatalf("env PrometheusPort=%d, want 9191", cfg.PrometheusPort)
	}

	t.Setenv(EnvPrometheusPort, "0")
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.PrometheusPort != 0 {
		t.Fatalf("explicit disable PrometheusPort=%d, want 0", cfg.PrometheusPort)
	}
}

func TestPrometheusPortInvalidEnvDoesNotOverride(t *testing.T) {
	cfg := &WorkerConfig{PrometheusPort: 9090}
	t.Setenv(EnvPrometheusPort, "not-a-port")
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.PrometheusPort != 9090 {
		t.Fatalf("invalid env changed PrometheusPort=%d, want 9090", cfg.PrometheusPort)
	}
}
