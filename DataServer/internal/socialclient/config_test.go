package socialclient

import (
	"testing"
	"time"
)

// TestConfigFromEnv_DropsLegacySocialGatewayAliases locks in the contract
// that the legacy SOCIAL_GATEWAY_* env vars are NOT honored after the
// deprecation cycle. An operator that still carries only the legacy
// aliases in /etc/velox-server.env must observe an empty Config
// (ErrNotConfigured at DeliverArtifact time), NOT a silent fallback to
// the legacy value.
//
// Companion to the canonical-positive test below; together they form
// the package-boundary boundary for the YouTube→Social cleanup.
//
// The canonical envs are SOCIAL_API_URL / SOCIAL_API_TOKEN /
// SOCIAL_CALLBACK_BASE_URL. SOCIAL_API_TIMEOUT_MS and SOCIAL_API_RETRIES
// never had a legacy alias and are unaffected by this removal.
func TestConfigFromEnv_DropsLegacySocialGatewayAliases(t *testing.T) {
	// Force every relevant env var to the empty string during this
	// test, then set ONLY the legacy aliases. t.Setenv registers
	// cleanup that restores the pre-test value at end-of-test.
	for _, k := range []string{
		"SOCIAL_API_URL",
		"SOCIAL_API_TOKEN",
		"SOCIAL_CALLBACK_BASE_URL",
		"SOCIAL_GATEWAY_URL",
		"SOCIAL_GATEWAY_API_KEY",
		"SOCIAL_GATEWAY_CALLBACK_BASE_URL",
		"SOCIAL_API_TIMEOUT_MS",
		"SOCIAL_API_RETRIES",
	} {
		t.Setenv(k, "")
	}

	t.Setenv("SOCIAL_GATEWAY_URL", "https://legacy-only.example.com")
	t.Setenv("SOCIAL_GATEWAY_API_KEY", "legacy-token")
	t.Setenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL", "https://legacy-only-callback.example.com")

	cfg := ConfigFromEnv()

	if cfg.BaseURL != "" {
		t.Errorf("legacy SOCIAL_GATEWAY_URL must NOT be honored: got BaseURL=%q", cfg.BaseURL)
	}
	if cfg.APIKey != "" {
		t.Errorf("legacy SOCIAL_GATEWAY_API_KEY must NOT be honored: got APIKey len=%d", len(cfg.APIKey))
	}
	if cfg.CallbackBaseURL != "" {
		t.Errorf("legacy SOCIAL_GATEWAY_CALLBACK_BASE_URL must NOT be honored: got CallbackBaseURL=%q", cfg.CallbackBaseURL)
	}

	// Defaults (Timeout=30s, MaxRetries=0) are unaffected by the legacy
	// alias removal and must remain observable here.
	if cfg.Timeout != 30*time.Second {
		t.Errorf("default Timeout expected 30s, got %s", cfg.Timeout)
	}
	if cfg.MaxRetries != 0 {
		t.Errorf("default MaxRetries expected 0, got %d", cfg.MaxRetries)
	}
}

// TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs pins the canonical
// path so a future regression that re-introduces a fallback or breaks
// the canonical lookup is caught at unit-test time. Companion to the
// negative test above.
func TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs(t *testing.T) {
	// Ensure clean baseline + restore at end.
	for _, k := range []string{
		"SOCIAL_API_URL",
		"SOCIAL_API_TOKEN",
		"SOCIAL_CALLBACK_BASE_URL",
		"SOCIAL_API_TIMEOUT_MS",
		"SOCIAL_API_RETRIES",
	} {
		t.Setenv(k, "")
	}

	t.Setenv("SOCIAL_API_URL", "https://canonical.example.com")
	t.Setenv("SOCIAL_API_TOKEN", "canonical-token")
	t.Setenv("SOCIAL_CALLBACK_BASE_URL", "https://canonical-callback.example.com")
	t.Setenv("SOCIAL_API_TIMEOUT_MS", "7000")
	t.Setenv("SOCIAL_API_RETRIES", "2")

	cfg := ConfigFromEnv()

	if cfg.BaseURL != "https://canonical.example.com" {
		t.Errorf("SOCIAL_API_URL not honored: got %q", cfg.BaseURL)
	}
	if cfg.APIKey != "canonical-token" {
		t.Errorf("SOCIAL_API_TOKEN not honored: got %q", cfg.APIKey)
	}
	if cfg.CallbackBaseURL != "https://canonical-callback.example.com" {
		t.Errorf("SOCIAL_CALLBACK_BASE_URL not honored: got %q", cfg.CallbackBaseURL)
	}
	if cfg.Timeout != 7*time.Second {
		t.Errorf("SOCIAL_API_TIMEOUT_MS not honored: got %s", cfg.Timeout)
	}
	if cfg.MaxRetries != 2 {
		t.Errorf("SOCIAL_API_RETRIES not honored: got %d", cfg.MaxRetries)
	}
}
