package config

import (
	"strings"
	"testing"
)

// =====================================================================
// Validate() / String() / RW-PROD-001 B1 worker_id shape
// =====================================================================
//
// Verifies the non-TLS Validate() guards: nil-config rejection, missing
// required fields (table-driven), log_level value parsing (table-driven),
// Stringer representation including the PR 1 `environment` tag, and the
// RW-PROD-001 B1 worker_id regex `[a-z][a-z0-9_-]{2,62}`. TLS-specific
// Validate() rules (full triple / partial / cert-file / key-permissions)
// live in config_tls_test.go.

// =====================================================================
//  Existing tests preserved + adapted to the new dev-env convention.
// =====================================================================

func TestValidateSuccess(t *testing.T) {
	cfg := devValidBase()

	if err := cfg.Validate(); err != nil {
		t.Errorf("Expected validation to pass in dev mode, got error: %v", err)
	}
}

func TestValidateNil(t *testing.T) {
	var cfg *WorkerConfig

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

func TestValidateMissingFields(t *testing.T) {
	tests := []struct {
		name   string
		config *WorkerConfig
	}{
		{
			name: "missing master_url",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.MasterURL = ""
				return c
			}(),
		},
		{
			name: "missing worker_id",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.WorkerID = ""
				return c
			}(),
		},
		{
			name: "missing work_dir",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.WorkDir = ""
				return c
			}(),
		},
		{
			name: "missing control_grpc_url",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.ControlGRPCURL = ""
				return c
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Errorf("Expected validation error for %s", tt.name)
			}
		})
	}
}

func TestValidateInvalidLogLevel(t *testing.T) {
	cfg := devValidBase()
	cfg.LogLevel = "invalid"

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid log level")
	}
}

func TestValidateLogLevels(t *testing.T) {
	validLevels := []string{"", "debug", "info", "warn", "error"}

	for _, level := range validLevels {
		t.Run("log_level_"+level, func(t *testing.T) {
			cfg := devValidBase()
			cfg.LogLevel = level

			if err := cfg.Validate(); err != nil {
				t.Errorf("Expected validation to pass for log_level %q, got error: %v", level, err)
			}
		})
	}
}

func TestString(t *testing.T) {
	cfg := devValidBase()

	str := cfg.String()

	if str == "" {
		t.Error("Expected non-empty string representation")
	}

	if !strings.Contains(str, "test-worker-001") {
		t.Error("Expected worker_id in string representation")
	}

	if !strings.Contains(str, "Test Worker") {
		t.Error("Expected worker_name in string representation")
	}

	// PR 1: Environment is now part of String() output so log lines and
	// debug dumps reveal the deployment lifecycle tag.
	if !strings.Contains(str, "dev") {
		t.Error("Expected environment tag in string representation")
	}
}

func TestStringNil(t *testing.T) {
	var cfg *WorkerConfig

	str := cfg.String()

	if str != "WorkerConfig{nil}" {
		t.Errorf("Expected 'WorkerConfig{nil}', got %q", str)
	}
}

// =====================================================================
//  RW-PROD-001 — production hardening for the validation chain.
// =====================================================================

// TestRWProd001_WorkerIDShape covers TS-1.6 from RW-PROD-001 §5.
//
// Note on error messages: the empty case produces two distinct error
// messages depending on which branch fires first. With B1's regex
// `[a-z][a-z0-9_-]{2,62}`:
//   - empty input → "worker_id is required" (the missing-field branch,
//     earlier in Validate's order of checks, fires before the shape check).
//   - non-empty but shape-invalid → "invalid worker_id shape: ..."
//
// We therefore assert on err==nil vs err!=err, plus a substring check
// that is loose enough to cover both messages.
func TestRWProd001_WorkerIDShape(t *testing.T) {
	cases := []struct {
		name      string
		id        string
		wantErr   bool
		errSubstr string // substring expected in err message when wantErr is true
	}{
		{"canonical worker-xxxxxxxx (15 chars)", "worker-8e98ce85", false, ""},
		{"host prefix after Normalize", "host_57_129_132_133", false, ""},
		{"underscore allowed (B1)", "worker_001", false, ""},
		{"minimum 3 chars", "abc", false, ""},
		{"max length 63", "a" + strings.Repeat("a", 62), false, ""},
		// Negative cases
		{"empty", "", true, "worker_id is required"},
		{"too short (2 chars)", "ab", true, "invalid worker_id shape"},
		{"too long (64 chars)", "a" + strings.Repeat("a", 63), true, "invalid worker_id shape"},
		{"uppercase letter", "Worker-001", true, "invalid worker_id shape"},
		{"starts with digit", "1-worker", true, "invalid worker_id shape"},
		{"starts with hyphen", "-worker", true, "invalid worker_id shape"},
		{"non-ASCII letter", "wörker-001", true, "invalid worker_id shape"},
		{"special char", "worker@1", true, "invalid worker_id shape"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := devValidBase()
			cfg.WorkerID = tc.id
			err := cfg.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected rejection for %q, got nil", tc.id)
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("expected error to contain %q for %q, got: %v",
						tc.errSubstr, tc.id, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected rejection for %q: %v", tc.id, err)
				}
			}
		})
	}
}
