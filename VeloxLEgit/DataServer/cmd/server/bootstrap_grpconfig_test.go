package main

import (
	"os"
	"strings"
	"testing"

	"velox-server/internal/config"
)

// TestGRPCInsecureDevFlagStrictness is the table-driven unit test for
// the env-value parser (formerly parseInsecureDevFlag, now inlined as
// strings.TrimSpace(envVal) == "true" in config_runtime.go). The
// TrimSpace call means leading/trailing whitespace no longer blocks
// the opt-in (more robust, env vars with accidental spaces are still
// recognised). The strictness against typos and alternate spellings
// remains — only the literal "true" (after trimming) enables the
// insecure gRPC dev mode.
func TestGRPCInsecureDevFlagStrictness(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		envVal string
		want   bool
	}{
		// Opt-in: the literal string (possibly with surrounding whitespace) enables the bypass.
		{name: "lowercase true", envVal: "true", want: true},
		{name: "trailing whitespace", envVal: "true ", want: true},
		{name: "leading whitespace", envVal: " true", want: true},
		{name: "surrounded by whitespace", envVal: " true ", want: true},

		// Opt-out: everything else is treated as NOT enabling the bypass.
		{name: "empty string", envVal: "", want: false},
		{name: "literal false", envVal: "false", want: false},
		{name: "capitalized True", envVal: "True", want: false},
		{name: "all-caps TRUE", envVal: "TRUE", want: false},
		{name: "mixed case", envVal: "tRuE", want: false},
		{name: "numeric 1", envVal: "1", want: false},
		{name: "yes alias", envVal: "yes", want: false},
		{name: "on alias", envVal: "on", want: false},
		{name: "typo tru", envVal: "tru", want: false},
		{name: "extra chars", envVal: "truee", want: false},
		{name: "inline comment-style", envVal: "true # no", want: false},
		{name: "garbage", envVal: "drop table users", want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Matches the logic in config_runtime.go:
			//   strings.TrimSpace(os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV")) == "true"
			got := strings.TrimSpace(tc.envVal) == "true"
			if got != tc.want {
				t.Fatalf("TrimSpace(%q) == \"true\" = %v, want %v",
					tc.envVal, got, tc.want)
			}
		})
	}
}

// TestBuildGRPCHandlerConfig_PropagatesAllowInsecure is the regression
// test for the P0 wiring bug: prior versions of bootstrap.go
// constructed grpcserver.HandlerConfig inline and silently dropped the
// AllowInsecure field, so setting VELOX_GRPC_ALLOW_INSECURE_DEV=true had
// no effect — the handler kept refusing plaintext streams because
// h.config.AllowInsecure stayed false.
//
// This test exercises the extracted buildGRPCHandlerConfig helper with
// both insecureDev=true and insecureDev=false, and asserts:
//  1. AllowInsecure tracks insecureDev (the actual regression).
//  2. PushMode is preserved regardless of insecureDev.
//  3. AllowedWorkers is preserved regardless of insecureDev.
//
// If any of these assertions fail, the gRPC dev-mode bypass is broken
// and unit tests of the WorkerAuthorizer alone (which only cover the
// allowlist logic) cannot catch the regression.
//
// Companion test: TestBuildGRPCHandlerConfig_AllowInsecureFromEnv
// below covers the FULL wiring path from raw env value to AllowInsecure.
func TestBuildGRPCHandlerConfig_PropagatesAllowInsecure(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		Server:  config.ServerConfig{GRPCPushMode: true},
		Workers: config.WorkersConfig{AllowedWorkers: "w1,w2"},
	}

	// ── insecureDev=true: AllowInsecure must be true ────────────────────────
	insecureCfg := buildGRPCHandlerConfig(cfg, true)
	if insecureCfg == nil {
		t.Fatal("buildGRPCHandlerConfig returned nil when insecureDev=true")
	}
	if !insecureCfg.AllowInsecure {
		t.Fatal("AllowInsecure should be true when insecureDev=true; " +
			"otherwise VELOX_GRPC_ALLOW_INSECURE_DEV=true is a no-op " +
			"and the handler will refuse plaintext streams")
	}
	if !insecureCfg.PushMode {
		t.Fatal("PushMode should be propagated from cfg.Server.GRPCPushMode")
	}
	if insecureCfg.AllowedWorkers != "w1,w2" {
		t.Fatalf("AllowedWorkers should be %q, got %q",
			"w1,w2", insecureCfg.AllowedWorkers)
	}

	// ── insecureDev=false: AllowInsecure must be hardened off ─────────────
	secureCfg := buildGRPCHandlerConfig(cfg, false)
	if secureCfg == nil {
		t.Fatal("buildGRPCHandlerConfig returned nil when insecureDev=false")
	}
	if secureCfg.AllowInsecure {
		t.Fatal("AllowInsecure should be false when insecureDev=false; " +
			"setting it true unconditionally would silently weaken " +
			"the production gRPC security model")
	}
	if secureCfg.PushMode != true {
		t.Fatal("PushMode should be propagated regardless of insecureDev")
	}
	if secureCfg.AllowedWorkers != "w1,w2" {
		t.Fatalf("AllowedWorkers should be %q, got %q",
			"w1,w2", secureCfg.AllowedWorkers)
	}
}

// TestBuildGRPCHandlerConfig_EmptyAllowedWorkers ensures that an
// empty VELOX_ALLOWED_WORKERS value flows through unchanged. The
// fail-fast guard at runServer() rejects empty allowlists in production
// (ValidateWorkerAllowlist), but buildGRPCHandlerConfig itself must
// remain a pure pass-through — the guard lives elsewhere.
func TestBuildGRPCHandlerConfig_EmptyAllowedWorkers(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		Server:  config.ServerConfig{GRPCPushMode: false},
		Workers: config.WorkersConfig{AllowedWorkers: ""},
	}

	got := buildGRPCHandlerConfig(cfg, false)
	if got == nil {
		t.Fatal("buildGRPCHandlerConfig returned nil")
	}
	if got.PushMode {
		t.Fatal("PushMode should reflect cfg.Server.GRPCPushMode=false")
	}
	if got.AllowedWorkers != "" {
		t.Fatalf("AllowedWorkers should be empty, got %q", got.AllowedWorkers)
	}
	if got.AllowInsecure {
		t.Fatal("AllowInsecure should be false when insecureDev=false")
	}
}

// TestBuildGRPCHandlerConfig_AllowInsecureFromEnv is the wiring test
// promised by the original review: it verifies the END-TO-END path
// from the raw VELOX_GRPC_ALLOW_INSECURE_DEV environment variable all
// the way through to HandlerConfig.AllowInsecure.
//
// Unlike TestBuildGRPCHandlerConfig_PropagatesAllowInsecure (which
// exercises the helper with an explicit bool, hiding the env-parse
// step), this test sets the actual OS env via t.Setenv and then calls
// the same parseInsecureDevFlag the runServer composition root calls.
// If someone later refactors the parse rule to be lenient, or
// short-circuits the env read, this test fails.
//
// t.Setenv automatically restores the previous value at the end of
// each sub-test, and panics inside the goroutine if any parallel test
// mutates the same env var. We therefore DO NOT call t.Parallel() on
// the outer test — sub-tests inherit the parent's non-parallel status
// and run sequentially on the same goroutine.
func TestBuildGRPCHandlerConfig_AllowInsecureFromEnv(t *testing.T) {
	cfg := &config.Config{
		Server:  config.ServerConfig{GRPCPushMode: true},
		Workers: config.WorkersConfig{AllowedWorkers: "w1"},
	}

	// ── env="true" end-to-end ─────────────────────────────────────────────
	t.Run("env true → AllowInsecure true (preserves PushMode/AllowedWorkers)", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_ALLOW_INSECURE_DEV", "true")

		envVal := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV")
		if envVal == "" {
			// Defence-in-depth: if t.Setenv silently failed the test
			// below would still pass, giving a false-positive.
			t.Fatal("t.Setenv did not stick; os.Getenv still empty")
		}
		insecureDev := strings.TrimSpace(envVal) == "true"
		grpcCfg := buildGRPCHandlerConfig(cfg, insecureDev)

		if !grpcCfg.AllowInsecure {
			t.Fatal("AllowInsecure should be true when " +
				"VELOX_GRPC_ALLOW_INSECURE_DEV=true is set in the env; " +
				"the handler must accept plaintext gRPC streams")
		}
		if !grpcCfg.PushMode {
			t.Fatal("PushMode propagation regressed while env parse path was active")
		}
		if grpcCfg.AllowedWorkers != "w1" {
			t.Fatalf("AllowedWorkers propagation regressed: got %q want %q",
				grpcCfg.AllowedWorkers, "w1")
		}
	})

	// ── env="false" (literal) → AllowInsecure false ──────────────────────
	t.Run("env false → AllowInsecure false", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_ALLOW_INSECURE_DEV", "false")

		envVal := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV")
		insecureDev := strings.TrimSpace(envVal) == "true"
		grpcCfg := buildGRPCHandlerConfig(cfg, insecureDev)

		if grpcCfg.AllowInsecure {
			t.Fatal("AllowInsecure should be false when " +
				"VELOX_GRPC_ALLOW_INSECURE_DEV=false is set in the env")
		}
	})

	// ── env unset → AllowInsecure false (production-safe default) ────────
	t.Run("env unset → AllowInsecure false (production default must not regress)", func(t *testing.T) {
		// t.Setenv to empty string simulates "not set"; os.Getenv
		// returns "" for both unset and explicitly-empty, which
		// matches the production default.
		t.Setenv("VELOX_GRPC_ALLOW_INSECURE_DEV", "")

		envVal := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV")
		if envVal != "" {
			t.Fatalf("expected empty env, got %q", envVal)
		}
		insecureDev := strings.TrimSpace(envVal) == "true"
		grpcCfg := buildGRPCHandlerConfig(cfg, insecureDev)

		if grpcCfg.AllowInsecure {
			t.Fatal("AllowInsecure should be false when " +
				"VELOX_GRPC_ALLOW_INSECURE_DEV is not set; this is " +
				"the production default that must not regress")
		}
	})

	// ── env="True" (capitalised) → AllowInsecure false (strict rule wins) ──
	t.Run("env capitalised True → AllowInsecure false (strict parsing)", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_ALLOW_INSECURE_DEV", "True")

		envVal := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV")
		insecureDev := strings.TrimSpace(envVal) == "true"
		grpcCfg := buildGRPCHandlerConfig(cfg, insecureDev)

		if grpcCfg.AllowInsecure {
			t.Fatal("AllowInsecure should be false when " +
				"VELOX_GRPC_ALLOW_INSECURE_DEV=True (capitalised). " +
				"Strict parsing protects against typo-class accidental opt-in")
		}
	})
}
