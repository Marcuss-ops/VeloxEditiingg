package cosign

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// stubCosignScript creates a temporary shell script that exits
// with `exitCode` and (optionally) writes `errOut` to stderr. The
// script is invoked via ExternalCosignVerifier with the configured
// BinaryPath; this avoids any dependency on the real `cosign` CLI
// being installed on the test runner.
//
// Caller MUST NOT clean up explicitly — t.TempDir() + t.Cleanup
// handle removal.
func stubCosignScript(t *testing.T, exitCode int, errOut string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unix shell stub; Windows is out of scope here")
	}
	dir := t.TempDir()
	var script strings.Builder
	script.WriteString("#!/usr/bin/env bash\n")
	if errOut != "" {
		script.WriteString("echo " + shellQuote(errOut) + " >&2\n")
	}
	script.WriteString("exit " + strconv.Itoa(exitCode) + "\n")
	path := filepath.Join(dir, "cosign-stub.sh")
	if err := os.WriteFile(path, []byte(script.String()), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	return path
}

// shellQuote wraps `s` in single quotes for safe interpolation in
// a POSIX shell — single quotes do not interpret anything inside.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// TestExternalCosignVerifier_Positive pins the 0-exit-code happy
// path: Verify returns nil when the stub exits 0 (the binary
// signals "looks good to me").
func TestExternalCosignVerifier_Positive(t *testing.T) {
	bin := stubCosignScript(t, 0, "")
	v := &ExternalCosignVerifier{BinaryPath: bin, VerifyTimeout: 5 * time.Second}
	if err := v.Verify(context.Background(),
		"ghcr.io/o/r@sha256:"+strings.Repeat("a", 64)); err != nil {
		t.Errorf("positive verify: got %v, want nil", err)
	}
}

// TestExternalCosignVerifier_Negative asserts a non-zero exit
// wraps the stderr in the returned error so the audit trail can
// see what cosign complained about.
func TestExternalCosignVerifier_Negative(t *testing.T) {
	bin := stubCosignScript(t, 1, "bad signature")
	v := &ExternalCosignVerifier{BinaryPath: bin, VerifyTimeout: 5 * time.Second}
	err := v.Verify(context.Background(),
		"ghcr.io/o/r@sha256:"+strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("expected error from negative verify, got nil")
	}
	if !strings.Contains(err.Error(), "bad signature") {
		t.Errorf("error %q should wrap stderr 'bad signature'", err)
	}
}

// TestExternalCosignVerifier_Override short-circuits when the env
// override is set. The stub is intentionally broken (exit 1 with
// stderr "should not see this") — the override MUST skip the
// binary entirely and return ErrSkippedByOverride.
func TestExternalCosignVerifier_Override(t *testing.T) {
	bin := stubCosignScript(t, 1, "should not see this")
	t.Setenv("VELOX_SKIP_COSIGN_VERIFY", "1")
	v := &ExternalCosignVerifier{BinaryPath: bin, VerifyTimeout: 5 * time.Second}
	err := v.Verify(context.Background(),
		"ghcr.io/o/r@sha256:"+strings.Repeat("a", 64))
	if !errors.Is(err, ErrSkippedByOverride) {
		t.Errorf("override path = %v, want ErrSkippedByOverride-wrapped", err)
	}
}

// TestExternalCosignVerifier_MissingBinary asserts an unreachable
// binary path wraps ErrBinaryMissing — distinct from a verify
// failure so the operator metrics differentiate "infra missing"
// vs "signature mismatch".
func TestExternalCosignVerifier_MissingBinary(t *testing.T) {
	v := &ExternalCosignVerifier{
		BinaryPath:    "/nonexistent/cosign/binary/path/that/does/not/exist",
		VerifyTimeout: 100 * time.Millisecond,
	}
	err := v.Verify(context.Background(),
		"ghcr.io/o/r@sha256:"+strings.Repeat("a", 64))
	if !errors.Is(err, ErrBinaryMissing) {
		t.Errorf("missing binary path = %v, want ErrBinaryMissing-wrapped", err)
	}
}

// TestStubVerifier_AlwaysAccepts pins the StubVerifier contract:
// always nil, regardless of ref shape. Used to grep test files
// for accidental production misuse (StubVerifier in a production
// wiring is a security regression; this test's existence makes
// that searchable).
func TestStubVerifier_AlwaysAccepts(t *testing.T) {
	var v CosignVerifier = StubVerifier{}
	if err := v.Verify(context.Background(), "any-ref-at-all"); err != nil {
		t.Errorf("StubVerifier: got %v, want nil", err)
	}
}
