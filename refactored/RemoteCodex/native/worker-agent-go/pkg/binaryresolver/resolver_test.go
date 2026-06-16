package binaryresolver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFakeBinary(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func TestResolverEnvOverride(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeBinary(t, dir, "my-bin")
	t.Setenv("MY_BIN", bin)

	r := Resolver{Name: "my-bin", EnvVar: "MY_BIN"}
	got, err := r.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != bin {
		t.Fatalf("expected %q, got %q", bin, got)
	}
}

func TestResolverAbsoluteCandidate(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeBinary(t, dir, "abs-bin")

	r := Resolver{Name: "abs-bin", AbsCandidates: []string{bin}}
	got, err := r.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != bin {
		t.Fatalf("expected %q, got %q", bin, got)
	}
}

func TestResolverRelativeCandidateSkippedInIsolation(t *testing.T) {
	// RelOffsets rely on runtime.Caller to compute the source-relative path,
	// which is this test file itself. We exercise the absolute path branch
	// here; an integration-style test would mock runtime.Caller via a build
	// tag or by introducing an indirection seam.
	t.Skip("relative offsets are exercised by integration tests; runtime.Caller is non-mockable in-process")
	_ = filepath.Join
	_ = strings.HasSuffix
}

func TestResolverNotFound(t *testing.T) {
	r := Resolver{Name: "missing", EnvVar: "VELOX_NONEXISTENT", AbsCandidates: []string{"/no/such/path"}}
	t.Setenv("VELOX_NONEXISTENT", "")
	_, err := r.Resolve(0)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error lacks identifier: %v", err)
	}
}
