package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeDigestArgAcceptsPinnedImageReference(t *testing.T) {
	ref := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	got, err := normalizeDigestArg(ref)
	if err != nil {
		t.Fatalf("normalize pinned image: %v", err)
	}
	want := "sha256:" + strings.Repeat("a", 64)
	if got != want {
		t.Fatalf("normalized digest = %q, want %q", got, want)
	}
}
func TestNormalizeDigestArgRejectsInvalidPinnedImage(t *testing.T) {
	if _, err := normalizeDigestArg("ghcr.io/example/velox-worker:latest"); err == nil {
		t.Fatal("mutable image reference must be rejected")
	}
}
func TestValidateDigest_AcceptsCanonicalLowercase(t *testing.T) {
	canonical := "sha256:" + strings.Repeat("a", 64)
	if err := validateDigest(canonical); err != nil {
		t.Errorf("canonical lowercase digest must validate, got %v", err)
	}
}
func TestValidateDigest_RejectsUppercase(t *testing.T) {
	upper := "sha256:" + strings.Repeat("A", 64)
	if err := validateDigest(upper); err == nil {
		t.Errorf("uppercase hex must fail regex (Cosign emits lowercase; mixed-case is operator error)")
	}
}
func TestValidateDigest_RejectsLengthTooShort(t *testing.T) {
	short := "sha256:" + strings.Repeat("a", 63)
	err := validateDigest(short)
	if err == nil {
		t.Fatalf("63-hex digest must fail length check")
	}
	if !strings.Contains(err.Error(), "wrong length") {
		t.Errorf("error must mention length specifically: %v", err)
	}
}
func TestValidateDigest_RejectsMobileRefs(t *testing.T) {
	for _, ref := range []string{
		"ghcr.io/foo/bar:latest",
		"ghcr.io/foo/bar:main",
		"ghcr.io/foo/bar:stable",
	} {
		err := validateDigest(ref)
		if err == nil {
			t.Errorf("mobile ref %q must be rejected with mobile-ref-specific message", ref)
			continue
		}
		if !strings.Contains(err.Error(), "mobile ref") {
			t.Errorf("error message must mention 'mobile ref' for %q: %v", ref, err)
		}
	}
}
func TestMapHTTPStatusToOpExit(t *testing.T) {
	cases := []struct {
		httpStatus int
		wantExit   int
	}{
		{404, ExitWorkerNotFound},
		{409, ExitLeaseUnavailable},
		{400, ExitImageInvalid},
		{422, ExitImageInvalid},
		{401, ExitMisuse},
		{403, ExitMisuse},
		{500, ExitUnexpected},
		{502, ExitUnexpected},
		{503, ExitUnexpected},
		{418, ExitUnexpected}, // unhandled → unexpected
	}
	for _, c := range cases {
		got := MapHTTPStatusToOpExit(c.httpStatus)
		if got != c.wantExit {
			t.Errorf("MapHTTPStatusToOpExit(%d) = %d, want %d", c.httpStatus, got, c.wantExit)
		}
	}
}
func TestMapOperationKindToExit(t *testing.T) {
	cases := map[string]int{
		"smoke":    ExitSmokeFailed,
		"rollback": ExitRollbackFailed,
		"update":   ExitUnexpected,
		"drain":    ExitUnexpected,
		"resume":   ExitUnexpected,
		"unknown":  ExitUnexpected,
	}
	for kind, want := range cases {
		got := MapOperationKindToExit(kind)
		if got != want {
			t.Errorf("MapOperationKindToExit(%q) = %d, want %d", kind, got, want)
		}
	}
}
func TestParseInspectArgs_RejectsExtraPositionals(t *testing.T) {
	if _, _, err := parseInspectArgs([]string{"w-1", "w-2"}); err == nil {
		t.Fatal("two positional worker_ids must be rejected")
	}
	if _, _, err := parseInspectArgs([]string{"w-1", "--bogus"}); err == nil {
		t.Fatal("unknown flag must be rejected")
	}
	if _, _, err := parseInspectArgs(nil); err == nil {
		t.Fatal("missing worker_id must be rejected")
	}
}
func TestParseInspectArgs_AcceptsGlobalFlagForms(t *testing.T) {
	// Space-form global flags must be skipped like parseOperationsArgs /
	// parseWaitReadyArgs do, and the worker_id must still resolve.
	workerID, jsonOutput, err := parseInspectArgs([]string{"--master", "http://master:8000", "--token-file", "/tmp/tok", "--verbose", "--json", "w-1"})
	if err != nil {
		t.Fatalf("space-form globals + --json must parse: %v", err)
	}
	if workerID != "w-1" || !jsonOutput {
		t.Fatalf("parsed = (%q, %t), want (w-1, true)", workerID, jsonOutput)
	}
	// Equals-form with a value.
	workerID, _, err = parseInspectArgs([]string{"--master=http://m", "w-2"})
	if err != nil || workerID != "w-2" {
		t.Fatalf("equals-form globals: workerID=%q err=%v", workerID, err)
	}
	// Missing value for a space-form global must fail loudly.
	if _, _, err := parseInspectArgs([]string{"--token-file"}); err == nil {
		t.Fatal("--token-file without a value must be rejected")
	}
}
func TestParseImageMutationArgsSupportsLegacyAndFlagForms(t *testing.T) {
	pinned := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name   string
		args   []string
		action string
		worker string
		image  string
		reason string
	}{
		{
			name: "legacy positional",
			args: []string{"worker-1", pinned, "manual update"}, action: "update",
			worker: "worker-1", image: pinned, reason: "manual update",
		},
		{
			name: "flags after worker",
			args: []string{"worker-1", "--digest", pinned, "--reason", "manual rollback", "--master=http://master"}, action: "rollback",
			worker: "worker-1", image: pinned, reason: "manual rollback",
		},
		{
			name: "flags before worker",
			args: []string{"--reason=ordered", "--digest=" + pinned, "worker-1"}, action: "update",
			worker: "worker-1", image: pinned, reason: "ordered",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker, image, reason, err := parseImageMutationArgs(tc.action, tc.args)
			if err != nil {
				t.Fatalf("parse args: %v", err)
			}
			if worker != tc.worker || image != tc.image || reason != tc.reason {
				t.Fatalf("parsed = (%q, %q, %q), want (%q, %q, %q)", worker, image, reason, tc.worker, tc.image, tc.reason)
			}
		})
	}
}
func TestResolveTokenAdvanced_EnvPrecedence(t *testing.T) {
	// Env var is consulted before canonicalTokenPaths in
	// resolveTokenAdvanced only when no explicitFile is set;
	// explicit file beats env.
	t.Setenv("VELOX_ADMIN_TOKEN", "env-token")
	got, err := resolveTokenAdvanced("")
	if err != nil {
		t.Fatalf("env token must resolve without error: %v", err)
	}
	if got != "env-token" {
		t.Errorf("resolveTokenAdvanced with empty file returns %q, want %q", got, "env-token")
	}
}
func TestResolveTokenAdvanced_ExplicitFileBeatsEnv(t *testing.T) {
	tmp := t.TempDir()
	tokPath := filepath.Join(tmp, "tok")
	_ = os.WriteFile(tokPath, []byte("file-token"), 0600)
	t.Setenv("VELOX_ADMIN_TOKEN", "env-token")
	got, err := resolveTokenAdvanced(tokPath)
	if err != nil {
		t.Fatalf("explicit file must resolve: %v", err)
	}
	if got != "file-token" {
		t.Errorf("explicit file beats env: got %q, want %q", got, "file-token")
	}
}
func TestParseRolloutArgs_FlagAndPositionalForms(t *testing.T) {
	pinned := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name      string
		args      []string
		image     string
		selection string
		reason    string
		waitReady bool
	}{
		{
			name:  "flags with --digest and --workers and --wait-ready",
			args:  []string{"--digest", pinned, "--workers", "worker-1,worker-2", "--wait-ready"},
			image: pinned, selection: "worker-1,worker-2", reason: "fleetctl-rollout", waitReady: true,
		},
		{
			name:  "equals forms and custom reason",
			args:  []string{"--digest=" + pinned, "--workers=all", "--reason=release-v2"},
			image: pinned, selection: "all", reason: "release-v2", waitReady: false,
		},
		{
			name:  "positional image default selection all",
			args:  []string{pinned, "--serial"},
			image: pinned, selection: "all", reason: "fleetctl-rollout", waitReady: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := parseRolloutArgs(tc.args)
			if err != nil {
				t.Fatalf("parse rollout: %v", err)
			}
			if opts.image != tc.image || opts.selection != tc.selection || opts.reason != tc.reason || opts.waitReady != tc.waitReady {
				t.Fatalf("parsed rollout = %+v, want image=%q selection=%q reason=%q waitReady=%t", opts, tc.image, tc.selection, tc.reason, tc.waitReady)
			}
		})
	}
}
func TestParseRolloutArgs_IgnoresSpaceFormGlobalFlags(t *testing.T) {
	pinned := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	opts, err := parseRolloutArgs([]string{"--digest", pinned, "--master", "http://master:8000", "--token-file", "/tmp/tok", "--verbose"})
	if err != nil {
		t.Fatalf("space-form global flags must be ignored: %v", err)
	}
	if opts.image != pinned || opts.selection != "all" {
		t.Fatalf("parsed rollout = %+v", opts)
	}
	if _, err := parseRolloutArgs([]string{"--digest", pinned, "--master"}); err == nil {
		t.Fatal("--master without a value must be rejected")
	}
}
func TestParseRolloutArgs_RejectsMisuse(t *testing.T) {
	pinned := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	cases := [][]string{
		{},                            // no image
		{"--workers=worker-1"},        // no image
		{pinned, pinned},              // duplicate positional
		{"--digest", pinned, pinned},  // duplicate image
		{"--parallel"},                // serial-only
		{"--digest", pinned, "--wat"}, // unknown flag
	}
	for _, args := range cases {
		if _, err := parseRolloutArgs(args); err == nil {
			t.Errorf("parseRolloutArgs(%v) must return an error", args)
		}
	}
}
