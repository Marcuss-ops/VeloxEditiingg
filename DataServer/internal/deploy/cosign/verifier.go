// Package cosign — Step 5/15 fleet-operator surface.
//
// Cosign verification of an operator-supplied image ref. This
// package owns the EXTERNAL verifier implementation (shells to
// the `cosign verify` CLI) and the canonical interface every
// code path that needs to confirm a digest↔signature pair MUST
// satisfy.
//
// IMPORTANT: the canonical Cosign verify at container pull time
// lives in deploy/runtime/prepare-host.sh:174-189 (worker side).
// The Master uses THIS verifier to:
//
//  1. Validate an operator-supplied ref BEFORE persisting it to
//     the deployment_records ledger. The Fleet Controller (Step 2)
//     will call cosign.Verify(ref) before InsertDeploymentRecord.
//  2. Optionally verify previously-recorded target_digests during
//     dashboard queries — operator trust surface, not the security
//     boundary. The worker-side verify remains authoritative, this
//     is a redundant check operators can read on the dashboard.
//
// The Master does NOT pull images; it only validates string refs
// against the registry's transparency-log entries. The external
// binary is the simplest verifier surface — it inherits the same
// OIDC keyless config that prepare-host.sh uses, so the same
// keyless flow that authorises a deploy on the worker also
// authorises a ref validation on the Master.
package cosign

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"velox-server/internal/config"
)

// ErrSkippedByOverride is returned when VELOX_SKIP_COSIGN_VERIFY=1
// is set together with a non-empty VELOX_COSIGN_OVERRIDE_REASON.
// This mirrors the production override path in
// deploy/runtime/prepare-host.sh — incident-response override for
// when the cosign infra is down and the operator needs to deploy
// anyway. MUST be logged + audit-trailed by callers; the security
// model collapses under the override.
var ErrSkippedByOverride = errors.New("cosign verification skipped by VELOX_SKIP_COSIGN_VERIFY=1 override")

// ErrBinaryMissing is returned when the configured cosign binary
// cannot be located on PATH or at the configured path. This is
// distinct from a verify FAILURE — the diagnosis is "the verify
// infrastructure is not present" rather than "the signature
// didn't match", and the operator should treat each differently.
var ErrBinaryMissing = errors.New("cosign CLI not found at configured path or on PATH")

// ErrOverrideReasonMissing is returned when the emergency bypass is
// requested without the required audit reason. The bypass must never be
// silently enabled by a bare environment flag.
var ErrOverrideReasonMissing = errors.New("cosign override requires VELOX_COSIGN_OVERRIDE_REASON")

const (
	// cosignIdentityRegexp must match the identity used by the worker-image
	// GitHub Actions workflow in the canonical repository.
	cosignIdentityRegexp = `^https://github.com/Marcuss-ops/VeloxEditiingg/\.github/workflows/worker-image\.yml@refs/(tags/worker-v.+|heads/.+)`
	cosignOIDCIssuer     = "https://token.actions.githubusercontent.com"
)

// CosignVerifier abstracts the verify surface so callers (the
// Fleet Controller (Step 2) and admin/dashboard paths) can mock
// or stub the implementation in tests without shelling out to
// the real cosign binary at every unit test.
type CosignVerifier interface {
	// Verify checks the OIDC keyless signature on `ref`. Returns
	// nil on success, an error wrapping the case distinction
	// (skipped by override, binary missing, verified-failed) on
	// failure. The exact error string is logged by the caller for
	// the audit trail; structured error types are exposed as
	// sentinel values (ErrSkippedByOverride, ErrBinaryMissing)
	// for tests and metrics.
	Verify(ctx context.Context, ref string) error
}

// ExternalCosignVerifier shells out to a `cosign` binary. It is
// the production-default implementation. The binary path defaults
// to "cosign" (resolved against PATH); callers in production can
// override it for environments that pin cosign to a known path.
type ExternalCosignVerifier struct {
	// BinaryPath is the absolute or PATH-resolvable path of the
	// cosign binary. Empty string means "look up cosign on PATH".
	BinaryPath string

	// VerifyTimeout caps a single verify call. The cosign CLI
	// can hang on a registry that's slow to fetch transparency
	// log entries, so we cap at 30s by default.
	VerifyTimeout time.Duration
}

// NewExternalCosignVerifier returns an ExternalCosignVerifier
// with the canonical defaults. Timeout=30s matches the operator-
// friendly floor; prepare-host.sh:174-189 has no explicit timeout
// and relies on operator supervision, which is incompatible with
// a server-side deadline.
func NewExternalCosignVerifier() *ExternalCosignVerifier {
	return &ExternalCosignVerifier{
		BinaryPath:    "",
		VerifyTimeout: 30 * time.Second,
	}
}

// Verify shells to `cosign verify <ref>` and returns nil on a 0
// exit code, the wrapped stderr on a non-zero exit, or one of
// ErrSkippedByOverride / ErrBinaryMissing for the override /
// missing-binary cases.
//
// VELOX_SKIP_COSIGN_VERIFY=1 short-circuits to ErrSkippedByOverride
// only when VELOX_COSIGN_OVERRIDE_REASON is non-empty; otherwise it
// returns ErrOverrideReasonMissing. The binary is never invoked by
// the override path.
func (e *ExternalCosignVerifier) Verify(ctx context.Context, ref string) error {
	if config.Getenv("VELOX_SKIP_COSIGN_VERIFY") == "1" {
		reason := strings.TrimSpace(config.Getenv("VELOX_COSIGN_OVERRIDE_REASON"))
		if reason == "" {
			return ErrOverrideReasonMissing
		}
		return fmt.Errorf("%w: reason=%s", ErrSkippedByOverride, reason)
	}
	binary := e.BinaryPath
	if binary == "" {
		binary = "cosign"
	}
	path, lookErr := exec.LookPath(binary)
	if lookErr != nil {
		return fmt.Errorf("%w: %v (binary=%q)", ErrBinaryMissing, lookErr, binary)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, e.VerifyTimeout)
	defer cancel()

	cmd := exec.CommandContext(timeoutCtx, path,
		"verify",
		"--certificate-identity-regexp="+cosignIdentityRegexp,
		"--certificate-oidc-issuer="+cosignOIDCIssuer,
		ref,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &bytes.Buffer{} // discard stdout — only exit code matters

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cosign verify failed for %q (rc=%v): %s", ref, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// StubVerifier returns nil for every ref. It is the test-only
// implementation used in unit tests that exercise the
// VELOX_SKIP_COSIGN_VERIFY=1-style "I trust this would pass"
// semantics without invoking a real binary.
//
// NEVER use this in production — the security model collapses.
type StubVerifier struct{}

// Verify satisfies CosignVerifier with a no-op success.
func (StubVerifier) Verify(_ context.Context, _ string) error { return nil }
