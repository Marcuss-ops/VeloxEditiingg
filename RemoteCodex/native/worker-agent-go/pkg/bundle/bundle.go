// Package bundle reconciles the worker bundle identity (cfg.BundleHash)
// against the on-disk build artefact (BUNDLE_HASH.txt).
//
// RW-PROD-003 §3 A8: a worker that boots with a cfg.BundleHash that
// diverges from the BUNDLE_HASH.txt that physically accompanies the
// binary on disk has definitely been mis-deployed. Silent acceptance
// would let a tampered or incomplete bundle enter the production pool.
//
// The fundamental invariant: cfg.BundleHash is content-addressed; it
// must equal the SHA class hash of the bundle that the binary expects
// to be running on. BundleVersion is metadata-only and is NOT compared
// here — version labels without content hashes cannot detect tampering.
//
// All errors emitted by this package carry the stable code
// "bundle_version_mismatch" so the JSON report (RW-PROD-003 §6) and
// downstream alerting can grep for it without parsing prose.
package bundle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// BundleHashFilename is the canonical name of the bundle-identity file
// produced by the build pipeline. The composition root (cmd/velox-worker-agent/main.go)
// already reads it via readTextFileFirst(cfg.WorkDir, "BUNDLE_HASH.txt")
// to populate cfg.BundleHash — pkg/bundle re-reads it FRESH at boot so
// a mid-flight re-deploy (e.g. via ansible pull) is caught.
const BundleHashFilename = "BUNDLE_HASH.txt"

// VersionFilename is the sibling metadata file. It is NOT content-
// addressed and is therefore NOT compared by BundleHashMatches.
const VersionFilename = "VERSION.txt"

// LocateBundleHash searches the canonical candidate paths under
// baseDir and returns the FIRST existing BUNDLE_HASH.txt content
// (trimmed) along with the absolute path that produced it. The path
// search mirrors cmd/velox-worker-agent/main.go's readTextFileFirst:
//
//	<baseDir>/BUNDLE_HASH.txt
//	<baseDir>/versions/current/BUNDLE_HASH.txt
//	/opt/velox/BUNDLE_HASH.txt
//
// Returns ErrNotFound if no candidate exists — a CRITICAL fail-closed
// condition because a worker that boots without a bundle identity
// cannot reconcile against anything.
func LocateBundleHash(baseDir string) (hash string, path string, err error) {
	candidates := canonicalCandidates(baseDir, BundleHashFilename)
	seen := make(map[string]bool)
	for _, c := range candidates {
		abs, abErr := filepath.Abs(c)
		if abErr != nil {
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		data, rErr := os.ReadFile(abs)
		if rErr != nil {
			if errors.Is(rErr, os.ErrNotExist) {
				continue
			}
			return "", "", fmt.Errorf("bundle.LocateBundleHash: read %s: %w", c, rErr)
		}
		v := strings.TrimSpace(string(data))
		if v == "" {
			// An empty BUNDLE_HASH.txt is treated the same as a missing one:
			// a blank hash silently matches any empty-string cfg.BundleHash,
			// which would re-introduce the very mismatch class we are
			// protecting against. Skip + continue to the next candidate.
			continue
		}
		return v, abs, nil
	}
	return "", "", ErrNotFound
}

// ErrNotFound is returned (wrapped) when no BUNDLE_HASH.txt exists in any
// of the canonical candidate paths. This is a CRITICAL fail-closed
// condition — ops must install a complete bundle before the worker can
// boot. BundleHashMatches wraps this in a "bundle_version_mismatch"
// error so the surface-level code stays stable for dashboarding.
var ErrNotFound = errors.New("BUNDLE_HASH.txt not found in canonical candidates")

// BundleHashMatches returns nil iff expected matches the on-disk
// BUNDLE_HASH.txt content under baseDir. Otherwise it returns an error
// carrying the stable code "bundle_version_mismatch" so the runtime
// report (RW-PROD-003 §6) and downstream alerting can grep the code
// without parsing prose.
//
// Comparison rules:
//
//   - Empty expected ⇒ mismatch (the operator must declare a hash).
//   - File missing under all candidates ⇒ mismatch (operator must install).
//   - File present but blank ⇒ mismatch (skipped at LocateBundleHash).
//   - expected != actual ⇒ mismatch (cfg was overridden via VELOX_BUNDLE_HASH
//     but the on-disk hash tells a different story).
//
// This function NEVER compares against bundleVersion or VERSION.txt.
// BundleVersion is metadata-only and cannot detect tampering.
func BundleHashMatches(expected string, baseDir string) error {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return fmt.Errorf("bundle_version_mismatch: cfg.BundleHash is empty for worker in %q — operator must declare VELOX_BUNDLE_HASH or install BUNDLE_HASH.txt", baseDir)
	}
	actual, actualPath, err := LocateBundleHash(baseDir)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("bundle_version_mismatch: no BUNDLE_HASH.txt under %q — operator must install a complete bundle", baseDir)
		}
		return fmt.Errorf("bundle_version_mismatch: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("bundle_version_mismatch: cfg=%q disk=%q path=%q (RW-PROD-003 A8; re-deploy the bundle or revert VELOX_BUNDLE_HASH)",
			expected, actual, actualPath)
	}
	return nil
}

// CanonicalCandidates returns the absolute candidate paths used by both
// LocateBundleHash and the composition root's readTextFileFirst. Exposed
// for tests so the location logic has a single home.
func CanonicalCandidates(baseDir, filename string) []string {
	return canonicalCandidates(baseDir, filename)
}

func canonicalCandidates(baseDir, filename string) []string {
	if baseDir == "" {
		baseDir = "/opt/velox"
	}
	return []string{
		filepath.Join(baseDir, filename),
		filepath.Join(baseDir, "versions", "current", filename),
		filepath.Join("/opt/velox", filename),
	}
}
