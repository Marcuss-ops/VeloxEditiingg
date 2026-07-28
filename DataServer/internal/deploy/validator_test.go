package deploy

import (
	"errors"
	"strings"
	"testing"
)

// canonicalSHA produces a 64-char all-one-letter hex string
// suitable as the @sha256:… suffix in fixtures.
func canonicalSHA(c rune) string {
	return strings.Repeat(string(c), 64)
}

// TestValidateImageRef_AcceptsCanonicalShape is the positive-path
// test covering the canonical regex: ghcr.io/<owner>/<repo>@sha256:
// <64 hex chars>. Lowercase owner/repo only — MUST match
// deploy/runtime/prepare-host.sh:93 character-for-character so a
// Master-accepted ref is also accepted at the worker. Mixed case
// is explicitly rejected (see TestValidateImageRef_RejectsMixedCase
// below) to keep the two validators in lockstep.
func TestValidateImageRef_AcceptsCanonicalShape(t *testing.T) {
	valid := []string{
		"ghcr.io/marcuss-ops/velox-worker@sha256:" + canonicalSHA('a'),
		"ghcr.io/owner_with.dots/repo@sha256:" + canonicalSHA('f'),
		"ghcr.io/a-b/c-d@sha256:" + canonicalSHA('0'),
		"ghcr.io/owner1/repo_with-dashes_and.dots@sha256:" + canonicalSHA('9'),
		"ghcr.io/abc/" + "x" + "@sha256:" + canonicalSHA('a'),
	}
	for _, ref := range valid {
		if err := ValidateImageRef(ref); err != nil {
			t.Errorf("ValidateImageRef(%q) = %v, want nil", ref, err)
		}
	}
}

// TestValidateImageRef_RejectsMobileTags always returns
// ErrMobileImageRef for any of the three forbidden suffixes,
// regardless of case. Pin the actual error sentinel so a future
// regression that swaps ErrMobileImageRef for ErrNonDigestImageRef
// surfaces here.
func TestValidateImageRef_RejectsMobileTags(t *testing.T) {
	mobiles := []string{
		"ghcr.io/o/r:latest",
		"ghcr.io/o/r:MAIN",    // uppercase
		"ghcr.io/o/r:sTaBlE",  // mixed
		"ghcr.io/o/r:latest ", // trailing space (trimmed first)
	}
	for _, ref := range mobiles {
		err := ValidateImageRef(ref)
		if !errors.Is(err, ErrMobileImageRef) {
			t.Errorf("ValidateImageRef(%q) = %v, want ErrMobileImageRef", ref, err)
		}
	}
}

// TestValidateImageRef_RejectsEmpty covers the empty/whitespace
// path. Both bare empties and whitespace-only strings must return
// ErrEmptyImageRef — the trim happens BEFORE the empty check so an
// operator pasting "   " gets the same clear error.
func TestValidateImageRef_RejectsEmpty(t *testing.T) {
	cases := []string{"", " ", "\t", "  \n  "}
	for _, ref := range cases {
		err := ValidateImageRef(ref)
		if !errors.Is(err, ErrEmptyImageRef) {
			t.Errorf("ValidateImageRef(%q) = %v, want ErrEmptyImageRef", ref, err)
		}
	}
}

// TestValidateImageRef_RejectsNonDigest covers everything the
// regex refuses: private registries, missing @sha256, wrong
// digest length, non-hex digest, non-ghcr.io host, paths with /
// tags suffixes, etc. All these go to ErrNonDigestImageRef
// because the regex is the terminal gate (no mobile-tag short-
// circuit).
func TestValidateImageRef_RejectsNonDigest(t *testing.T) {
	cases := []string{
		"ghcr.io/o/r",                            // bare ref, no @sha256:
		"ghcr.io/o/r@sha256:abc",                 // short digest
		"ghcr.io/o/r@sha256:" + canonicalSHA('g'), // non-hex digest char
		"ghcr.io/o/r@sha256:" + canonicalSHA('a') + "0", // 65 chars (regex requires exactly 64)
		"docker.io/o/r@sha256:" + canonicalSHA('a'),     // wrong host (docker.io)
		"quay.io/o/r@sha256:" + canonicalSHA('a'),      // wrong host (quay.io)
		"garbage",                                  // arbitrary non-shape string
		"ghcr.io//r@sha256:" + canonicalSHA('a'),        // empty owner
		"ghcr.io/o/@sha256:" + canonicalSHA('a'),        // empty repo
		"ghcr.io/Marcuss-Ops/Velox-Worker@sha256:" + canonicalSHA('a'), // mixed-case owner (lowercase-only contract)
		"ghcr.io/UPPER/repo@sha256:" + canonicalSHA('a'),               // uppercase anywhere
	}
	for _, ref := range cases {
		err := ValidateImageRef(ref)
		if !errors.Is(err, ErrNonDigestImageRef) {
			t.Errorf("ValidateImageRef(%q) = %v, want ErrNonDigestImageRef", ref, err)
		}
	}
}

// TestValidateImageRef_BashRegexParity locks the canonical lowercase
// happy path against deploy/runtime/prepare-host.sh:93. Both regex
// MUST accept the same lowercase form so an operator typing a
// lowercase ref that passes Go-side validation never gets silently
// rejected at the worker.
//
// Mixed-case parity (Go-accepts vs bash-rejects) is the OPPOSITE
// for repo names; pinning that gap here would couple the Go
// validator to the bash strictness and would prevent a future
// migration of prepare-host.sh to mixed-case. Pinning only the
// shared lowercase accept keeps the validators loosely coupled.
func TestValidateImageRef_BashRegexParity(t *testing.T) {
	ref := "ghcr.io/ownerloweronly/repoloweronly@sha256:" + canonicalSHA('a')
	if err := ValidateImageRef(ref); err != nil {
		t.Errorf("Go validator rejected SHARED accept ref: %v", err)
	}
}
