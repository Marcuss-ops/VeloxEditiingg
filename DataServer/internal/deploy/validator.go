// Package deploy — Step 5/15 fleet-operator surface.
//
// internal/deploy owns the LITERAL invariants every operator-supplied
// deploy request MUST obey before reaching the deployment_records
// ledger:
//
//   - the image ref is parseable and pinned (sha256-only, no
//     :latest/:main/:stable references)
//   - the target_digest, if verified end-to-end, passes a Cosign
//     signature check (the actual verify call is in the cosign
//     sub-package — the validator surface stops at "is the ref
//     typed correctly").
//
// This package is the SINGLE source of the canonical image-ref
// regex. Both the Go-side ValidateImageRef function AND the
// bash-side deploy/runtime/prepare-host.sh validation pull from
// the same shape documented in deploy/validate-master-env.sh:91.
//
// WHAT THIS PACKAGE DOES NOT OWN:
//   - The DB ledger (lives in internal/store/store_deployment_records.go)
//   - The Cosign verifier impl (lives in internal/deploy/cosign/verifier.go)
//   - The drain/resume/rollback state machine (lands in Step 6)
package deploy

import (
	"errors"
	"regexp"
	"strings"
)

// CanonicalImageRefRegex is the canonical API regex for an operator-
// deployable image ref. It accepts a GHCR owner/repository path with
// non-empty lowercase path components and an immutable sha256 digest.
//
// The API is intentionally fail-closed: malformed path components are
// rejected before worker lookup or operation publication. Worker-side
// deployment scripts still enforce the immutable sha256 suffix before pull.
//
// ghcr.io normalises repo paths to lowercase on lookup, so a
// lowercase-only pattern is the right contract. A future widening
// (mixed-case owner/repo) would require a coordinated PR against
// prepare-host.sh's pattern, not a unilateral Go-side change.
//
// This validator is the API boundary; deployment scripts independently
// enforce their worker-host image-pull invariant.
var CanonicalImageRefRegex = regexp.MustCompile(
	`^ghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+(/[a-z0-9._-]+)*@sha256:[a-f0-9]{64}$`,
)

// ErrEmptyImageRef is returned by ValidateImageRef when the input
// is the empty string or whitespace-only. Treated as a 400-class
// error at the API boundary — operators should detect "missing
// image ref" before constructing a deploy record.
var ErrEmptyImageRef = errors.New("image ref cannot be empty")

// ErrMobileImageRef is returned by ValidateImageRef when the input
// terminates in `:latest`, `:main`, or `:stable`. Mobile references
// are forbidden because they remove the immutability guarantee
// that the digest pinning + Cosign chain depends on — the Master
// cannot trust a worker image whose bytes can shift between
// verification and pull.
//
// All case variations (`:latest`, `:LATEST`, `:Main`, etc.) are
// caught — ghcr.io is case-insensitive on the tag, but operators
// may legitimately type their prefered casing.
var ErrMobileImageRef = errors.New(
	"mobile tags (:latest, :main, :stable) are forbidden — only pinned sha256 digests allowed",
)

// ErrNonDigestImageRef is returned by ValidateImageRef when the
// input is non-empty but does not match the canonical regex.
// Catches arbitrary garbage, mistyped refs, refs to private
// registries the Master is not configured to verify, refs without
// @sha256: digests, and refs whose digest length is not exactly 64
// hex chars.
var ErrNonDigestImageRef = errors.New(
	"image ref must be a pinned ghcr.io/<owner>/<repo>@sha256:<64 hex> digest",
)

// mobileTagSentinels short-circuits ValidateImageRef BEFORE the
// regex when the suffix is one of the operator-forbidden mobile
// tags. The regex itself would also reject them (no @sha256:
// suffix), but the short-circuit produces a clearer error message
// and a more obvious failure mode in operator logs.
var mobileTagSentinels = [...]string{
	":latest", ":main", ":stable",
}

// ValidateImageRef checks that `ref` is a non-empty pinned image
// reference. Returns nil on accept; one of ErrEmptyImageRef,
// ErrMobileImageRef, ErrNonDigestImageRef on reject.
//
// Validation order (intentional, surfaces clear errors):
//
//  1. Trim spaces, then empty-check (ErrEmptyImageRef)
//  2. Mobile-tag suffix check (ErrMobileImageRef) — short-circuits
//     before regex; an operator typo of ":latest" deserves a clear
//     error, not "must be pinned sha256"
//  3. Canonical regex match (ErrNonDigestImageRef) — any other
//     shape (private registry, missing @sha256:, typo, garbage)
//     lands here. The 64-hex-char prefix is the digest length
//     guarantee that SHA-256 produces.
//
// The function is pure — no I/O — so the Go caller (the Fleet
// Controller (Step 2) or the repo writer) can validate eagerly
// before opening a transaction. The Cosign signature verify runs
// AFTER this function returns nil; the cosign sub-package owns
// that surface.
func ValidateImageRef(ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ErrEmptyImageRef
	}
	lower := strings.ToLower(ref)
	for _, tag := range mobileTagSentinels {
		if strings.HasSuffix(lower, tag) {
			return ErrMobileImageRef
		}
	}
	if !CanonicalImageRefRegex.MatchString(ref) {
		return ErrNonDigestImageRef
	}
	return nil
}
