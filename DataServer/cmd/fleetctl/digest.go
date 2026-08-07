// digest.go — Step 5/15 sha256-digest client-side validator per
// design Q5 of the Step 15/15 thinker call.
//
// Matches the regex enforced server-side by deploy.ValidateImageRef
// (Step 5/15) + the fleet-update.yml pre_assert in Step 11/15:
//
//   ^sha256:[0-9a-f]{64}$  → acceptable
//   :latest, :main, :stable → rejected (mobile refs)
//   anything else          → rejected (non-conforming)
//
// Validation runs BEFORE any HTTP call so an operator who
// mistypes `--digest sha=256:...` (or copy-pastes a wrong
// format) gets an immediate exit-7 hint without waiting for
// the Master's response.

package main

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// digestRegex is the canonical sha256-pinned pattern. The
// character class [0-9a-f] deliberately excludes uppercase
// because Cosign's digest output is lowercase by convention;
// rejecting uppercase keeps the regex strict and surfaces
// typos early.
var digestRegex = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// validateDigest returns nil when `d` matches the digestRegex.
// Returns ErrImageInvalid otherwise with a stable error
// message; handlers map to ExitImageInvalid (7).
func validateDigest(d string) error {
	if d == "" {
		return errors.New("digest is empty")
	}
	if !digestRegex.MatchString(d) {
		// Per Q5 design: reject mobile refs explicitly so the
		// operator's brain sees "you typed :latest, not
		// sha256:..." rather than a generic regex mismatch.
		if strings.HasSuffix(d, ":latest") || strings.HasSuffix(d, ":main") ||
			strings.HasSuffix(d, ":stable") || strings.HasSuffix(d, ":latest,") ||
			strings.HasSuffix(d, ":main,") || strings.HasSuffix(d, ":stable,") {
			return fmt.Errorf("digest %q is a mobile ref (:latest/:main/:stable); pin via sha256:64-hex", d)
		}
		if strings.HasPrefix(d, "sha256:") {
			return fmt.Errorf("digest %q has wrong length after 'sha256:' (need exactly 64 lowercase hex)", d)
		}
		return fmt.Errorf("digest %q does not match ^sha256:[0-9a-f]{64}$", d)
	}
	return nil
}

// workerImageRef converts the operator-facing digest into the full immutable
// image reference required by the Master update API. The repository can be
// overridden for non-production environments, while production defaults to
// the canonical GHCR repository.
func workerImageRef(digest string) string {
	repository := strings.TrimRight(strings.TrimSpace(os.Getenv("GHCR_WORKER_REPOSITORY")), "/")
	if repository == "" {
		repository = "ghcr.io/marcuss-ops/velox-worker"
	}
	return repository + "@" + digest
}
