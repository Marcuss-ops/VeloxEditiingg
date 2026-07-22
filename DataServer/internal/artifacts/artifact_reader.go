// Package artifacts / artifact_reader.go
//
// Consumer-owned read-only artifact port. The artifacts domain defines
// the contract it needs; concrete SQL adapters live in
// internal/store and implement this interface.
package artifacts

import (
	"context"

	"velox-server/internal/store"
)

// ArtifactReader is the read-only artifact contract: one method,
// one projection, no tx (statements run on the connection pool).
//
// Invariants:
//   - GetByID returns (nil, nil) when the row is absent so the caller
//     can decide whether absence is a bug or expected (the finalize
//     post-tx wraps nil as a hard error because a successful CAS on
//     the same id guarantees the row exists).
//   - SELECT column list is the canonical *store.Artifact projection;
//     adding verified_at_full / retention_class / etc. happens here
//     in exactly one place.
//
// Preconditions:
//   - id non-empty.
//
// Error behavior:
//   - Empty id                  → fmt.Errorf("artifacts: GetByID: empty id").
//   - Scan error ≠ ErrNoRows    → wrapped ("...: %w").
type ArtifactReader interface {
	GetByID(ctx context.Context, id string) (*store.Artifact, error)
}
