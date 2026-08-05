// Package artifacts / retry.go — idempotent quarantine orchestration.
// SQL persistence is owned by internal/store.
package artifacts

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/store"
)

// ErrArtifactAlreadyQuarantined is returned when a concurrent reconciler
// already moved the artifact out of READY.
var ErrArtifactAlreadyQuarantined = errors.New("reconciler: artifact already terminal")

// ErrQuarantineStatusOnly is retained for compatibility with stats consumers;
// the store repository now commits the status and outbox event atomically, so
// new code does not produce this state.
var ErrQuarantineStatusOnly = errors.New("reconciler: quarantine status committed but outbox event deferred")

func (r *Reconciler) quarantineArtifactTx(ctx context.Context, artifactID, reason string) error {
	if artifactID == "" {
		return fmt.Errorf("reconciler: quarantineArtifactTx: empty artifactID")
	}
	if err := r.artifactRepo.QuarantineReadyArtifact(ctx, artifactID, reason, r.clock.Now()); err != nil {
		if errors.Is(err, store.ErrArtifactAlreadyQuarantined) {
			return ErrArtifactAlreadyQuarantined
		}
		return err
	}
	return nil
}
