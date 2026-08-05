package artifacts

import (
	"context"

	"velox-server/internal/store"
)

// COMPATIBILITY:
// Owner:        P0.4 artifacts-store migration
// Remove after: 2026-09-30
// Read-only:    yes (delegates to the store finalizer; no second write path)
type SQLiteFinalizeWriter struct {
	inner *store.SQLiteArtifactFinalizer
}

// NewSQLiteFinalizeWriter supports both existing (db, reader, resolver)
// callers and the store-owned finalizer form.
func NewSQLiteFinalizeWriter(inner *store.SQLiteArtifactFinalizer) *SQLiteFinalizeWriter {
	if inner == nil {
		panic("artifacts: NewSQLiteFinalizeWriter requires a non-nil store finalizer")
	}
	return &SQLiteFinalizeWriter{inner: inner}
}

var _ FinalizationWriter = (*SQLiteFinalizeWriter)(nil)

func (w *SQLiteFinalizeWriter) FinalizeVerified(ctx context.Context, cmd FinalizeVerifiedCommand) (*store.Artifact, error) {
	out, err := w.inner.FinalizeVerified(ctx, store.FinalizeVerifiedParams{
		UploadID: cmd.UploadID, ArtifactID: cmd.ArtifactID, JobID: cmd.JobID,
		AttemptID: cmd.AttemptID, WorkerID: cmd.WorkerID, LeaseID: cmd.LeaseID,
		AttemptNumber: cmd.AttemptNumber, ExpectedRevision: cmd.ExpectedRevision,
		StorageProvider: cmd.StorageProvider, StorageKey: cmd.StorageKey,
		SHA256: cmd.SHA256, SizeBytes: cmd.SizeBytes, MIMEType: cmd.MIMEType,
		VerifiedAt: cmd.VerifiedAt, DestinationID: cmd.DestinationID,
	})
	if err != nil {
		return nil, translateStoreErr(err)
	}
	return out, nil
}
