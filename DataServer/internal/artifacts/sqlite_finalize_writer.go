package artifacts

import (
	"context"

	"velox-server/internal/artifactsstore"
	"velox-server/internal/store"
)

// SQLiteFinalizeWriter adapts the artifactsstore leaf finalizer to the
// FinalizationWriter port. It owns the command→params projection and the
// store-error translation at the Service boundary; the leaf owns the
// transaction and all SQL statements.
type SQLiteFinalizeWriter struct {
	inner *artifactsstore.SQLiteArtifactFinalizer
}

// NewSQLiteFinalizeWriter binds the leaf finalizer. The leaf owns the
// transaction and all SQL statements.
func NewSQLiteFinalizeWriter(inner *artifactsstore.SQLiteArtifactFinalizer) *SQLiteFinalizeWriter {
	if inner == nil {
		panic("artifacts: NewSQLiteFinalizeWriter requires a non-nil artifactsstore finalizer")
	}
	return &SQLiteFinalizeWriter{inner: inner}
}

var _ FinalizationWriter = (*SQLiteFinalizeWriter)(nil)

func (w *SQLiteFinalizeWriter) FinalizeVerified(ctx context.Context, cmd FinalizeVerifiedCommand) (*store.Artifact, error) {
	out, err := w.inner.FinalizeVerified(ctx, artifactsstore.FinalizeVerifiedParams{
		UploadID: cmd.UploadID, ArtifactID: cmd.ArtifactID, JobID: cmd.JobID,
		AttemptID: cmd.AttemptID, WorkerID: cmd.WorkerID, LeaseID: cmd.LeaseID,
		AttemptNumber: cmd.AttemptNumber, ExpectedRevision: cmd.ExpectedRevision,
		StorageProvider: cmd.StorageProvider, StorageKey: cmd.StorageKey,
		SHA256: cmd.SHA256, SizeBytes: cmd.SizeBytes, MIMEType: cmd.MIMEType,
		VerifiedAt: cmd.VerifiedAt, DestinationID: cmd.DestinationID,
		AsyncProbe: cmd.AsyncProbe, ExpectedAudioStreams: cmd.ExpectedAudioStreams,
	})
	if err != nil {
		return nil, translateStoreErr(err)
	}
	return out, nil
}
