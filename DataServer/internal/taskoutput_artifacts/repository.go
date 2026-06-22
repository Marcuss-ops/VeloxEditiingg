// Package taskoutput_artifacts persists the worker's
// TaskResult.output_artifacts declarations. The model is intentionally
// narrow: the worker is the authority on which artifacts it produced;
// the artifact upload pipeline later reads this table to validate that
// the bytes uploaded match what the worker promised.
//
// See feat/task-report-ingestion + migration 051 for the schema and
// the audit P1.4 closure rationale.
package taskoutput_artifacts

import (
	"context"
	"errors"
)

// OutputArtifact is a single worker-declared artifact descriptor, captured
// at TaskResult ingestion time. Distinct from the artifacts table (which is
// the master-side VERIFIED record). Cross-reference between the two is
// keyed on (task_id, artifact_id).
type OutputArtifact struct {
	TaskID         string
	AttemptID      string // empty when worker did not declare an attempt_id
	ArtifactID     string
	ArtifactType   string // optional worker-declared type (e.g. "video", "audio")
	DeclaredPath   string // worker-supplied hint; not authoritative
	DeclaredSize   int64
	DeclaredSHA256 string // optional worker-supplied hash; verified by master during upload
	MetadataJSON   string // free-form, JSON-encoded
}

// ErrAlreadyRegistered is returned when the same (task_id, artifact_id)
// pair has already been persisted. Idempotent on replay: a duplicate
// Ingest() for the same TaskResult that retries this table is a no-op,
// and IngestResult.ArtifactsRegistered counts it as a skip (not as an
// insert).
var ErrAlreadyRegistered = errors.New("taskoutput_artifacts: already registered")

// Writer persists output artifact declarations.
//
// The interface is intentionally narrow so the IngestionService has a
// single concrete collaborator to mock in tests. Idempotency constraint:
// Register must be safe to invoke twice for the same (task_id,
// artifact_id); the second call returns ErrAlreadyRegistered (not an
// error condition — Ingest translates it to a counted skip).
type Writer interface {
	// Register inserts an output artifact declaration. Returns
	// ErrAlreadyRegistered when the (task_id, artifact_id) tuple already
	// exists. Any other error is treated as a fatal Ingest failure
	// (the artifact declaration will not be visible to the upload
	// pipeline).
	Register(ctx context.Context, a OutputArtifact) error
}

// Reader exposes lookup of declared output artifacts for forensic
// queries (debugging mismatches between worker promises and upload bytes).
// Not exercised by Ingest; provided for operator tooling.
type Reader interface {
	// ListByTask returns all declarations for a single task, ordered by
	// registration time ascending.
	ListByTask(ctx context.Context, taskID string) ([]OutputArtifact, error)
}

// Repository combines Writer + Reader.
type Repository interface {
	Writer
	Reader
}
