package repository

import (
	"context"
	"time"
)

// UploadSession is the persistent state of one upload.
//
// Receive / Finalize mutate it through UploadRepository.UpdateUploadStatus.
// CreatedAt is server time; ExpiresAt is CreatedAt + uploadTTL.
type UploadSession struct {
	UploadID         string
	ArtifactID       string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	Kind         string
	ExpectedMIME string

	TemporaryStorageKey string

	ExpectedSizeBytes int64
	ExpectedSHA256    string

	ReceivedSizeBytes int64
	ReceivedSHA256    string

	// CREATED | UPLOADING | RECEIVED | FINALIZING | COMPLETED | FAILED | EXPIRED.
	Status    string
	CreatedAt time.Time
	ExpiresAt time.Time
	// Equals zero value when the session has not been completed.
	CompletedAt time.Time
}

// UploadFields lets the caller update a subset of an UploadSession row.
// Each pointer is optional: nil leaves the column untouched. Status is
// required for any UpdateUploadStatus call (state machine must advance).
type UploadFields struct {
	Status            *string
	ReceivedSizeBytes *int64
	ReceivedSHA256    *string
	CompletedAt       *time.Time
}

// ChunkRecord represents one chunk in a chunked upload session.
type ChunkRecord struct {
	UploadID   string
	ChunkIndex int
	SizeBytes  int64
	SHA256     string
	StorageKey string
	ReceivedAt time.Time
}

// UploadRepository is the narrow persistence contract for
// artifact_uploads rows. All methods treat upload_id as the canonical
// key. Application-level invariants (status state machine) live in the
// service layer — SQL CHECK constraints only block blatantly malformed rows.
type UploadRepository interface {
	GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error)
	UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error
	DeleteUploadSession(ctx context.Context, uploadID string) error
	FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error)

	// TransitionUploadStatus atomically CAS-flips status from `from`
	// to `to`. Returns ErrUploadStateInvalid when 0 rows are affected
	// (row missing OR source status doesn't match). Used by Finalize
	// to serialize concurrent finalize callers at the SQL layer.
	TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error

	// GetActiveUploadByJob returns the most recent CREATED/UPLOADING
	// upload session for a job_id. Returns (nil, nil) if none exists.
	GetActiveUploadByJob(ctx context.Context, jobID string) (*UploadSession, error)

	// Chunk methods (chunked upload persistence).
	InsertChunk(ctx context.Context, c ChunkRecord) error
	GetChunk(ctx context.Context, uploadID string, chunkIndex int) (*ChunkRecord, error)
	ListChunks(ctx context.Context, uploadID string) ([]ChunkRecord, error)
	DeleteChunks(ctx context.Context, uploadID string) error
}
