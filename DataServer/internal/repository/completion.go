package repository

import (
	"context"
	"errors"
	"time"
)

// CompletionStore is the persistence boundary for the artifact completion
// protocol. Application packages submit typed operations; the store package
// owns the SQLite connection, transaction lifecycle, SQL, and row projections.
type CompletionStore interface {
	Run(ctx context.Context, fn func(CompletionTx) error) error
	ListCompletionUploadBindings(ctx context.Context, commitID string) ([]CompletionUploadBinding, error)
	GetCompletionUploadBinding(ctx context.Context, uploadID string) (*CompletionUploadBinding, error)
	BindCompletionUpload(ctx context.Context, declarationID, uploadID, artifactID string) error
	GetCompletionCommitTokenHash(ctx context.Context, commitID string) (string, error)
	ScanCompletionCandidates(ctx context.Context, now, deadlineCutoff, progressCutoff, outboxCutoff string, limit int) ([]CompletionReconcileCandidate, int64, error)
}

// CompletionTx is a transaction-bound, typed repository surface. The caller
// never receives *sql.Tx and cannot accidentally commit only part of the
// completion protocol.
type CompletionTx interface {
	ReadCompletionFence(ctx context.Context, fence CompletionFence, allowMissing bool) (*CompletionAttemptState, error)
	InsertCompletionAttempt(ctx context.Context, p CompletionDeclareParams) (string, error)
	InsertCompletionDeclaration(ctx context.Context, p CompletionDeclarationParams) error
	GetCompletionDeclarationID(ctx context.Context, commitID, outputKind, logicalName string) (string, error)
	GetCompletionUploadState(ctx context.Context, uploadID string) (*CompletionUploadState, error)
	CompleteCompletionUpload(ctx context.Context, verdict CompletionArtifactVerdict, uploadID, serverSHA, now string) error
	StampCompletionArtifact(ctx context.Context, artifactID, storageKey, sha string, size int64) error
	UpdateCompletionProgress(ctx context.Context, commitID, now, deadline string) (int64, error)
	UpdateCompletionUploadedBytes(ctx context.Context, fence CompletionFence, uploadID string, uploadedBytes int64, now string) error
	UpdateCompletionReadyCount(ctx context.Context, fence CompletionFence, now string) error
	ExpireCompletionAttempt(ctx context.Context, fence CompletionFence, now string) error
	ExpireCompletionAttemptByID(ctx context.Context, commitID, now string) error
	MarkCompletionCommitted(ctx context.Context, commitID, now string) error
	MarkCompletionTaskAttemptSucceeded(ctx context.Context, attemptID, workerID, leaseID, now string) error
	MarkCompletionTaskSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, now string) error
	MarkCompletionJobSucceededIfTasksDone(ctx context.Context, jobID, now string) error
	InsertCompletionDeliveries(ctx context.Context, jobID, now string) error
	InsertCompletionOutbox(ctx context.Context, eventID, aggregateType, aggregateID, eventType, payloadJSON, now string) error
	FindCompletionAttempt(ctx context.Context, commitID string) (*CompletionAttemptRow, error)
	GetCompletionResult(ctx context.Context, commitID string) (*CompletionCommitResult, error)
}

type CompletionFence struct {
	TaskID, AttemptID, WorkerID, LeaseID string
	Revision                             int
}

type CompletionAttemptState struct {
	CommitID     string
	Status       string
	TaskRevision int
}

type CompletionAttemptRow struct {
	CommitID, TaskID, AttemptID, JobID, WorkerID, LeaseID, Status, CommitDeadlineAt string
	RequiredOutputCount, ReadyOutputCount                                           int
}

type CompletionDeclareParams struct {
	CommitID, TaskID, AttemptID, JobID, WorkerID, LeaseID string
	Revision, RequiredOutputCount                         int
	TokenHash, Deadline, Now                              string
}

type CompletionDeclarationParams struct {
	DeclarationID, CommitID, TaskID, AttemptID string
	OutputKind, LogicalName, MimeType          string
	SizeBytes                                  int64
	SHA256, WorkerSpoolKey, Now                string
}

type CompletionUploadState struct {
	UploadID, ExpectedSHA256, ReceivedSHA256, Status string
	ArtifactID, TemporaryStorageKey, MimeType        string
	SizeBytes                                        int64
}

type CompletionArtifactVerdict int

const (
	CompletionKeepVerifying CompletionArtifactVerdict = iota
	CompletionReady
)

type CompletionCommitResult struct {
	CommitID, TaskID, AttemptID, JobID, TaskStatus, JobStatus string
	ArtifactIDs                                               []string
	CommittedAt                                               *time.Time
}

type CompletionUploadBinding struct {
	DeclarationID, CommitID, UploadID, ArtifactID string
	TaskID, AttemptID, WorkerID, LeaseID          string
	Revision                                      int
	OutputKind, LogicalName                       string
}

type CompletionReconcileCandidate struct {
	CommitID, Case string
}

var (
	ErrCompletionAttemptNotFound    = errors.New("store: completion attempt not found")
	ErrCompletionTransitionConflict = errors.New("store: completion transition conflict")
	ErrCompletionCanonicalConflict  = errors.New("store: completion canonical artifact conflict")
	ErrCompletionBindingConflict    = errors.New("store: completion upload binding conflict")
)
