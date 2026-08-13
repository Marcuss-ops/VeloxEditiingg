package store

import (
	"velox-server/internal/completionstore"
	"velox-server/internal/repository"
)

// CompletionStore is re-exported from the repository leaf package.
type CompletionStore = repository.CompletionStore

// CompletionTx is re-exported from the repository leaf package.
type CompletionTx = repository.CompletionTx

type CompletionFence = repository.CompletionFence
type CompletionAttemptState = repository.CompletionAttemptState
type CompletionAttemptRow = repository.CompletionAttemptRow
type CompletionDeclareParams = repository.CompletionDeclareParams
type CompletionDeclarationParams = repository.CompletionDeclarationParams
type CompletionUploadState = repository.CompletionUploadState
type CompletionArtifactVerdict = repository.CompletionArtifactVerdict

const (
	CompletionKeepVerifying = repository.CompletionKeepVerifying
	CompletionReady         = repository.CompletionReady
)

type CompletionCommitResult = repository.CompletionCommitResult
type CompletionUploadBinding = repository.CompletionUploadBinding
type CompletionReconcileCandidate = repository.CompletionReconcileCandidate

var (
	ErrCompletionAttemptNotFound    = repository.ErrCompletionAttemptNotFound
	ErrCompletionTransitionConflict = repository.ErrCompletionTransitionConflict
	ErrCompletionCanonicalConflict  = repository.ErrCompletionCanonicalConflict
	ErrCompletionBindingConflict    = repository.ErrCompletionBindingConflict
)

// SQLiteCompletionStore is re-exported from the completionstore package,
// which owns the SQLite completion-protocol implementation.
type SQLiteCompletionStore = completionstore.SQLiteCompletionStore

// NewSQLiteCompletionStore is re-exported from the completionstore package.
var NewSQLiteCompletionStore = completionstore.NewSQLiteCompletionStore
