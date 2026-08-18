// Package forwardingstore is the SQLite persistence for creator_forwardings.
// It was split out of the internal/store god-package: the domain model
// (status vocabulary + row/lease shapes) lives in internal/forwardingcontract,
// the business state machine lives in internal/forwarding and
// internal/creatorflow, and this package owns only the SQLite SQL/CAS.
//
// It depends on forwardingcontract and storecore (the shared DB primitive),
// never on internal/store — internal/store re-exports its surface as a
// compatibility facade.
package forwardingstore

import (
	"context"
	"database/sql"
	"time"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/jobs"
	"velox-server/internal/storecore"
	"velox-server/internal/taskgraph"
)

// JobTaskTxCreator is the cross-domain injection point for the atomic
// forwarding transaction. AtomicForwardAndEnqueue must create the Job+Task+
// TaskSpec rows inside the SAME transaction as the forwarding CAS, so this
// leaf accepts a narrow Tx-scoped creator instead of importing the store
// god-package (which would create a store ↔ forwardingstore cycle).
// store.AtomicJobTaskCreator implements it.
type JobTaskTxCreator interface {
	CreateJobWithTaskTx(ctx context.Context, tx *sql.Tx, job *jobs.Job, taskSpec *taskgraph.TaskSpec, priority int) error
}

// SQLiteForwardingStore implements the creator_forwardings persistence
// surface against a *sql.DB.
type SQLiteForwardingStore struct {
	db             *sql.DB
	jobTaskCreator JobTaskTxCreator
}

// NewSQLiteForwardingStore wraps an existing *sql.DB as a
// SQLiteForwardingStore.
func NewSQLiteForwardingStore(db *sql.DB) *SQLiteForwardingStore {
	if db == nil {
		panic("store: NewSQLiteForwardingStore requires a non-nil *sql.DB")
	}
	return &SQLiteForwardingStore{db: db}
}

// WithJobTaskCreator injects the cross-domain Job+Task creator used by
// AtomicForwardAndEnqueue. Without it the atomic enqueue fails closed,
// because the forwarding CAS and the Job+Task+TaskSpec INSERTs must share
// one transaction (see forwardingstore_atomic.go).
func (s *SQLiteForwardingStore) WithJobTaskCreator(c JobTaskTxCreator) *SQLiteForwardingStore {
	if s == nil {
		return s
	}
	s.jobTaskCreator = c
	return s
}

// nowRFC3339 returns the current UTC time formatted for a store timestamp
// column (second precision, RFC3339). Mirrors the store-package helper so the
// leaf's timestamp serialization stays identical.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// nullIfEmpty returns nil for empty strings, otherwise the string itself.
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

type creatorForwardingRowScanner interface {
	Scan(dest ...any) error
}

func scanCreatorForwarding(row creatorForwardingRowScanner) (*forwardingcontract.CreatorForwarding, error) {
	var cf forwardingcontract.CreatorForwarding
	err := row.Scan(
		&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
		&cf.TargetExecutorID, &cf.TargetJobID,
		&cf.PayloadJSON, &cf.PayloadSHA256,
		&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
		&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt, &cf.LastRemoteStatus,
		&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
		&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
		&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
	)
	if err == sql.ErrNoRows {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("scan creator forwarding", err)
	}
	return &cf, nil
}

// scanCreatorForwardingWithExternalClient is kept separate from the legacy
// scanner because older forwarding queries intentionally select the original
// column set. New ownership-sensitive reads select external_client_id
// explicitly and normalize NULL legacy rows to the empty string.
func scanCreatorForwardingWithExternalClient(row creatorForwardingRowScanner) (*forwardingcontract.CreatorForwarding, error) {
	var cf forwardingcontract.CreatorForwarding
	err := row.Scan(
		&cf.ForwardingID, &cf.ExternalClientID,
		&cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
		&cf.TargetExecutorID, &cf.TargetJobID,
		&cf.PayloadJSON, &cf.PayloadSHA256,
		&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
		&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt, &cf.LastRemoteStatus,
		&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
		&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
		&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
	)
	if err == sql.ErrNoRows {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("scan creator forwarding with client", err)
	}
	return &cf, nil
}
