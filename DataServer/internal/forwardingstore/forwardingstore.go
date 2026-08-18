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
	"database/sql"
	"time"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/storecore"
)

// SQLiteForwardingStore implements the creator_forwardings persistence
// surface against a *sql.DB.
type SQLiteForwardingStore struct {
	db *sql.DB
}

// NewSQLiteForwardingStore wraps an existing *sql.DB as a
// SQLiteForwardingStore.
func NewSQLiteForwardingStore(db *sql.DB) *SQLiteForwardingStore {
	if db == nil {
		panic("store: NewSQLiteForwardingStore requires a non-nil *sql.DB")
	}
	return &SQLiteForwardingStore{db: db}
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
