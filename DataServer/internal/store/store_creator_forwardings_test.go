package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func setupForwardingTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cf_lease_test.sqlite")
	dbStore, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { dbStore.Close() })
	return dbStore
}

func insertTestForwarding(t *testing.T, db *SQLiteStore, forwardingID, provider, sourceJobID, executorID, status string) {
	t.Helper()
	cf := &CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   provider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: executorID,
		Status:           status,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.Forwarding().InsertCreatorForwarding(context.Background(), cf); err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
}

func insertTestForwardingWithPayload(t *testing.T, db *SQLiteStore, forwardingID, provider, sourceJobID, executorID, status, payloadJSON, payloadSHA256 string) {
	t.Helper()
	cf := &CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   provider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: executorID,
		Status:           status,
		PayloadJSON:      payloadJSON,
		PayloadSHA256:    payloadSHA256,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.Forwarding().InsertCreatorForwarding(context.Background(), cf); err != nil {
		t.Fatalf("insert forwarding with payload: %v", err)
	}
}
