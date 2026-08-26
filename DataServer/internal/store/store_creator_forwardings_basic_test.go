package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/forwardingstore"
)

func TestInsertAndGetForwarding(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-001", "openai", "creator-job-1", "scene.composite.v1", "PENDING")

	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-001")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING", cf.Status)
	}
	if cf.SourceProvider != "openai" {
		t.Errorf("source_provider = %q, want openai", cf.SourceProvider)
	}
	if cf.AttemptCount != 0 {
		t.Errorf("attempt_count = %d, want 0", cf.AttemptCount)
	}
}

func TestInsertForwarding_Idempotent(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	// First insert
	insertTestForwarding(t, db, "cf-idem", "openai", "creator-job-2", "scene.composite.v1", "PENDING")

	// Second insert with same unique key should be ignored
	cf2 := &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf-idem-2",
		SourceProvider:   "openai",
		SourceJobID:      "creator-job-2",
		TargetExecutorID: "scene.composite.v1",
		Status:           "PENDING",
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, cf2); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	// First record should still exist
	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-idem")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.ForwardingID != "cf-idem" {
		t.Errorf("forwarding_id = %q, want cf-idem", cf.ForwardingID)
	}

	// Second record should NOT exist (ignored by UNIQUE)
	_, err = db.Forwarding().GetCreatorForwarding(ctx, "cf-idem-2")
	if err != forwardingstore.ErrCreatorForwardingNoRow {
		t.Errorf("expected forwardingstore.ErrCreatorForwardingNoRow, got %v", err)
	}
}

func TestGetForwardingBySource(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-src", "openai", "creator-job-3", "scene.composite.v1", "PENDING")

	cf, err := db.Forwarding().GetCreatorForwardingBySource(ctx, "openai", "creator-job-3", "scene.composite.v1")
	if err != nil {
		t.Fatalf("GetCreatorForwardingBySource: %v", err)
	}
	if cf.ForwardingID != "cf-src" {
		t.Errorf("forwarding_id = %q, want cf-src", cf.ForwardingID)
	}
}

func TestGetForwardingMising(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	_, err := db.Forwarding().GetCreatorForwarding(ctx, "nonexistent")
	if err != forwardingstore.ErrCreatorForwardingNoRow {
		t.Errorf("expected forwardingstore.ErrCreatorForwardingNoRow, got %v", err)
	}
}
