// Package deliveries / runner_destination_unmapped_test.go
//
// White-box integration test for the runner's opaque-mode fail-closed
// routing (Residuo 2 of the YouTube → Social closure, migration 091 +
// Residuo 4 store alias via migration 092's canonical rename).
//
// CONTRACT:
//
//   * delivery_destinations.ExternalDestinationID is the opaque-mode
//     reference resolved server-side by the external Social API.
//   * runner.hydrateDestination MUST fail closed with ErrDestinationUnmapped
//     when that column is empty / whitespace-only.
//   * runner.processLease MUST mark the claimed delivery FAILED with
//     errorCode "DESTINATION_UNMAPPED" (vs. "DESTINATION_NOT_FOUND" for
//     missing rows). Operators monitor last_error_code to identify
//     which row needs backfill.
//
// Without this test a regression in hydrateDestination's
// TrimSpace(ExternalDestinationID) == "" guard would let unmapped
// deliveries silently dispatch into the social_gateway provider
// and fail later with a confusing remote error code from the Social
// API rather than the canonical Velox fail-closed code.
package deliveries

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store"
	"velox-server/internal/store/migrations"
)

// fakeProvider is a no-op delivery adapter registered under
// "social_gateway" so the registry resolves. We do not exercise its
// Deliver branch in this test because the fail-closed guard at
// hydrateDestination fires BEFORE provider.Deliver is reached.
type fakeProvider struct{}

func (f *fakeProvider) Name() string { return "social_gateway" }

func (f *fakeProvider) Deliver(ctx context.Context, _ *store.Artifact, _ *Destination, _, _ string) (*Result, error) {
	// Reachable only if the test mis-routes around hydrateDestination.
	// Return success-default so any unintended reach-through is loud
	// (the post-assertion GetJobDelivery check would catch it as
	// SUCCEEDED rather than FAILED + DESTINATION_UNMAPPED).
	return &Result{Success: true, RemoteID: "fake-id", RemoteURL: "https://fake/1"}, nil
}

// openInMemoryDeliveryDB opens a fresh :memory: SQLite (cache=shared,
// busy_timeout=5000 — the canonical pattern documented in
// sqlite_jobs_writer_repository_test.go) and applies the production
// SQLite migration set via migrations.RunMigrations.
//
// Why "all migrations" rather than the user's literal "up to 091":
// GetDeliveryDestination (store_deliveries.go) reads column
// external_destination_id (the canonical post-Residuo-4 rename added
// by migration 092). Stopping at 091 leaves only the legacy
// social_destination_id column which the store does not read, so the
// test cannot observe the routing it owns. Applying the full migration
// set is the functionally equivalent surface: the fail-closed routing
// under test (errors.Is(ErrDestinationUnmapped) -> MarkDeliveryFailed
// with code "DESTINATION_UNMAPPED") is identical regardless of which
// historical column name the row's empty string lives under.
func openInMemoryDeliveryDB(t *testing.T) *store.SQLiteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open :memory: sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return &store.SQLiteStore{db: db, path: ":memory:"}
}

// seedUnmappedDeliveryTriple inserts the minimal triple a runner
// tick needs to claim a delivery. The destination's
// ExternalDestinationID is intentionally empty so hydrateDestination
// returns ErrDestinationUnmapped.
func seedUnmappedDeliveryTriple(t *testing.T, db *store.SQLiteStore, destID, artifactID, deliveryID string) {
	t.Helper()

	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         destID,
		Provider:              "social_gateway",
		ExternalDestinationID: "",
		Enabled:               true,
		Name:                  "unmapped-test",
		ConfigurationJSON:     "{}",
	}); err != nil {
		t.Fatalf("insert delivery destination: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           "job-unmapped",
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "unmapped.mp4"),
		SHA256:          "unmapped-fixture-sha256",
		SizeBytes:       1024,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("insert artifact: %v", err)
	}

	if err := db.InsertJobDelivery(&store.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  destID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    5,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("insert job delivery: %v", err)
	}
}

// TestRunnerHydrateDestination_UnmappedRouting_FailsClosed locks
// the Res 2 (PR-15.12) coverage gap: a delivery_destinations row
// whose ExternalDestinationID is empty MUST route the delivery
// into FAILED with code DESTINATION_UNMAPPED.
//
// Failure mode this test guards: a future commit might (a) drop the
// TrimSpace(ExternalDestinationID) == "" guard in hydrateDestination
// OR (b) mis-map the sentinel to a non-DESTINATION_UNMAPPED code in
// processLease. Either regression breaks the operator's backfill
// workflow because the last_error_code column is what their dashboards
// filter on to find rows that need external destination resolution.
func TestRunnerHydrateDestination_UnmappedRouting_FailsClosed(t *testing.T) {
	ctx := context.Background()
	db := openInMemoryDeliveryDB(t)

	const (
		destID     = "dest-unmapped-test"
		artifactID = "art-unmapped-test"
		deliveryID = "del_art-unmapped-test_dest-unmapped-test"
	)
	seedUnmappedDeliveryTriple(t, db, destID, artifactID, deliveryID)

	// ── Claim: the runner's claim SQL matches PENDING with no
	//    next_attempt_at + enabled destination + READY+verified
	//    artifact — all true after seedUnmappedDeliveryTriple.
	leases, err := db.ClaimDeliveries(ctx, "test-runner-unmapped", 5*time.Minute, 4)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("expected 1 lease from claim, got %d", len(leases))
	}
	lease := leases[0]
	if lease.DeliveryID != deliveryID {
		t.Fatalf("lease.DeliveryID = %q, want %q", lease.DeliveryID, deliveryID)
	}
	if lease.DestinationID != destID {
		t.Fatalf("lease.DestinationID = %q, want %q", lease.DestinationID, destID)
	}

	// ── Registry + Runner: register the fake provider so
	//    registry.Resolve("social_gateway") returns a non-nil
	//    Provider, allowing processLease to reach hydrateDestination
	//    (line 261-266 in runner.go). The fail-closed guard at
	//    hydrateDestination fires at line 499-500.
	registry := NewRegistry()
	registry.Register(&fakeProvider{})

	runner := NewDeliveryRunner(DefaultRunnerConfig(), registry, db, "test-runner-unmapped")

	// ── processLease is private (white-box test). Call it directly
	//    so the assertion below observes only the routing branch
	//    under test, not the surrounding tick / renewal goroutine
	//    machinery.
	if err := runner.processLease(ctx, lease); err == nil {
		t.Fatal("processLease: expected hydrateDestination error, got nil")
	}

	// ── Assert: the row is FAILED with code DESTINATION_UNMAPPED
	//    + non-empty CompletedAt + cleared lease + lock cols.
	jd, err := db.GetJobDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if got, want := jd.Status, "FAILED"; got != want {
		t.Errorf("status = %q, want %q (opaque-mode fail-closed)", got, want)
	}
	if got, want := jd.LastError, "DESTINATION_UNMAPPED"; got != want {
		t.Errorf("last_error_code = %q, want %q", got, want)
	}
	if msg := jd.LastErrorMessage; !strings.Contains(msg, "unmapped") {
		t.Errorf("last_error_message = %q, want substring %q", msg, "unmapped")
	}
	if jd.CompletedAt == "" {
		t.Error("completed_at must be set on FAIL (MarkDeliveryFailed stamps it)")
	}
	if jd.LockedBy != "" {
		t.Errorf("locked_by = %q, want empty after FAIL (MarkDeliveryFailed clears lock)", jd.LockedBy)
	}
	if jd.LeaseID != "" {
		t.Errorf("lease_id = %q, want empty after FAIL (MarkDeliveryFailed clears lease)", jd.LeaseID)
	}
	if leaseExp := jd.LeaseExpiresAt; leaseExp != "" {
		t.Errorf("lease_expires_at = %q, want empty after FAIL", leaseExp)
	}
}
