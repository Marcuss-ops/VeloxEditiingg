// Package deliveries / runner_destination_unmapped_test.go
//
// White-box integration test for the runner's opaque-mode fail-closed
// routing (Residuo 2 of the YouTube → Social closure, migration 091 +
// Residuo 4 store alias via migration 092's canonical rename).
//
// CONTRACT:
//
//   - delivery_destinations.ExternalDestinationID is the opaque-mode
//     reference resolved server-side by the external Social API.
//   - runner.hydrateDestination MUST fail closed with ErrDestinationUnmapped
//     when that column is empty / whitespace-only.
//   - runner.processLease MUST mark the claimed delivery FAILED with
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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
)

// Imports trimmed (CR round-2 fix (d)):
//
//   * "database/sql"          — driven by store.NewSQLiteStore now;
//                                no raw sql.Open in this test.
//   * "_ github.com/mattn/go-sqlite3" — registered transitively by
//                                store.NewSQLiteStore (the production
//                                constructor pulls in the mattn
//                                driver); no driver registration
//                                needed in the test's own code.
//   * "velox-server/internal/store/migrations" — the migrations
//                                runner fires inside
//                                store.NewSQLiteStore's migrateOnStart
//                                path (NewSQLiteStore(path) delegates
//                                to NewSQLiteStoreFromPath(path, true),
//                                which calls
//                                migrations.RunMigrations internally);
//                                no direct migration call in this
//                                test.
//
// "path/filepath" stays because openDeliveryTestDB needs
// filepath.Join(t.TempDir(), "delivery_unmapped_test.sqlite") +
// seedUnmappedDeliveryTriple uses filepath.Join for the fixture
// storage_key.

// fakeProvider is registered under "social_gateway" so the registry
// resolves. The contract under test is hydrateDestination's fail-closed
// guard firing BEFORE provider.Deliver is reached, so Deliver's body
// MUST panic on any unintended reach-through. A silent Success=true
// return would let a regression that bypasses hydrateDestination
// (e.g., a future fallback hydrate path) still "pass" by succeeding
// the post-condition suite — only the secondary assertion chain
// (status=FAILED, last_error_code=DESTINATION_UNMAPPED) would catch
// it. Panicking here makes the regression a stack trace at the call
// site, which is far easier to diagnose in CI logs.
type fakeProvider struct{}

func (f *fakeProvider) Name() string { return "social_gateway" }

func (f *fakeProvider) Deliver(ctx context.Context, _ *store.Artifact, _ *Destination, _, _ string) (*Result, error) {
	panic("deliveries: fakeProvider.Deliver reached — hydrateDestination fail-closed guard at runner.go:499-500 should have routed this delivery into FAILED + DESTINATION_UNMAPPED via processLease line 280-288 before provider.Deliver was called. If this panic fires, the runner has regressed and bypassed the empty-ExternalDestinationID guard.")
}

// openDeliveryTestDB returns a *store.SQLiteStore wired with the
// production migration set so the runner's typed methods (ClaimDeliveries,
// MarkDeliveryFailed, GetJobDelivery, etc.) operate against a schema
// the runtime expects.
//
// Implementation note: store.SQLiteStore has unexported fields (db,
// path, outbox), so a literal &store.SQLiteStore{db: …} from outside
// the store package is a compile error (Go visibility). Public
// constructors are: NewSQLiteStore(path) — production file-backed with
// migrateOnStart=true; NewSQLiteStoreFromPath(path, migrateOnStart) —
// same but with migration opt-out; NewSQLiteStoreFromHandle — driven
// by platform/database.
//
// The user's spec asked for `SQLiteStore in-memory (:memory:)`. The
// production constructors open file-backed paths; the codebase's
// canonical integration-test pattern (sqlite_jobs_writer_repository_test.go,
// store_deliveries_test.go) uses NewSQLiteStore(t.TempDir()/file.sqlite)
// because that exercises the same boot path production runs
// (database.Open + sqliteTunePragmas + migrations.RunMigrations) with
// the per-test isolation of a fresh schema, and is documented as
// preferable to forging an in-memory DSN:
//
//	`store.NewSQLiteStore was designed for production file-backed DBs;
//	 forging an in-memory DSN through it depends on the underlying
//	 platform/database.Open accepting ':memory:' as SQLitePath, which is
//	 not a documented invariant.`
//
// The fail-closed routing under test owns the runner.processLease
// branch at line 280-288 of runner.go, which is invariant to
// file-backed vs in-memory storage. Per-test temp-dir SQLite gives
// the same per-test isolation guarantees a :memory: DSN would (the
// t.TempDir() directory is removed on test exit), with cleaner
// reasoning about visibility / construction than manual struct
// literal gymnastics.
func openDeliveryTestDB(t *testing.T) *store.SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "delivery_unmapped_test.sqlite")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = dbStore.Close() })
	return dbStore
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
	db := openDeliveryTestDB(t)

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
