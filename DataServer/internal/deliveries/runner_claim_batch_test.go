package deliveries

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/store"
)

// fastDriveTestProvider is a monolithic "drive" provider that succeeds
// immediately, so a tick can drain a burst without touching the real
// Drive adapter or the credential vault.
type fastDriveTestProvider struct {
	delivered int32
}

func (p *fastDriveTestProvider) Name() string { return "drive" }

func (p *fastDriveTestProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	atomic.AddInt32(&p.delivered, 1)
	return &Result{Success: true, RemoteID: "drive-burst-remote", RemoteURL: "https://drive.example/burst"}, nil
}

// seedDriveDeliveryTriple inserts the minimal triple (enabled "drive"
// destination + READY verified artifact + PENDING delivery) that a runner
// tick needs to claim one delivery.
func seedDriveDeliveryTriple(t *testing.T, db *store.SQLiteStore, destID, artifactID, deliveryID, jobID string) {
	t.Helper()

	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         destID,
		Provider:              "drive",
		ExternalDestinationID: "ext-" + destID,
		Enabled:               true,
		Name:                  "drive-burst-test",
		ConfigurationJSON:     "{}",
	}); err != nil {
		t.Fatalf("insert delivery destination: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           jobID,
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "burst.mp4"),
		SHA256:          "burst-fixture-sha256",
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

// TestTick_ClaimBatchExceedsConcurrency_AbsorbsBurst proves the amended
// P0-02 contract: ClaimBatch may exceed Concurrency because a queued lease
// is renewed while waiting on the semaphore. Six PENDING deliveries are
// claimed in one tick (ClaimBatch=6) despite Concurrency=2, and every one
// of them is drained to SUCCEEDED without deadlock.
func TestTick_ClaimBatchExceedsConcurrency_AbsorbsBurst(t *testing.T) {
	ctx := context.Background()
	db := openDeliveryTestDB(t)

	const n = 6
	for i := 0; i < n; i++ {
		seedDriveDeliveryTriple(t, db,
			fmt.Sprintf("dest-burst-%d", i),
			fmt.Sprintf("art-burst-%d", i),
			fmt.Sprintf("delivery-burst-%d", i),
			fmt.Sprintf("job-burst-%d", i),
		)
	}

	provider := &fastDriveTestProvider{}
	registry := NewRegistry()
	registry.Register(provider)

	runner := NewDeliveryRunner(&RunnerConfig{
		PollInterval:    time.Second,
		LeaseDuration:   time.Minute,
		ClaimBatch:      n, // 6 > Concurrency(2)
		Concurrency:     2,
		MaxAttempts:     3,
		BackoffSchedule: []time.Duration{0},
	}, registry, db.Delivery(), db, "claim-batch-runner")

	done := make(chan struct{})
	go func() {
		_ = runner.tick(ctx)
		close(done)
	}()

	select {
	case <-done:
		// No deadlock.
	case <-time.After(30 * time.Second):
		t.Fatal("tick did not complete (semaphore may not have been released)")
	}

	if got := int(atomic.LoadInt32(&provider.delivered)); got != n {
		t.Fatalf("provider delivered %d, want %d", got, n)
	}

	// All n deliveries must be SUCCEEDED (not claimable): a second claim
	// from a different runner returns zero.
	remaining, err := db.ClaimDeliveries(ctx, "second-runner", time.Minute, 100)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 claimable deliveries after tick, got %d", len(remaining))
	}
}
