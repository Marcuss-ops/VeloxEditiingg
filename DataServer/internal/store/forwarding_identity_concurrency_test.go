package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"velox-server/internal/forwardingcontract"
)

// TestInsertCreatorForwarding_ConcurrentRetriesConverge verifies the storage
// boundary used by idempotent retries: concurrent attempts for the same
// (source_provider, source_job_id, target_executor_id) tuple must converge on
// one persisted forwarding_id, even when each caller proposes a different
// fresh UUID.
func TestInsertCreatorForwarding_ConcurrentRetriesConverge(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	const callers = 12
	const provider = "remote_engine"
	const sourceJobID = "creator-concurrent-retry"
	const executorID = "scene.composite.v1"

	start := make(chan struct{})
	results := make([]*forwardingcontract.InsertCreatorForwardingResult, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)

	for i := 0; i < callers; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			result, err := db.Forwarding().InsertCreatorForwarding(ctx, &forwardingcontract.CreatorForwarding{
				ForwardingID:     fmt.Sprintf("cf-concurrent-%d", index),
				SourceProvider:   provider,
				SourceJobID:      sourceJobID,
				TargetExecutorID: executorID,
				Status:           string(forwardingcontract.CFStatusPending),
				CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
				UpdatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
			})
			results[index] = result
			errs[index] = err
		}(i)
	}
	close(start)
	wg.Wait()

	var canonicalID string
	createdCount := 0
	for i, result := range results {
		if errs[i] != nil {
			t.Fatalf("concurrent insert %d: %v", i, errs[i])
		}
		if result == nil || result.Forwarding == nil {
			t.Fatalf("concurrent insert %d returned an empty result: %#v", i, result)
		}
		if result.Created {
			createdCount++
		}
		if canonicalID == "" {
			canonicalID = result.Forwarding.ForwardingID
		}
		if result.Forwarding.ForwardingID != canonicalID {
			t.Errorf("concurrent insert %d converged on %q, want %q", i, result.Forwarding.ForwardingID, canonicalID)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1", createdCount)
	}

	persisted, err := db.Forwarding().GetCreatorForwardingBySource(ctx, provider, sourceJobID, executorID)
	if err != nil {
		t.Fatalf("get canonical forwarding: %v", err)
	}
	if persisted == nil || persisted.ForwardingID != canonicalID {
		t.Fatalf("persisted forwarding = %#v, want forwarding_id %q", persisted, canonicalID)
	}
}
