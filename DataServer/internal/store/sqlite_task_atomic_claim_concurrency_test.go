package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskgraph"
)

// TestClaimTaskForWorkerAtomic_AlreadyClaimed: two concurrent claims
// race on the same task → one wins, the other gets ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_AlreadyClaimed(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-race", "blender", 4, 0)

	claim := func(workerID, leaseID string) error {
		cmd := taskgraph.ClaimTaskForWorkerCommand{
			TaskID:               "T-claim-race",
			ExpectedTaskRevision: 0,
			WorkerID:             workerID,
			SessionID:            "sess-" + workerID,
			LeaseID:              leaseID,
			ExecutorID:           "blender",
			ExecutorVersion:      4,
			CapabilityRevision:   1,
		}
		_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
		return err
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs <- claim(
				fmt.Sprintf("w-race-%d", idx),
				fmt.Sprintf("L-race-%d", idx),
			)
		}(i)
	}
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, taskgraph.ErrTransitionConflict) ||
			strings.Contains(err.Error(), "database table is locked") {
			conflicts++
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if successes != 1 {
		t.Errorf("concurrent claims: successes=%d; want exactly 1", successes)
	}
	if conflicts != 1 {
		t.Errorf("concurrent claims: conflicts=%d; want exactly 1", conflicts)
	}

	// Verify exactly one LEASED row exists.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE task_id = 'T-claim-race' AND status = 'LEASED'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("LEASED tasks = %d; want 1", count)
	}

	// Verify exactly one PENDING attempt.
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = 'T-claim-race'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("task_attempts rows = %d; want 1", count)
	}
}
