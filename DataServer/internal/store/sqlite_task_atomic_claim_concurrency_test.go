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
	"velox-shared/contract/domain"
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
	infrastructure := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		if errors.Is(err, taskgraph.ErrTransitionConflict) {
			conflicts++
			continue
		}
		// SQLite may reject the losing writer before its CAS executes.
		// That is a driver/infrastructure failure, not an application
		// conflict; the store must expose it as the canonical typed
		// infrastructure error rather than relying on message parsing.
		if derr, ok := domain.AsDomainError(err); ok && derr.Code == domain.CodeInfrastructure {
			infrastructure++
			continue
		}
		if strings.Contains(err.Error(), "database table is locked") {
			// Compatibility for an unclassified driver error from an
			// older test/driver combination.
			infrastructure++
			continue
		}
		t.Errorf("unexpected error: %v", err)
	}
	// A shared in-memory SQLite can reject both simultaneous writers
	// before either reaches its CAS, even with busy_timeout configured.
	// That is an infrastructure outcome, not a duplicate claim. Retry
	// once outside the contended window so the test still verifies that
	// exactly one task is ultimately leased and one attempt is created.
	recoveredAfterLock := false
	if successes == 0 && infrastructure == 2 && conflicts == 0 {
		if retryErr := claim("w-race-retry", "L-race-retry"); retryErr == nil {
			successes = 1
			recoveredAfterLock = true
		} else {
			t.Errorf("retry after simultaneous SQLite lock failures: %v", retryErr)
		}
	}
	if successes != 1 {
		t.Errorf("concurrent claims: successes=%d; want exactly 1", successes)
	}
	if !recoveredAfterLock && conflicts+infrastructure != 1 {
		t.Errorf("concurrent claims: losers=%d (conflicts=%d infrastructure=%d); want exactly 1", conflicts+infrastructure, conflicts, infrastructure)
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
