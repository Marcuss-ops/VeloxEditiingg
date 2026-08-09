package enqueue

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/routing"
	"velox-server/internal/store"
)

// TestEnqueue_ConcurrentForwardingRetriesConverge verifies the full retry
// contract, including the TOCTOU window between the idempotency read and the
// atomic insert. Every caller has an independent payload map but the same
// forwarding key; all successful calls must return the same deterministic
// job_id and only one Job/Task graph may be committed.
func TestEnqueue_ConcurrentForwardingRetriesConverge(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "enqueue.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	// This test targets idempotency convergence, not SQLite connection-pool
	// throughput. Pin the fixture to SQLite's single-writer model so the
	// barrier opens one deterministic persistence race without introducing
	// unrelated pool-level lock noise under -race.
	db.DB().SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	seedDestinations(t, db, map[string]bool{"drive-main": true})

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		newTestPlanResolver(),
	)

	const callers = 12
	forwardingKey := routing.FormatForwardingKey(
		"remote_engine", "creator-concurrent-enqueue", "scene.composite.v1",
	).String()
	expectedJobID := DeriveForwardingJobID(forwardingKey)

	start := make(chan struct{})
	ready := make(chan struct{}, callers)
	responses := make([]map[string]interface{}, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(index int) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			payload := map[string]interface{}{
				"video_name":             "Concurrent forwarded video",
				"script_text":            "same canonical forwarded input",
				routing.KeyForwardingKey: forwardingKey,
				"scenes": []interface{}{
					map[string]interface{}{"scene": "intro", "voiceover": "v1"},
				},
				"voiceover_paths": []string{"/tmp/concurrent-forward.mp3"},
				"delivery_plan": []interface{}{
					map[string]interface{}{
						"destination_id": "drive-main",
						"retry_budget":   3,
						"priority":       0,
					},
				},
			}
			responses[index], errs[index] = enq.Enqueue(
				context.Background(), payload, costmodel.DefaultRequirements(),
			)
		}(i)
	}
	for i := 0; i < callers; i++ {
		<-ready
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent retry %d: %v", i, err)
		}
		jobID, _ := responses[i]["job_id"].(string)
		if jobID != expectedJobID {
			t.Errorf("concurrent retry %d job_id = %q, want %q", i, jobID, expectedJobID)
		}
	}

	var jobsCount, tasksCount int
	if err := db.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, expectedJobID,
	).Scan(&jobsCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if err := db.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, expectedJobID,
	).Scan(&tasksCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if jobsCount != 1 || tasksCount != 1 {
		t.Fatalf("persisted graph = jobs:%d tasks:%d, want jobs:1 tasks:1", jobsCount, tasksCount)
	}
}
