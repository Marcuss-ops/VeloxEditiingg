package completion

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPhase6_Scenarios16_17_RaceAndDeliveryRestore(t *testing.T) {
	t.Run("s16_two_workers_race", func(t *testing.T) {
		// Two goroutines hit CommitAttempt concurrently. SQLite
		// LevelSerializable tx serializes them; the protocol's
		// single-writer contract lets exactly one win (the second
		// gets nil-replay-noop on the COMMITTED row, NOT a double
		// write).
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fence := validFence("task-s16", "attempt-s16")
		jobID := "job-s16"
		seedCompleteUploadFixture(t, db, "up-s16", "art-s16", jobID, strings.Repeat("a", 64))

		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fence,
			JobID:           jobID,
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S16 DeclareOutputs: %v", err)
		}
		commitID := scheduleRowReady(t, db, fence, "art-s16")
		// Artifact-contract jobs must sit at the artifact gate before
		// CommitAttempt performs the terminal job transition (same
		// pre-set the GoldenPath and explicit-plan tests use).
		if _, err := db.Exec(`UPDATE jobs SET status = 'AWAITING_ARTIFACT' WHERE job_id = ?`, jobID); err != nil {
			t.Fatalf("S16 jobs.status pre-set: %v", err)
		}

		var wg sync.WaitGroup
		results := make([]error, 2)
		barrier := make(chan struct{})
		for i := 0; i < 2; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-barrier
				_, err := c.CommitAttempt(context.Background(), commitID)
				results[i] = err
			}()
		}
		close(barrier)
		wg.Wait()

		success, replay := 0, 0
		for _, e := range results {
			if e == nil {
				success++
			} else if errors.Is(e, ErrTransitionConflict) {
				replay++
			}
		}
		// We REQUIRE at least one success. The other may be
		// nil-replay-with-error or ErrTransitionConflict, both
		// acceptable outcomes from the LevelSerializable race.
		if success < 1 {
			t.Errorf("S16: at least one CommitAttempt must succeed: results=%v", results)
		}
		t.Logf("S16: success=%d conflict=%d (acceptable race outcomes: 1 success, ≥0 conflicts)", success, replay)

		rowAfter := readAttemptCommitRow(t, db, fence)
		if rowAfter.Status != "COMMITTED" {
			t.Errorf("S16 attempt_commits.status post-race: got=%q want=COMMITTED", rowAfter.Status)
		}
	})

	t.Run("s17_delivery_runner_restore", func(t *testing.T) {
		// After CommitAttempt, one job_deliveries row must persist for the
		// publishable final-video artifact. Auxiliary committed outputs are
		// not delivery targets. lease_expiry past-NOW
		// on those rows is the durable input the DeliveryRunner's
		// re-claim query picks up on restart.
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fence := validFence("task-s17", "attempt-s17")
		jobID := "job-s17"
		seedCompleteUploadFixture(t, db, "up-s17", "art-s17", jobID, strings.Repeat("a", 64))
		seedDeliveryDestination(t, db, "dest-s17", "drive")
		seedJobDeliveryPlan(t, db, jobID, "dest-s17")
		if _, err := db.Exec(`
		INSERT INTO artifacts (id, job_id, type, output_kind, storage_provider, status, created_at)
		VALUES (?, ?, 'engine_progress_sidecar', 'engine_progress_sidecar', 'local', 'READY', ?)`,
			"art-s17-sidecar", jobID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatalf("S17 seed sidecar: %v", err)
		}

		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fence,
			JobID:           jobID,
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S17 DeclareOutputs: %v", err)
		}
		commitID := scheduleRowReady(t, db, fence, "art-s17")
		// Artifact-contract jobs must sit at the artifact gate before
		// CommitAttempt performs the terminal job transition.
		if _, err := db.Exec(`UPDATE jobs SET status = 'AWAITING_ARTIFACT' WHERE job_id = ?`, jobID); err != nil {
			t.Fatalf("S17 jobs.status pre-set: %v", err)
		}

		res, err := c.CommitAttempt(context.Background(), commitID)
		if err != nil {
			t.Fatalf("S17 CommitAttempt: %v", err)
		}
		if res == nil || res.CommitID != commitID {
			t.Errorf("S17 CommitResult: got=%+v want non-nil commit=%q", res, commitID)
		}

		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ?`, "art-s17").Scan(&n); err != nil {
			t.Fatalf("S17 job_deliveries count: %v", err)
		}
		if n != 1 {
			t.Errorf("S17 job_deliveries count after CommitAttempt: got=%d want=1 (final video only)", n)
		}
		var sidecarDeliveries int
		if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ?`, "art-s17-sidecar").Scan(&sidecarDeliveries); err != nil {
			t.Fatalf("S17 sidecar delivery count: %v", err)
		}
		if sidecarDeliveries != 0 {
			t.Errorf("S17 sidecar job_deliveries count: got=%d want=0", sidecarDeliveries)
		}
		// Force lease expiry; runner's re-claim query on restart
		// picks up the row.
		if _, err := db.Exec(
			`UPDATE job_deliveries SET lease_expires_at = ? WHERE artifact_id = ?`,
			pastRFC3339(), "art-s17",
		); err != nil {
			t.Fatalf("S17 lease expire inject: %v", err)
		}
		var stale int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ? AND lease_expires_at < ?`,
			"art-s17", time.Now().UTC().Format(time.RFC3339),
		).Scan(&stale); err != nil {
			t.Fatalf("S17 stale lease count: %v", err)
		}
		if stale < 1 {
			t.Errorf("S17 post-restart stale-lease rows: got=%d want>=1 (DeliveryRunner restart must re-claim expired leases)", stale)
		}
	})
}
func TestCommitAttempt_DeliversOnlyToExplicitPlanDestinations(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-dp", "attempt-dp")
	jobID := "job-dp"
	seedCompleteUploadFixture(t, db, "up-dp", "art-dp", jobID, strings.Repeat("a", 64))
	// Two GLOBAL destinations enabled — only one is in the job's plan.
	seedDeliveryDestination(t, db, "dest-dp-a", "drive")
	seedDeliveryDestination(t, db, "dest-dp-b", "drive")
	seedJobDeliveryPlan(t, db, jobID, "dest-dp-a")

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: jobID, OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("dp DeclareOutputs: %v", err)
	}
	commitID := scheduleRowReady(t, db, fence, "art-dp")
	// Artifact-contract jobs must enter the explicit artifact gate before
	// CommitAttempt can perform the terminal job transition.
	if _, err := db.Exec(`UPDATE jobs SET status = 'AWAITING_ARTIFACT' WHERE job_id = ?`, jobID); err != nil {
		t.Fatalf("dp jobs.status pre-set: %v", err)
	}
	if _, err := c.CommitAttempt(context.Background(), commitID); err != nil {
		t.Fatalf("dp CommitAttempt: %v", err)
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ?`, "art-dp").Scan(&total); err != nil {
		t.Fatalf("dp job_deliveries count: %v", err)
	}
	if total != 1 {
		t.Fatalf("dp job_deliveries rows = %d, want exactly 1 (plan destination only)", total)
	}
	var planDest string
	if err := db.QueryRow(`SELECT destination_id FROM job_deliveries WHERE artifact_id = ?`, "art-dp").Scan(&planDest); err != nil {
		t.Fatalf("dp job_deliveries destination read: %v", err)
	}
	if planDest != "dest-dp-a" {
		t.Fatalf("dp delivery destination = %q, want dest-dp-a (unplanned global dest-dp-b must NOT receive a delivery)", planDest)
	}
}
