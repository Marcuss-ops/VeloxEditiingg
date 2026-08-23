package completion

import (
	"context"
	"strings"
	"testing"
)

func TestPhase6_Scenario04_AckLoss(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-s04", "attempt-s04")
	jobID := "job-s04"
	seedCompleteUploadFixture(t, db, "up-s04", "art-s04", jobID, strings.Repeat("a", 64))

	plan1, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           fence,
		JobID:           jobID,
		OutputManifests: validManifests(),
	})
	if err != nil {
		t.Fatalf("S04 first DeclareOutputs: %v", err)
	}
	if plan1 == nil || plan1.CommitID == "" {
		t.Fatalf("S04 plan1: nil or empty commit_id")
	}
	rowBefore := readAttemptCommitRow(t, db, fence)

	// Replay DeclareOutputs — must be replay-safe no-op, identical plan.
	plan2, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           fence,
		JobID:           jobID,
		OutputManifests: validManifests(),
	})
	if err != nil {
		t.Fatalf("S04 replay DeclareOutputs: %v", err)
	}
	if plan2.CommitID != plan1.CommitID {
		t.Errorf("S04 replay commit_id diverged: got=%q want=%q (non-deterministic commit_token derivation)",
			plan2.CommitID, plan1.CommitID)
	}
	rowAfter := readAttemptCommitRow(t, db, fence)
	if rowAfter.CommitTokenHash != rowBefore.CommitTokenHash {
		t.Errorf("S04 replay overwrote commit_token_hash: before=%q after=%q",
			rowBefore.CommitTokenHash, rowAfter.CommitTokenHash)
	}

	// Drive a successful CompleteUpload, then replay: the second call
	// against an already-COMPLETED artifact_uploads row is a nil-
	// returning no-op per CompleteUpload's guard in coordinator.go.
	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-s04",
		WorkerSHA256: strings.Repeat("a", 64),
		ServerSHA256: strings.Repeat("a", 64),
	}); err != nil {
		t.Fatalf("S04 first CompleteUpload: %v", err)
	}
	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-s04",
		WorkerSHA256: strings.Repeat("a", 64),
		ServerSHA256: strings.Repeat("a", 64),
	}); err != nil {
		t.Fatalf("S04 replay CompleteUpload must be no-op: err=%v", err)
	}
	if got := readArtifactStatus(t, db, "art-s04"); got != "READY" {
		t.Errorf("S04 artifact status after replay: got=%q want=READY", got)
	}
}
func TestPhase6_Scenarios05_07_WorkerCrash(t *testing.T) {
	cases := []struct {
		name      string
		stopAfter string // "declare" | "progress" | "" (full)
		scenario  string
	}{
		{"s05_worker_crash_post_render", "declare", "S05"},
		{"s06_worker_crash_mid_upload", "progress", "S06"},
		{"s07_worker_crash_post_upload", "", "S07"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := openCoordinatorTestDB(t)
			c := newTestCoordinator(db)
			fence := validFence("task-"+tc.scenario, "attempt-"+tc.scenario)
			jobID := "job-" + tc.scenario
			seedCompleteUploadFixture(t, db, "up-"+tc.scenario, "art-"+tc.scenario, jobID, strings.Repeat("a", 64))

			plan, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
				Fence:           fence,
				JobID:           jobID,
				OutputManifests: validManifests(),
			})
			if err != nil {
				t.Fatalf("%s DeclareOutputs: %v", tc.scenario, err)
			}
			commitID := plan.CommitID

			if tc.stopAfter != "declare" {
				_ = c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
					Fence:         fence,
					UploadID:      "up-" + tc.scenario,
					UploadedBytes: 512,
				})
			}
			if tc.stopAfter == "" {
				if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
					Fence:        fence,
					UploadID:     "up-" + tc.scenario,
					WorkerSHA256: strings.Repeat("a", 64),
					ServerSHA256: strings.Repeat("a", 64),
				}); err != nil {
					t.Fatalf("%s CompleteUpload: %v", tc.scenario, err)
				}
			}

			// Inject: deadline elapsed.
			if _, err := db.Exec(
				`UPDATE attempt_commits SET commit_deadline_at = ? WHERE commit_id = ?`,
				pastRFC3339(), commitID,
			); err != nil {
				t.Fatalf("%s deadline inject: %v", tc.scenario, err)
			}

			res, err := c.ReconcileAttempt(context.Background(), commitID)
			if err != nil {
				t.Fatalf("%s ReconcileAttempt: %v", tc.scenario, err)
			}
			if res == nil || res.CommitID != commitID {
				t.Errorf("%s ReconcileAttempt res: got=%+v want non-nil commit=%q", tc.scenario, res, commitID)
			}
			row := readAttemptCommitRow(t, db, fence)
			if row.Status != "EXPIRED" {
				t.Errorf("%s attempt_commits.status: got=%q want=EXPIRED", tc.scenario, row.Status)
			}
			var outboxCount int
			if err := db.QueryRow(
				`SELECT COUNT(*) FROM outbox_events WHERE event_type = 'commit_protocol.expired' AND aggregate_id = ?`,
				fence.TaskID,
			).Scan(&outboxCount); err != nil {
				t.Fatalf("%s outbox count query: %v", tc.scenario, err)
			}
			if outboxCount != 1 {
				t.Errorf("%s outbox_events count: got=%d want=1", tc.scenario, outboxCount)
			}

			// S07 sanity: artifact already READY from CompleteUpload;
			// reconcile is on the commit row, not the artifact.
			if tc.scenario == "S07" {
				if got := readArtifactStatus(t, db, "art-"+tc.scenario); got != "READY" {
					t.Errorf("S07 artifact status post-reconcile: got=%q want=READY", got)
				}
			}
		})
	}
}
func TestPhase6_Scenarios08_10_MasterCrash(t *testing.T) {
	t.Run("s08_master_crash_during_receive", func(t *testing.T) {
		// Crash during receive: attempt_commits row in DECLARED,
		// no bytes promoted. The worker's retry is a normal
		// declare→progress→complete flow; the protocol converges.
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fence := validFence("task-s08", "attempt-s08")
		jobID := "job-s08"
		seedCompleteUploadFixture(t, db, "up-s08", "art-s08", jobID, strings.Repeat("a", 64))

		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fence,
			JobID:           jobID,
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S08 DeclareOutputs: %v", err)
		}
		row := readAttemptCommitRow(t, db, fence)
		if row.Status != "DECLARED" {
			t.Errorf("S08 pre-crash status: got=%q want=DECLARED", row.Status)
		}

		// Worker resumes heartbeat → upload → completion.
		_ = c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
			Fence:         fence,
			UploadID:      "up-s08",
			UploadedBytes: 256,
		})
		if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
			Fence:        fence,
			UploadID:     "up-s08",
			WorkerSHA256: strings.Repeat("a", 64),
			ServerSHA256: strings.Repeat("a", 64),
		}); err != nil {
			t.Fatalf("S08 CompleteUpload: %v", err)
		}
		if got := readArtifactStatus(t, db, "art-s08"); got != "READY" {
			t.Errorf("S08 artifact status after crash-resume: got=%q want=READY", got)
		}
	})

	t.Run("s09_master_crash_post_blob_pre_db", func(t *testing.T) {
		// Crash after blob promotion but before the COMPLETED write
		// to artifact_uploads + artifacts advances to READY.
		// Simulate: artifact_uploads.status='COMPLETED', received_sha256
		// stamped, but artifact.status still 'STAGING'. Worker's
		// retry CompleteUpload → Branch C → artifact advances to
		// READY.
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fence := validFence("task-s09", "attempt-s09")
		jobID := "job-s09"
		seedCompleteUploadFixture(t, db, "up-s09", "art-s09", jobID, strings.Repeat("a", 64))

		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fence,
			JobID:           jobID,
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S09 DeclareOutputs: %v", err)
		}
		if _, err := db.Exec(
			`UPDATE artifact_uploads SET received_sha256 = ? WHERE upload_id = ?`,
			strings.Repeat("a", 64), "up-s09",
		); err != nil {
			t.Fatalf("S09 partial-promotion inject: %v", err)
		}

		// Worker retry — converges to READY.
		if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
			Fence:        fence,
			UploadID:     "up-s09",
			WorkerSHA256: strings.Repeat("a", 64),
			ServerSHA256: strings.Repeat("a", 64),
		}); err != nil {
			t.Fatalf("S09 retry CompleteUpload: %v", err)
		}
		if got := readArtifactStatus(t, db, "art-s09"); got != "READY" {
			t.Errorf("S09 artifact status after retry: got=%q want=READY", got)
		}
	})

	t.Run("s10_master_crash_post_db_pre_ack", func(t *testing.T) {
		// Crash after the entire COMMIT tx completed on master side
		// but before ack returned. Simulate by completing the happy
		// path. Worker-side replay CompleteUpload against the now-
		// COMPLETED row → nil-returning no-op.
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fence := validFence("task-s10", "attempt-s10")
		jobID := "job-s10"
		seedCompleteUploadFixture(t, db, "up-s10", "art-s10", jobID, strings.Repeat("a", 64))

		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fence,
			JobID:           jobID,
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S10 DeclareOutputs: %v", err)
		}
		if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
			Fence:        fence,
			UploadID:     "up-s10",
			WorkerSHA256: strings.Repeat("a", 64),
			ServerSHA256: strings.Repeat("a", 64),
		}); err != nil {
			t.Fatalf("S10 first CompleteUpload: %v", err)
		}
		// Worker-side replay: nil-returning no-op.
		if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
			Fence:        fence,
			UploadID:     "up-s10",
			WorkerSHA256: strings.Repeat("a", 64),
			ServerSHA256: strings.Repeat("a", 64),
		}); err != nil {
			t.Fatalf("S10 replay CompleteUpload must be no-op: got err=%v", err)
		}
		if got := readArtifactStatus(t, db, "art-s10"); got != "READY" {
			t.Errorf("S10 artifact status after replay: got=%q want=READY", got)
		}
	})
}
