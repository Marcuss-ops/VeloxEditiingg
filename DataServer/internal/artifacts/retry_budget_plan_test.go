package artifacts_test

import (
	"context"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

func TestFinalizeVerified_StampsRetryBudgetFromPlan(t *testing.T) {
	cases := []struct {
		name     string
		plans    []phase5Plan
		expected map[string]int
	}{
		{"single destination retry_budget=3", []phase5Plan{{"primary", 1, 3, true}}, map[string]int{"primary": 3}},
		{"two destinations retry_budget=2 and 5", []phase5Plan{{"primary", 1, 2, true}, {"secondary", 2, 5, true}}, map[string]int{"primary": 2, "secondary": 5}},
		{"retry_budget=1", []phase5Plan{{"primary", 1, 1, true}}, map[string]int{"primary": 1}},
		{"retry_budget=0 uses schema default", []phase5Plan{{"primary", 1, 0, true}}, map[string]int{"primary": 5}},
		{"retry_budget=10", []phase5Plan{{"primary", 1, 10, true}}, map[string]int{"primary": 10}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openPropagationDB(t)
			seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-prop", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-prop", UploadID: "up-prop"})
			seedDeliveryPlans(t, db, "J-prop", c.plans)
			resolver := deliveries.NewSQLiteDeliveryPlanResolver(db)
			runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: "up-prop", ArtifactID: "art-prop", JobID: "J-prop", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-prop/1", SHA256: testSHA256, SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
			for destID, expected := range c.expected {
				var got int
				if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries WHERE artifact_id = ? AND destination_id = ?`, "art-prop", destID).Scan(&got); err != nil {
					t.Fatalf("query %s: %v", destID, err)
				}
				if got != expected {
					t.Errorf("%s: %s max_attempts=%d want %d", c.name, destID, got, expected)
				}
			}
		})
	}
}

func TestFinalizeVerified_MissingPlanFailsClosedWithoutDeliveries(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-no-plan", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-no-plan", UploadID: "up-no-plan", RequestJSON: `{}`})
	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db)
	writer := artifacts.NewSQLiteFinalizeWriter(store.NewSQLiteArtifactFinalizer(db, resolver))
	_, err := writer.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{UploadID: "up-no-plan", ArtifactID: "art-no-plan", JobID: "J-no-plan", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-no-plan/1", SHA256: testSHA256, SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	if err == nil || !errors.Is(err, deliveries.ErrNoExplicitPlan) {
		t.Fatalf("missing explicit plan error = %v, want ErrNoExplicitPlan", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='art-no-plan'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("missing explicit plan created %d deliveries, want 0", count)
	}
}

func TestFinalizeVerified_RenderOnlySucceedsWithoutPlan(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-render-only", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-render-only", UploadID: "up-render-only", RequestJSON: `{"render_only":true}`, Status: "AWAITING_ARTIFACT"})
	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db)
	writer := artifacts.NewSQLiteFinalizeWriter(store.NewSQLiteArtifactFinalizer(db, resolver))
	_, err := writer.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{UploadID: "up-render-only", ArtifactID: "art-render-only", JobID: "J-render-only", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-render-only/1", SHA256: testSHA256, SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='J-render-only'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("job status=%s want SUCCEEDED", status)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='art-render-only'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("render-only created %d deliveries, want 0", count)
	}
}

func TestFinalizeVerified_SingleDestinationDefaultsToFive(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-single", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-single", UploadID: "up-single"})
	seedDeliveryPlans(t, db, "J-single", []phase5Plan{{"primary", 1, 10, true}})
	fin := artifacts.NewSQLiteFinalizeWriter(store.NewSQLiteArtifactFinalizer(db, nil))
	_, err := fin.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{UploadID: "up-single", ArtifactID: "art-single", JobID: "J-single", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, DestinationID: "primary", StorageProvider: "local", StorageKey: "artifacts/J-single/1", SHA256: testSHA256, SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	var got int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, "art-single").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("max_attempts=%d want 5", got)
	}
}

func TestFinalizeVerified_ZeroMaxAttemptsRevertsToDefault(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-zero", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-zero", UploadID: "up-zero"})
	resolver := &zeroBudgetResolver{dests: []artifacts.DeliveryDestination{{DestinationID: "primary", MaxAttempts: 0}}}
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: "up-zero", ArtifactID: "art-zero", JobID: "J-zero", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-zero/1", SHA256: testSHA256, SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	var got int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, "art-zero").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("max_attempts=%d want 5", got)
	}
}
