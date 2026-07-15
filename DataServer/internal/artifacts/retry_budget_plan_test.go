package artifacts_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
	"velox-server/internal/deliveries"
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
		{"retry_budget=0 falls back", []phase5Plan{{"primary", 1, 0, true}}, map[string]int{"primary": 5}},
		{"retry_budget=10", []phase5Plan{{"primary", 1, 10, true}}, map[string]int{"primary": 10}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openPropagationDB(t)
			seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-prop", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-prop", UploadID: "up-prop"})
			seedDeliveryPlans(t, db, "J-prop", c.plans)
			resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, false)
			runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: "up-prop", ArtifactID: "art-prop", JobID: "J-prop", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-prop/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
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

func TestFinalizeVerified_FallbackMaxAttemptsWhenNoResolver(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-fb", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-fb", UploadID: "up-fb"})
	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, true)
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: "up-fb", ArtifactID: "art-fb", JobID: "J-fb", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-fb/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	rows, err := db.Query(`SELECT destination_id, max_attempts FROM job_deliveries WHERE artifact_id = ? ORDER BY destination_id`, "art-fb")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var d string
		var m int
		if err := rows.Scan(&d, &m); err != nil {
			t.Fatal(err)
		}
		count++
		if m != 5 {
			t.Errorf("fallback %s max_attempts=%d want 5", d, m)
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 fallback rows, got %d", count)
	}
}

func TestFinalizeVerified_SingleDestinationDefaultsToFive(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-single", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-single", UploadID: "up-single"})
	seedDeliveryPlans(t, db, "J-single", []phase5Plan{{"primary", 1, 10, true}})
	reader := artifacts.NewSQLiteArtifactReader(db)
	fin := artifacts.NewSQLiteFinalizeWriter(db, reader, nil)
	_, err := fin.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{UploadID: "up-single", ArtifactID: "art-single", JobID: "J-single", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, DestinationID: "primary", StorageProvider: "local", StorageKey: "artifacts/J-single/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
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
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: "up-zero", ArtifactID: "art-zero", JobID: "J-zero", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-zero/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	var got int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, "art-zero").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("max_attempts=%d want 5", got)
	}
}
