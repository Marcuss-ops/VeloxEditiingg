package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// seedDeliveringJob inserts a job in DELIVERING with one READY artifact
// and the given delivery rows (status, attempt_count, max_attempts).
func seedDeliveringJob(t *testing.T, db *sql.DB, jobID string, old string, deliveries []struct {
	id, status   string
	attempt, max int
	leaseExpires string
}) {
	t.Helper()
	exec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO jobs (job_id, status, created_at, updated_at, migrated_at, revision) VALUES (?, 'DELIVERING', ?, ?, ?, 0)`, jobID, old, old, old)
	exec(`INSERT INTO artifacts (id, job_id, status, created_at) VALUES (?, ?, 'READY', ?)`, "art-"+jobID, jobID, old)
	for _, d := range deliveries {
		// job_deliveries is UNIQUE(artifact_id, destination_id): each fixture
		// delivery gets its own destination so multiple rows per job are legal.
		exec(`INSERT INTO delivery_destinations
			(destination_id, provider, name, configuration_json, created_at, updated_at)
			VALUES (?, 'test', ?, '{}', ?, ?)`,
			"dst-"+d.id, "test-"+d.id, old, old)
		exec(`INSERT INTO job_deliveries (delivery_id, artifact_id, destination_id, status, idempotency_key, created_at, updated_at, attempt_count, max_attempts, lease_expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			d.id, "art-"+jobID, "dst-"+d.id, d.status, d.id, old, old, d.attempt, d.max, d.leaseExpires)
	}
}

func newDeliveryPendingReconciler(t *testing.T, store *SQLiteStore) *DeliveryPendingReconciler {
	t.Helper()
	r, err := NewDeliveryPendingReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	r.SetStaleAfter(6 * time.Hour)
	return r
}

func TestDeliveryPendingReconciler_RollsUpJobToFailed(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-failed", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-ok", status: "SUCCEEDED", attempt: 2, max: 5, leaseExpires: ""},
		{id: "del-ko", status: "FAILED", attempt: 5, max: 5, leaseExpires: ""},
	})

	r := newDeliveryPendingReconciler(t, store)
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var status, reason string
	if err := db.QueryRow(`SELECT status, reconciliation_reason FROM jobs WHERE job_id='job-failed'`).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", status)
	}
	if reason != ReconciliationReasonStaleDeliveryPending {
		t.Fatalf("reconciliation_reason = %q, want %q", reason, ReconciliationReasonStaleDeliveryPending)
	}
}

func TestDeliveryPendingReconciler_RollsUpJobToSucceeded(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-ok", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-ok1", status: "SUCCEEDED", attempt: 1, max: 5, leaseExpires: ""},
		{id: "del-ok2", status: "SUCCEEDED", attempt: 1, max: 5, leaseExpires: ""},
	})

	r := newDeliveryPendingReconciler(t, store)
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-ok'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("status = %q, want SUCCEEDED", status)
	}
}

// TestDeliveryPendingReconciler_KeepsJobWithRunningDelivery pins the
// guard: a DELIVERING job with a non-terminal delivery is never
// rolled up.
func TestDeliveryPendingReconciler_KeepsJobWithRunningDelivery(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-running", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-run", status: "RUNNING", attempt: 1, max: 5, leaseExpires: now.Add(time.Hour).Format(time.RFC3339Nano)},
	})

	r := newDeliveryPendingReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range candidates {
		if c.Kind == DeliveryPendingJob && c.ID == "job-running" {
			t.Fatal("DELIVERING job with a RUNNING delivery must not be a candidate")
		}
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-running'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "DELIVERING" {
		t.Fatalf("status = %q, want DELIVERING (untouched)", status)
	}
}

func TestDeliveryPendingReconciler_FailsBudgetExhaustedDelivery(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-exhaust", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-exhaust", status: "PENDING", attempt: 5, max: 5, leaseExpires: ""},
	})

	r := newDeliveryPendingReconciler(t, store)
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var status, code string
	if err := db.QueryRow(`SELECT status, COALESCE(last_error_code,'') FROM job_deliveries WHERE delivery_id='del-exhaust'`).Scan(&status, &code); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" {
		t.Fatalf("delivery status = %q, want FAILED", status)
	}
	if code != ReconciliationReasonStaleDeliveryPending {
		t.Fatalf("last_error_code = %q, want %q", code, ReconciliationReasonStaleDeliveryPending)
	}
	// The parent job had only this delivery; it must roll up FAILED in
	// the same transaction.
	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-exhaust'`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "FAILED" {
		t.Fatalf("parent job status = %q, want FAILED (rolled up)", jobStatus)
	}
}

// TestDeliveryPendingReconciler_SkipsActiveLease pins the guard: a
// RUNNING delivery with a valid lease is actively processing even when
// it exhausted its budget.
func TestDeliveryPendingReconciler_SkipsActiveLease(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	futureLease := now.Add(time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-lease", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-lease", status: "RUNNING", attempt: 5, max: 5, leaseExpires: futureLease},
	})

	r := newDeliveryPendingReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range candidates {
		if c.Kind == DeliveryPendingDelivery && c.ID == "del-lease" {
			t.Fatal("RUNNING delivery with valid lease must not be a candidate")
		}
	}
}

// TestDeliveryPendingReconciler_SkipsDeliveryWithBudget pins the
// guard: a delivery that still has retry budget is the runner's job.
func TestDeliveryPendingReconciler_SkipsDeliveryWithBudget(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-budget", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-budget", status: "PENDING", attempt: 1, max: 5, leaseExpires: ""},
	})

	r := newDeliveryPendingReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range candidates {
		if c.Kind == DeliveryPendingDelivery && c.ID == "del-budget" {
			t.Fatal("delivery with remaining budget must not be a candidate")
		}
	}
}

func TestDeliveryPendingReconciler_IsIdempotent(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedDeliveringJob(t, db, "job-ido", old, []struct {
		id, status   string
		attempt, max int
		leaseExpires string
	}{
		{id: "del-ido", status: "FAILED", attempt: 5, max: 5, leaseExpires: ""},
	})

	r := newDeliveryPendingReconciler(t, store)
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(reconciliation_version,0) FROM jobs WHERE job_id='job-ido'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("version after first pass = %d, want 1", version)
	}
	if err := r.Reconcile(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var versionAfter int
	if err := db.QueryRow(`SELECT COALESCE(reconciliation_version,0) FROM jobs WHERE job_id='job-ido'`).Scan(&versionAfter); err != nil {
		t.Fatal(err)
	}
	if versionAfter != 1 {
		t.Fatalf("version after second pass = %d, want 1 (idempotent)", versionAfter)
	}
}
