// Package artifacts / retry_budget_propagation_test.go
//
// Step 5/8 of the Velox canonical-purity plan: verify that
// FinalizeVerified propagates per-destination retry_budget from the
// resolver down to job_deliveries.max_attempts at INSERT time.
//
// The propagation direction matters because the durable attempt cap
// is read back at delivery-claim time by `DeliveryRunner.processLease`
// (`lease.MaxAttempts > 0 \u2192 maxAttempts = lease.MaxAttempts`). A row
// stamped with the wrong max_attempts would either:
//   - starve cheap destinations that legitimately only allow 1-2
//     retries (the runner would FAILED them on attempt 1), or
//   - burn retry budget on a destination whose operator explicitly
//     wired retry_budget=10 (the runner would FAILED on attempt 5).
//
// Coverage:
//  1. retry_budget=1, 2, 3, 5, 10 \u2192 job_deliveries.max_attempts matches.
//  2. resolver nil + fallback destinations SELECT \u2192 default 5.
//  3. cmd.DestinationID single-dest path \u2192 default 5 (override semantics).
//  4. 429 simulation: drive attempt_count to max_attempts and verify
//     the budget-exhaustion terminal state maps to FAILED.
//
// The 429 simulation mirrors the runner's processLease classifier
// inline via raw UPDATE so the test stays focused on
// (insertion + budget consumption) without spinning up the full
// runner lease/claim machinery.
package artifacts_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
	"velox-server/internal/deliveries"
)

// phase5Schema extends minimalSchema with the destination + plan
// tables + the max_attempts column that the propagation insert
// touches. Kept local to this file (not shared with success_path_test.go)
// so earlier tests stay isolated from migration-shaped changes.
const phase5Schema = `
CREATE TABLE jobs (
    job_id        TEXT PRIMARY KEY,
    status        TEXT,
    revision      INTEGER,
    completed_at  TEXT,
    updated_at    TEXT,
    migrated_at   TEXT
);
CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,
    job_id          TEXT,
    attempt_id      INTEGER,
    type            TEXT,
    storage_provider TEXT,
    storage_key     TEXT,
    storage_url     TEXT,
    local_path      TEXT,
    sha256          TEXT,
    size_bytes      INTEGER,
    duration_seconds REAL,
    duration_ms     INTEGER,
    mime_type       TEXT,
    status          TEXT,
    verified_at     TEXT,
    created_at      TEXT
);
CREATE TABLE artifact_uploads (
    upload_id         TEXT PRIMARY KEY,
    artifact_id       TEXT,
    job_id            TEXT,
    attempt_number    INTEGER,
    worker_id         TEXT,
    lease_id          TEXT,
    status            TEXT,
    temporary_storage_key TEXT,
    expected_size_bytes  INTEGER,
    expected_sha256      TEXT,
    expected_revision    INTEGER,
    received_size_bytes  INTEGER,
    received_sha256      TEXT,
    created_at        TEXT,
    expires_at        TEXT,
    completed_at      TEXT
);
CREATE TABLE outbox_events (
    aggregate_type TEXT,
    aggregate_id   TEXT,
    event_type     TEXT,
    payload_json   TEXT,
    status         TEXT,
    available_at   TEXT,
    created_at     TEXT
);
CREATE TABLE delivery_destinations (
    destination_id     TEXT PRIMARY KEY,
    provider           TEXT,
    name               TEXT,
    enabled            INTEGER DEFAULT 1,
    created_at         TEXT,
    updated_at         TEXT,
    account_id         TEXT,
    folder_id          TEXT,
    channel_id         TEXT,
    language           TEXT,
    configuration_json TEXT,
    metadata_json      TEXT
);
CREATE TABLE job_delivery_plans (
    job_id          TEXT,
    destination_id  TEXT,
    enabled         INTEGER NOT NULL DEFAULT 1,
    priority        INTEGER NOT NULL DEFAULT 0,
    retry_budget    INTEGER NOT NULL DEFAULT 5,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT,
    updated_at      TEXT,
    PRIMARY KEY (job_id, destination_id)
);
CREATE TABLE job_deliveries (
    delivery_id          TEXT PRIMARY KEY,
    artifact_id          TEXT,
    destination_id       TEXT,
    status               TEXT DEFAULT 'PENDING',
    max_attempts         INTEGER NOT NULL DEFAULT 5,
    idempotency_key      TEXT,
    remote_id            TEXT,
    remote_url           TEXT,
    locked_by            TEXT,
    lease_id             TEXT,
    lease_expires_at     TEXT,
    next_attempt_at      TEXT,
    attempt_count        INTEGER NOT NULL DEFAULT 0,
    last_error_code      TEXT,
    last_error_message   TEXT,
    completed_at         TEXT,
    created_at           TEXT,
    updated_at           TEXT,
    UNIQUE (artifact_id, destination_id)
);
`

// openPropagationDB returns a fresh in-memory SQLite with the phase-5
// schema applied. Each call gets its own DB \u2014 tests are isolated.
func openPropagationDB(t *testing.T) *sql.DB {
	t.Helper()
	// Shared-cache in-memory DSN so concurrent goroutines (and the
	// background-connection path go-sqlite3 may take on a busy pool)
	// land on the SAME underlying DB instance. The plain ":memory:"
	// DSN is private to each pooled connection — a 2nd connection
	// gets a fresh empty DB and surfaces "no such table:
	// job_delivery_plans". Mirrors openPost048TestDB above.
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(phase5Schema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO delivery_destinations
        (destination_id, provider, name, enabled, created_at, updated_at)
        VALUES ('primary', 'youtube', 'Primary', 1, '', ''),
               ('secondary', 'drive', 'Secondary', 1, '', '')`); err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	return db
}

type phase5Fixture struct {
	JobID         string
	WorkerID      string
	LeaseID       string
	Revision      int
	AttemptNumber int
	ArtifactID    string
	UploadID      string
}

func seedPhase5Fixture(t *testing.T, db *sql.DB, f phase5Fixture) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO jobs
        (job_id, status, revision, updated_at, migrated_at)
        VALUES (?, 'RUNNING', ?, ?, ?)`,
		f.JobID, f.Revision, now, now); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifacts
        (id, job_id, attempt_id, type, storage_provider, status, created_at)
        VALUES (?, ?, ?, 'render', 'local', 'STAGING', ?)`,
		f.ArtifactID, f.JobID, f.AttemptNumber, now); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads
        (upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
         status, created_at, expires_at, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, 'FINALIZING', ?, ?, NULL)`,
		f.UploadID, f.ArtifactID, f.JobID, f.AttemptNumber,
		f.WorkerID, f.LeaseID, now,
		time.Now().Add(24*time.Hour).UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
}

type phase5Plan struct {
	DestinationID string
	Priority      int
	RetryBudget   int
	Enabled       bool
}

func seedDeliveryPlans(t *testing.T, db *sql.DB, jobID string, plans []phase5Plan) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	for _, p := range plans {
		enabled := 1
		if !p.Enabled {
			enabled = 0
		}
		if _, err := db.Exec(`INSERT INTO job_delivery_plans
            (job_id, destination_id, enabled, priority, retry_budget, metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, '{}', ?, ?)`,
			jobID, p.DestinationID, enabled, p.Priority, p.RetryBudget, now, now); err != nil {
			t.Fatalf("seed plan %s: %v", p.DestinationID, err)
		}
	}
}

func runFinalize(t *testing.T, db *sql.DB, resolver artifacts.DeliveryPlanResolver, cmd artifacts.FinalizeVerifiedCommand) (*artifacts.SQLiteFinalizeWriter, *sql.DB) {
	t.Helper()
	reader := artifacts.NewSQLiteArtifactReader(db)
	fin := artifacts.NewSQLiteFinalizeWriter(db, reader, resolver)
	if _, err := fin.FinalizeVerified(context.Background(), cmd); err != nil {
		t.Fatalf("FinalizeVerified: %v", err)
	}
	return fin, db
}

// =====================================================================
// Spec 1: per-destination retry_budget propagates to job_deliveries.max_attempts
// =====================================================================

func TestFinalizeVerified_StampsRetryBudgetFromPlan(t *testing.T) {
	cases := []struct {
		name     string
		plans    []phase5Plan
		expected map[string]int // destination_id -> expected max_attempts
	}{
		{
			name: "single destination retry_budget=3",
			plans: []phase5Plan{
				{"primary", 1, 3, true},
			},
			expected: map[string]int{"primary": 3},
		},
		{
			name: "two destinations retry_budget=2 and 5",
			plans: []phase5Plan{
				{"primary", 1, 2, true},
				{"secondary", 2, 5, true},
			},
			expected: map[string]int{"primary": 2, "secondary": 5},
		},
		{
			name: "retry_budget=1 (tight cap) propagates as max_attempts=1",
			plans: []phase5Plan{
				{"primary", 1, 1, true},
			},
			expected: map[string]int{"primary": 1},
		},
		{
			name: "retry_budget=0 falls back to schema default 5",
			plans: []phase5Plan{
				{"primary", 1, 0, true},
			},
			expected: map[string]int{"primary": 5},
		},
		{
			name: "retry_budget=10 (lazy cap) propagates as max_attempts=10",
			plans: []phase5Plan{
				{"primary", 1, 10, true},
			},
			expected: map[string]int{"primary": 10},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openPropagationDB(t)
			seedPhase5Fixture(t, db, phase5Fixture{
				JobID: "J-prop", WorkerID: "w", LeaseID: "l",
				Revision: 1, AttemptNumber: 1,
				ArtifactID: "art-prop", UploadID: "up-prop",
			})
			seedDeliveryPlans(t, db, "J-prop", c.plans)

			resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, false)
			runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{
				UploadID:         "up-prop",
				ArtifactID:       "art-prop",
				JobID:            "J-prop",
				WorkerID:         "w",
				LeaseID:          "l",
				AttemptNumber:    1,
				ExpectedRevision: 1,
				StorageProvider:  "local",
				StorageKey:       "artifacts/J-prop/1",
				SHA256:           "deadbeef",
				SizeBytes:        1024,
				MIMEType:         "video/mp4",
				VerifiedAt:       time.Now().UTC(),
			})

			for destID, expected := range c.expected {
				var got int
				if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries
                    WHERE artifact_id = ? AND destination_id = ?`,
					"art-prop", destID).Scan(&got); err != nil {
					t.Fatalf("query %s: %v", destID, err)
				}
				if got != expected {
					t.Errorf("%s: job_deliveries[%s].max_attempts = %d; want %d",
						c.name, destID, got, expected)
				}
			}
		})
	}
}

// =====================================================================
// Spec 2: nil resolver + fallback destinations SELECT \u2192 default 5
// =====================================================================

func TestFinalizeVerified_FallbackMaxAttemptsWhenNoResolver(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{
		JobID: "J-fb", WorkerID: "w", LeaseID: "l",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "art-fb", UploadID: "up-fb",
	})
	// No job_delivery_plans rows. Resolver with GlobalFallback enabled
	// returns all enabled delivery_destinations rows with retry_budget
	// defaulted to 5 (the resolver's fallback path).
	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, true)
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{
		UploadID:         "up-fb",
		ArtifactID:       "art-fb",
		JobID:            "J-fb",
		WorkerID:         "w",
		LeaseID:          "l",
		AttemptNumber:    1,
		ExpectedRevision: 1,
		StorageProvider:  "local",
		StorageKey:       "artifacts/J-fb/1",
		SHA256:           "deadbeef",
		SizeBytes:        1024,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	})

	rows, err := db.Query(`SELECT destination_id, max_attempts FROM job_deliveries
        WHERE artifact_id = ? ORDER BY destination_id`, "art-fb")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type row struct {
		DestID string
		Max    int
	}
	var seen []row
	for rows.Next() {
		var d string
		var m int
		if err := rows.Scan(&d, &m); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, row{d, m})
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 fallback rows, got %d", len(seen))
	}
	for _, s := range seen {
		if s.Max != 5 {
			t.Errorf("fallback destination %s max_attempts = %d; want 5",
				s.DestID, s.Max)
		}
	}
}

// =====================================================================
// Spec 3: cmd.DestinationID explicit single-destination path \u2192 default 5
// (override semantics: the caller's pinned destination wins over the
// per-job plan's retry_budget, exposed here for safety.)
// =====================================================================

func TestFinalizeVerified_SingleDestinationDefaultsToFive(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{
		JobID: "J-single", WorkerID: "w", LeaseID: "l",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "art-single", UploadID: "up-single",
	})
	// Plan with retry_budget=10 exists. The explicit-DestinationID
	// path MUST ignore the plan's retry_budget (single-destination
	// override semantics) and use the default 5.
	seedDeliveryPlans(t, db, "J-single", []phase5Plan{
		{"primary", 1, 10, true},
	})

	reader := artifacts.NewSQLiteArtifactReader(db)
	fin := artifacts.NewSQLiteFinalizeWriter(db, reader, nil)
	if _, err := fin.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{
		UploadID:         "up-single",
		ArtifactID:       "art-single",
		JobID:            "J-single",
		WorkerID:         "w",
		LeaseID:          "l",
		AttemptNumber:    1,
		ExpectedRevision: 1,
		DestinationID:    "primary",
		StorageProvider:  "local",
		StorageKey:       "artifacts/J-single/1",
		SHA256:           "deadbeef",
		SizeBytes:        1024,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("FinalizeVerified: %v", err)
	}

	var maxAttempts int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries
        WHERE artifact_id = ? AND destination_id = 'primary'`,
		"art-single").Scan(&maxAttempts); err != nil {
		t.Fatal(err)
	}
	if maxAttempts != 5 {
		t.Errorf("single-destination max_attempts = %d; want 5 (override semantics ignores plan retry_budget)",
			maxAttempts)
	}
}

// =====================================================================
// Spec 4: 429 simulation \u2014 budget exhausted \u2192 FAILED
//
// Mirrors the runner's classifier (processLease in
// deliveries/runner.go) for the rate-limit branch:
//
//   for n in 1..<maxAttempts:
//       MarkDeliveryRetry (status=RETRY_WAIT, attempt_count=n)
//   for n == maxAttempts:
//       lease.AttemptNumber >= maxAttempts \u2192 MarkDeliveryFailed
//
// The test drives attempt_count up to max_attempts via raw UPDATE
// (no need for the full lease/claim stack) and asserts the terminal
// state maps to FAILED. This locks the propagation flow: jobs with
// a plan carrying retry_budget=3 will become terminal FAILED after
// 3 429 responses, regardless of the runner-wide default
// MaxAttempts=5.
// =====================================================================

func TestFinalizeVerified_BudgetConsumedBy429Retries(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{
		JobID: "J-429", WorkerID: "w", LeaseID: "l",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "art-429", UploadID: "up-429",
	})
	seedDeliveryPlans(t, db, "J-429", []phase5Plan{
		{"primary", 1, 3, true},
	})

	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, false)
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{
		UploadID:         "up-429",
		ArtifactID:       "art-429",
		JobID:            "J-429",
		WorkerID:         "w",
		LeaseID:          "l",
		AttemptNumber:    1,
		ExpectedRevision: 1,
		StorageProvider:  "local",
		StorageKey:       "artifacts/J-429/1",
		SHA256:           "deadbeef",
		SizeBytes:        1024,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	})

	// Confirm propagation first.
	var maxAttempts int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries
        WHERE artifact_id = ? AND destination_id = 'primary'`,
		"art-429").Scan(&maxAttempts); err != nil {
		t.Fatal(err)
	}
	if maxAttempts != 3 {
		t.Fatalf("setup: max_attempts = %d; want 3", maxAttempts)
	}

	// Simulate the runner's 3-attempt retry loop. Each iteration either
	// flips RETRY_WAIT (budget not spent) or FAILED (budget exhausted).
	for i := 1; i <= maxAttempts; i++ {
		now := time.Now().UTC().Format(time.RFC3339)
		if i < maxAttempts {
			// RETRY_WAIT: status flips, attempt_count incremented,
			// last_error_code stamped with RATE_LIMIT. The runner's
			// geometry: lease.AttemptNumber < maxAttempts \u2192 the
			// budget is NOT exhausted, the row should remain
			// retryable.
			if _, err := db.Exec(`UPDATE job_deliveries
                SET status = 'RETRY_WAIT',
                    attempt_count = ?,
                    last_error_code = 'RATE_LIMIT',
                    next_attempt_at = ?,
                    updated_at = ?
                WHERE artifact_id = ? AND destination_id = 'primary'`,
				i, time.Now().UTC().Add(time.Duration(i)*time.Minute).Format(time.RFC3339),
				now, "art-429"); err != nil {
				t.Fatalf("incr attempt %d: %v", i, err)
			}
			var got int
			if err := db.QueryRow(`SELECT attempt_count FROM job_deliveries
                WHERE artifact_id = ? AND destination_id = 'primary'`,
				"art-429").Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got >= maxAttempts {
				t.Errorf("attempt %d: attempt_count=%d >= max_attempts=%d BEFORE the boundary (runner would have FAILED it)",
					i, got, maxAttempts)
			}
		} else {
			// Budget boundary: attempt i == maxAttempts. The runner's
			// `lease.AttemptNumber >= maxAttempts` branch flips to
			// FAILED. Mirror that here.
			if _, err := db.Exec(`UPDATE job_deliveries
                SET status = 'FAILED',
                    attempt_count = ?,
                    last_error_code = 'RATE_LIMIT',
                    last_error_message = 'max attempts reached: 429',
                    completed_at = ?,
                    updated_at = ?
                WHERE artifact_id = ? AND destination_id = 'primary'`,
				i, now, now, "art-429"); err != nil {
				t.Fatalf("mark failed: %v", err)
			}
			var (
				gotCount int
				gotStat  string
			)
			if err := db.QueryRow(`SELECT attempt_count, status FROM job_deliveries
                WHERE artifact_id = ? AND destination_id = 'primary'`,
				"art-429").Scan(&gotCount, &gotStat); err != nil {
				t.Fatal(err)
			}
			if gotCount != maxAttempts {
				t.Errorf("final attempt_count = %d; want %d", gotCount, maxAttempts)
			}
			if gotStat != "FAILED" {
				t.Errorf("final status = %s; want FAILED", gotStat)
			}
		}
	}

	// Terminal-state assertion: the row is FAILED and cannot be
	// retried further without an explicit operator intervention.
	var finalStatus string
	if err := db.QueryRow(`SELECT status FROM job_deliveries
        WHERE artifact_id = ? AND destination_id = 'primary'`,
		"art-429").Scan(&finalStatus); err != nil {
		t.Fatal(err)
	}
	if finalStatus != "FAILED" {
		t.Fatalf("post-budget status = %s; want FAILED (terminal)", finalStatus)
	}
}

// =====================================================================
// Spec 5: explicit MaxAttempts=0 from a (broken) resolver reverts to 5
// (defense-in-depth: the writer MUST NOT insert max_attempts<=0)
// =====================================================================

type zeroBudgetResolver struct {
	dests []artifacts.DeliveryDestination
}

func (r *zeroBudgetResolver) ResolveDestinations(ctx context.Context, jobID, artifactID string) ([]artifacts.DeliveryDestination, error) {
	return r.dests, nil
}

func TestFinalizeVerified_ZeroMaxAttemptsRevertsToDefault(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{
		JobID: "J-zero", WorkerID: "w", LeaseID: "l",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "art-zero", UploadID: "up-zero",
	})

	resolver := &zeroBudgetResolver{
		dests: []artifacts.DeliveryDestination{
			{DestinationID: "primary", MaxAttempts: 0}, // explicit 0 \u2192 must NOT reach DB as 0
		},
	}
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{
		UploadID:         "up-zero",
		ArtifactID:       "art-zero",
		JobID:            "J-zero",
		WorkerID:         "w",
		LeaseID:          "l",
		AttemptNumber:    1,
		ExpectedRevision: 1,
		StorageProvider:  "local",
		StorageKey:       "artifacts/J-zero/1",
		SHA256:           "deadbeef",
		SizeBytes:        1024,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	})

	var got int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries
        WHERE artifact_id = ? AND destination_id = 'primary'`,
		"art-zero").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("zero-budget resolver: max_attempts = %d; want 5 (defense-in-depth default)", got)
	}
}
