package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// =====================================================================
// PR-05 follow-up: master-side lease — RenewLease tests.
//
// RenewLease CAS tuple: task_id + status='LEASED' + worker_id + lease_id +
// revision. Tests below cover the three rejection paths (stale revision,
// wrong leaseID, empty identity) plus the happy path. Schema is a minimal
// subset of the post-049 columns RenewLease actually touches — mirrors
// the fixtures approach used by sqlite_task_reaper_test.go.
// =====================================================================

const taskRenewSchema = `
CREATE TABLE tasks (
	task_id          TEXT PRIMARY KEY,
	job_id           TEXT,
	status           TEXT,
	revision         INTEGER NOT NULL DEFAULT 0,
	worker_id        TEXT,
	lease_id         TEXT,
	lease_expires_at TEXT,
	created_at       TEXT,
	updated_at       TEXT
);
`

func openTaskRenewTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite (task renew): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(taskRenewSchema); err != nil {
		t.Fatalf("apply renew schema: %v", err)
	}
	return &SQLiteStore{db: db}
}

// seedRenewTask inserts a focused task row with the exact identity triple
// RenewLease CAS-gates on. Worker (w)/Lease (L) defaults are stable so
// tests can override only the fields they care about.
func seedRenewTask(t *testing.T, db *sql.DB,
	taskID, status, workerID, leaseID string, revision int, leaseExpiresAt string,
) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(
		`INSERT INTO tasks
		 (task_id, job_id, status, revision, worker_id, lease_id,
		  lease_expires_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, "job-"+taskID, status, revision,
		workerID, leaseID, leaseExpiresAt, now, now,
	); err != nil {
		t.Fatalf("seed renew task %q: %v", taskID, err)
	}
}

// TestRenewLease_HappyPath: a LEASED task with matching identity AND
// current revision gets lease_expires_at extended and no error.
func TestRenewLease_HappyPath(t *testing.T) {
	s := openTaskRenewTestDB(t)
	pastExpiry := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedRenewTask(t, s.db, "T-renew", "LEASED", "w-renew", "lease-renew", 7, pastExpiry)

	repo := NewSQLiteTaskRepository(s)
	newExpiry := time.Now().UTC().Add(45 * time.Minute)
	if err := repo.RenewLease(context.Background(), "T-renew", "w-renew", "lease-renew", newExpiry, 7); err != nil {
		t.Fatalf("RenewLease happy path: %v", err)
	}

	var gotLease string
	var gotUpdated string
	if err := s.db.QueryRow(
		`SELECT lease_expires_at, updated_at FROM tasks WHERE task_id='T-renew'`,
	).Scan(&gotLease, &gotUpdated); err != nil {
		t.Fatal(err)
	}
	wantLease := newExpiry.UTC().Format(time.RFC3339)
	if gotLease != wantLease {
		t.Errorf("lease_expires_at = %q; want %q", gotLease, wantLease)
	}
	// updated_at must also be set on success.
	if gotUpdated == "" || gotUpdated == pastExpiry {
		t.Errorf("updated_at = %q; want non-empty and ≠ stale lease_expiry", gotUpdated)
	}
}

// TestRenewLease_DoesNotBumpRevision: a successful renewal leaves the
// revision column untouched. This is intentional (see interface comment):
// workers reference the revision in their in-flight UpdateResult messages,
// so we must keep it stable across renewals.
func TestRenewLease_DoesNotBumpRevision(t *testing.T) {
	s := openTaskRenewTestDB(t)
	pastExpiry := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedRenewTask(t, s.db, "T-rev", "LEASED", "w-rev", "L-rev", 11, pastExpiry)

	repo := NewSQLiteTaskRepository(s)
	if err := repo.RenewLease(context.Background(), "T-rev", "w-rev", "L-rev",
		time.Now().UTC().Add(45*time.Minute), 11); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	var rev int
	if err := s.db.QueryRow(`SELECT revision FROM tasks WHERE task_id='T-rev'`).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	if rev != 11 {
		t.Errorf("revision = %d; want 11 (renewals must not bump revision)", rev)
	}
}

// TestRenewLease_StaleRevisionReturnsConflict: revision-mismatch is the
// canonical "stale lease" path; no row is mutated.
func TestRenewLease_StaleRevisionReturnsConflict(t *testing.T) {
	s := openTaskRenewTestDB(t)
	pastExpiry := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedRenewTask(t, s.db, "T-stale", "LEASED", "w-stale", "L-stale", 3, pastExpiry)

	repo := NewSQLiteTaskRepository(s)
	err := repo.RenewLease(context.Background(), "T-stale", "w-stale", "L-stale",
		time.Now().UTC().Add(45*time.Minute), 999) // 999 != 3
	if err == nil {
		t.Fatal("expected error on stale revision, got nil")
	}
	if !strings.Contains(err.Error(), "renew lease") {
		t.Errorf("error should mention renew lease; got %v", err)
	}
	// Confirm row was NOT mutated: lease_expires_at must still equal the
	// pre-renewal value.
	var stillStale string
	if err := s.db.QueryRow(
		`SELECT lease_expires_at FROM tasks WHERE task_id='T-stale'`,
	).Scan(&stillStale); err != nil {
		t.Fatal(err)
	}
	if stillStale != pastExpiry {
		t.Errorf("stale-row lease_expires_at mutated; got %q want %q", stillStale, pastExpiry)
	}
}

// TestRenewLease_WrongLeaseIDRejected: a worker whose lease has been
// reaped (different leaseID re-issued to another worker) is unable to
// extend its own lease. This is the safety case PR-05 follow-up nails
// down: a stale worker must not be able to "phantom extend" a lease that
// no longer belongs to it.
func TestRenewLease_WrongLeaseIDRejected(t *testing.T) {
	s := openTaskRenewTestDB(t)
	originalExpiry := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
	// The reaper has issued a different leaseID (L-NEW) to a different
	// worker (w-NEW); the original worker still holds the task addr only.
	seedRenewTask(t, s.db, "T-reaped", "LEASED", "w-new", "L-NEW", 0, originalExpiry)

	repo := NewSQLiteTaskRepository(s)
	err := repo.RenewLease(context.Background(), "T-reaped", "w-new", "L-OLD",
		time.Now().UTC().Add(45*time.Minute), 0)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on wrong leaseID, got nil")
	}

	// lease_id and lease_expires_at must both be unchanged.
	var gotLeaseID, gotExpiry string
	if err := s.db.QueryRow(
		`SELECT lease_id, lease_expires_at FROM tasks WHERE task_id='T-reaped'`,
	).Scan(&gotLeaseID, &gotExpiry); err != nil {
		t.Fatal(err)
	}
	if gotLeaseID != "L-NEW" {
		t.Errorf("lease_id mutated; got %q want %q", gotLeaseID, "L-NEW")
	}
	if gotExpiry != originalExpiry {
		t.Errorf("lease_expires_at mutated; got %q want %q", gotExpiry, originalExpiry)
	}
}

// TestRenewLease_RejectsEmptyIdentity: any of id / workerID / leaseID /
// expiry zero returns an error before touching the DB.
func TestRenewLease_RejectsEmptyIdentity(t *testing.T) {
	s := openTaskRenewTestDB(t)
	repo := NewSQLiteTaskRepository(s)
	exp := time.Now().UTC().Add(45 * time.Minute)

	cases := []struct {
		name string
		fn   func() error
	}{
		{"empty id", func() error {
			return repo.RenewLease(context.Background(), "", "w", "L", exp, 0)
		}},
		{"empty workerID", func() error {
			return repo.RenewLease(context.Background(), "T", "", "L", exp, 0)
		}},
		{"empty leaseID", func() error {
			return repo.RenewLease(context.Background(), "T", "w", "", exp, 0)
		}},
		{"zero expiry", func() error {
			return repo.RenewLease(context.Background(), "T", "w", "L", time.Time{}, 0)
		}},
	}
	for _, c := range cases {
		if err := c.fn(); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}
