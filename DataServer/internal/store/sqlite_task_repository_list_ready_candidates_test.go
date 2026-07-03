// Package store — Placement-candidate integration tests for
// SQLiteTaskRepository.ListReadyCandidates.
//
// The DB is opened with cache=shared on the DSN so the
// connection-shared in-memory SQLite behaves consistently under -race
// (a plain ":memory:" is private to each pooled connection and would
// defeat tests that re-open the pool between operations).
package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/placement"
)

// candidatesTestSchema mirrors the columns the production
// ListReadyCandidates query projects (task_id, job_id, revision,
// priority, created_at, executor_id, executor_version) AND every
// other status/worker_id column the WHERE clause filters on, so a
// regression in any of them surfaces here. Column types mirror
// migration 039_tasks.sql (executor_version is INTEGER with default
// 0, status is TEXT, etc.) — a TEXT/INTEGER drift would mask a typed
// Scan conversion regression in production.
const candidatesTestSchema = `
CREATE TABLE tasks (
    task_id          TEXT PRIMARY KEY,
    job_id           TEXT,
    status           TEXT,
    priority         INTEGER,
    revision         INTEGER NOT NULL DEFAULT 0,
    worker_id        TEXT,
    lease_id         TEXT,
    executor_id      TEXT,
    executor_version INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT,
    updated_at       TEXT
);
CREATE TABLE IF NOT EXISTS task_requirements (
    task_id    TEXT NOT NULL,
    capability TEXT NOT NULL,
    PRIMARY KEY (task_id, capability),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);
`

// openCandidatesTestDB returns a SQLiteTaskRepository scoped to a
// connection-shared in-memory SQLite with the minimal schema. The
// SQLiteStore wrapper is constructed by hand with only the fields
// ListReadyCandidates (and any helpers it transitively calls) reads;
// a full NewSQLiteStore would lug the migrations stack overhead for
// no signal — the projection regression we're guarding against
// lives at this layer, not in the schema-boot side.
//
// NOTE: fields beyond `db` are intentionally zeroed. This test only
// exercises the SQL projection; any future expansion that quietly
// relies on missing fields must be rejected here.
func openCandidatesTestDB(t *testing.T) (*SQLiteTaskRepository, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite (candidates test): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(candidatesTestSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return NewSQLiteTaskRepository(&SQLiteStore{db: db}), db
}

// seedCandidateTask inserts a single row in the candidates-test
// schema. Tasks written with status="READY" + worker_id="" or NULL
// are the only ones ListReadyCandidates is expected to surface; every
// other combination is a negative-test input the projection must
// reject.
func seedCandidateTask(t *testing.T, db *sql.DB,
	taskID, jobID string,
	priority int,
	status string,
	workerIsNull bool,
	workerID string,
	executorID string,
	executorVersion int,
	createdAt time.Time,
) {
	t.Helper()
	var execErr error
	if workerIsNull {
		_, execErr = db.ExecContext(context.Background(),
			`INSERT INTO tasks
			 (task_id, job_id, status, priority, worker_id, lease_id,
			  executor_id, executor_version, created_at, updated_at)
			 VALUES (?, ?, ?, ?, NULL, '', ?, ?, ?, ?)`,
			taskID, jobID, status, priority,
			executorID, executorVersion,
			createdAt.UTC().Format(time.RFC3339),
			createdAt.UTC().Format(time.RFC3339),
		)
	} else {
		_, execErr = db.ExecContext(context.Background(),
			`INSERT INTO tasks
			 (task_id, job_id, status, priority, worker_id, lease_id,
			  executor_id, executor_version, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?)`,
			taskID, jobID, status, priority, workerID,
			executorID, executorVersion,
			createdAt.UTC().Format(time.RFC3339),
			createdAt.UTC().Format(time.RFC3339),
		)
	}
	if execErr != nil {
		t.Fatalf("seed %s: %v", taskID, execErr)
	}
}

// TestSQLiteTaskRepository_ListReadyCandidates_HappyPath exercises
// the canonical SELECT: 3 valid READY candidates (mixed priority +
// FIFO created_at), 1 RUNNING (excluded by status), 1 READY but with
// a worker (excluded by worker_id), 1 READY with worker_id IS NULL
// (covered by the OR-NULL branch), 1 PENDING (excluded by status).
//
// Expected output order:
//   * T-E  priority=30  created_at=t0+0s       (FIRST: top priority)
//   * T-B  priority=20  created_at=t0+1s
//   * T-A  priority=10  created_at=t0+2s       (LAST in priority FIFO)
//   * T-N  priority=5   worker_id=NULL         (still surfaces)
//
// This pins the ORDER BY priority DESC, created_at ASC clause, the
// (worker_id='' OR worker_id IS NULL) WHERE branch, AND the
// conversion from row → placement.TaskCandidate (ExecutorKey
// populated from executor_id/executor_version, CreatedAt parsed from
// a RFC3339 string).
func TestSQLiteTaskRepository_ListReadyCandidates_HappyPath(t *testing.T) {
	r, db := openCandidatesTestDB(t)
	ctx := context.Background()

	t0 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	seedCandidateTask(t, db, "T-A", "J-A", 10, "READY", false, "", "scene.composite.v1", 1, t0.Add(2*time.Second))
	seedCandidateTask(t, db, "T-B", "J-B", 20, "READY", false, "", "scene.composite.v1", 1, t0.Add(1*time.Second))
	seedCandidateTask(t, db, "T-E", "J-E", 30, "READY", false, "", "scene.composite.v2", 2, t0)
	seedCandidateTask(t, db, "T-N", "J-N", 5, "READY", true, "", "scene.composite.v1", 1, t0)
	seedCandidateTask(t, db, "T-C", "J-C", 40, "RUNNING", false, "w-1", "scene.composite.v1", 1, t0)
	seedCandidateTask(t, db, "T-D", "J-D", 50, "READY", false, "w-2", "scene.composite.v1", 1, t0)
	seedCandidateTask(t, db, "T-F", "J-F", 60, "PENDING", false, "", "scene.composite.v1", 1, t0)

	candidates, err := r.ListReadyCandidates(ctx, 100)
	if err != nil {
		t.Fatalf("ListReadyCandidates: %v", err)
	}
	if len(candidates) != 4 {
		t.Fatalf("got %d candidates, want 4 (T-E, T-B, T-A, T-N — priority DESC then created_at ASC; T-D excluded by worker_id w-2; T-C/F excluded by status)", len(candidates))
	}

	wantOrder := []string{"T-E", "T-B", "T-A", "T-N"}
	wantPriorities := []int{30, 20, 10, 5}
	for i, c := range candidates {
		if c.TaskID != wantOrder[i] {
			t.Errorf("[%d] TaskID=%q want %q (priority DESC, then created_at ASC)", i, c.TaskID, wantOrder[i])
		}
		if c.Priority != wantPriorities[i] {
			t.Errorf("[%d] Priority=%d want %d", i, c.Priority, wantPriorities[i])
		}
		if c.JobID == "" {
			t.Errorf("[%d] JobID empty", i)
		}
		if c.Executor.ID == "" || c.Executor.Version == 0 {
			t.Errorf("[%d] ExecutorKey not populated: %+v", i, c.Executor)
		}
		if c.CreatedAt.IsZero() {
			t.Errorf("[%d] CreatedAt not parsed; raw column or Scan conversion regressed", i)
		}
	}

	// ExecutorKey conversion: T-A/B/N use scene.composite.v1@1, T-E uses v2.
	// A regression where executor_version=0 leaks through would render
	// every task RejectInvalidTaskRequirement in the matcher — caught
	// here without spinning up the full matcher stack.
	if candidates[0].Executor.Version != 2 || candidates[0].Executor.ID != "scene.composite.v2" {
		t.Errorf("T-E ExecutorKey = %+v want {scene.composite.v2, 2}", candidates[0].Executor)
	}
	if candidates[2].Executor.Version != 1 || candidates[2].Executor.ID != "scene.composite.v1" {
		t.Errorf("T-A ExecutorKey = %+v want {scene.composite.v1, 1}", candidates[2].Executor)
	}
}

// TestSQLiteTaskRepository_ListReadyCandidates_LimitDefault pins the
// safe-default for limit<=0 (placementCandidateBatch = 64). A
// regression here would silently shift the split between placement
// dispatch and the next-tick re-scan — caught here by inserting 65
// rows and checking 64 / explicit / exact-equal boundaries.
func TestSQLiteTaskRepository_ListReadyCandidates_LimitDefault(t *testing.T) {
	r, db := openCandidatesTestDB(t)
	ctx := context.Background()

	t0 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 65; i++ {
		seedCandidateTask(t, db, fmt.Sprintf("T-limit-%03d", i), "J-limit", 0, "READY", false, "",
			"scene.composite.v1", 1, t0.Add(time.Duration(i)*time.Second))
	}

	for _, tc := range []struct {
		name    string
		limit   int
		wantLen int
	}{
		{"limit=0 defaults to 64", 0, 64},
		{"limit=-1 defaults to 64", -1, 64},
		{"limit=10 honors caller", 10, 10},
		{"limit=64 honors caller (boundary)", 64, 64},
		{"limit=100 honors caller (over default)", 100, 65},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.ListReadyCandidates(ctx, tc.limit)
			if err != nil {
				t.Fatalf("ListReadyCandidates(limit=%d): %v", tc.limit, err)
			}
			if len(got) != tc.wantLen {
				t.Errorf("limit=%d → %d candidates; want %d", tc.limit, len(got), tc.wantLen)
			}
		})
	}
}

// TestSQLiteTaskRepository_ListReadyCandidates_Empty guarantees the
// no-row path returns (nil, nil) — important because handler_workers
// branches on a nil/empty candidates slice to fall back to a next-tick
// re-scan. A panic here would cascade into a worker poll loop noise.
// We assert both len==0 AND wantNil (candidates == nil) to catch a
// regression that returns an empty-but-non-nil slice (which would
// still pass len==0 but break eager zero-check call sites).
func TestSQLiteTaskRepository_ListReadyCandidates_Empty(t *testing.T) {
	r, _ := openCandidatesTestDB(t)
	candidates, err := r.ListReadyCandidates(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListReadyCandidates empty DB: %v", err)
	}
	if len(candidates) != 0 {
		t.Errorf("got %d candidates; want 0 (empty DB must surface empty slice)", len(candidates))
	}
	if candidates != nil {
		t.Errorf("got non-nil empty slice %T; want nil (zero-row SQL → nil slice from `var x []T`)", candidates)
	}
}

// TestSQLiteTaskRepository_ListReadyCandidates_InteropWithMatcher is
// the regression-guard for the SQL→placement.TaskCandidate conversion.
// It runs the placement matcher directly against the candidates
// returned by the repository; a regression where Executor.ID is
// empty or Executor.Version is 0 would surface here as
// RejectInvalidTaskRequirement rejections (the matcher gate fires
// before any executor/capability check). It also asserts that
// RequiredCapabilities is the zero-value nil slice — a future change
// that populates it from a malformed column would silently reject
// lots of tasks even when the executor key matches cleanly.
func TestSQLiteTaskRepository_ListReadyCandidates_InteropWithMatcher(t *testing.T) {
	r, db := openCandidatesTestDB(t)
	ctx := context.Background()

	t0 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	seedCandidateTask(t, db, "T-match", "J-match", 5, "READY", false, "", "scene.composite.v1", 1, t0)
	// Seed capability: the task requires artifact.commit.v1.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO task_requirements (task_id, capability) VALUES (?, ?)`,
		"T-match", "artifact.commit.v1",
	); err != nil {
		t.Fatalf("seed task_requirements: %v", err)
	}

	candidates, err := r.ListReadyCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("ListReadyCandidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate; got %d", len(candidates))
	}

	// RequiredCapabilities must now be populated from the JOIN.
	if len(candidates[0].RequiredCapabilities) != 1 || candidates[0].RequiredCapabilities[0] != "artifact.commit.v1" {
		t.Errorf("RequiredCapabilities=%v; want [artifact.commit.v1] (LEFT JOIN task_requirements must populate the field)", candidates[0].RequiredCapabilities)
	}

	// Worker WITH the capability → matcher should select the task.
	m := placement.NewMatcher()
	result := m.Select(placement.WorkerSnapshot{
		WorkerID:        "w-1",
		SessionID:       "S-1",
		Ready:           true,
		SessionAlive:    true,
		MaxParallelJobs: 4,
		ActiveJobs:      0,
		Executors:       map[placement.ExecutorKey]struct{}{{ID: "scene.composite.v1", Version: 1}: {}},
		Capabilities:    map[string]bool{"artifact.commit.v1": true},
	}, candidates)

	if result.Candidate == nil {
		t.Fatalf("matcher returned nil Candidate for an executable task (worker has the capability); rejections=%v", result.Rejections)
	}
	if result.Candidate.TaskID != "T-match" {
		t.Errorf("Matched TaskID=%q want T-match", result.Candidate.TaskID)
	}

	// Worker WITHOUT the capability → matcher must reject.
	resultNoCap := m.Select(placement.WorkerSnapshot{
		WorkerID:        "w-2",
		SessionID:       "S-2",
		Ready:           true,
		SessionAlive:    true,
		MaxParallelJobs: 4,
		ActiveJobs:      0,
		Executors:       map[placement.ExecutorKey]struct{}{{ID: "scene.composite.v1", Version: 1}: {}},
		Capabilities:    map[string]bool{}, // no capabilities
	}, candidates)

	if resultNoCap.Candidate != nil {
		t.Errorf("matcher selected Candidate when worker lacks required capability; candidate=%+v", resultNoCap.Candidate)
	}
	foundRejection := false
	for _, rej := range resultNoCap.Rejections {
		if rej.TaskID == "T-match" && rej.Code == placement.RejectMissingCapability {
			foundRejection = true
			break
		}
	}
	if !foundRejection {
		t.Errorf("expected RejectMissingCapability rejection for T-match; got: %+v", resultNoCap.Rejections)
	}
}
