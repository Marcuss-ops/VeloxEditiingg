package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/taskcontract"

	_ "github.com/mattn/go-sqlite3"
)

// openExpandTestDB builds a minimal migration-176-shape tasks table wired to
// the REAL SQLiteTaskRepository, so the fan-out service is exercised against
// actual persistence (not a stub).
func openExpandTestDB(t *testing.T) (*SQLiteStore, taskgraph.Repository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	schema := `
CREATE TABLE tasks (
	task_id            TEXT PRIMARY KEY,
	job_id             TEXT,
	project_id         TEXT,
	render_plan_id     TEXT,
	executor_id        TEXT,
	executor_version   INTEGER,
	status             TEXT,
	priority           INTEGER,
	revision           INTEGER NOT NULL DEFAULT 0,
	attempt_count      INTEGER NOT NULL DEFAULT 0,
	worker_id          TEXT,
	lease_id           TEXT,
	attempt_id         TEXT,
	attempt_number     INTEGER NOT NULL DEFAULT 0, -- migration 052
	lease_expires_at   TEXT,
	ready_at           TEXT,
	started_at         TEXT,
	completed_at       TEXT,
	created_at         TEXT,
	updated_at         TEXT,
	depends_on         TEXT NOT NULL DEFAULT '[]'
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE task_specs (task_id TEXT PRIMARY KEY, spec_version INTEGER NOT NULL, spec_hash TEXT NOT NULL, executor_id TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL)`); err != nil {
		t.Fatalf("create task_specs: %v", err)
	}
	s := &SQLiteStore{db: db}
	return s, NewSQLiteTaskRepository(s)
}

func TestExpand_PersistsFanOutAndTickReadiesDependents(t *testing.T) {
	store, repo := openExpandTestDB(t)
	svc, err := taskgraph.NewExpandService(repo)
	if err != nil {
		t.Fatalf("NewExpandService: %v", err)
	}
	ctx := context.Background()

	created, err := svc.Expand(ctx, taskgraph.ExpandPlan{
		JobID: "job-fanout-1",
		Tasks: []taskgraph.ExpandedTask{
			{ExecutorID: "asset.prep.v1", ExecutorVer: 1, DependsOn: nil, Payload: map[string]interface{}{"asset_id": "a"}},
			{ExecutorID: "audio.mix.v1", ExecutorVer: 1, DependsOn: []string{"job-fanout-1-t01"}},
			{ExecutorID: "video.concat.v1", ExecutorVer: 1, DependsOn: []string{"job-fanout-1-t02"}},
		},
	})
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if len(created) != 3 {
		t.Fatalf("created = %d tasks, want 3", len(created))
	}
	var specExecutor, specPayload, specHash string
	if err := store.db.QueryRowContext(ctx, `SELECT executor_id,payload_json,spec_hash FROM task_specs WHERE task_id=?`, created[0].ID).Scan(&specExecutor, &specPayload, &specHash); err != nil {
		t.Fatalf("read atomically persisted task spec: %v", err)
	}
	if specExecutor != "asset.prep.v1" || specPayload != `{"asset_id":"a"}` {
		t.Fatalf("persisted spec executor=%q payload=%s", specExecutor, specPayload)
	}
	wantHash := (&taskcontract.TaskSpec{Version: taskcontract.SpecVersion, JobID: "job-fanout-1", ExecutorID: "asset.prep.v1", Payload: map[string]interface{}{"asset_id": "a"}}).MustSpecHash()
	if specHash != wantHash {
		t.Fatalf("persisted task spec hash=%q, want %q", specHash, wantHash)
	}
	// Root must be PENDING with no edges; leaves carry their edges.
	if created[0].Status != taskgraph.StatusPending || len(created[0].DependsOn) != 0 {
		t.Fatalf("root task wrong: %+v", created[0])
	}
	if len(created[1].DependsOn) != 1 || created[1].DependsOn[0] != "job-fanout-1-t01" {
		t.Fatalf("mix edges wrong: %+v", created[1].DependsOn)
	}

	// Root becomes READY on the first tick; dependents stay PENDING.
	ls, lsErr := taskgraph.NewLifecycleService(repo)
	if lsErr != nil {
		t.Fatalf("NewLifecycleService: %v", lsErr)
	}
	if n, err := ls.TickReadiness(ctx, 100); err != nil || n != 1 {
		t.Fatalf("tick 1: transitioned=%d err=%v, want 1 nil", n, err)
	}
	got, err := repo.Get(ctx, created[0].ID)
	if err != nil || got.Status != taskgraph.StatusReady {
		t.Fatalf("root should be READY, got %+v err %v", got, err)
	}
	got2, _ := repo.Get(ctx, created[1].ID)
	if got2.Status != taskgraph.StatusPending {
		t.Fatalf("dependent must stay PENDING, got %s", got2.Status)
	}

	// Root completes its lifecycle → the direct dependent becomes READY, the
	// tail stays. The legal terminal path from READY is the claim chain
	// READY→LEASED→RUNNING→SUCCEEDED (statemachine DomainTask rules); the
	// worker report path (TransitionTaskToTerminalAtomic) is what flips a
	// RUNNING task to SUCCEEDED in production. Here the CAS writer SetStatus
	// walks each legal hop, re-reading the revision between hops (SetStatus
	// bumps revision on every successful CAS), so the test exercises exactly
	// the rules the production state machine enforces.
	step := func(from, to taskgraph.Status) {
		t.Helper()
		cur, err := repo.Get(ctx, created[0].ID)
		if err != nil || cur == nil {
			t.Fatalf("read root before %s→%s: %v", from, to, err)
		}
		if err := repo.SetStatus(ctx, cur.ID, from, to, cur.Revision); err != nil {
			t.Fatalf("root %s→%s: %v", from, to, err)
		}
	}
	step(taskgraph.StatusReady, taskgraph.StatusLeased)
	step(taskgraph.StatusLeased, taskgraph.StatusRunning)
	step(taskgraph.StatusRunning, taskgraph.StatusSucceeded)
	if n, err := ls.TickReadiness(ctx, 100); err != nil || n != 1 {
		t.Fatalf("tick 2: transitioned=%d err=%v, want 1 nil", n, err)
	}
	got2, _ = repo.Get(ctx, created[1].ID)
	if got2.Status != taskgraph.StatusReady {
		t.Fatalf("dependent should be READY after root success, got %s", got2.Status)
	}
	got3, _ := repo.Get(ctx, created[2].ID)
	if got3.Status != taskgraph.StatusPending {
		t.Fatalf("tail must stay PENDING behind READY (not SUCCEEDED) parent, got %s", got3.Status)
	}
}

func TestExpand_ExtendAssemblyWithPreparationTasksAtomically(t *testing.T) {
	store, repo := openExpandTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	parentPayload := map[string]interface{}{"runtime_assets_pending": true, "runtime_finalize_status": "RUNNING"}
	parentSpec := &taskcontract.TaskSpec{Version: taskcontract.SpecVersion, JobID: "job-video-dag", ExecutorID: "video.assemble.copy.v1", Payload: parentPayload}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tasks
		(task_id,job_id,project_id,render_plan_id,executor_id,executor_version,status,priority,worker_id,lease_id,created_at,updated_at,depends_on)
		VALUES('assembly-1','job-video-dag','','','video.assemble.copy.v1',1,'PENDING',0,'','',?,?,'[]')`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO task_specs(task_id,spec_version,spec_hash,executor_id,payload_json,created_at)
		VALUES('assembly-1',?,?,?,?,?)`, parentSpec.Version, parentSpec.MustSpecHash(), parentSpec.ExecutorID, `{"runtime_assets_pending":true,"runtime_finalize_status":"RUNNING"}`, now); err != nil {
		t.Fatal(err)
	}
	svc, err := taskgraph.NewExpandService(repo)
	if err != nil {
		t.Fatal(err)
	}
	created, err := svc.Extend(ctx, "assembly-1", []taskgraph.ExpandedTask{{
		TaskID: "prepare-1", ExecutorID: "video.prepare.window.v1", ExecutorVer: 1,
		Payload: map[string]interface{}{"video_prepare_window": map[string]interface{}{"version": 1}},
	}}, []string{"prepare-1"}, map[string]interface{}{"runtime_finalize_status": "WAITING_PREPARATION", "runtime_prepare_task_ids": []string{"prepare-1"}})
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if len(created) != 1 || created[0].ID != "prepare-1" {
		t.Fatalf("created tasks = %+v", created)
	}
	parent, err := repo.Get(ctx, "assembly-1")
	if err != nil || parent == nil || len(parent.DependsOn) != 1 || parent.DependsOn[0] != "prepare-1" {
		t.Fatalf("assembly graph edge = %+v, err=%v", parent, err)
	}
	var payloadJSON string
	if err := store.db.QueryRowContext(ctx, `SELECT payload_json FROM task_specs WHERE task_id='assembly-1'`).Scan(&payloadJSON); err != nil {
		t.Fatal(err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["runtime_finalize_status"] != "WAITING_PREPARATION" || payload["runtime_assets_pending"] != true {
		t.Fatalf("assembly payload should stay gated until preparation evidence is resolved: %s", payloadJSON)
	}
	prep, err := repo.Get(ctx, "prepare-1")
	if err != nil || prep == nil || prep.Status != taskgraph.StatusPending {
		t.Fatalf("preparation task = %+v, err=%v", prep, err)
	}
}

func TestExpand_RejectsCycleWithoutSideEffects(t *testing.T) {
	_, repo := openExpandTestDB(t)
	svc, _ := taskgraph.NewExpandService(repo)
	ctx := context.Background()

	_, err := svc.Expand(ctx, taskgraph.ExpandPlan{
		JobID: "job-cycle",
		Tasks: []taskgraph.ExpandedTask{
			{ExecutorID: "a", DependsOn: []string{"job-cycle-t02"}},
			{ExecutorID: "b", DependsOn: []string{"job-cycle-t01"}},
		},
	})
	if err == nil {
		t.Fatal("cyclic plan must be rejected fail-closed")
	}
	tasks, listErr := repo.List(ctx, taskgraph.Filter{JobIDs: []string{"job-cycle"}})
	if listErr != nil {
		t.Fatalf("list: %v", listErr)
	}
	if len(tasks) != 0 {
		t.Fatalf("rejected plan must leave no rows behind, got %d", len(tasks))
	}
}

func TestExpand_RollsBackTasksWhenSpecPersistenceFails(t *testing.T) {
	store, repo := openExpandTestDB(t)
	svc, err := taskgraph.NewExpandService(repo)
	if err != nil {
		t.Fatalf("NewExpandService: %v", err)
	}
	if _, err := store.db.Exec(`DROP TABLE task_specs`); err != nil {
		t.Fatalf("drop task_specs: %v", err)
	}
	if _, err := svc.Expand(context.Background(), taskgraph.ExpandPlan{
		JobID: "job-spec-failure",
		Tasks: []taskgraph.ExpandedTask{{ExecutorID: "asset.prep.v1", ExecutorVer: 1, Payload: map[string]interface{}{"asset_id": "a"}}},
	}); err == nil {
		t.Fatal("fan-out must fail if executable task spec cannot be persisted")
	}
	tasks, err := repo.List(context.Background(), taskgraph.Filter{JobIDs: []string{"job-spec-failure"}})
	if err != nil {
		t.Fatalf("list tasks after rollback: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("task row escaped failed atomic task/spec transaction: %+v", tasks)
	}
}

func TestExpand_RejectsDanglingEdge(t *testing.T) {
	_, repo := openExpandTestDB(t)
	svc, _ := taskgraph.NewExpandService(repo)

	if _, err := svc.Expand(context.Background(), taskgraph.ExpandPlan{
		JobID: "job-dangling",
		Tasks: []taskgraph.ExpandedTask{
			{ExecutorID: "a", DependsOn: []string{"ghost-task"}},
		},
	}); err == nil {
		t.Fatal("dangling edge must be rejected")
	}
}

func TestExpand_RejectsEmptyPlanAndCompensatesOnFailure(t *testing.T) {
	_, repo := openExpandTestDB(t)
	svc, _ := taskgraph.NewExpandService(repo)
	ctx := context.Background()

	if _, err := svc.Expand(ctx, taskgraph.ExpandPlan{JobID: "job-empty"}); err == nil {
		t.Fatal("empty plan must be rejected")
	}

	// Seed a row that collides with the plan's deterministic second ID, then
	// confirm the atomic fan-out transaction leaves no first-task residue.
	if err := repo.Create(ctx, &taskgraph.Task{ID: "job-collide-t02", JobID: "job-collide"}); err != nil {
		t.Fatalf("seed collision: %v", err)
	}
	if _, err := svc.Expand(ctx, taskgraph.ExpandPlan{
		JobID: "job-collide",
		Tasks: []taskgraph.ExpandedTask{
			{ExecutorID: "a"},
			{ExecutorID: "b"},
		},
	}); err == nil {
		t.Fatal("colliding expansion must fail")
	}
	tasks, _ := repo.List(ctx, taskgraph.Filter{JobIDs: []string{"job-collide"}})
	// Exactly the seeded row remains; the transaction rolled back completely.
	if len(tasks) != 1 || tasks[0].ID != "job-collide-t02" {
		t.Fatalf("compensation must leave only the pre-existing row, got %+v", tasks)
	}
}
