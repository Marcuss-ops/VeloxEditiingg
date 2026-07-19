package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// newMinimalWorker constructs a *Worker with just the fields
// needed by the master-restart recovery path. We avoid the full
// worker.New() because that wires telemetry transports, executor
// registry, etc. — the snapshot/replay path doesn't depend on any
// of those.
func newMinimalWorker(testID, stateDir string) *Worker {
	return &Worker{
		config: &config.WorkerConfig{
			WorkerID: testID,
			StateDir: stateDir,
		},
		logger:             logger.New(logger.InfoLevel, nil),
		seenCommands:       make(map[string]time.Time),
		commandMu:          sync.Mutex{},
		pendingTasks:       make(map[string]*PendingTaskExecution),
		pendingTasksMu:     sync.Mutex{},
		activeTaskLeases:   make(map[string]*ActiveTaskLease),
		activeTaskLeasesMu: sync.RWMutex{},
		activeTasks:        make(map[string]*ActiveTaskExecution),
		activeTasksMu:      sync.RWMutex{},
	}
}

// TestMasterRestartRecovery_RoundtripAndIdempotence drives the
// master-restart recovery contract:
//
//  1. Worker A is constructed with a temp StateDir; its lifecycle
//     maps (pendingTasks, activeTaskLeases, activeTasks) are
//     populated directly so we exercise the snapshot path.
//  2. snapshotRecoveryState writes the recovery JSON to disk. The
//     recovery payload is round-tripped through json.Unmarshal to
//     confirm every field survives the wire format.
//  3. Worker B is constructed against the same StateDir; its
//     loadRecoveryState path replays the snapshot into the in-memory
//     maps. Asserts:
//     - pendingTasks restored with the right identity tuple
//     - activeTaskLeases restored with the right identity tuple
//     - activeTasks intentionally NOT restored (Cancel funcs
//     are dead; master will remint).
//  4. applyRecoverySnapshot is called a second time on Worker B
//     with the same snapshot — this simulates two consecutive
//     crashes overlapping the replay window. Asserts no map grows
//     (idempotence keyed on TaskID).
func TestMasterRestartRecovery_RoundtripAndIdempotence(t *testing.T) {
	stateDir := t.TempDir()

	// Worker A — populate the three lifecycle maps directly.
	wA := newMinimalWorker("w-recovery-A", stateDir)
	wA.pendingTasks["task-pending-1"] = &PendingTaskExecution{
		TaskID:          "task-pending-1",
		JobID:           "job-pending-1",
		JobRevision:     7,
		AttemptID:       "attempt-pending-1",
		AttemptNumber:   1,
		LeaseID:         "lease-pending-1",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		Revision:        1,
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-pending-1",
			ExecutorID: "scene.composite.v1",
			Payload:    map[string]interface{}{"hook": "from-test"},
		},
	}
	wA.activeTaskLeases["task-lease-1"] = &ActiveTaskLease{
		TaskID: "task-lease-1", JobID: "job-1",
		AttemptID: "attempt-1", LeaseID: "lease-1",
		AttemptNumber: 1, Revision: 1,
	}
	wA.activeTaskLeases["task-lease-2"] = &ActiveTaskLease{
		TaskID: "task-lease-2", JobID: "job-2",
		AttemptID: "attempt-2", LeaseID: "lease-2",
		AttemptNumber: 2, Revision: 3,
	}
	wA.activeTasks["task-running-1"] = &ActiveTaskExecution{
		TaskID:    "task-running-1",
		AttemptID: "attempt-running-1",
		JobID:     "job-running-1",
		LeaseID:   "lease-running-1",
		StartedAt: time.Now().UTC(),
		Cancel:    nil, // ignored by the capture path
		Progress:  JobProgress{},
	}

	// Capture to disk.
	if err := wA.snapshotRecoveryState(); err != nil {
		t.Fatalf("Worker A snapshot: %v", err)
	}

	// Verify the recovery JSON exists.
	path := filepath.Join(stateDir, "worker_recovery.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("recovery file should exist after snapshot: %v", err)
	}

	// Round-trip the JSON: every seeded identity must survive.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read recovery file: %v", err)
	}
	var snap RecoverySnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatalf("unmarshal recovery: %v", err)
	}
	if snap.CapturedAt.IsZero() {
		t.Fatal("CapturedAt must be set")
	}
	if len(snap.PendingTasks) != 1 {
		t.Fatalf("PendingTasks round-trip count=%d, want 1", len(snap.PendingTasks))
	}
	ptSnap := snap.PendingTasks[0]
	if ptSnap.TaskID != "task-pending-1" ||
		ptSnap.JobID != "job-pending-1" ||
		ptSnap.AttemptID != "attempt-pending-1" ||
		ptSnap.ExecutorID != "scene.composite.v1" ||
		ptSnap.JobRevision != 7 {
		t.Fatalf("PendingTasks[0] round-trip drifted: %+v", ptSnap)
	}
	if ptSnap.Spec.JobID != "job-pending-1" ||
		ptSnap.Spec.Version != 1 ||
		ptSnap.Spec.Payload["hook"] != "from-test" {
		t.Fatalf("PendingTasks[0].Spec round-trip drifted: %+v", ptSnap.Spec)
	}
	if len(snap.ActiveLeases) != 2 {
		t.Fatalf("ActiveLeases round-trip count=%d, want 2", len(snap.ActiveLeases))
	}
	leaseIDs := map[string]bool{}
	for _, al := range snap.ActiveLeases {
		leaseIDs[al.TaskID] = true
	}
	if !leaseIDs["task-lease-1"] || !leaseIDs["task-lease-2"] {
		t.Fatalf("ActiveLeases round-trip drifted: %+v", leaseIDs)
	}
	// Active tasks MUST be present for diagnostic completeness (the
	// replay path drops them, but they exist on disk for audit).
	if len(snap.ActiveTasks) != 1 {
		t.Fatalf("ActiveTasks round-trip count=%d, want 1 (audit-only)", len(snap.ActiveTasks))
	}
	if snap.ActiveTasks[0].TaskID != "task-running-1" {
		t.Fatalf("ActiveTasks[0] round-trip drifted: %+v", snap.ActiveTasks[0])
	}

	// Worker B — share StateDir, replay from disk.
	wB := newMinimalWorker("w-recovery-B", stateDir)
	wB.loadRecoveryState()

	// Assert: pendingTasks restored.
	if len(wB.pendingTasks) != 1 {
		t.Fatalf("Worker B pendingTasks count=%d, want 1", len(wB.pendingTasks))
	}
	ptRestored, ok := wB.pendingTasks["task-pending-1"]
	if !ok {
		t.Fatal("Worker B pendingTasks[task-pending-1] should exist")
	}
	if ptRestored.JobID != "job-pending-1" ||
		ptRestored.JobRevision != 7 ||
		ptRestored.Spec.ExecutorID != "scene.composite.v1" ||
		ptRestored.Spec.Payload["hook"] != "from-test" {
		t.Fatalf("Worker B pendingTasks[task-pending-1] reconstructed badly: %+v", ptRestored)
	}

	// Assert: activeTaskLeases restored.
	if len(wB.activeTaskLeases) != 2 {
		t.Fatalf("Worker B activeTaskLeases count=%d, want 2", len(wB.activeTaskLeases))
	}
	if al := wB.activeTaskLeases["task-lease-1"]; al == nil || al.AttemptNumber != 1 || al.Revision != 1 {
		t.Fatalf("Worker B activeTaskLeases[task-lease-1] reconstructed badly: %+v", al)
	}
	if al := wB.activeTaskLeases["task-lease-2"]; al == nil || al.AttemptNumber != 2 || al.Revision != 3 {
		t.Fatalf("Worker B activeTaskLeases[task-lease-2] reconstructed badly: %+v", al)
	}

	// Assert: activeTasks NOT restored (Cancel funcs + goroutines
	// are dead across a worker restart).
	if len(wB.activeTasks) != 0 {
		t.Fatalf("Worker B activeTasks count=%d, want 0 (active tasks are not replayed)", len(wB.activeTasks))
	}

	// Idempotence: re-apply the same snapshot. Maps must NOT grow.
	tasks2, leases2, pending2, err := wB.applyRecoverySnapshot(snap)
	if err != nil {
		t.Fatalf("idempotent re-apply: %v", err)
	}
	if tasks2 != 0 || leases2 != 0 || pending2 != 0 {
		t.Fatalf("idempotent re-apply restored something (tasks=%d leases=%d pending=%d), want all zero",
			tasks2, leases2, pending2)
	}
	if len(wB.pendingTasks) != 1 || len(wB.activeTaskLeases) != 2 {
		t.Fatalf("idempotent re-apply grew maps: pending=%d leases=%d",
			len(wB.pendingTasks), len(wB.activeTaskLeases))
	}

	// Idempotence under a different captured-at timestamp: simulate
	// two distinct recovery events landing in quick succession.
	snap2 := snap
	snap2.CapturedAt = snap.CapturedAt.Add(5 * time.Second)
	tasks3, leases3, pending3, err := wB.applyRecoverySnapshot(snap2)
	if err != nil {
		t.Fatalf("distinct re-apply: %v", err)
	}
	if tasks3 != 0 || leases3 != 0 || pending3 != 0 {
		t.Fatalf("distinct re-apply restored something (tasks=%d leases=%d pending=%d)",
			tasks3, leases3, pending3)
	}

	// Capture-after-mutate: modify Worker B's pendingTasks + active
	// leases to a NEW identity, re-snapshot, and confirm the new
	// values overwrite the old (replay is keyed by TaskID, the
	// file is overwritten on disk).
	wB.pendingTasks["task-pending-2"] = &PendingTaskExecution{
		TaskID: "task-pending-2", JobID: "job-pending-2",
		AttemptID: "attempt-pending-2", AttemptNumber: 1,
		LeaseID: "lease-pending-2", ExecutorID: "scene.composite.v1",
		Spec: executor.TaskSpec{JobID: "job-pending-2", ExecutorID: "scene.composite.v1"},
	}
	if err := wB.snapshotRecoveryState(); err != nil {
		t.Fatalf("Worker B re-snapshot: %v", err)
	}
	raw3, _ := os.ReadFile(filepath.Join(stateDir, "worker_recovery.json"))
	var snap3 RecoverySnapshot
	if err := json.Unmarshal(raw3, &snap3); err != nil {
		t.Fatalf("Worker B re-unmarshal: %v", err)
	}
	if len(snap3.PendingTasks) != 2 {
		t.Fatalf("after re-snapshot PendingTasks count=%d, want 2 "+
			"(task-pending-1 carried over + new task-pending-2)",
			len(snap3.PendingTasks))
	}
	ptIDs := map[string]bool{}
	for _, pt := range snap3.PendingTasks {
		ptIDs[pt.TaskID] = true
	}
	if !ptIDs["task-pending-1"] || !ptIDs["task-pending-2"] {
		t.Fatalf("after re-snapshot PendingTasks drifted: %+v", ptIDs)
	}
}
