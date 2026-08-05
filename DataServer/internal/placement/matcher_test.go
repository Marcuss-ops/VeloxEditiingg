package placement

import (
	"testing"
	"time"

	"velox-shared/controltransport"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newWorkerSnapshot(executors map[ExecutorKey]struct{}, caps map[string]bool, ready bool, draining bool, freeSlots int, maxParallel int) WorkerSnapshot {
	return WorkerSnapshot{
		WorkerID:         "w-1",
		SessionID:        "s-1",
		Ready:            ready,
		Draining:         draining,
		SessionAlive:     true,
		MaxParallelJobs:  maxParallel,
		ActiveJobs:       maxParallel - freeSlots,
		ExecutorRegistry: registryFromExecutorKeys(executors),
		Capabilities:     caps,
	}
}

func registryFromExecutorKeys(keys map[ExecutorKey]struct{}) controltransport.ExecutorRegistry {
	capabilities := make([]controltransport.ExecutorCapability, 0, len(keys))
	for key := range keys {
		capabilities = append(capabilities, controltransport.ExecutorCapability{ID: key.ID, Version: key.Version})
	}
	registry, err := controltransport.NewExecutorRegistry(capabilities...)
	if err != nil {
		panic(err)
	}
	return registry
}

func executorKeys(keys ...ExecutorKey) map[ExecutorKey]struct{} {
	m := make(map[ExecutorKey]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

func capMap(caps ...string) map[string]bool {
	m := make(map[string]bool, len(caps))
	for _, c := range caps {
		m[c] = true
	}
	return m
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

func TestMatcherRejectsMissingExecutor(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		nil, true, false, 1, 1,
	)

	candidates := []TaskCandidate{
		{
			TaskID:   "t-ffmpeg",
			Priority: 10,
			Executor: ExecutorKey{ID: "ffmpeg.v1", Version: 1},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("expected nil candidate, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(result.Rejections))
	}
	if result.Rejections[0].Code != RejectUnsupportedExecutor {
		t.Fatalf("expected RejectUnsupportedExecutor, got %s", result.Rejections[0].Code)
	}
}

func TestMatcherRejectsWrongExecutorVersion(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 2}),
		nil, true, false, 1, 1,
	)

	candidates := []TaskCandidate{
		{
			TaskID:   "t-scene",
			Priority: 10,
			Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("expected nil candidate with version mismatch, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(result.Rejections))
	}
	if result.Rejections[0].Code != RejectUnsupportedExecutor {
		t.Fatalf("expected RejectUnsupportedExecutor, got %s", result.Rejections[0].Code)
	}
}

func TestMatcherRejectsWorkerWithoutFreeSlots(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		nil, true, false, 0, 1,
	)

	candidates := []TaskCandidate{
		{
			TaskID:   "t-1",
			Priority: 10,
			Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("expected nil candidate with full capacity, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(result.Rejections))
	}
	if result.Rejections[0].Code != RejectCapacityFull {
		t.Fatalf("expected RejectCapacityFull, got %s", result.Rejections[0].Code)
	}
}

func TestMatcherRejectsDrainingWorker(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		nil, true, true, 5, 10,
	)

	candidates := []TaskCandidate{
		{
			TaskID:   "t-1",
			Priority: 10,
			Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("expected nil candidate for draining worker, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(result.Rejections))
	}
	if result.Rejections[0].Code != RejectWorkerDraining {
		t.Fatalf("expected RejectWorkerDraining, got %s", result.Rejections[0].Code)
	}
}

func TestMatcherRejectsMissingRequiredCapability(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("some.other.cap.v1"),
		true, false, 1, 1,
	)

	candidates := []TaskCandidate{
		{
			TaskID:               "t-1",
			Priority:             10,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("expected nil candidate with missing capability, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(result.Rejections))
	}
	if result.Rejections[0].Code != RejectMissingCapability {
		t.Fatalf("expected RejectMissingCapability, got %s", result.Rejections[0].Code)
	}
}

func TestMatcherSelectsHighestPriorityCompatibleTask(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	candidates := []TaskCandidate{
		{
			TaskID:               "t-low",
			Priority:             1,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
		{
			TaskID:               "t-high",
			Priority:             100,
			CreatedAt:            now.Add(1 * time.Hour),
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate == nil {
		t.Fatal("expected a candidate, got nil")
	}
	if result.Candidate.TaskID != "t-high" {
		t.Fatalf("expected highest priority task t-high, got %s", result.Candidate.TaskID)
	}
}

func TestMatcherPrefersWarmCacheWithinPriority(t *testing.T) {
	m := NewMatcher()
	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		nil, true, false, 1, 1,
	)
	worker.CachedAssetKeys = map[string]struct{}{"clip-shared": {}}
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	result := m.Select(worker, []TaskCandidate{
		{TaskID: "cold", Priority: 10, CreatedAt: now, Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1}, RequiredAssetKeys: []string{"clip-cold"}},
		{TaskID: "warm", Priority: 10, CreatedAt: now.Add(time.Minute), Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1}, RequiredAssetKeys: []string{"clip-shared"}},
	})
	if result.Candidate == nil || result.Candidate.TaskID != "warm" {
		t.Fatalf("candidate=%v; want warm-cache task", result.Candidate)
	}
}

func TestMatcherWarmCacheDoesNotOverrideHigherPriority(t *testing.T) {
	m := NewMatcher()
	worker := newWorkerSnapshot(executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}), nil, true, false, 1, 1)
	worker.CachedAssetKeys = map[string]struct{}{"clip-shared": {}}
	result := m.Select(worker, []TaskCandidate{
		{TaskID: "high-cold", Priority: 20, Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1}, RequiredAssetKeys: []string{"clip-cold"}},
		{TaskID: "low-warm", Priority: 10, Executor: ExecutorKey{ID: "scene.composite.v1", Version: 1}, RequiredAssetKeys: []string{"clip-shared"}},
	})
	if result.Candidate == nil || result.Candidate.TaskID != "high-cold" {
		t.Fatalf("candidate=%v; warm cache must not override priority", result.Candidate)
	}
}

func TestMatcherKeepsFIFOWithinSamePriority(t *testing.T) {
	m := NewMatcher()

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	candidates := []TaskCandidate{
		{
			TaskID:               "t-later",
			Priority:             10,
			CreatedAt:            now.Add(1 * time.Hour),
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
		{
			TaskID:               "t-earlier",
			Priority:             10,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate == nil {
		t.Fatal("expected a candidate, got nil")
	}
	if result.Candidate.TaskID != "t-earlier" {
		t.Fatalf("expected FIFO t-earlier within same priority, got %s", result.Candidate.TaskID)
	}
}

func TestMatcherSkipsIncompatibleAndSelectsNextCompatible(t *testing.T) {
	m := NewMatcher()

	// Worker supports only scene.composite.v1@1
	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	candidates := []TaskCandidate{
		{
			// Task A: requires unsupported executor, higher priority
			TaskID:    "t-unsupported",
			Priority:  100,
			CreatedAt: now,
			Executor:  ExecutorKey{ID: "ffmpeg.v1", Version: 1},
		},
		{
			// Task B: supported executor, lower priority
			TaskID:               "t-scene",
			Priority:             10,
			CreatedAt:            now.Add(1 * time.Hour),
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate == nil {
		t.Fatal("expected a candidate, got nil")
	}
	if result.Candidate.TaskID != "t-scene" {
		t.Fatalf("expected matcher to skip t-unsupported and select t-scene, got %s", result.Candidate.TaskID)
	}
	// Verify Task A was rejected.
	found := false
	for _, r := range result.Rejections {
		if r.TaskID == "t-unsupported" && r.Code == RejectUnsupportedExecutor {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected t-unsupported to be rejected as unsupported_executor, rejections: %+v", result.Rejections)
	}
}

// TestMatcherRejectsWorkerExcludedByPlacementPin — when VELOX_PLACEMENT_PIN_
// WORKER_ID is set to a target worker, every other worker_id receives a
// single RejectPlacementPinExcluded and is short-circuited BEFORE any
// executor/capability gate. Powers tests/worker-cert/smoke_one.sh.
func TestMatcherRejectsWorkerExcludedByPlacementPin(t *testing.T) {
	m := NewMatcher()
	m.SetPin("w-target")

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)
	worker.WorkerID = "w-other"

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	candidates := []TaskCandidate{
		{
			TaskID:               "t-scene",
			Priority:             10,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("expected nil candidate, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection from pin gate, got %d: %+v", len(result.Rejections), result.Rejections)
	}
	r := result.Rejections[0]
	if r.Code != RejectPlacementPinExcluded {
		t.Fatalf("expected RejectPlacementPinExcluded, got %s", r.Code)
	}
	if r.TaskID != "" {
		t.Fatalf("expected empty TaskID on pin terminal gate, got %s", r.TaskID)
	}
}

// TestMatcherAllowsPinnedWorker — the pin gate is transparent for the
// pinned worker_id itself; subsequent capability/capacity gates still
// apply normally.
func TestMatcherAllowsPinnedWorker(t *testing.T) {
	m := NewMatcher()
	m.SetPin("w-target")

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)
	worker.WorkerID = "w-target"

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	candidates := []TaskCandidate{
		{
			TaskID:               "t-scene",
			Priority:             10,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate == nil {
		t.Fatal("pinned worker should still match a compatible candidate")
	}
	if result.Candidate.TaskID != "t-scene" {
		t.Fatalf("expected t-scene selected, got %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 0 {
		t.Fatalf("pinned-compatible worker should have no rejections, got %+v", result.Rejections)
	}
}

// TestMatcherPinDisabledByDefault — a freshly-constructed Matcher with
// no SetPin call behaves exactly as the pre-pin stateless engine: a
// non-target worker with matching capabilities still gets a candidate.
func TestMatcherPinDisabledByDefault(t *testing.T) {
	m := NewMatcher() // no SetPin → pin empty

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)
	worker.WorkerID = "w-other"

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	candidates := []TaskCandidate{
		{
			TaskID:               "t-scene",
			Priority:             10,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate == nil {
		t.Fatal("without a pin installed, a compatible worker should still be selectable")
	}
	if result.Candidate.TaskID != "t-scene" {
		t.Fatalf("expected t-scene selected, got %s", result.Candidate.TaskID)
	}
	for _, r := range result.Rejections {
		if r.Code == RejectPlacementPinExcluded {
			t.Fatalf("pin-disable regression: pin exclusion fired with no SetPin call; rejections: %+v", result.Rejections)
		}
	}
}

// TestMatcherPinWhitespaceOnlyIsTreatedAsNoPin — SetPin applies TrimSpace
// before storing the value via atomic.Value, so a whitespace-only arg
// collapses to the empty (no-pin) state. This prevents an operator
// typo like `VELOX_PLACEMENT_PIN_WORKER_ID="   "` from accidentally
// pinning the master to a worker_id that no real worker can match.
func TestMatcherPinWhitespaceOnlyIsTreatedAsNoPin(t *testing.T) {
	m := NewMatcher()
	m.SetPin("   \t  \n")

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)
	worker.WorkerID = "w-other"

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	candidates := []TaskCandidate{
		{
			TaskID:               "t-scene",
			Priority:             10,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate == nil {
		t.Fatal("whitespace-only pin should be treated as no pin; expected a candidate")
	}
	if result.Candidate.TaskID != "t-scene" {
		t.Fatalf("expected t-scene, got %s", result.Candidate.TaskID)
	}
	for _, r := range result.Rejections {
		if r.Code == RejectPlacementPinExcluded {
			t.Fatalf("whitespace-only pin must NOT trigger pin exclusion; rejections: %+v", result.Rejections)
		}
	}
}

// TestMatcherRejectsEmptyWorkerIdWhenPinIsSet — worker_id="" can never
// match a non-empty pin, so the pin-exclusion fires on this defensive
// abnormal state. Documents the boundary explicitly: the operator
// reads this as "an empty worker_id is never the intended pin
// recipient" rather than an exception.
func TestMatcherRejectsEmptyWorkerIdWhenPinIsSet(t *testing.T) {
	m := NewMatcher()
	m.SetPin("w-target")

	worker := newWorkerSnapshot(
		executorKeys(ExecutorKey{ID: "scene.composite.v1", Version: 1}),
		capMap("artifact.commit.v1"),
		true, false, 2, 2,
	)
	worker.WorkerID = "" // defensive abnormal state

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	candidates := []TaskCandidate{
		{
			TaskID:               "t-scene",
			Priority:             10,
			CreatedAt:            now,
			Executor:             ExecutorKey{ID: "scene.composite.v1", Version: 1},
			RequiredCapabilities: []string{"artifact.commit.v1"},
		},
	}

	result := m.Select(worker, candidates)

	if result.Candidate != nil {
		t.Fatalf("worker_id=\"\" + pin=\"w-target\" should be excluded; got candidate %s", result.Candidate.TaskID)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 pin-exclusion rejection, got %d: %+v", len(result.Rejections), result.Rejections)
	}
	if result.Rejections[0].Code != RejectPlacementPinExcluded {
		t.Fatalf("expected RejectPlacementPinExcluded, got %s", result.Rejections[0].Code)
	}
}
