package placement

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newWorkerSnapshot(executors map[ExecutorKey]struct{}, caps map[string]bool, ready bool, draining bool, freeSlots int, maxParallel int) WorkerSnapshot {
	return WorkerSnapshot{
		WorkerID:        "w-1",
		SessionID:       "s-1",
		Ready:           ready,
		Draining:        draining,
		SessionAlive:    true,
		MaxParallelJobs: maxParallel,
		ActiveJobs:      maxParallel - freeSlots,
		Executors:       executors,
		Capabilities:    caps,
	}
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
			TaskID:   "t-unsupported",
			Priority: 100,
			CreatedAt: now,
			Executor: ExecutorKey{ID: "ffmpeg.v1", Version: 1},
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


