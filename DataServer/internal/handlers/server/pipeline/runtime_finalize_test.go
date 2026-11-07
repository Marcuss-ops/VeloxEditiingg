package pipeline

import (
	"context"
	"testing"

	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

type runtimeFinalizeTestSpecStore struct {
	payload    map[string]interface{}
	executorID string
}

func (s *runtimeFinalizeTestSpecStore) FinalizeTaskSpec(_ context.Context, _ string, executorID string, patch map[string]interface{}) (map[string]interface{}, error) {
	s.executorID = executorID
	return s.MergeTaskSpecPayload(context.Background(), "", patch)
}

func (s *runtimeFinalizeTestSpecStore) GetTaskSpecPayload(context.Context, string) (map[string]interface{}, error) {
	copy := make(map[string]interface{}, len(s.payload))
	for key, value := range s.payload {
		copy[key] = value
	}
	return copy, nil
}

func (s *runtimeFinalizeTestSpecStore) MergeTaskSpecPayload(_ context.Context, _ string, patch map[string]interface{}) (map[string]interface{}, error) {
	for key, value := range patch {
		if value == nil {
			delete(s.payload, key)
		} else {
			s.payload[key] = value
		}
	}
	return s.payload, nil
}

type runtimeFinalizeTestQueue struct{}

func (runtimeFinalizeTestQueue) RecoverInterruptedRuntimeFinalizations(context.Context) error {
	return nil
}
func (runtimeFinalizeTestQueue) ListRuntimeFinalizationCandidates(context.Context, int) ([]taskgraph.RuntimeFinalizationCandidate, error) {
	return nil, nil
}

type runtimeFinalizeTestPreparer struct{}

func (runtimeFinalizeTestPreparer) PrepareDriveStockWindows(context.Context, string, []map[string]interface{}) (string, string, error) {
	return "", "", nil
}

type runtimeFinalizeTestNotifier struct{ jobs []string }

func (n *runtimeFinalizeTestNotifier) NotifyTaskUpdated(_ context.Context, jobID string) {
	n.jobs = append(n.jobs, jobID)
}

func TestRuntimeFinalizationProcessorCommitsBeforeNotifying(t *testing.T) {
	store := &runtimeFinalizeTestSpecStore{payload: map[string]interface{}{
		"runtime_assets_pending":                  true,
		contract.PayloadKeyCompiledRenderPlanJSON: `{"plan_version":2,"assets":[]}`,
		runtimeFinalizeStatusKey:                  runtimeFinalizeQueued,
		runtimeFinalizeRequestKey: map[string]interface{}{
			"idempotency_key": "finalize-1",
			"runtime_assets":  []interface{}{},
			"runtime_payload": map[string]interface{}{"render_quality": "high"},
		},
	}}
	notifier := &runtimeFinalizeTestNotifier{}
	processor := &RuntimeFinalizationProcessor{
		queue:     runtimeFinalizeTestQueue{},
		specs:     store,
		finalizer: store,
		assets:    runtimeFinalizeTestPreparer{},
		notifier:  notifier,
	}
	if err := processor.processOne(context.Background(), taskgraph.RuntimeFinalizationCandidate{TaskID: "task-1", JobID: "job-1"}); err != nil {
		t.Fatalf("processOne: %v", err)
	}
	if store.payload[runtimeFinalizeStatusKey] != runtimeFinalizeSucceeded {
		t.Fatalf("finalization status = %v, want %s", store.payload[runtimeFinalizeStatusKey], runtimeFinalizeSucceeded)
	}
	if store.payload["runtime_assets_pending"] != false {
		t.Fatalf("runtime_assets_pending = %v, want false", store.payload["runtime_assets_pending"])
	}
	if store.payload["runtime_finalize_idempotency_key"] != "finalize-1" {
		t.Fatalf("idempotency key = %v", store.payload["runtime_finalize_idempotency_key"])
	}
	if store.executorID != contract.VideoAssemblyCopyExecutorID {
		t.Fatalf("final executor = %q, want %q", store.executorID, contract.VideoAssemblyCopyExecutorID)
	}
	if _, exists := store.payload[runtimeFinalizeRequestKey]; exists {
		t.Fatal("completed durable request was not cleared")
	}
	if len(notifier.jobs) != 1 || notifier.jobs[0] != "job-1" {
		t.Fatalf("notifications = %v, want [job-1]", notifier.jobs)
	}
}
