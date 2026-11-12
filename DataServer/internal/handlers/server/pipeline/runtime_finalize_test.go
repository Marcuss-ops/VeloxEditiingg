package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

func TestRuntimeFinalizationShutdownDoesNotFailJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &RuntimeFinalizationProcessor{}
	if err := p.fail(ctx, taskgraph.RuntimeFinalizationCandidate{TaskID: "task-1", JobID: "job-1"}, errors.New("preparation interrupted")); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown failure = %v, want context cancellation", err)
	}
}

type runtimeFailureRecorder struct {
	scheduled                bool
	scheduleCalls, failCalls int
}

func (r *runtimeFailureRecorder) ScheduleRuntimeFinalizationRetry(context.Context, string, string) (bool, error) {
	r.scheduleCalls++
	return r.scheduled, nil
}
func (r *runtimeFailureRecorder) FailRuntimeFinalization(context.Context, string, string, string) error {
	r.failCalls++
	return nil
}
func (r *runtimeFailureRecorder) ReconcileFailedRuntimePreparations(context.Context, int) (int, error) {
	return 0, nil
}

func TestRuntimeFinalizationRetriesTransientErrorsBeforeFailing(t *testing.T) {
	for _, scheduled := range []bool{true, false} {
		r := &runtimeFailureRecorder{scheduled: scheduled}
		p := &RuntimeFinalizationProcessor{failures: r}
		if err := p.fail(context.Background(), taskgraph.RuntimeFinalizationCandidate{TaskID: "task-1", JobID: "job-1"}, fmt.Errorf("fetch: %w", context.DeadlineExceeded)); err != nil {
			t.Fatal(err)
		}
		wantFail := 0
		if !scheduled {
			wantFail = 1
		}
		if r.scheduleCalls != 1 || r.failCalls != wantFail {
			t.Fatalf("scheduled=%v schedule calls=%d failure calls=%d", scheduled, r.scheduleCalls, r.failCalls)
		}
	}
}

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
func (runtimeFinalizeTestQueue) ClaimRuntimeFinalization(context.Context, string) (bool, error) {
	return true, nil
}

type runtimeFinalizeTestPreparer struct{}

func (runtimeFinalizeTestPreparer) PrepareDriveStockWindows(context.Context, string, []map[string]interface{}) (string, string, error) {
	return "", "", nil
}

type runtimeFinalizeTestNotifier struct{ jobs []string }

func (n *runtimeFinalizeTestNotifier) NotifyTaskUpdated(_ context.Context, jobID string) {
	n.jobs = append(n.jobs, jobID)
}

type runtimeFinalizeTestExpander struct {
	parent       string
	additions    []taskgraph.ExpandedTask
	dependencies []string
	patch        map[string]interface{}
}

func (e *runtimeFinalizeTestExpander) Extend(_ context.Context, parent string, additions []taskgraph.ExpandedTask, dependencies []string, patch map[string]interface{}) ([]taskgraph.Task, error) {
	e.parent, e.additions, e.dependencies, e.patch = parent, additions, dependencies, patch
	return nil, nil
}

func TestRuntimeFinalizationPublishesDurablePreparationFanOut(t *testing.T) {
	expander := &runtimeFinalizeTestExpander{}
	notifier := &runtimeFinalizeTestNotifier{}
	processor := &RuntimeFinalizationProcessor{expander: expander, notifier: notifier}
	identity := strings.Repeat("a", 64)
	preparation := contract.VideoPrepareWindowTaskV1{
		Identity:   identity,
		Request:    contract.VideoPrepareWindowV1{Version: contract.VideoPrepareWindowVersion},
		SegmentIDs: []string{"segment-1", "segment-2"},
	}
	bindings := []map[string]interface{}{{"asset_id": "source-1", "source_uri": "https://media.example/source.mp4"}}
	if err := processor.publishWorkerPreparations(context.Background(), taskgraph.RuntimeFinalizationCandidate{TaskID: "assembly-1", JobID: "job-1"}, bindings, []contract.VideoPrepareWindowTaskV1{preparation}, nil); err != nil {
		t.Fatalf("publishWorkerPreparations: %v", err)
	}
	wantTaskID := "assembly-1-prep-" + identity[:24]
	if expander.parent != "assembly-1" || len(expander.additions) != 1 || expander.additions[0].TaskID != wantTaskID ||
		expander.additions[0].ExecutorID != contract.VideoPrepareWindowExecutorID || expander.dependencies[0] != wantTaskID {
		t.Fatalf("published graph parent=%q additions=%+v dependencies=%v", expander.parent, expander.additions, expander.dependencies)
	}
	if expander.patch[runtimeFinalizeStatusKey] != runtimeFinalizeWaitingPreparation || expander.patch["runtime_assets_pending"] != true {
		t.Fatalf("assembly patch must remain gated while fragments prepare: %#v", expander.patch)
	}
	if len(notifier.jobs) != 1 || notifier.jobs[0] != "job-1" {
		t.Fatalf("notified jobs = %v, want [job-1]", notifier.jobs)
	}
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
