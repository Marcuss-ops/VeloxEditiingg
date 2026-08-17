package creatorflow

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"velox-server/internal/store"
)

// fakeIntakeRecorder is a test double for IntakeSourceRecorder that
// records every IncAccepted call in order.
type fakeIntakeRecorder struct {
	mu      sync.Mutex
	sources []string
}

func (f *fakeIntakeRecorder) IncAccepted(source string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sources = append(f.sources, source)
}

func (f *fakeIntakeRecorder) Calls() []string {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.sources))
	copy(out, f.sources)
	return out
}

// newTestSubmitterStack builds a real Resolver backed by an in-memory
// SQLite store + enqueuer (same pattern as TestResolverEnqueuesWorkerJob)
// so CanonicalJobSubmitter.Submit runs the full atomic path.
func newTestSubmitterStack(t *testing.T) *Resolver {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	_, err = db.DB().Exec(`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('drive-main', 'google_drive', 'Drive Main', 1, '{}', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	enqueuer := newTestEnqueuer(t, db)
	rs := NewResolverFromDeps(enqueuer, db, tempDir, filepath.Join(tempDir, "videos"), "")
	if rs == nil {
		t.Fatalf("resolver construction failed")
	}
	return rs
}

// canonicalSubmitPayload is a completed-creator payload that passes
// ShouldForwardPipelineResult and the atomic write path (mirrors the
// happy-path fixture in TestResolverEnqueuesWorkerJob).
func canonicalSubmitPayload() map[string]interface{} {
	return map[string]interface{}{
		"ok":     true,
		"status": "completed",
		"result": map[string]interface{}{
			"video_name":  "Intake Source Test",
			"script_text": "Creator script",
			"scenes_json": `[{"text":"Scene 1","image":{"asset_id":"scene-image","url":"velox-asset://scene-image"},"voiceover":{"asset_id":"voice","url":"velox-asset://voice","duration_ms":5000}}]`,
		},
	}
}

// canonicalSubmitDeliveryPlan is the separate control-plane delivery
// envelope the resolver consumes (the same shape deliveryplan.ExtractEnvelope
// produces on the real adapter paths).
func canonicalSubmitDeliveryPlan() map[string]interface{} {
	return map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
	}
}

// TestCanonicalJobSubmitter_RecordsIntakeSource verifies that an accepted
// submission is recorded with the producer's intake source.
func TestCanonicalJobSubmitter_RecordsIntakeSource(t *testing.T) {
	rs := newTestSubmitterStack(t)
	recorder := &fakeIntakeRecorder{}
	submitter := NewCanonicalJobSubmitter(rs).WithIntakeSourceRecorder(recorder)
	if submitter == nil {
		t.Fatal("submitter construction failed")
	}

	out, err := submitter.Submit(context.Background(), CanonicalJobSubmission{
		IntakeSource:     IntakeSourceCreator,
		SourceProvider:   "remote_engine",
		SourceJobID:      "intake-source-1",
		TargetExecutorID: "scene.composite.v1",
		Payload:          canonicalSubmitPayload(),
		DeliveryPlan:     canonicalSubmitDeliveryPlan(),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if out == nil || out.Response == nil {
		t.Fatal("want non-nil resolve output")
	}
	if calls := recorder.Calls(); len(calls) != 1 || calls[0] != IntakeSourceCreator {
		t.Fatalf("recorder calls = %v, want [%s]", calls, IntakeSourceCreator)
	}
}

// TestCanonicalJobSubmitter_DefaultIntakeSource verifies that a producer
// that forgets to stamp IntakeSource is still measured under the canonical
// default (never an unnamed series).
func TestCanonicalJobSubmitter_DefaultIntakeSource(t *testing.T) {
	rs := newTestSubmitterStack(t)
	recorder := &fakeIntakeRecorder{}
	submitter := NewCanonicalJobSubmitter(rs).WithIntakeSourceRecorder(recorder)

	_, err := submitter.Submit(context.Background(), CanonicalJobSubmission{
		SourceProvider:   "remote_engine",
		SourceJobID:      "intake-default-1",
		TargetExecutorID: "scene.composite.v1",
		Payload:          canonicalSubmitPayload(),
		DeliveryPlan:     canonicalSubmitDeliveryPlan(),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if calls := recorder.Calls(); len(calls) != 1 || calls[0] != IntakeSourceCanonical {
		t.Fatalf("recorder calls = %v, want [%s]", calls, IntakeSourceCanonical)
	}
}

// TestCanonicalJobSubmitter_NoRecordOnError verifies that a rejected
// submission (resolver error) is NOT counted as accepted.
func TestCanonicalJobSubmitter_NoRecordOnError(t *testing.T) {
	rs := newTestSubmitterStack(t)
	recorder := &fakeIntakeRecorder{}
	submitter := NewCanonicalJobSubmitter(rs).WithIntakeSourceRecorder(recorder)

	// Missing source_job_id fails the resolver; nothing is accepted.
	_, err := submitter.Submit(context.Background(), CanonicalJobSubmission{
		IntakeSource:   IntakeSourceCreator,
		SourceProvider: "remote_engine",
		Payload:        canonicalSubmitPayload(),
	})
	if err == nil {
		t.Fatal("want error for missing source_job_id")
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("recorder calls = %v, want none on error", calls)
	}
}

// TestCanonicalJobSubmitter_NilRecorderIsNoop verifies that an unwired
// recorder does not panic and records nothing.
func TestCanonicalJobSubmitter_NilRecorderIsNoop(t *testing.T) {
	rs := newTestSubmitterStack(t)
	submitter := NewCanonicalJobSubmitter(rs) // no recorder wired
	out, err := submitter.Submit(context.Background(), CanonicalJobSubmission{
		IntakeSource:     IntakeSourceCanonical,
		SourceProvider:   "remote_engine",
		SourceJobID:      "intake-nil-1",
		TargetExecutorID: "scene.composite.v1",
		Payload:          canonicalSubmitPayload(),
		DeliveryPlan:     canonicalSubmitDeliveryPlan(),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if out == nil {
		t.Fatal("want non-nil resolve output")
	}
}

// TestJobSubmissionServiceAliasCompiles pins the deprecated alias so the
// migration contract (old name still resolves to CanonicalJobSubmitter) is
// explicit and guarded against accidental removal. It uses a nil resolver
// (nil submitter) because the test only checks the type contract.
func TestJobSubmissionServiceAliasCompiles(t *testing.T) {
	var _ *JobSubmissionService = NewJobSubmissionService(nil)
	var _ *CanonicalJobSubmitter = NewCanonicalJobSubmitter(nil)
}
