package observability

import (
	"context"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

type inspectionJobReader struct {
	job *jobs.Job
}

func (r *inspectionJobReader) Get(context.Context, string) (*jobs.Job, error) { return r.job, nil }
func (r *inspectionJobReader) List(context.Context, jobs.Filter) ([]jobs.Job, error) {
	if r.job == nil {
		return nil, nil
	}
	return []jobs.Job{*r.job}, nil
}
func (r *inspectionJobReader) Counts(context.Context) (jobs.Counts, error) { return nil, nil }

type inspectionExtras struct{}

func (inspectionExtras) ListJobEvents(context.Context, string, int) ([]JobEvent, error) {
	return []JobEvent{{Timestamp: "2026-08-09T10:00:00Z", JobID: "J-1", Event: "TASK_ACCEPTED"}}, nil
}
func (inspectionExtras) ListArtifacts(context.Context, string, int) ([]ArtifactSnapshot, error) {
	return []ArtifactSnapshot{{ID: "artifact-1", Status: "READY", SizeBytes: 42}}, nil
}
func (inspectionExtras) ListDeliveries(context.Context, string) ([]DeliverySnapshot, error) {
	return []DeliverySnapshot{{DeliveryID: "delivery-1", Status: "SUCCEEDED"}}, nil
}

func TestInspectJobComposesOperatorReadModel(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-1"] = &taskgraph.Task{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, AttemptCount: 1}
	svc.WithJobs(&inspectionJobReader{job: &jobs.Job{ID: "J-1", Status: jobs.StatusSucceeded}}).
		WithJobInspection(inspectionExtras{})

	result, err := svc.InspectJob(context.Background(), "J-1")
	if err != nil {
		t.Fatalf("InspectJob() error: %v", err)
	}
	if result.Job == nil || result.Job.ID != "J-1" {
		t.Fatalf("job = %#v, want J-1", result.Job)
	}
	if result.Execution == nil || result.Execution.TaskID != "T-1" {
		t.Fatalf("execution = %#v, want task T-1", result.Execution)
	}
	if len(result.Events) != 1 || result.Events[0].Event != "TASK_ACCEPTED" {
		t.Fatalf("events = %#v", result.Events)
	}
	if len(result.Artifacts) != 1 || len(result.Deliveries) != 1 {
		t.Fatalf("artifacts/deliveries = %d/%d", len(result.Artifacts), len(result.Deliveries))
	}
}

func TestReadinessVerdictFailsClosed(t *testing.T) {
	status, _ := readinessVerdict(nil)
	if status != "UNKNOWN" {
		t.Fatalf("missing readiness status = %q, want UNKNOWN", status)
	}
	status, detail := readinessVerdict(map[string]any{"cache_protection_ready": false})
	if status != "FAIL" || detail == "" {
		t.Fatalf("failed readiness = %q/%q", status, detail)
	}
}

func TestProductionDoctorDetectsReadinessAndDigestDrift(t *testing.T) {
	svc, _, _, _, workers := newTestService()
	workers.workers = []map[string]any{{
		"worker_id": "worker-1", "worker_name": "velox-worker-01",
		"status": "CONNECTED", "readiness": map[string]any{"cache_protection_ready": false},
		"target_digest": "sha256:desired", "image_digest": "sha256:running",
	}}
	svc.WithWorkers(workers)
	result, err := svc.ProductionDoctor(context.Background())
	if err != nil {
		t.Fatalf("ProductionDoctor() error: %v", err)
	}
	if result.Healthy {
		t.Fatal("ProductionDoctor() reported unhealthy worker as healthy")
	}
	if len(result.Checks) != 4 {
		t.Fatalf("checks = %d, want 4", len(result.Checks))
	}
}
