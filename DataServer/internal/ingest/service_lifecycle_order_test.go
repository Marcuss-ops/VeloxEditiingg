package ingest

import (
	"context"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// TestIngestionLifecycleStopsAtAwaitingArtifactBeforeVerifiedSuccess pins the
// observable hand-off: a successful task result rolls the job to
// AWAITING_ARTIFACT, never directly to SUCCEEDED. The final SUCCEEDED write is
// intentionally owned by artifact finalization and is not performed by ingest.
func TestIngestionLifecycleStopsAtAwaitingArtifactBeforeVerifiedSuccess(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{
		listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded}},
	}
	jobsRepo := &stubIngestJobsRepo{
		getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3},
	}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, newStubIngestOutputArtifacts())

	result, err := svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID:        "T1",
		AttemptID:     "A1",
		LeaseID:       "L1",
		WorkerID:      "w-1",
		JobID:         "J1",
		AttemptNumber: 1,
		Status:        "succeeded",
		OutputArtifacts: []DeclaredArtifact{
			{ArtifactID: "artifact-lifecycle", ArtifactType: "video"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.JobTransitioned || result.JobNewStatus != string(jobs.StatusAwaitingArtifact) {
		t.Fatalf("ingest result=%+v, want one transition to AWAITING_ARTIFACT", result)
	}
	if jobsRepo.setStatusCalls != 1 || jobsRepo.lastFrom != jobs.StatusRunning || jobsRepo.lastTo != jobs.StatusAwaitingArtifact {
		t.Fatalf("job transition=(calls=%d, from=%q, to=%q), want (1, RUNNING, AWAITING_ARTIFACT)", jobsRepo.setStatusCalls, jobsRepo.lastFrom, jobsRepo.lastTo)
	}
	if jobsRepo.getJob.Status != jobs.StatusAwaitingArtifact {
		t.Fatalf("job status=%q, want AWAITING_ARTIFACT", jobsRepo.getJob.Status)
	}
}
