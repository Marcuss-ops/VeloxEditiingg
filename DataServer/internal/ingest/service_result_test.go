package ingest

import (
	"context"
	"strings"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

func TestIngestionService_HappyPathSucceeded(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded}}}
	jobsRepo := &stubIngestJobsRepo{getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0}}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, newStubIngestOutputArtifacts())
	res, err := svc.IngestTaskResult(context.Background(), IngestCommand{TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1", AttemptNumber: 1, Status: "succeeded", OutputArtifacts: []DeclaredArtifact{{ArtifactID: "art-1", ArtifactType: "video"}, {ArtifactID: "art-2", ArtifactType: "video"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.AttemptClosed || res.ArtifactsNew != 2 || !res.JobTransitioned || res.JobNewStatus != string(jobs.StatusAwaitingArtifact) {
		t.Fatalf("unexpected result: %+v", res)
	}
	if taskRepo.transitionCalls != 1 || taskRepo.transitionedState != taskgraph.StatusSucceeded || jobsRepo.setStatusCalls != 1 {
		t.Fatalf("unexpected side effects")
	}
}

func TestIngestionService_IdempotentReplay(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{transitionErr: taskgraph.ErrTransitionConflict, listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded}}}
	jobsRepo := &stubIngestJobsRepo{getJob: &jobs.Job{ID: "J1", Status: jobs.StatusAwaitingArtifact, Revision: 0}}
	out := newStubIngestOutputArtifacts()
	for _, id := range []string{"art-1", "art-2"} {
		_ = out.Register(context.Background(), taskoutput_artifacts.OutputArtifact{TaskID: "T1", ArtifactID: id, AttemptID: "A1"})
	}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, out)
	res, err := svc.IngestTaskResult(context.Background(), IngestCommand{TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1", AttemptNumber: 1, Status: "succeeded", OutputArtifacts: []DeclaredArtifact{{ArtifactID: "art-1"}, {ArtifactID: "art-2"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.AttemptClosed || res.ArtifactsNew != 0 || res.ArtifactsSkips != 0 || jobsRepo.setStatusCalls != 0 {
		t.Fatalf("unexpected replay result: %+v", res)
	}
}

func TestIngestionService_CanonicalizesDetailedPhaseIdentityBeforeAtomicIngest(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{
		listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded}},
		nowTask: taskgraph.Task{
			ID: "T1", JobID: "J1", ExecutorID: "executor.master", ExecutorVersion: 7,
		},
	}
	jobsRepo := &stubIngestJobsRepo{getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0}}
	attempts := &stubIngestAttemptRepo{}
	attempts.seedAttempt("T1", "w-1", "L1")
	attempts.attempts["T1|w-1|L1"].WorkerSnapshotID = "snapshot.master"
	taskRepo.allCommitsCommitted = true
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1",
		AttemptNumber: 1, Status: "failed",
		ExecutorID: "executor.attacker", ExecutorVersion: 999,
		PhaseTimings: []taskattempts.PhaseTimingDetailed{{
			AttemptID: "spoofed-attempt", TaskID: "spoofed-task", JobID: "spoofed-job", WorkerID: "spoofed-worker",
			WorkerSnapshotID: "snapshot.attacker", ExecutorID: "executor.attacker", ExecutorVersion: 999,
			Origin: "engine", Scope: "segment", EventIndex: 4, Component: "engine", Action: "encode", Status: "failed",
		}},
		PartialPhaseMetrics: []taskattempts.PhaseTimingDetailed{{
			AttemptID: "spoofed-attempt-2", TaskID: "spoofed-task-2", JobID: "spoofed-job-2", WorkerID: "spoofed-worker-2",
			WorkerSnapshotID: "snapshot.attacker-2", ExecutorID: "executor.attacker", ExecutorVersion: 999,
			Origin: "worker", Scope: "attempt", EventIndex: 5, Component: "worker", Action: "cleanup", Status: "ok",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got := taskRepo.lastIngestCommand
	if len(got.PhaseTimings) != 1 || len(got.PartialPhaseMetrics) != 1 {
		t.Fatalf("persisted phase slices = (%d, %d); want (1, 1)", len(got.PhaseTimings), len(got.PartialPhaseMetrics))
	}
	for name, phase := range map[string]taskattempts.PhaseTimingDetailed{
		"phase": got.PhaseTimings[0], "partial": got.PartialPhaseMetrics[0],
	} {
		if phase.AttemptID != "A1" || phase.TaskID != "T1" || phase.JobID != "J1" || phase.WorkerID != "w-1" {
			t.Errorf("%s canonical tuple = (%q, %q, %q, %q); want (A1, T1, J1, w-1)", name, phase.AttemptID, phase.TaskID, phase.JobID, phase.WorkerID)
		}
		if phase.WorkerSnapshotID != "snapshot.master" || phase.ExecutorID != "executor.master" || phase.ExecutorVersion != 7 {
			t.Errorf("%s canonical runtime identity = snapshot=%q executor=(%q, %d); want snapshot.master/executor.master/7", name, phase.WorkerSnapshotID, phase.ExecutorID, phase.ExecutorVersion)
		}
	}
}

func TestIngestionService_CanonicalizesDetailedPhaseIdentityBeforeAtomicIngest_LegacyEmptyExecutor(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{
		listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusFailed}},
		nowTask:   taskgraph.Task{ID: "T1", JobID: "J1"},
	}
	jobsRepo := &stubIngestJobsRepo{getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0}}
	attempts := &stubIngestAttemptRepo{}
	attempts.seedAttempt("T1", "w-1", "L1")
	taskRepo.allCommitsCommitted = true
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1", AttemptNumber: 1,
		Status: "failed", PhaseTimings: []taskattempts.PhaseTimingDetailed{{Component: "worker", Action: "run"}},
	})
	if err != nil {
		t.Fatalf("legacy empty executor report rejected: %v", err)
	}
}

func TestIngestionService_StampsRenderIdentityFromReport(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded}}}
	jobsRepo := &stubIngestJobsRepo{getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0}}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, newStubIngestOutputArtifacts())

	const (
		wantEngine = "velox-engine/v2.9.1"
		wantFinal  = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		wantThumb  = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	)
	_, err := svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1",
		AttemptNumber: 1, Status: "succeeded",
		EngineVersion: wantEngine,
		// The final video is the SECOND declaration and the first with a
		// non-empty declared SHA (thumbnail declares none) — the derivation
		// must pick the first non-empty SHA in declaration order.
		OutputArtifacts: []DeclaredArtifact{
			{ArtifactID: "art-thumb", ArtifactType: "image", SHA256: ""},
			{ArtifactID: "art-final", ArtifactType: "video", SHA256: wantFinal},
			{ArtifactID: "art-audio", ArtifactType: "audio", SHA256: wantThumb},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got := taskRepo.lastIngestCommand
	if got.RendererVersion != wantEngine {
		t.Errorf("RendererVersion = %q; want %q", got.RendererVersion, wantEngine)
	}
	if got.ArtifactSHA256 != wantFinal {
		t.Errorf("ArtifactSHA256 = %q; want first non-empty declared SHA %q", got.ArtifactSHA256, wantFinal)
	}

	// No declared SHA → chain tail must not fabricate a value.
	taskRepo2 := &stubIngestTaskRepo{listTasks: []taskgraph.Task{{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded}}}
	jobsRepo2 := &stubIngestJobsRepo{getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0}}
	svc2 := newWiredSvc(t, taskRepo2, jobsRepo2, &stubIngestAttemptRepo{}, newStubIngestOutputArtifacts())
	if _, err := svc2.IngestTaskResult(context.Background(), IngestCommand{
		TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1",
		AttemptNumber: 1, Status: "failed", EngineVersion: "",
		OutputArtifacts: []DeclaredArtifact{{ArtifactID: "art-final", ArtifactType: "video", SHA256: ""}},
	}); err != nil {
		t.Fatal(err)
	}
	if got2 := taskRepo2.lastIngestCommand; got2.RendererVersion != "" || got2.ArtifactSHA256 != "" {
		t.Errorf("empty report fabricated identity: renderer=%q artifact_sha=%q; want empty/empty", got2.RendererVersion, got2.ArtifactSHA256)
	}
}

func TestIngestionService_RequiresAllDeps(t *testing.T) {
	out := newStubIngestOutputArtifacts()
	attempts := &stubIngestAttemptRepo{}
	cases := []struct {
		name, want string
		build      func() error
	}{{"task", "taskRepo", func() error { _, e := NewTaskReportIngestionService(nil, nil, attempts, out); return e }}, {"jobs", "jobsRepo", func() error {
		_, e := NewTaskReportIngestionService(&stubIngestTaskRepo{}, nil, attempts, out)
		return e
	}}, {"attempt", "attemptRepo", func() error {
		_, e := NewTaskReportIngestionService(&stubIngestTaskRepo{}, &stubIngestJobsRepo{}, nil, out)
		return e
	}}, {"output", "outputArtRepo", func() error {
		_, e := NewTaskReportIngestionService(&stubIngestTaskRepo{}, &stubIngestJobsRepo{}, attempts, nil)
		return e
	}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v; want %s", err, tc.want)
			}
		})
	}
}
