package ingest

import (
	"context"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

func TestIngestionService_SiblingsStillRunningNoJobRollUp(t *testing.T){ taskRepo:=&stubIngestTaskRepo{listTasks:[]taskgraph.Task{{ID:"T1",JobID:"J1",Status:taskgraph.StatusLeased},{ID:"T2",JobID:"J1",Status:taskgraph.StatusPending}}}; jobsRepo:=&stubIngestJobsRepo{getJob:&jobs.Job{ID:"J1",Status:jobs.StatusRunning,Revision:0}}; svc:=newWiredSvc(t,taskRepo,jobsRepo,&stubIngestAttemptRepo{},newStubIngestOutputArtifacts()); res,err:=svc.IngestTaskResult(context.Background(),IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1,Status:"failed",ErrorCode:"RENDER_ERROR"}); if err!=nil { t.Fatal(err) }; if res.JobTransitioned || jobsRepo.setStatusCalls!=0 { t.Fatalf("unexpected rollup: %+v calls=%d",res,jobsRepo.setStatusCalls) } }
