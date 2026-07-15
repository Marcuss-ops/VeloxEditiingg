package ingest

import (
	"context"
	"strings"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

func TestIngestionService_HappyPathSucceeded(t *testing.T){ taskRepo:=&stubIngestTaskRepo{listTasks:[]taskgraph.Task{{ID:"T1",JobID:"J1",Status:taskgraph.StatusSucceeded}}}; jobsRepo:=&stubIngestJobsRepo{getJob:&jobs.Job{ID:"J1",Status:jobs.StatusRunning,MaxRetries:3,Revision:0}}; svc:=newWiredSvc(t,taskRepo,jobsRepo,&stubIngestAttemptRepo{},newStubIngestOutputArtifacts()); res,err:=svc.IngestTaskResult(context.Background(),IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1,Status:"succeeded",OutputArtifacts:[]DeclaredArtifact{{ArtifactID:"art-1",ArtifactType:"video"},{ArtifactID:"art-2",ArtifactType:"video"}}}); if err!=nil { t.Fatal(err) }; if !res.AttemptClosed || res.ArtifactsNew!=2 || !res.JobTransitioned || res.JobNewStatus!=string(jobs.StatusAwaitingArtifact) { t.Fatalf("unexpected result: %+v",res) }; if taskRepo.transitionCalls!=1 || taskRepo.transitionedState!=taskgraph.StatusSucceeded || jobsRepo.setStatusCalls!=1 { t.Fatalf("unexpected side effects") } }

func TestIngestionService_IdempotentReplay(t *testing.T){ taskRepo:=&stubIngestTaskRepo{transitionErr:taskgraph.ErrTransitionConflict,listTasks:[]taskgraph.Task{{ID:"T1",JobID:"J1",Status:taskgraph.StatusSucceeded}}}; jobsRepo:=&stubIngestJobsRepo{getJob:&jobs.Job{ID:"J1",Status:jobs.StatusAwaitingArtifact,Revision:0}}; out:=newStubIngestOutputArtifacts(); for _,id:=range []string{"art-1","art-2"}{ _=out.Register(context.Background(),taskoutput_artifacts.OutputArtifact{TaskID:"T1",ArtifactID:id,AttemptID:"A1"}) }; svc:=newWiredSvc(t,taskRepo,jobsRepo,&stubIngestAttemptRepo{},out); res,err:=svc.IngestTaskResult(context.Background(),IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1,Status:"succeeded",OutputArtifacts:[]DeclaredArtifact{{ArtifactID:"art-1"},{ArtifactID:"art-2"}}}); if err!=nil { t.Fatal(err) }; if res.AttemptClosed || res.ArtifactsNew!=0 || res.ArtifactsSkips!=0 || jobsRepo.setStatusCalls!=0 { t.Fatalf("unexpected replay result: %+v",res) } }

func TestIngestionService_RequiresAllDeps(t *testing.T){ out:=newStubIngestOutputArtifacts(); attempts:=&stubIngestAttemptRepo{}; cases:=[]struct{name,want string; build func()error}{{"task","taskRepo",func()error{_,e:=NewTaskReportIngestionService(nil,nil,attempts,out);return e}},{"jobs","jobsRepo",func()error{_,e:=NewTaskReportIngestionService(&stubIngestTaskRepo{},nil,attempts,out);return e}},{"attempt","attemptRepo",func()error{_,e:=NewTaskReportIngestionService(&stubIngestTaskRepo{},&stubIngestJobsRepo{},nil,out);return e}},{"output","outputArtRepo",func()error{_,e:=NewTaskReportIngestionService(&stubIngestTaskRepo{},&stubIngestJobsRepo{},attempts,nil);return e}}}; for _,tc:=range cases { t.Run(tc.name,func(t *testing.T){ err:=tc.build(); if err==nil || !strings.Contains(err.Error(),tc.want){ t.Fatalf("got %v; want %s",err,tc.want) } }) } }
