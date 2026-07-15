package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"velox-server/internal/taskattempts"
)

func newIdentitySvc(t *testing.T, attempts *stubIngestAttemptRepo) *TaskReportIngestionService { t.Helper(); svc,err:=NewTaskReportIngestionService(&stubIngestTaskRepo{},&stubIngestJobsRepo{},attempts,newStubIngestOutputArtifacts()); if err!=nil { t.Fatal(err) }; return svc }
func assertIdentityMismatch(t *testing.T,err error){ t.Helper(); if err==nil || !errors.Is(err,taskattempts.ErrIdentityMismatch){ t.Fatalf("got %v; want ErrIdentityMismatch",err) } }

func TestIngestionService_ValidateIdentityTuple_WireAttemptIDMismatch(t *testing.T){ a:=&stubIngestAttemptRepo{}; a.seedAttemptWithNumber("T4","w4","L4","A-canonical","J4",1); svc:=newIdentitySvc(t,a); assertIdentityMismatch(t,svc.ValidateIdentityTuple(context.Background(),IngestCommand{TaskID:"T4",AttemptID:"A-attacker",LeaseID:"L4",WorkerID:"w4",JobID:"J4",AttemptNumber:1})) }
func TestIngestionService_ValidateIdentityTuple_WireAttemptNumberMismatch(t *testing.T){ a:=&stubIngestAttemptRepo{}; a.seedAttemptWithNumber("T2","w2","L2","A2","J2",3); svc:=newIdentitySvc(t,a); assertIdentityMismatch(t,svc.ValidateIdentityTuple(context.Background(),IngestCommand{TaskID:"T2",AttemptID:"A2",LeaseID:"L2",WorkerID:"w2",JobID:"J2",AttemptNumber:2})) }
func TestIngestionService_ValidateIdentityTuple_WireJobIDMismatch(t *testing.T){ a:=&stubIngestAttemptRepo{}; a.seedAttemptWithNumber("T3","w3","L3","A3","J-canonical",1); svc:=newIdentitySvc(t,a); assertIdentityMismatch(t,svc.ValidateIdentityTuple(context.Background(),IngestCommand{TaskID:"T3",AttemptID:"A3",LeaseID:"L3",WorkerID:"w3",JobID:"J-wire",AttemptNumber:1})) }
func TestIngestionService_ValidateIdentityTuple_HappyPath(t *testing.T){ a:=&stubIngestAttemptRepo{}; a.seedAttempt("T1","w-1","L1"); svc:=newIdentitySvc(t,a); if err:=svc.ValidateIdentityTuple(context.Background(),IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1}); err!=nil { t.Fatal(err) } }
func TestIngestionService_ValidateIdentityTuple_MismatchReturnsCanonicalSentinel(t *testing.T){ svc:=newIdentitySvc(t,&stubIngestAttemptRepo{}); assertIdentityMismatch(t,svc.ValidateIdentityTuple(context.Background(),IngestCommand{TaskID:"T-x",AttemptID:"A-x",LeaseID:"L-x",WorkerID:"w-x",JobID:"J-x",AttemptNumber:1})) }
func TestIngestionService_ValidateIdentityTuple_EmptyFields(t *testing.T){ svc:=newIdentitySvc(t,&stubIngestAttemptRepo{}); cases:=[]struct{name string; cmd IngestCommand; want string}{{"empty TaskID",IngestCommand{AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1},"TaskID is required"},{"empty AttemptID",IngestCommand{TaskID:"T1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1},"AttemptID is required"},{"empty LeaseID",IngestCommand{TaskID:"T1",AttemptID:"A1",WorkerID:"w-1",JobID:"J1",AttemptNumber:1},"LeaseID is required"},{"empty WorkerID",IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",JobID:"J1",AttemptNumber:1},"WorkerID is required"},{"empty JobID",IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",AttemptNumber:1},"JobID is required"},{"zero AttemptNumber",IngestCommand{TaskID:"T1",AttemptID:"A1",LeaseID:"L1",WorkerID:"w-1",JobID:"J1"},"AttemptNumber must be >0"}}; for _,tc:=range cases { t.Run(tc.name,func(t *testing.T){ err:=svc.ValidateIdentityTuple(context.Background(),tc.cmd); if err==nil || !strings.Contains(err.Error(),tc.want){ t.Fatalf("got %v; want %q",err,tc.want) } }) } }
