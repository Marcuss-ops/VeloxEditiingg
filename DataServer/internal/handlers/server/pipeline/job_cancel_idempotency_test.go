package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/jobs"
	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

type cancelIdempotencyJobs struct {
	job         *jobs.Job
	cancelCalls int
	cancelErr   error
	afterCancel func()
}

func (s *cancelIdempotencyJobs) Get(context.Context, string) (*jobs.Job, error) { return s.job, nil }
func (s *cancelIdempotencyJobs) List(context.Context, jobs.Filter) ([]jobs.Job, error) {
	return nil, nil
}
func (s *cancelIdempotencyJobs) Counts(context.Context) (jobs.Counts, error) { return nil, nil }
func (s *cancelIdempotencyJobs) SetStatus(context.Context, string, jobs.JobStatus, jobs.JobStatus) error {
	return nil
}
func (s *cancelIdempotencyJobs) Fail(context.Context, string, string) error { return nil }
func (s *cancelIdempotencyJobs) Cancel(context.Context, string, string, int) error {
	s.cancelCalls++
	if s.afterCancel != nil {
		s.afterCancel()
	}
	return s.cancelErr
}
func (s *cancelIdempotencyJobs) Delete(context.Context, string) error { return nil }

func TestCancelSubmittedJobReturnsTerminalMasterStateIdempotently(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "master.db"))
	if err != nil {
		t.Fatal(err)
	}
	const jobID = "job-cancel-terminal"
	_, err = db.Forwarding().InsertCreatorForwarding(context.Background(), &forwardingcontract.CreatorForwarding{
		ForwardingID: "cf-cancel-terminal", ExternalClientID: "client-a", SourceProvider: "remote_engine", SourceJobID: "remote-1",
		TargetExecutorID: JobSubmitTargetExecutorID, TargetJobID: jobID, Status: string(forwardingcontract.CFStatusForwarded),
	})
	if err != nil {
		t.Fatal(err)
	}
	deps := &cancelIdempotencyJobs{job: &jobs.Job{ID: jobID, Status: jobs.StatusSucceeded}}
	h := &Handlers{store: db, jobs: JobsDeps{Reader: deps, Writer: deps}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/jobs/:id/cancel", func(c *gin.Context) { c.Set(m2mCtxKeyClientID, "client-a"); h.CancelSubmittedJob()(c) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/cancel", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", w.Code, w.Body.String())
	}
	if deps.cancelCalls != 0 {
		t.Fatalf("Cancel called %d times for already SUCCEEDED job", deps.cancelCalls)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"status":"SUCCEEDED"`) || !strings.Contains(body, `"status_url":"/api/v1/jobs/`) {
		t.Fatalf("response lacks terminal outcome/reconciliation URL: %s", body)
	}
}

func TestCancelSubmittedJobRereadsTerminalRace(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "master.db"))
	if err != nil {
		t.Fatal(err)
	}
	const jobID = "job-cancel-race"
	_, err = db.Forwarding().InsertCreatorForwarding(context.Background(), &forwardingcontract.CreatorForwarding{
		ForwardingID: "cf-cancel-race", ExternalClientID: "client-a", SourceProvider: "remote_engine", SourceJobID: "remote-race",
		TargetExecutorID: JobSubmitTargetExecutorID, TargetJobID: jobID, Status: string(forwardingcontract.CFStatusForwarded),
	})
	if err != nil {
		t.Fatal(err)
	}
	deps := &cancelIdempotencyJobs{job: &jobs.Job{ID: jobID, Status: jobs.StatusRunning}, cancelErr: errors.New("terminal race")}
	deps.afterCancel = func() { deps.job.Status = jobs.StatusSucceeded }
	h := &Handlers{store: db, jobs: JobsDeps{Reader: deps, Writer: deps}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/jobs/:id/cancel", func(c *gin.Context) { c.Set(m2mCtxKeyClientID, "client-a"); h.CancelSubmittedJob()(c) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/cancel", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"SUCCEEDED"`) {
		t.Fatalf("cancel race status=%d body=%s", w.Code, w.Body.String())
	}
	if deps.cancelCalls != 1 {
		t.Fatalf("Cancel calls=%d, want 1", deps.cancelCalls)
	}
}
