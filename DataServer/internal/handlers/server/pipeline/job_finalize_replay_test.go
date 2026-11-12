package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/taskgraph"
)

type finalizeReplayStore struct {
	payload    map[string]interface{}
	writes     int
	taskStatus taskgraph.Status
}

func (s *finalizeReplayStore) Get(context.Context, string) (*taskgraph.Task, error) { return nil, nil }
func (s *finalizeReplayStore) List(context.Context, taskgraph.Filter) ([]taskgraph.Task, error) {
	return nil, nil
}
func (s *finalizeReplayStore) GetByJobID(context.Context, string) (*taskgraph.Task, error) {
	status := s.taskStatus
	if status == "" {
		status = taskgraph.StatusReady
	}
	return &taskgraph.Task{ID: "task-1", JobID: "job-1", Status: status}, nil
}
func (s *finalizeReplayStore) GetTaskSpecPayload(context.Context, string) (map[string]interface{}, error) {
	return s.payload, nil
}
func (s *finalizeReplayStore) MergeTaskSpecPayload(context.Context, string, map[string]interface{}) (map[string]interface{}, error) {
	s.writes++
	return s.payload, nil
}

func TestFinalizeReplayPreservesWaitingAndFailedStates(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		state  string
		status int
		code   string
	}{
		{runtimeFinalizeQueued, http.StatusAccepted, ""},
		{runtimeFinalizeRunning, http.StatusAccepted, ""},
		{runtimeFinalizeRetryWait, http.StatusAccepted, ""},
		{runtimeFinalizeWaitingPreparation, http.StatusAccepted, ""},
		{runtimeFinalizeFailed, http.StatusConflict, "runtime_finalize_failed"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			s := &finalizeReplayStore{payload: map[string]interface{}{
				"runtime_finalize_idempotency_key": "finalize-1",
				"runtime_assets_pending":           true,
				runtimeFinalizeStatusKey:           tc.state,
				runtimeFinalizeErrorKey:            "preparation failed",
			}}
			h := &Handlers{cfg: &config.Config{}, jobs: JobsDeps{TaskReader: s}}
			r := gin.New()
			r.POST("/jobs/:id/finalize", h.FinalizeJob())
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/jobs/job-1/finalize", strings.NewReader(`{"idempotency_key":"finalize-1"}`))
			r.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["finalization_status"] != tc.state || (tc.code != "" && body["error"] != tc.code) {
				t.Fatalf("unexpected response: %v", body)
			}
			if s.writes != 0 {
				t.Fatalf("replay changed task spec %d times", s.writes)
			}
		})
	}
}

func TestFinalizeReplayReportsTerminalPreparationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := &finalizeReplayStore{taskStatus: taskgraph.StatusFailed, payload: map[string]interface{}{
		"runtime_finalize_idempotency_key": "finalize-1",
		"runtime_assets_pending":           true,
		runtimeFinalizeStatusKey:           runtimeFinalizeFailed,
		runtimeFinalizeErrorKey:            "preparation task failed",
		"runtime_finalize_error_code":      "PREPARATION_TASK_FAILED",
	}}
	h := &Handlers{cfg: &config.Config{}, jobs: JobsDeps{TaskReader: s}}
	r := gin.New()
	r.POST("/jobs/:id/finalize", h.FinalizeJob())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/jobs/job-1/finalize", strings.NewReader(`{"idempotency_key":"finalize-1"}`)))
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusConflict || body["failure_code"] != "PREPARATION_TASK_FAILED" || body["message"] != "preparation task failed" || s.writes != 0 {
		t.Fatalf("status=%d body=%v writes=%d", w.Code, body, s.writes)
	}
}
