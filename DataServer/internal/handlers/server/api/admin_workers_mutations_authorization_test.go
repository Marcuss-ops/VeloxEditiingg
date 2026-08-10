package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/fleet"
)

func TestResumeWorker_FailingResumeGateReturns503(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	h.SetResumeGate(func() error { return errors.New("Level-D smoke capability DISABLED") })
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("failing resume gate -> %d, want 503: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Level-D smoke capability DISABLED") {
		t.Fatalf("503 body must expose the capability reason: %s", w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("blocked resume published %d operations, want 0", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || info.Drain || info.Quarantined || info.Resuming {
		t.Fatalf("blocked resume changed worker state: %+v", info)
	}
}

func TestAuthorizeMutation_ResumeUsesResumeGate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	updateCalled := false
	resumeCalled := false
	h := &AdminWorkersMutationsHandler{
		updateGate: func() error {
			updateCalled = true
			return errors.New("update gate should not run")
		},
		resumeGate: func() error {
			resumeCalled = true
			return errors.New("smoke capability disabled")
		},
	}

	if h.authorizeMutation(ctx, fleet.OperationKindResume) {
		t.Fatal("resume with a failing resume gate must be rejected")
	}
	if !resumeCalled {
		t.Fatal("resume gate was not evaluated")
	}
	if updateCalled {
		t.Fatal("update gate was evaluated for a resume mutation")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}
