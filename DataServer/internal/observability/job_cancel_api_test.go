package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/jobs"
)

type cancelRecordingWriter struct {
	id     string
	reason string
	rev    int
	err    error
}

func (w *cancelRecordingWriter) SetStatus(context.Context, string, jobs.Status, jobs.Status) error {
	return nil
}
func (w *cancelRecordingWriter) Fail(context.Context, string, string) error { return nil }
func (w *cancelRecordingWriter) Cancel(_ context.Context, id, reason string, revision int) error {
	w.id, w.reason, w.rev = id, reason, revision
	return w.err
}
func (w *cancelRecordingWriter) Delete(context.Context, string) error { return nil }

func TestAdminJobCancelAPIUsesCanonicalJobWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, _, _, _, _ := newTestService()
	writer := &cancelRecordingWriter{}
	svc.WithJobWriter(writer)
	r := gin.New()
	NewModule(svc).RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/jobs/job-cancel-1/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST cancel = %d: %s", w.Code, w.Body.String())
	}
	if writer.id != "job-cancel-1" || writer.reason != "cancelled by admin operator" || writer.rev != -1 {
		t.Fatalf("cancel writer call = id=%q reason=%q revision=%d", writer.id, writer.reason, writer.rev)
	}
}

func TestAdminJobCancelAPIFailsClosedWithoutWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, _, _, _, _ := newTestService()
	r := gin.New()
	NewModule(svc).RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/jobs/job-cancel-2/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("POST unconfigured cancel = %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminJobCancelAPIPropagatesWriterFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, _, _, _, _ := newTestService()
	writer := &cancelRecordingWriter{err: errors.New("cancel conflict")}
	svc.WithJobWriter(writer)
	r := gin.New()
	NewModule(svc).RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/jobs/job-cancel-3/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("POST failed cancel = %d: %s", w.Code, w.Body.String())
	}
}
