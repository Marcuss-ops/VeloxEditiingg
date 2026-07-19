package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// fakeSessionsReader is the test double for SessionsReader.
type fakeSessionsReader struct {
	rows []store.WorkerSessionRow
	err  error
	// lastCall captures the query params the handler passed in.
	lastWorkerID       string
	lastIncludeRevoked bool
	lastLimit          int
}

func (f *fakeSessionsReader) ListWorkerSessions(_ context.Context, workerID string, includeRevoked bool, limit int) ([]store.WorkerSessionRow, error) {
	f.lastWorkerID = workerID
	f.lastIncludeRevoked = includeRevoked
	f.lastLimit = limit
	return f.rows, f.err
}

func newSessionsRouter(h *SessionsHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/workers/:worker_id/sessions", h.ListWorkerSessions())
	return r
}

func TestListWorkerSessions_Success(t *testing.T) {
	fake := &fakeSessionsReader{rows: []store.WorkerSessionRow{
		{
			SessionID:       "sess-1",
			WorkerID:        "w-a",
			SessionType:     "control",
			IPAddress:       "192.168.1.10",
			CreatedAt:       "2026-01-01T00:00:00Z",
			ExpiresAt:       "2026-12-31T00:00:00Z",
			ConnectedAt:     "2026-01-01T00:00:00Z",
			LastSeenAt:      "2026-01-02T00:00:00Z",
			Status:          "ACTIVE",
			ProtocolVersion: "v3",
		},
	}}
	h := NewSessionsHandler(fake)
	r := newSessionsRouter(h)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers/w-a/sessions", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp WorkerSessionsListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 1 || len(resp.Sessions) != 1 {
		t.Fatalf("Count=%d len=%d, want 1/1", resp.Count, len(resp.Sessions))
	}
	// SECURITY: IP MUST be redacted via sanitiseHostname().
	if resp.Sessions[0].IPAddress == "192.168.1.10" {
		t.Errorf("IPAddress not redacted: %q", resp.Sessions[0].IPAddress)
	}
	if resp.Sessions[0].SessionID != "sess-1" {
		t.Errorf("SessionID = %q, want sess-1", resp.Sessions[0].SessionID)
	}
}

func TestListWorkerSessions_IncludeRevokedParsing(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"", false},
		{"?include_revoked=true", true},
		{"?include_revoked=TRUE", true},
		{"?include_revoked=1", true},
		{"?include_revoked=yes", true},
		{"?include_revoked=false", false},
		{"?include_revoked=0", false},
		{"?include_revoked=garbage", false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			fake := &fakeSessionsReader{}
			h := NewSessionsHandler(fake)
			r := newSessionsRouter(h)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers/w-a/sessions"+tc.query, nil))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d", w.Code)
			}
			if fake.lastIncludeRevoked != tc.want {
				t.Errorf("lastIncludeRevoked = %v, want %v", fake.lastIncludeRevoked, tc.want)
			}
		})
	}
}

func TestListWorkerSessions_NilHandler(t *testing.T) {
	if NewSessionsHandler(nil) != nil {
		t.Errorf("NewSessionsHandler(nil) = non-nil, want nil")
	}
	h2 := &SessionsHandler{}
	r := newSessionsRouter(h2)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers/w-a/sessions", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil reader: status = %d, want 503", w.Code)
	}
}

func TestParseBoolQuery(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"1", true},
		{"yes", true},
		{"YES", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"garbage", false},
		{"  true  ", true},
		{" 1 ", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := parseBoolQuery(tc.in); got != tc.want {
				t.Errorf("parseBoolQuery(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
