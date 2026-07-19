package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// fakeEventsReader is the test double for EventsReader.
type fakeEventsReader struct {
	rows []store.WorkerEventRow
	err  error
	// lastCall captures the query params the handler passed in.
	lastWorkerID  string
	lastEventType string
	lastSince     string
	lastLimit     int
}

func (f *fakeEventsReader) ListWorkerEvents(_ context.Context, workerID, eventType, since string, limit int) ([]store.WorkerEventRow, error) {
	f.lastWorkerID = workerID
	f.lastEventType = eventType
	f.lastSince = since
	f.lastLimit = limit
	return f.rows, f.err
}

func newEventsRouter(h *EventsHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/workers/:worker_id/events", h.ListWorkerEvents())
	return r
}

func TestListWorkerEvents_Success(t *testing.T) {
	fake := &fakeEventsReader{rows: []store.WorkerEventRow{
		{
			EventID:     "evt-1",
			WorkerID:    sqlNullStr("w-a"),
			SessionID:   sqlNullStr("sess-1"),
			EventType:   "WORKER_STALE_DETECTED",
			Severity:    "WARN",
			ReasonCode:  sqlNullStr("heartbeat_delayed"),
			DetailsJSON: `{"last_heartbeat_at":"2026-01-01T00:00:00Z","stale_threshold_sec":150}`,
			CreatedAt:   "2026-01-01T00:00:01Z",
		},
	}}
	h := NewEventsHandler(fake)
	r := newEventsRouter(h)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers/w-a/events", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp WorkerEventsListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 1 || len(resp.Events) != 1 {
		t.Fatalf("Count=%d len=%d, want 1/1", resp.Count, len(resp.Events))
	}
	ev := resp.Events[0]
	if ev.EventID != "evt-1" {
		t.Errorf("EventID = %q", ev.EventID)
	}
	if ev.EventType != "WORKER_STALE_DETECTED" {
		t.Errorf("EventType = %q", ev.EventType)
	}
	if ev.Details["stale_threshold_sec"].(float64) != 150 {
		t.Errorf("Details[stale_threshold_sec] = %v, want 150", ev.Details["stale_threshold_sec"])
	}
}

// TestListWorkerEvents_DetailsSanitisation pins the security-critical
// claim that any IP embedded in details_json is redacted by
// sanitiseHostname() before the row lands in the response.
func TestListWorkerEvents_DetailsSanitisation(t *testing.T) {
	fake := &fakeEventsReader{rows: []store.WorkerEventRow{
		{
			EventID:     "evt-ip",
			WorkerID:    sqlNullStr("w-a"),
			EventType:   "WORKER_PARTITION_DETECTED",
			Severity:    "ERROR",
			DetailsJSON: `{"last_heartbeat_at":"","partition_threshold_sec":300,"origin_ip":"192.168.1.100"}`,
			CreatedAt:   "2026-01-01T00:00:00Z",
		},
	}}
	h := NewEventsHandler(fake)
	r := newEventsRouter(h)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers/w-a/events", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if contains(body, "192.168") {
		t.Errorf("details_json leaked IP 192.168.1.100: %s", body)
	}
	// Sanitised token MUST appear in its place.
	if !contains(body, "redacted") {
		t.Errorf("expected redaction token in body: %s", body)
	}
}

// TestParseAndSanitiseDetails_Fallback covers the malformed-JSON
// path: a parse failure surfaces the raw string under the "_raw"
// key (still sanitised) so the audit row is NEVER silently dropped.
func TestParseAndSanitiseDetails_Fallback(t *testing.T) {
	cases := []struct {
		name       string
		raw        string
		wantKey    string
		wantNoLeak []string
	}{
		{
			name:       "valid json",
			raw:        `{"k":"v"}`,
			wantKey:    "k",
			wantNoLeak: nil,
		},
		{
			name:       "valid json with ip",
			raw:        `{"host":"10.0.0.5","name":"render-01"}`,
			wantKey:    "name",
			wantNoLeak: []string{"10.0.0"},
		},
		{
			name:       "malformed json falls back to _raw",
			raw:        `{not-json}`,
			wantKey:    "_raw",
			wantNoLeak: nil,
		},
		{
			name:       "malformed json with ip still sanitised in _raw",
			raw:        `garbled-with-192.168.5.5-payload`,
			wantKey:    "_raw",
			wantNoLeak: []string{"192.168"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := parseAndSanitiseDetails(tc.raw)
			if _, ok := m[tc.wantKey]; !ok {
				t.Errorf("missing key %q in result: %#v", tc.wantKey, m)
			}
			for _, leak := range tc.wantNoLeak {
				if contains(asJSON(t, m), leak) {
					t.Errorf("result leaked %q: %s", leak, asJSON(t, m))
				}
			}
		})
	}
}

func TestListWorkerEvents_FiltersPassThrough(t *testing.T) {
	fake := &fakeEventsReader{}
	h := NewEventsHandler(fake)
	r := newEventsRouter(h)
	w := httptest.NewRecorder()
	q := "/api/v1/workers/w-a/events?event_type=WORKER_STALE_DETECTED&since=2026-01-01T00:00:00Z&limit=25"
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, q, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if fake.lastEventType != "WORKER_STALE_DETECTED" {
		t.Errorf("lastEventType = %q", fake.lastEventType)
	}
	if fake.lastSince != "2026-01-01T00:00:00Z" {
		t.Errorf("lastSince = %q", fake.lastSince)
	}
	if fake.lastLimit != 25 {
		t.Errorf("lastLimit = %d, want 25", fake.lastLimit)
	}
}

func TestListWorkerEvents_NilHandler(t *testing.T) {
	if NewEventsHandler(nil) != nil {
		t.Errorf("NewEventsHandler(nil) = non-nil, want nil")
	}
	h2 := &EventsHandler{}
	r := newEventsRouter(h2)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers/w-a/events", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil reader: status = %d, want 503", w.Code)
	}
}

// sqlNullStr is a tiny helper to build sql.NullString literals in
// the test rows above. Keeps the test rows compact and matches the
// schema's nullable columns (worker_id / session_id / job_id /
// task_id / attempt_id / reason_code on worker_events).
func sqlNullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: true}
}

// asJSON serialises v to a JSON string for substring assertions in
// the sanitisation tests. Test-only helper.
func asJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
