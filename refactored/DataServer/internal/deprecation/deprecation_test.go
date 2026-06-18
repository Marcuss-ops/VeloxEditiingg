package deprecation_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/deprecation"
)

func newTestRegistry(t *testing.T) *deprecation.Registry {
	t.Helper()
	now := time.Now().UTC()
	return deprecation.New(now, now.Add(72*time.Hour))
}

func TestTrackIncrementsHitsAndSetsHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := newTestRegistry(t)
	reg.Register("POST", "/api/test", "/api/v2/test")

	r := gin.New()
	r.POST("/api/test", reg.Track("POST", "/api/test"), func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Deprecation"); got != "true" {
		t.Errorf("Deprecation header = %q, want 'true'", got)
	}
	if got := w.Header().Get("Sunset"); got == "" {
		t.Errorf("Sunset header missing")
	}
	if got := w.Header().Get("Link"); got == "" {
		t.Errorf("Link successor-version header missing")
	}

	snap := reg.Snapshot()
	if len(snap.Stats) != 1 {
		t.Fatalf("expected exactly 1 stat, got %d", len(snap.Stats))
	}
	if snap.Stats[0].Hits != 1 {
		t.Errorf("Hits = %d, want 1", snap.Stats[0].Hits)
	}
	if snap.Stats[0].Errors != 0 {
		t.Errorf("Errors = %d, want 0", snap.Stats[0].Errors)
	}
	if snap.Stats[0].FirstHitAt == "" {
		t.Errorf("FirstHitAt should be set after first call")
	}
	if snap.Stats[0].LastClient != "10.0.0.1" {
		t.Errorf("LastClient = %q, want 10.0.0.1", snap.Stats[0].LastClient)
	}
}

func TestTrackIncrementsErrorsOn5xx(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := newTestRegistry(t)
	reg.Register("POST", "/api/test", "")

	r := gin.New()
	r.POST("/api/test", reg.Track("POST", "/api/test"), func(c *gin.Context) {
		c.String(http.StatusInternalServerError, "boom")
	})

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
	}

	snap := reg.Snapshot()
	if snap.Stats[0].Hits != 3 {
		t.Errorf("Hits = %d, want 3", snap.Stats[0].Hits)
	}
	if snap.Stats[0].Errors != 3 {
		t.Errorf("Errors = %d, want 3", snap.Stats[0].Errors)
	}
	if snap.Stats[0].LastErrorAt == "" {
		t.Errorf("LastErrorAt should be set")
	}
}

func TestTrackIncrementsErrorsOn4xx(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := newTestRegistry(t)
	reg.Register("GET", "/api/legacy", "/api/v2/new")

	r := gin.New()
	r.GET("/api/legacy", reg.Track("GET", "/api/legacy"), func(c *gin.Context) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "nope"})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/legacy", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	snap := reg.Snapshot()
	if snap.Stats[0].Hits != 1 {
		t.Errorf("Hits = %d, want 1", snap.Stats[0].Hits)
	}
	if snap.Stats[0].Errors != 1 {
		t.Errorf("Errors = %d, want 1 (4xx counts as error)", snap.Stats[0].Errors)
	}
}

func TestUnregisteredPathPassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := newTestRegistry(t)
	// No Register call.

	r := gin.New()
	r.POST("/api/test", reg.Track("POST", "/api/test"), func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	snap := reg.Snapshot()
	if len(snap.Stats) != 0 {
		t.Errorf("stats entries = %d, want 0 (no Register call)", len(snap.Stats))
	}
	// No Deprecation header since no Register was made.
	if got := w.Header().Get("Deprecation"); got != "" {
		t.Errorf("Deprecation header = %q on unregistered path, want empty", got)
	}
}

func TestSnapshotRegistersDatesAndDaysToSunset(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC().Truncate(time.Second)
	reg := deprecation.New(now, now.Add(14*24*time.Hour))
	reg.Register("POST", "/api/a", "/api/v2/a")
	reg.Register("POST", "/api/b", "")

	snap := reg.Snapshot()
	if snap.RegisteredAt == "" || snap.SunsetAt == "" {
		t.Fatalf("expected both dates in snapshot, got %+v", snap)
	}
	if snap.DaysToSunset <= 0 {
		t.Errorf("DaysToSunset = %d, want > 0", snap.DaysToSunset)
	}
	if len(snap.Stats) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(snap.Stats))
	}
	// Stats are sorted by name: POST /api/a < POST /api/b
	if snap.Stats[0].Path != "/api/a" || snap.Stats[1].Path != "/api/b" {
		t.Errorf("paths not sorted: %s, %s", snap.Stats[0].Path, snap.Stats[1].Path)
	}
	// JSON round-trip works.
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !json.Valid(b) {
		t.Errorf("snapshot does not marshal to valid JSON: %s", string(b))
	}
}

func TestRegisterIdempotent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := newTestRegistry(t)
	reg.Register("POST", "/api/test", "/first")
	reg.Register("POST", "/api/test", "/second") // must be ignored

	snap := reg.Snapshot()
	if len(snap.Stats) != 1 {
		t.Fatalf("Register must be idempotent but found %d entries", len(snap.Stats))
	}
	if snap.Stats[0].Successor != "/first" {
		t.Errorf("Successor = %q, want %q (first registration wins)", snap.Stats[0].Successor, "/first")
	}
}

func TestNewFallbacksWhenSunsetBeforeNotice(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC()
	reg := deprecation.New(now, now.Add(-1*time.Hour)) // sunset in the past

	snap := reg.Snapshot()
	// String compare works because both fields are RFC3339 UTC: lexical
	// order matches chrono order.
	if !(snap.SunsetAt > snap.RegisteredAt) {
		t.Errorf("SunsetAt must be after RegisteredAt even for caller bug: got reg=%s sunset=%s",
			snap.RegisteredAt, snap.SunsetAt)
	}
}
