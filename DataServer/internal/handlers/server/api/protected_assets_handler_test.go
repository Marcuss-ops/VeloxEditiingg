// Package api — tests for ProtectedAssetsHandler (Pass 6).
//
// Each test wires the real WorkerOrAdminAuthMiddleware in front of the
// handler so the layered auth/snapshot contract is exercised end-to-end:
//
//	401 → middleware rejects (no/wrong token before handler runs)
//	403 → middleware rejects browser Origin before handler runs
//	503 → handler runs, snapshot.Version == 0 short-circuits
//	200 → handler runs, snapshot served
//
// The snapshot is populated by using the production
// `protectedasset.NewService` with a tiny interface-compatible fake
// (struct that satisfies `protectedasset.Repo`), avoiding the need
// for go-sqlmock. The worker-token branch of the auth middleware is
// intentionally NOT exercised here — the admin-token branch returns
// the same status code, and the worker-token machinery is covered
// separately by api_v1_test.go (TBD). The Pass 6 surface is the
// handler × middleware chain, not the auth provider.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/protectedasset"

	"velox-shared/dispatchable"
)

const (
	testRoutePass6 = "/api/v1/workers/cache/protected-assets"
	testAdminPass6 = "test-admin-token-pass-6"
)

// testPinnedTimePass6 is a `var` (NOT `const`) because time.Date is a
// function call, not a compile-time constant. Used by newPass6Service
// to drive Service.SetClock deterministically.
var testPinnedTimePass6 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// helpers
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// newPass6Router wires the real WorkerOrAdminAuthMiddleware in front
// of the handler. tokenMgr is intentionally nil — tests exercise the
// admin-token branch only (see package doc).
func newPass6Router(h *ProtectedAssetsHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		Auth: config.AuthConfig{
			AdminToken: testAdminPass6,
		},
	}
	r := gin.New()
	auth := WorkerOrAdminAuthMiddleware(cfg, nil)
	r.GET(testRoutePass6, auth, h.Snapshot())
	return r
}

// pass6FakeJobRepo is the smallest possible protectedasset.Repo
// implementation: it returns a preset list (clipped to limit) and
// never errors.
type pass6FakeJobRepo struct {
	jobs []dispatchable.Job
}

func (f *pass6FakeJobRepo) ListNextDispatchableJobs(_ context.Context, limit int) ([]dispatchable.Job, error) {
	if limit > 0 && len(f.jobs) > limit {
		return f.jobs[:limit], nil
	}
	return f.jobs, nil
}

// newPass6Service produces a wired service with a known clock so
// assertions on GeneratedAt are deterministic. If refresh is true the
// Service goes through Refresh immediately; otherwise the snapshot is
// left at the zero-value (Version == 0) for the 503 branch.
//
// limit is always protectedasset.DefaultLookahead — the fake repo
// clips to limit anyway, so passing the explicit size of `jobs` adds
// no useful coverage while introducing a redundant branch.
func newPass6Service(t *testing.T, refresh bool, jobs []dispatchable.Job) *protectedasset.Service {
	t.Helper()
	repo := &pass6FakeJobRepo{jobs: jobs}
	svc := protectedasset.NewService(
		protectedasset.RepoFunc(repo.ListNextDispatchableJobs),
		protectedasset.DefaultLookahead,
	).SetClock(func() time.Time { return testPinnedTimePass6 })
	if refresh {
		if err := svc.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh failed: %v", err)
		}
	}
	return svc
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// TestPass6_NilServiceShortCircuits enforces the constructor contract
// from NewProtectedAssetsHandler: nil in → nil out so the route
// registration helper can safely skip wiring a non-existent service.
func TestPass6_NilServiceShortCircuits(t *testing.T) {
	if h := NewProtectedAssetsHandler(nil); h != nil {
		t.Errorf("NewProtectedAssetsHandler(nil) returned non-nil: %v", h)
	}
}

// TestPass6_Auth_NoToken confirms WorkerOrAdminAuthMiddleware
// short-circuits BEFORE the handler runs — the handler never sees
// this request.
func TestPass6_Auth_NoToken(t *testing.T) {
	svc := newPass6Service(t, true, nil)
	r := newPass6Router(NewProtectedAssetsHandler(svc))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, testRoutePass6, nil))

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want=401 body=%s", w.Code, w.Body.String())
	}
}

// TestPass6_Auth_WrongToken covers the admin-token path with a
// mismatched bearer. middleware should 401, no handler invocation.
func TestPass6_Auth_WrongToken(t *testing.T) {
	svc := newPass6Service(t, true, nil)
	r := newPass6Router(NewProtectedAssetsHandler(svc))

	req := httptest.NewRequest(http.MethodGet, testRoutePass6, nil)
	req.Header.Set("Authorization", "Bearer wrong-bearer-pass-6")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want=401 body=%s", w.Code, w.Body.String())
	}
}

// TestPass6_Auth_BrowserOrigin covers the explicit "no browser
// callers" hard-stop in WorkerOrAdminAuthMiddleware — even a valid
// admin token cannot be presented from a cross-origin browser
// request. The handler never sees this request either.
func TestPass6_Auth_BrowserOrigin(t *testing.T) {
	svc := newPass6Service(t, true, nil)
	r := newPass6Router(NewProtectedAssetsHandler(svc))

	req := httptest.NewRequest(http.MethodGet, testRoutePass6, nil)
	req.Header.Set("Authorization", "Bearer "+testAdminPass6)
	req.Header.Set("Origin", "https://attacker.example.com")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d want=403 body=%s", w.Code, w.Body.String())
	}
}

// TestPass6_VersionZero_Returns503 covers the snapshot-not-yet-
// generated branch. The handler short-circuits on Version==0 with
// the canonical error envelope.
func TestPass6_VersionZero_Returns503(t *testing.T) {
	svc := newPass6Service(t, false, nil)

	r := newPass6Router(NewProtectedAssetsHandler(svc))

	req := httptest.NewRequest(http.MethodGet, testRoutePass6, nil)
	req.Header.Set("Authorization", "Bearer "+testAdminPass6)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want=503 body=%s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, w.Body.String())
	}
	if got, want := body["error"], "protected-asset snapshot not yet generated"; got != want {
		t.Errorf("error=%q want %q", got, want)
	}
}

// TestPass6_NilHandler_Returns503 covers the nil-handler guard inside
// Snapshot(): a route mistakenly wired with a nil ProtectedAssetsHandler
// still returns 503 with the service-not-available envelope rather
// than panicking on h.svc.Snapshot().
//
// IMPORTANT: this contract is fragile — Snapshot() must not dereference
// `h` BEFORE the `if h == nil` guard. Adding e.g. a `h.logger.Info(...)`
// call at the top of Snapshot() would silently break this test. The
// `Snapshot()` method's godoc documents the nil-receiver contract.
func TestPass6_NilHandler_Returns503(t *testing.T) {
	r := newPass6Router(nil)

	req := httptest.NewRequest(http.MethodGet, testRoutePass6, nil)
	req.Header.Set("Authorization", "Bearer "+testAdminPass6)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want=503 body=%s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, w.Body.String())
	}
	if got, want := body["error"], "protected-asset service not available"; got != want {
		t.Errorf("error=%q want %q", got, want)
	}
}

// TestPass6_ValidSnapshot_Returns200_FullContract is the canonical
// happy path: a non-trivial Drive-ID mix (3 distinct + 1 duplicate
// across 3 fields/2 scenes) is reduced to a SORTED-UNIQUE list, the
// envelope fields match the canonical Snapshot shape, Content-Type
// is application/json, and the pinned clock is honoured.
func TestPass6_ValidSnapshot_Returns200_FullContract(t *testing.T) {
	jobs := []dispatchable.Job{
		{
			JobID: "job-pass6-1",
			Payload: json.RawMessage(`{
				"scenes": [
					{"clip_link": "https://drive.google.com/file/d/ABC123/view"},
					{"clip_links": [
						"https://drive.google.com/uc?id=ABC123",
						"https://drive.google.com/file/d/DEF456/view?usp=sharing"
					]},
					{"video_url": "https://drive.google.com/file/d/GHI789/view"}
				]
			}`),
		},
		{
			JobID:   "job-pass6-2",
			Payload: json.RawMessage(`{"scenes": [{"source_url": "https://drive.google.com/open?id=ABC123"}]}`),
		},
	}
	svc := newPass6Service(t, true, jobs)

	r := newPass6Router(NewProtectedAssetsHandler(svc))

	req := httptest.NewRequest(http.MethodGet, testRoutePass6, nil)
	req.Header.Set("Authorization", "Bearer "+testAdminPass6)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200 body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type=%q want application/json prefix", ct)
	}

	var resp protectedasset.Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, w.Body.String())
	}
	if resp.Version != 1 {
		t.Errorf("Version=%d want=1", resp.Version)
	}
	if resp.LookaheadJobs != len(jobs) {
		t.Errorf("LookaheadJobs=%d want %d", resp.LookaheadJobs, len(jobs))
	}
	wantIDs := []string{"ABC123", "DEF456", "GHI789"} // sorted + deduped
	if len(resp.DriveFileIDs) != len(wantIDs) {
		t.Fatalf("DriveFileIDs=%v want %v", resp.DriveFileIDs, wantIDs)
	}
	for i, id := range wantIDs {
		if resp.DriveFileIDs[i] != id {
			t.Errorf("DriveFileIDs[%d]=%q want %q", i, resp.DriveFileIDs[i], id)
		}
	}
	if !resp.GeneratedAt.Equal(testPinnedTimePass6) {
		t.Errorf("GeneratedAt=%v want %v", resp.GeneratedAt, testPinnedTimePass6)
	}
}
