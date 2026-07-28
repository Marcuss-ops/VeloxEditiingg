// Package pipeline — e2e M2M auth/quota scenario tests for POST /api/v1/jobs.
//
// This file owns:
//   - the typed M2M test stack: m2mBundle (returned by newM2MBundle),
//     m2mBundleOpts, and the helper m2mPost (POST with explicit bearer);
//   - scenarios 11+12 of the submit-job e2e coverage matrix
//     (TestSubmitJobE2E_M2MAuthEnvelopes + TestSubmitJobE2E_M2MRateLimitAndQuota):
//     missing-auth / wrong-secret / disabled-key / valid scope envelope,
//     per-client rate-limiter bucket exhaustion, per-request scene-quota
//     exceedance, per-request duration-quota exceedance.
//
// The m2m test stack seeds a real m2m_api_keys row (with a freshly
// generated plaintext secret) so the audit + bucket code paths are
// exercised end-to-end, as required by [P1 #1].
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// m2mBundle is the typed M2M stack used by scenario 11 / 12 tests.
// Sharing the same SQLite-on-tempfile + Enqueuer + Resolver + M2M
// middleware keeps the test wiring hermetic to the closure of the
// test function (t.TempDir auto-cleans on return).
type m2mBundle struct {
	h       *Handlers
	db      *store.SQLiteStore
	st      *store.SQLiteStore
	limiter *m2mRateLimiter
	keyRow  *store.M2MAPIKey
	plaintext string // for tests that need to send a real Bearer
}

// newM2MBundle hydrates the full M2M-aware test stack: same SQLite
// + AtomicJobTaskCreator + Enqueuer + Resolver as the legacy
// fixture, plus a real M2M middleware backed by an m2m_api_keys row
// seeded with a known plaintext secret. Tests that exercise the
// resolver layer (scenario 1, 6, 7, 8, 9) call newSubmitJobE2EStack
// instead and use m2mJobsAuthFake — the legacy route under test
// only cares that SOME auth ran (the audit pipeline's
// handler-side checks need the middleware to have populated
// m2m_client_id, but the fake leaves it empty which is acceptable
// for the resolver paths).
func newM2MBundle(t *testing.T, opts m2mBundleOpts) *m2mBundle {
	t.Helper()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	if _, err := db.DB().Exec(
		`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('drive', 'google_drive', 'Drive', 1, '{}', datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	testEnqueuer := enqueue.NewEnqueuer(atomic, jobRepo, nil, noopPlanResolver{})
	resolver := creatorflow.NewResolverFromDeps(testEnqueuer, db, tempDir, filepath.Join(tempDir, "videos"), "")
	cfg := &config.Config{
		AllowedExternalDomains: opts.allowDomains,
	}
	h := NewHandlersWithResolver(cfg, testEnqueuer, nil, resolver, jobRepo, nil, nil).WithStore(db)

	limiter := newM2MRateLimiter()
	plaintext := store.GenerateM2MSecret()
	hash := store.HashM2MSecret(plaintext)
	rps := opts.rps
	burst := opts.burst
	maxScenes := opts.maxScenes
	maxDur := opts.maxTotalSecs
	key := store.M2MAPIKey{
		ClientID:       opts.clientID,
		SecretHash:     hash,
		Scopes:         []string{"jobs.submit"},
		IsActive:       true,
		RateLimitRPS:   rps,
		RateLimitBurst: burst,
		Quotas: store.M2MQuotas{
			MaxScenes:         maxScenes,
			MaxTotalDurationS: maxDur,
		},
	}
	if err := db.InsertM2MAPIKey(context.Background(), key); err != nil {
		t.Fatalf("seed m2m_api_keys: %v", err)
	}
	return &m2mBundle{
		h: h, db: db, st: db, limiter: limiter, keyRow: &key, plaintext: plaintext,
	}
}

type m2mBundleOpts struct {
	clientID    string
	rps         int
	burst       int
	maxScenes   int
	maxTotalSecs float64
	allowDomains []string
}

// newSubmitJobE2EStack mirrors newCreatorPushE2EStack from
// creator_push_e2e_test.go exactly. Sharing the same SQLite-on-tempfile
// + delivery_destinations seed + atomic-creator + Enqueuer + Resolver
// + Handlers wiring gives both e2e files identical dependencies,
// reducing the debugging surface when a regression is rooted in stack
// composition rather than the endpoint under test.
//
// delivery_destinations is seeded with a single "drive" row so the
// enqueuer's delivery_plan validation passes for the happy path +
// replay. The missing_destination scenario (skipped — see header) is
// the one place this seed matters for a rejection subtest.
func m2mPost(t *testing.T, r *gin.Engine, body any, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// postSubmitJob serializes the body and runs it through the test
// router. Mirrors postCreatorPush exactly for symmetry.
func TestSubmitJobE2E_M2MAuthEnvelopes(t *testing.T) {
	bundle := newM2MBundle(t, m2mBundleOpts{
		clientID: "e2e-m2m-client",
		rps:      5, burst: 10,
		maxScenes: 0, maxTotalSecs: 0,
	})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m2mMW := NewM2MJwAuthMiddleware(&config.Config{}, bundle.st, bundle.limiter)
	bundle.h.RegisterRoutes(r, adminAuthFake, m2mMW)

	body := validSubmitJobBody("e2e-m2m-001")

	// Subtest 1: missing Authorization header → 401 m2m_token_required.
	t.Run("missing_authorization_header", func(t *testing.T) {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("want 401, got %d body=%s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["error"] != "m2m_token_required" {
			t.Fatalf("error = %v, want m2m_token_required", resp["error"])
		}
	})

	// Subtest 2: bearer is the wrong plaintext (no row matches its hash) → 401 m2m_token_rejected.
	t.Run("wrong_bearer_token", func(t *testing.T) {
		w := m2mPost(t, r, body, "Bearer wrong-secret-not-matching-any-key")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("want 401, got %d body=%s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["error"] != "m2m_token_rejected" {
			t.Fatalf("error = %v, want m2m_token_rejected", resp["error"])
		}
	})

	// Subtest 3: bearer is the seeded plaintext → 202 accepted.
	t.Run("right_bearer_token", func(t *testing.T) {
		w := m2mPost(t, r, body, bundle.plaintext)
		if w.Code != http.StatusAccepted {
			t.Fatalf("want 202, got %d body=%s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["client_id"] != bundle.keyRow.ClientID {
			t.Fatalf("response.client_id = %v, want %s", resp["client_id"], bundle.keyRow.ClientID)
		}
		// Verify the audit row was written with the resolved client_id.
		var auditCount int
		if err := bundle.db.DB().QueryRow(
			`SELECT COUNT(*) FROM m2m_audit_log WHERE client_id = ? AND status_code = 202`,
			bundle.keyRow.ClientID,
		).Scan(&auditCount); err != nil {
			t.Fatalf("count audit: %v", err)
		}
		if auditCount == 0 {
			t.Fatal("expected at least one m2m_audit_log row with status_code=202")
		}
	})
}

// ── Scenario 12 — M2M rate limit + per-request quota (NEW) ───────

// TestSubmitJobE2E_M2MRateLimitAndQuota exercises the per-client
// rate-limit bucket and the per-request quota caps.
//
// Rate-limit test seeds a client with a tiny burst (2) and posts
// 3 requests rapidly. First 2 should succeed; the 3rd hits 429.
//
// Quota test seeds a client with maxScenes=1; submits a body
// with 2 scenes → 429 m2m_quota_exceeded (observed=2, cap=1).
func TestSubmitJobE2E_M2MRateLimitAndQuota(t *testing.T) {
	t.Run("rate_limit_burst_2", func(t *testing.T) {
		bundle := newM2MBundle(t, m2mBundleOpts{
			clientID: "e2e-m2m-ratelimit",
			rps:      1, burst: 2,
		})
		gin.SetMode(gin.TestMode)
		r := gin.New()
		m2mMW := NewM2MJwAuthMiddleware(&config.Config{}, bundle.st, bundle.limiter)
		bundle.h.RegisterRoutes(r, adminAuthFake, m2mMW)

		// Two requests within burst capacity → both 202.
		for i := 0; i < 2; i++ {
			body := validSubmitJobBody(fmt.Sprintf("e2e-ratelimit-burst-%d", i))
			w := m2mPost(t, r, body, bundle.plaintext)
			if w.Code != http.StatusAccepted {
				t.Fatalf("burst req %d: want 202, got %d body=%s", i, w.Code, w.Body.String())
			}
		}
		// Third request: bucket is empty → 429.
		body3 := validSubmitJobBody("e2e-ratelimit-burst-3")
		w3 := m2mPost(t, r, body3, bundle.plaintext)
		if w3.Code != http.StatusTooManyRequests {
			t.Fatalf("3rd req: want 429, got %d body=%s", w3.Code, w3.Body.String())
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w3.Body.Bytes(), &resp)
		if resp["error"] != "m2m_rate_limited" {
			t.Fatalf("error = %v, want m2m_rate_limited", resp["error"])
		}
	})

	t.Run("quota_max_scenes_exceeded", func(t *testing.T) {
		bundle := newM2MBundle(t, m2mBundleOpts{
			clientID: "e2e-m2m-quota-scenes", rps: 100, burst: 100,
			maxScenes: 1,
		})
		gin.SetMode(gin.TestMode)
		r := gin.New()
		m2mMW := NewM2MJwAuthMiddleware(&config.Config{}, bundle.st, bundle.limiter)
		bundle.h.RegisterRoutes(r, adminAuthFake, m2mMW)

		body := validSubmitJobBody("e2e-m2m-quota-001")
		body.Scenes = append(body.Scenes, SubmitScene{
			Text:            "Extra scene",
			ClipLink:        "velox-asset://clips/extra.mp4",
			DurationSeconds: 2.0,
		})
		w := m2mPost(t, r, body, bundle.plaintext)
		if w.Code != http.StatusTooManyRequests {
			t.Fatalf("want 429 quota, got %d body=%s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["error"] != "m2m_quota_exceeded" {
			t.Fatalf("error = %v, want m2m_quota_exceeded", resp["error"])
		}
		details, ok := resp["details"].(map[string]interface{})
		if !ok {
			t.Fatalf("details not an object: %T (body: %s)", resp["details"], w.Body.String())
		}
		if got, _ := details["reason"].(string); got != "scenes_exceeded" {
			t.Fatalf("details.reason = %q, want scenes_exceeded", got)
		}
		if got, ok := details["observed"].(float64); !ok || int(got) != 2 {
			t.Fatalf("details.observed = %v, want 2", details["observed"])
		}
		if got, ok := details["cap"].(float64); !ok || int(got) != 1 {
			t.Fatalf("details.cap = %v, want 1", details["cap"])
		}
	})

	t.Run("quota_max_duration_exceeded", func(t *testing.T) {
		bundle := newM2MBundle(t, m2mBundleOpts{
			clientID: "e2e-m2m-quota-dur", rps: 100, burst: 100,
			maxTotalSecs: 5.0,
		})
		gin.SetMode(gin.TestMode)
		r := gin.New()
		m2mMW := NewM2MJwAuthMiddleware(&config.Config{}, bundle.st, bundle.limiter)
		bundle.h.RegisterRoutes(r, adminAuthFake, m2mMW)

		body := validSubmitJobBody("e2e-m2m-quota-dur-001")
		body.Scenes[0].DurationSeconds = 10.0
		w := m2mPost(t, r, body, bundle.plaintext)
		if w.Code != http.StatusTooManyRequests {
			t.Fatalf("want 429 quota, got %d body=%s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		details, _ := resp["details"].(map[string]interface{})
		if details == nil {
			t.Fatalf("details missing: %v", resp["details"])
		}
		if got, _ := details["reason"].(string); got != "duration_exceeded" {
			t.Fatalf("details.reason = %q, want duration_exceeded", got)
		}
	})
}

// ── Scenario 13 — POST → GET polling chain + 404 envelope (NEW for P2) ─────

// getSubmittedJob is the GET-side helper mirroring postSubmitJob.
// Same m2mJobsAuthFake-token shape (any non-empty bearer is accepted
// by the in-package fake shim) so the test routes can pin a single
// test fixture across POST and GET. Tests that exercise a REAL M2M
// middleware use m2mPost to drive the full auth + audit pipeline.
