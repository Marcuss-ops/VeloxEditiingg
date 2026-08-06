// Package lifecycle / worker_registration_test.go
//
// HTTP-layer tests for the worker allowlist gate on
// POST /api/v1/workers/register. Companion to registration_test.go
// (which covers the credential-validation paths against a mockStore).
//
// These tests pin the closed-enum semantics that were added when the
// HTTP 403 deny rule was introduced: a worker whose worker_id is not
// in VELOX_ALLOWED_WORKERS MUST receive HTTP 403 BEFORE any credential
// storage or in-memory registry insert (handler.go::IsWorkerAllowed,
// registration.go::RegisterV2Handler).
//
// All tests use httptest.NewRecorder + gin.New() so the full middleware
// chain (request decode → handler → response encode) is exercised
// end-to-end. The Handler is wired via NewHandler(cfg, reg, dbStore)
// against a real on-disk SQLite store (t.TempDir) so the registry +
// token manager + command manager are real — only the network is fake.
package lifecycle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// newRegistrationTestHandler builds a real Handler against an isolated
// temp-dir SQLite store. Tests pass allowedWorkersCSV to set the master-
// side allowlist; pass empty + insecureDev=true to exercise the dev-bypass
// path of IsWorkerAllowed.
func newRegistrationTestHandler(t *testing.T, allowedWorkersCSV string, insecureDev bool) *Handler {
	t.Helper()

	cfg := &config.Config{
		Workers: config.WorkersConfig{
			AllowedWorkerIDs: parseAllowedWorkerIDs(allowedWorkersCSV),
		},
		Runtime: config.RuntimeConfig{
			GRPCAllowInsecureDev: insecureDev,
		},
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("test fixture: NewSQLiteStore(%q): %v", dbPath, err)
	}

	// workersreg.New(dbStore) constructs a Registry backed by the
	// same SQLite store so worker persistence + heartbeat path
	// is exercised end-to-end in the test (not just stubbed).
	reg := workersreg.New(dbStore)
	return NewHandler(cfg, reg, dbStore)
}

func parseAllowedWorkerIDs(csv string) []string {
	var ids []string
	for _, value := range strings.Split(csv, ",") {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			ids = append(ids, trimmed)
		}
	}
	return ids
}

// doRegister POSTs a JSON body to the wired /api/v1/workers/register
// handler and returns the recorded response. body must be a JSON object.
func doRegister(h *Handler, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/workers/register", h.RegisterV2Handler())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestRegisterV2_AllowedWorker_Returns200 pins the happy-path: a
// worker_id present in the allowlist MUST receive HTTP 200 with the
// canonical envelope (ok:true, worker_id echoed, session_id issued).
// This is the regression guard for "operator forgot to add the new
// worker to VELOX_ALLOWED_WORKERS" — the canary golden-e2e worker
// (e2e-worker-1) relies on this path on every CI run.
func TestRegisterV2_AllowedWorker_Returns200(t *testing.T) {
	h := newRegistrationTestHandler(t,
		"velox-worker-1,velox-worker-2,e2e-worker-1",
		false, // production mode
	)

	w := doRegister(h, `{"worker_id":"e2e-worker-1","worker_name":"golden-e2e","ip":"127.0.0.1"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("got status=%d, want 200 OK\nbody=%s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v\nbody=%s", err, w.Body.String())
	}
	if resp["ok"] != true {
		t.Errorf("ok = %v, want true (200 envelope must include ok:true)", resp["ok"])
	}
	if resp["worker_id"] != "e2e-worker-1" {
		t.Errorf("worker_id = %v, want e2e-worker-1", resp["worker_id"])
	}
	if sid, _ := resp["session_id"].(string); sid == "" {
		t.Errorf("session_id = empty, want non-empty (the canonical registration receipt)")
	}
}

// TestRegisterV2_DeniedWorker_Returns403 pins the deny-rule core:
// a worker_id NOT in the allowlist MUST be rejected with HTTP 403
// and the canonical envelope (error=worker_not_allowed, ok:false).
// No session token is issued, so the worker cannot proceed to the
// gRPC stream with a stolen-looking token.
func TestRegisterV2_DeniedWorker_Returns403(t *testing.T) {
	h := newRegistrationTestHandler(t,
		"velox-worker-1,velox-worker-2", // unknown-attacker is NOT in this list
		false,
	)

	w := doRegister(h, `{"worker_id":"unknown-attacker","worker_name":"attacker"}`)

	if w.Code != http.StatusForbidden {
		t.Fatalf("got status=%d, want 403 Forbidden\nbody=%s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v\nbody=%s", err, w.Body.String())
	}
	if resp["ok"] != false {
		t.Errorf("ok = %v, want false (denied envelope must include ok:false)", resp["ok"])
	}
	if resp["error"] != "worker_not_allowed" {
		t.Errorf("error = %v, want worker_not_allowed (the canonical error code)", resp["error"])
	}
	if sid, present := resp["session_id"]; present && sid != "" {
		t.Errorf("session_id = %v on a denied request; want absent (deny MUST NOT issue a session token)", sid)
	}
}

// TestRegisterV2_AllowlistMatches_WhitespaceTrimmed pins the
// helper's trim policy: a worker_id with leading/trailing whitespace
// in BOTH the request body AND the CSV entry MUST match once both
// sides are trimmed. Without this the operator's CSV may silently
// admit workers via "velox-worker-1 " (the entry has a stray space).
func TestRegisterV2_AllowlistMatches_WhitespaceTrimmed(t *testing.T) {
	h := newRegistrationTestHandler(t,
		"  velox-worker-1 ,velox-worker-2  ", // CSV has stray whitespace
		false,
	)

	// "velox-worker-1 " (trailing space) MUST match " velox-worker-1 "
	// (CSV entry with both). IsWorkerAllowed trims both sides.
	w := doRegister(h, `{"worker_id":"velox-worker-1 ","worker_name":"t"}`)
	if w.Code != http.StatusOK {
		t.Errorf("got status=%d, want 200 OK (whitespace in body+CSV must match after trim)\nbody=%s", w.Code, w.Body.String())
	}
}

// TestRegisterV2_EmptyAllowlist_DenyInProduction pins the
// fail-closed production path: when VELOX_ALLOWED_WORKERS is empty
// (a misconfiguration the bootstrap ought to have caught), the
// HTTP layer still denies — defence in depth, mirroring the gRPC
// stream layer's behaviour in
// grpcserver/handler_stream.go::Stream + authorizer.go::IsAllowed.
func TestRegisterV2_EmptyAllowlist_DenyInProduction(t *testing.T) {
	h := newRegistrationTestHandler(t,
		"", // empty CSV — misconfiguration
		false,
	)

	w := doRegister(h, `{"worker_id":"any-worker","worker_name":"any"}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("got status=%d, want 403 Forbidden (empty allowlist in production MUST deny via defence in depth)\nbody=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["error"] != "worker_not_allowed" {
		t.Errorf("error = %v, want worker_not_allowed (empty prod allowlist 403 envelope)", resp["error"])
	}
}

// TestRegisterV2_EmptyAllowlist_AllowInDev pins the dev-only
// bypass: when GRPCAllowInsecureDev=true (typically local
// development), an empty allowlist MUST allow the worker so
// local Flask-style experimentation doesn't need operators to
// configure the allowlist. Production NEVER takes this path
// (the empty-allowlist case is fail-closed there — see
// TestRegisterV2_EmptyAllowlist_DenyInProduction).
func TestRegisterV2_EmptyAllowlist_AllowInDev(t *testing.T) {
	h := newRegistrationTestHandler(t,
		"",   // empty CSV — dev mode intentionally permissive
		true, // GRPCAllowInsecureDev=true → dev bypass
	)

	w := doRegister(h, `{"worker_id":"any-worker-dev","worker_name":"dev"}`)

	// In dev mode, IsWorkerAllowed returns true — the handler
	// must therefore proceed past the 403 gate. The 200 path
	// still requires a working registry + token manager.
	if w.Code == http.StatusForbidden {
		t.Errorf("got status=%d, want non-403 (dev-mode empty allowlist MUST allow)\nbody=%s", w.Code, w.Body.String())
	}
	if w.Code != http.StatusOK {
		t.Logf("got status=%d (acceptable; non-403 in dev-bypass mode); body=%s", w.Code, w.Body.String())
	}
}

// TestRegisterV2_NoCredentialProvided_StillAllowlistGated pins
// backward compatibility: a legacy client that omits the
// `credential` field (the pre-credential path —
// registration.go flow step 3) still must pass through the
// allowlist. Without this guard the allowlist gate could be
// trivially bypassed by omitting the credential field.
func TestRegisterV2_NoCredentialProvided_StillAllowlistGated(t *testing.T) {
	h := newRegistrationTestHandler(t,
		"allowed-worker-only",
		false,
	)

	// Legacy client sends NO credential; allowed-worker MUST get 200.
	w := doRegister(h, `{"worker_id":"allowed-worker-only","worker_name":"legacy"}`)
	if w.Code != http.StatusOK {
		t.Errorf("legacy-no-credential + allowed worker: got %d, want 200\nbody=%s", w.Code, w.Body.String())
	}

	// Legacy client sends NO credential; unlisted worker MUST still 403.
	w2 := doRegister(h, `{"worker_id":"unlisted-worker","worker_name":"legacy"}`)
	if w2.Code != http.StatusForbidden {
		t.Errorf("legacy-no-credential + unlisted worker: got %d, want 403 (allowlist gate is independent of credential field)\nbody=%s", w2.Code, w2.Body.String())
	}
}

// TestRegisterV2_AllowlistGate_BeforeCredentialStorage pins the
// order invariant documented in the gate's source comment: the
// allowlist check MUST run BEFORE credential validation, so an
// unlisted worker cannot cause a credential row to be stored in
// `worker_credentials`. We exercise this by sending an unlisted
// worker WITH a credential and asserting:
//
//  1. HTTP 403 (gate fires first)
//  2. No row in worker_credentials for that worker_id (side-effect
//     leak guard — the deny path must NOT touch the credentials
//     table at all).
func TestRegisterV2_AllowlistGate_BeforeCredentialStorage(t *testing.T) {
	h := newRegistrationTestHandler(t, "listed-only", false)

	w := doRegister(h, `{"worker_id":"unlisted-worker","credential":"sha256-leak-attempt"}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403 (unlisted worker MUST be rejected before credential storage)\nbody=%s", w.Code, w.Body.String())
	}

	// Resource-leak invariant: worker_credentials MUST NOT have a row
	// for "unlisted-worker". The 403 path must short-circuit before
	// reaching the credential insert.
	hasCred, err := h.dbStore.HasWorkerCredential("unlisted-worker")
	if err != nil {
		t.Fatalf("HasWorkerCredential post-deny: %v", err)
	}
	if hasCred {
		t.Errorf("worker_credentials has a row for unlisted-worker after a 403 — credential store MUST NOT see denied-registration traffic")
	}
}

// TestRegisterV2_StarWildcard_DenyInProduction pins the `*`
// wildcard semantics that mirror grpcserver/allowlistAuthorizer:
// when VELOX_ALLOWED_WORKERS="*" (a single asterisk) the master
// behaves as if the allowlist were empty. Bootstrap rejects this
// in production via ValidateProductionWorkers, but the HTTP layer
// remains fail-closed (defence in depth) so a hand-crafted config
// cannot silently admit workers.
func TestRegisterV2_StarWildcard_DenyInProduction(t *testing.T) {
	h := newRegistrationTestHandler(t, "*", false)

	w := doRegister(h, `{"worker_id":"any-worker","worker_name":"any"}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("got status=%d, want 403 Forbidden ('*' in production MUST deny via defence in depth)\nbody=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["error"] != "worker_not_allowed" {
		t.Errorf("error = %v, want worker_not_allowed", resp["error"])
	}
}

// TestRegisterV2_StarWildcard_AllowInDev pins the dev-bypass
// semantics: VELOX_ALLOWED_WORKERS="*" + GRPCAllowInsecureDev=true
// MUST allow all workers (mirrors grpcserver/allowlistAuthorizer's
// IsAllowed dev-bypass exactly). This is the contract operators
// rely on for local development without configuring individual
// worker IDs.
func TestRegisterV2_StarWildcard_AllowInDev(t *testing.T) {
	h := newRegistrationTestHandler(t, "*", true /* insecureDev */)

	w := doRegister(h, `{"worker_id":"any-worker-dev","worker_name":"dev"}`)
	if w.Code == http.StatusForbidden {
		t.Errorf("got status=%d, want non-403 ('*' + dev-mode MUST allow)\nbody=%s", w.Code, w.Body.String())
	}
	if w.Code != http.StatusOK {
		t.Logf("got status=%d (acceptable; non-403 in dev-bypass mode); body=%s", w.Code, w.Body.String())
	}
}
