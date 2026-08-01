package workers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/config"
	"velox-server/internal/outbox"
)

// ── Fixtures ────────────────────────────────────────────────────────────────

const testAdminToken = "test-admin-202-tokens"
const stubBundleMarker = "velox-bundler-stub-marker"

var testOutboxDsnCounter int

// newOutboxDB builds an in-memory SQLite with the canonical outbox_events
// schema from migration 026. Self-contained mirror of outbox_test.go's
// newTestDB so this test file does not cross-import the outbox_test
// package (which would risk test-binary coupling).
func newOutboxDB(t *testing.T) *sql.DB {
	t.Helper()
	testOutboxDsnCounter++
	dsn := fmt.Sprintf("file:bundle_rebuild_outbox_test-%d?mode=memory&cache=shared", testOutboxDsnCounter)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	schema := `CREATE TABLE IF NOT EXISTS outbox_events (
		event_id        TEXT PRIMARY KEY,
		aggregate_type  TEXT NOT NULL,
		aggregate_id    TEXT NOT NULL,
		event_type      TEXT NOT NULL,
		payload_json    TEXT NOT NULL DEFAULT '{}',
		status          TEXT NOT NULL DEFAULT 'PENDING',
		available_at    TEXT NOT NULL,
		attempt_count   INTEGER NOT NULL DEFAULT 0,
		locked_by       TEXT,
		locked_until    TEXT,
		fence_token     TEXT NOT NULL DEFAULT '',
		processed_at    TEXT,
		last_error      TEXT,
		created_at      TEXT NOT NULL
	)`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create outbox_events: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// stubVeloxBundler writes an executable shell script that fakes the
// velox-bundler binary. When invoked with --source <repoRoot> --output
// <bundleDir>, it creates bundleDir/worker_code_linux_x86_64.zip as a
// marker file, prints a single status line, and exits 0. Cleans up
// the script with t.Cleanup.
func stubVeloxBundler(t *testing.T, dir, marker string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir stub dir: %v", err)
	}
	path := filepath.Join(dir, "velox-bundler")
	tmpl := "#!/bin/sh\n" +
		"set -e\n" +
		"src=\"\"\n" +
		"out=\"\"\n" +
		"while [ $# -gt 0 ]; do\n" +
		"  case \"$1\" in\n" +
		"    --source) src=\"$2\"; shift 2;;\n" +
		"    --output) out=\"$2\"; shift 2;;\n" +
		"    *) shift;;\n" +
		"  esac\n" +
		"done\n" +
		"mkdir -p \"$out\"\n" +
		"printf 'MARKER\\n' > \"$out/worker_code_linux_x86_64.zip\"\n" +
		"echo \"stub-bundler: src=$src out=$out marker=MARKER\"\n"
	src := strings.ReplaceAll(tmpl, "MARKER", marker)
	if err := os.WriteFile(path, []byte(src), 0o755); err != nil {
		t.Fatalf("write stub script: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

// bundleFixture is the self-contained test fixture used by all four
// keystones. It wires:
//   - An in-memory SQLite with outbox_events
//   - An *outbox.Store bound to the DB (no dispatcher started)
//   - A WorkerUpdateHandler with the store injected + bundleDir set
//     (cfg is a real *config.Config so the constructor doesn't nil-deref)
//   - A real stub-bundler shell script at the production layout
//     (repoRoot/DataServer/bin/velox-bundler) so the handler's
//     getBundlerPath + exec.Command calls succeed
//   - A Gin router exposing POST /install_worker/force_regenerate_zip
type bundleFixture struct {
	server         *httptest.Server
	db             *sql.DB
	store          *outbox.Store
	repoRoot       string
	bundleDir      string
	stubBinaryPath string
	registry       *outbox.Registry
}

func newBundleFixture(t *testing.T) *bundleFixture {
	t.Helper()
	tmp := t.TempDir()

	// ── Stub-bundler at the production layout path ────────────────
	// findRepoRootFrom(bundleDir) → repoRoot, then
	// getBundlerPath(repoRoot) → repoRoot/DataServer/bin/velox-bundler.
	repoRoot := filepath.Join(tmp, "repo")
	dataServerDir := filepath.Join(repoRoot, "DataServer")
	stubDir := filepath.Join(dataServerDir, "bin")
	bundleDir := filepath.Join(repoRoot, "worker_downloads")

	if err := os.MkdirAll(stubDir, 0o755); err != nil {
		t.Fatalf("mkdir stubDir: %v", err)
	}
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatalf("mkdir bundleDir: %v", err)
	}
	stubPath := stubVeloxBundler(t, stubDir, stubBundleMarker)

	// ── Build the SQLite + outbox.Store + handler ─────────────────
	db := newOutboxDB(t)
	store := outbox.NewStore(db)
	registry := outbox.NewRegistry()
	RegisterBundleRebuildOutboxHandler(registry)

	// Empty *config.Config is sufficient: the constructor only reads
	// cfg.Workers.BundleDir and cfg.Workers.CodeVersion. BundleDir
	// comes up empty so all candidate scans fail and the constructor
	// falls back to filepath.Join(dataDir, "worker_downloads"),
	// which we then override explicitly below. CodeVersion is unused
	// by ForceRegenerateZipHandler.
	cfg := &config.Config{Workers: config.WorkersConfig{}}
	h := NewWorkerUpdateHandler(cfg, nil, nil, nil, tmp, store)
	h.bundleDir = bundleDir

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/install_worker/force_regenerate_zip", adminAuthForTest, h.ForceRegenerateZipHandler())
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)

	return &bundleFixture{
		server:         server,
		db:             db,
		store:          store,
		repoRoot:       repoRoot,
		bundleDir:      bundleDir,
		stubBinaryPath: stubPath,
		registry:       registry,
	}
}

// adminAuthForTest is the trivial admin-token middleware for the
// bundle rebuild tests. Real production auth has admin token in
// cfg.Auth.AdminToken; for these focused tests we hard-code.
func adminAuthForTest(c *gin.Context) {
	if c.GetHeader("X-Admin-Token") == testAdminToken {
		c.Next()
		return
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized (test)"})
}

// ── 1. CrashRecovery — the user-requested keystone ──────────────────────────
//
// Scenario: operator POSTs /install_worker/force_regenerate_zip?wait=0.
// The handler durably enqueues a WORKER_BUNDLE_REBUILD_REQUESTED event
// (status=PENDING) BEFORE ACKing 202. We then SIMULATE the master pod
// dying immediately after the ACK by:
//   - NEVER starting a dispatcher goroutine.
//   - Asserting NO side-effect happened on disk (no zip written).
//
// We then SIMULATE the master pod restarting on the same DB and run
// a single dispatcher tick to drain the row.
//
// We assert:
//   - HTTP 202 with event_id
//   - Exactly ONE PENDING row, correct event_type and payload
//   - After "restart" + dispatch tick: row goes PROCESSED
//   - After dispatch: the stub binary side-effect (marker file) DID happen
//
// This pins the user's requirement: "verifica che nessuna ACK 202 venga
// emessa prima della transazione completata" AND the recovery semantics.
func TestForceRegenerateZip_CrashRecovery_AsyncBundleRebuild(t *testing.T) {
	fx := newBundleFixture(t)

	resp, err := authedPost(fx.server.URL+"/install_worker/force_regenerate_zip?wait=0", `{}`)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 Accepted", resp.StatusCode)
	}
	var respBody struct {
		OK      bool   `json:"ok"`
		EventID string `json:"event_id"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !respBody.OK || respBody.EventID == "" {
		t.Fatalf("respBody = %+v, want ok=true event_id!=''", respBody)
	}
	if respBody.Message != "bundle rebuild queued for dispatch" {
		t.Errorf("message = %q, want \"bundle rebuild queued for dispatch\"", respBody.Message)
	}

	// ── Verify the durable record BEFORE the dispatcher runs ──────
	row := fx.db.QueryRowContext(context.Background(),
		`SELECT event_type, aggregate_type, aggregate_id, status, payload_json
		 FROM outbox_events WHERE event_id = ?`, respBody.EventID)
	var et, aggType, aggID, status, payload string
	if err := row.Scan(&et, &aggType, &aggID, &status, &payload); err != nil {
		t.Fatalf("scan outbox row event_id=%s: %v", respBody.EventID, err)
	}
	if et != BundleRebuildRequestedEventType {
		t.Errorf("event_type = %q, want %q", et, BundleRebuildRequestedEventType)
	}
	if aggType != "worker_bundle" {
		t.Errorf("aggregate_type = %q, want worker_bundle", aggType)
	}
	if aggID != "rebuild:"+fx.repoRoot {
		t.Errorf("aggregate_id = %q, want rebuild:%s", aggID, fx.repoRoot)
	}
	if status != "PENDING" {
		t.Errorf("status = %q, want PENDING (pre-crash invariant)", status)
	}
	var p bundleRebuildPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if p.RepoRoot == "" || p.BundleDir == "" {
		t.Errorf("payload missing repo_root/bundle_dir: %+v", p)
	}

	// AND no on-disk side-effect yet (the dispatcher never ran).
	if _, err := os.Stat(filepath.Join(fx.bundleDir, "worker_code_linux_x86_64.zip")); err == nil {
		t.Errorf("expected NO bundle on disk before dispatch; found %s", filepath.Join(fx.bundleDir, "worker_code_linux_x86_64.zip"))
	}

	// ── "Restart" + run a dispatcher tick ─────────────────────────
	d := outbox.NewDispatcher(fx.store, fx.registry, outbox.Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    4,
		LockDuration: 5 * time.Second,
		MaxAttempts:  3,
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := d.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		row := fx.db.QueryRowContext(context.Background(),
			`SELECT status FROM outbox_events WHERE event_id = ?`, respBody.EventID)
		var s string
		if err := row.Scan(&s); err != nil {
			t.Fatalf("scan post-poll status: %v", err)
		}
		if s == "PROCESSED" {
			break
		}
		if s == "FAILED" {
			lastErr := ""
			_ = fx.db.QueryRowContext(context.Background(),
				`SELECT last_error FROM outbox_events WHERE event_id = ?`, respBody.EventID).Scan(&lastErr)
			t.Fatalf("dispatch marked row FAILED: %s", lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	row = fx.db.QueryRowContext(context.Background(),
		`SELECT status FROM outbox_events WHERE event_id = ?`, respBody.EventID)
	var final string
	if err := row.Scan(&final); err != nil {
		t.Fatalf("final scan: %v", err)
	}
	if final != "PROCESSED" {
		t.Fatalf("final status = %q, want PROCESSED", final)
	}

	if _, err := os.Stat(filepath.Join(fx.bundleDir, "worker_code_linux_x86_64.zip")); err != nil {
		t.Errorf("expected bundle on disk after dispatch; got: %v", err)
	}
}

// ── 2. SyncBlock_NoOutboxRow ────────────────────────────────────────────────
//
// Pins that the sync wait=1 path UNCHANGED:
//   - returns 200 OK with new_bundle_hash
//   - runs velox-bundler inline (no dispatch row)
//   - writes ZERO rows to outbox_events
//
// Regression pin: sync was working; the conversion to outbox was
// strictly async. If a future refactor accidentally double-enqueues
// sync requests, this keystone fires.
func TestForceRegenerateZip_Sync_NoOutboxRow(t *testing.T) {
	fx := newBundleFixture(t)

	req, _ := http.NewRequest(http.MethodPost,
		fx.server.URL+"/install_worker/force_regenerate_zip?wait=1", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("X-Admin-Token", testAdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST wait=1: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 OK", resp.StatusCode)
	}
	var body struct {
		OK      bool   `json:"ok"`
		Hash    string `json:"new_bundle_hash"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.OK || body.Hash == "" {
		t.Fatalf("body = %+v, want ok=true new_bundle_hash!=''", body)
	}

	var n int
	if err := fx.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if n != 0 {
		t.Errorf("sync path wrote %d outbox rows; want 0", n)
	}
}

// ── 3. PreconditionFail_NoEnqueue ───────────────────────────────────────────
//
// Pins that an operator-misconfig (velox-bundler missing) surfaces as
// 404 + structured error AND does NOT enqueue a row that would
// dispatch against an absent binary. Failure mode doctrine: garbage
// in the queue ≫ operator-visible 404.
func TestForceRegenerateZip_PreconditionFail_NoEnqueue(t *testing.T) {
	fx := newBundleFixture(t)
	// Sabotage the stub binary BEFORE the request — simulates the
	// operator removing velox-bundler from the master pod at runtime.
	if err := os.Remove(fx.stubBinaryPath); err != nil {
		t.Fatalf("remove stub binary: %v", err)
	}

	resp, err := authedPost(fx.server.URL+"/install_worker/force_regenerate_zip?wait=0", `{}`)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var n int
	if err := fx.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("precondition-fail path wrote %d outbox rows; want 0", n)
	}
}

// ── 4. OutboxInsertFails_NoAck — the ACK-before-Insert contract pin ──────────
//
// User requirement: "verifica che nessuna ACK 202 venga emessa prima
// della transazione completata". This keystone is the tightest pin
// available: we close the *sql.DB BEFORE the HTTP request so
// outbox.Store.Insert is GUARANTEED to fail with "sql: database is
// closed" (or similar). The handler must NOT emit 202 in this case —
// the only acceptable status is 500 + no rows written.
//
// The companion PreconditionFail_NoEnqueue covers the "binary missing"
// branch; this test covers the "Insert fails" branch. Together they
// pin the no-ACK-before-commit contract end-to-end.
//
// Double-close note: newBundleFixture registers a t.Cleanup that also
// runs db.Close(); SQLite tolerates the second close as a no-op, and
// the order is at worst cosmetic. If a future driver enforces strict
// close semantics, this test must be migrated to an isolated DB
// lifecycle.
func TestForceRegenerateZip_OutboxInsertFails_NoAck(t *testing.T) {
	fx := newBundleFixture(t)
	// Close the underlying DB connection BEFORE issuing the request.
	// Now Store.Insert will return sql.ErrConnDone (or close-flavoured
	// error). Handler MUST surface a non-2xx + NOT 202.
	if err := fx.db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	resp, err := authedPost(fx.server.URL+"/install_worker/force_regenerate_zip?wait=0", `{}`)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		t.Fatalf("status = 202; the Insert-error path leaked a 202 — the canonical no-ACK-before-commit contract is broken")
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (Insert-failure path)", resp.StatusCode)
	}
	var body struct {
		Error  string `json:"error"`
		Detail string `json:"detail"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error != "outbox enqueue failed" {
		t.Errorf("body.error = %q, want \"outbox enqueue failed\"", body.Error)
	}
	if body.Detail == "" {
		t.Errorf("body.detail empty; want the underlying Insert error surfaced")
	}
}

// authedPost issues a POST with the test admin token header. Bundles
// the http.NewRequest + Header.Set + DefaultClient.Do ceremony that
// was duplicated in 3 of the 4 keystones.
func authedPost(url, body string) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", testAdminToken)
	return http.DefaultClient.Do(req)
}

// ── 5. Channel-2 completeness: workers.init() factory wired into the canonical registry
//
// Phase-5 wiring moved BundleRebuildHandler registration from an
// explicit bootstrap_workers.go call into workers.init() via
// outbox.RegisterHandlerFactory. This keystone PROVES that wiring
// works at runtime AND exercises the ResetHandlerFactoriesForTesting
// API surface so it has at least one in-package consumer (the prior
// review flagged it as YAGNI-exported-with-no-caller).
//
// Failure mode pinned: a regression that breaks the bootstrap-time
// factory registration (BuildTag, accidental init() removal,
// production-registry cache built BEFORE init() ran) leaves the
// dispatcher's "no handler → MarkFailed" branch firing for
// production rebuilds. Before this test, the gap was silent.
//
// Test flow:
//  1. Wipe the in-memory factory list + cached registry so the test
//     starts from a clean slate (mirrors a fresh-process boot).
//  2. Reregister the subsystem factory via RegisterHandlerFactory.
//  3. outbox.ProductionRegistry() rebuilds the cached registry from
//     the factory list.
//  4. Assert the workers-owned event_type is present, EventType()
//     matches.
//
// CrashRecovery keystone #1 already exercises the dispatch path
// end-to-end with a real row + claim + handle, so this test stays
// scoped to the registration/PRODUCTION side without duplicating
// CrashRecovery's coverage. Both ResetHandlerFactoriesForTesting
// and RegisterHandlerFactory are now consumed here, so neither is
// YAGNI surface area.
func TestBundleRebuildHandler_WiredIntoProductionRegistry(t *testing.T) {
	outbox.ResetHandlerFactoriesForTesting()
	outbox.RegisterHandlerFactory(RegisterBundleRebuildOutboxHandler)

	reg := outbox.ProductionRegistry()
	if reg == nil {
		t.Fatal("outbox.ProductionRegistry() returned nil after re-registration")
	}
	h, err := reg.Lookup(BundleRebuildRequestedEventType)
	if err != nil {
		t.Fatalf("registry missing handler for %q after RegisterHandlerFactory: %v",
			BundleRebuildRequestedEventType, err)
	}
	if h == nil {
		t.Fatalf("registry returned nil handler for %q", BundleRebuildRequestedEventType)
	}
	if ht := h.EventType(); ht != BundleRebuildRequestedEventType {
		t.Errorf("registered handler EventType() = %q, want %q", ht, BundleRebuildRequestedEventType)
	}
}
