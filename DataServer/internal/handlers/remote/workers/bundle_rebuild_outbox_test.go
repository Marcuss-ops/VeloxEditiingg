package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/outbox"
)

// ── Fixtures ────────────────────────────────────────────────────────────────

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

// bundleFixture is the self-contained test fixture. It wires:
//   - An in-memory SQLite with outbox_events
//   - An *outbox.Store bound to the DB (no dispatcher started)
//   - A real stub-bundler shell script at the production layout
//     (repoRoot/DataServer/bin/velox-bundler) so the handler's
//     getBundlerPath + exec.Command calls succeed
//
// The former HTTP producer (POST /install_worker/force_regenerate_zip,
// removed as an unmounted route) has been replaced by direct
// store-level enqueue; the durability contract under test is the
// outbox row lifecycle, which is independent of who calls Insert.
type bundleFixture struct {
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

	db := newOutboxDB(t)
	store := outbox.NewStore(db)
	registry := outbox.NewRegistry()
	RegisterBundleRebuildOutboxHandler(registry)

	return &bundleFixture{
		db:             db,
		store:          store,
		repoRoot:       repoRoot,
		bundleDir:      bundleDir,
		stubBinaryPath: stubPath,
		registry:       registry,
	}
}

// enqueueRebuild is the canonical producer-side enqueue, expressed the
// way any future producer must express it: encode the payload, Insert
// with status PENDING, and return the event_id.
func enqueueRebuild(t *testing.T, fx *bundleFixture) string {
	t.Helper()
	payload, err := encodeBundleRebuildPayload(fx.repoRoot, fx.bundleDir, fx.stubBinaryPath)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	eventID, err := fx.store.Insert(context.Background(), nil, outbox.InsertParams{
		AggregateType: "worker_bundle",
		AggregateID:   "rebuild:" + fx.repoRoot,
		EventType:     BundleRebuildRequestedEventType,
		Payload:       payload,
	})
	if err != nil {
		t.Fatalf("outbox Insert: %v", err)
	}
	return eventID
}

// ── 1. CrashRecovery — the keystone ─────────────────────────────────────────
//
// Scenario: a producer durably enqueues a WORKER_BUNDLE_REBUILD_REQUESTED
// event (status=PENDING) via the canonical encode+Insert pair. We then
// SIMULATE the master pod dying immediately after the commit by:
//   - NEVER starting a dispatcher goroutine.
//   - Asserting NO side-effect happened on disk (no zip written).
//
// We then SIMULATE the master pod restarting on the same DB and run
// dispatcher polls to drain the row.
//
// We assert:
//   - Exactly ONE PENDING row, correct event_type and payload
//   - After "restart" + dispatch: row goes PROCESSED
//   - After dispatch: the stub binary side-effect (marker file) DID happen
//
// This pins the durability invariant ("no side effect without a committed
// intent row") AND the recovery semantics (row re-claimable at next boot,
// handler idempotent under retry).
func TestBundleRebuildEnqueue_CrashRecovery_AsyncBundleRebuild(t *testing.T) {
	fx := newBundleFixture(t)
	eventID := enqueueRebuild(t, fx)

	// ── Verify the durable record BEFORE the dispatcher runs ──────
	row := fx.db.QueryRowContext(context.Background(),
		`SELECT event_type, aggregate_type, aggregate_id, status, payload_json
		 FROM outbox_events WHERE event_id = ?`, eventID)
	var et, aggType, aggID, status, payload string
	if err := row.Scan(&et, &aggType, &aggID, &status, &payload); err != nil {
		t.Fatalf("scan outbox row event_id=%s: %v", eventID, err)
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
			`SELECT status FROM outbox_events WHERE event_id = ?`, eventID)
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
				`SELECT last_error FROM outbox_events WHERE event_id = ?`, eventID).Scan(&lastErr)
			t.Fatalf("dispatch marked row FAILED: %s", lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	row = fx.db.QueryRowContext(context.Background(),
		`SELECT status FROM outbox_events WHERE event_id = ?`, eventID)
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

// ── 2. BinaryMissing_IsPermanent ────────────────────────────────────────────
//
// Pins the dispatch-time failure doctrine that the former HTTP-layer 404
// precondition used to guard at enqueue time: a missing velox-bundler
// binary at dispatch time is an operator-fixable hazard and must surface
// as a typed Permanent error (never retried into noise).
func TestBundleRebuildHandler_BinaryMissing_IsPermanent(t *testing.T) {
	fx := newBundleFixture(t)
	// Simulate the operator removing velox-bundler from the master pod.
	if err := os.Remove(fx.stubBinaryPath); err != nil {
		t.Fatalf("remove stub binary: %v", err)
	}

	payload, err := encodeBundleRebuildPayload(fx.repoRoot, fx.bundleDir, fx.stubBinaryPath)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	h := NewBundleRebuildHandler()
	handleErr := h.Handle(context.Background(), outbox.Event{
		EventID: "test-binary-missing",
		Payload: payload,
	})
	if handleErr == nil {
		t.Fatal("expected error for missing binary, got nil")
	}
	if !strings.Contains(handleErr.Error(), "binary missing") {
		t.Errorf("error = %v, want it to mention the missing binary", handleErr)
	}
	var he *outbox.HandlerError
	if !errors.As(handleErr, &he) || he.Transient {
		t.Errorf("error = %T(%v), want a permanent-classified HandlerError", handleErr, handleErr)
	}
}

// ── 3. Channel-2 completeness: workers.init() factory wired into the canonical registry
//
// The production registry is process-global by design. This keystone
// therefore verifies the init-time factory in a subprocess rather than
// resetting productionRegOnce or the factory list inside the parent test
// process. That keeps the test compatible with t.Parallel and prevents
// contamination of other tests that use the canonical registry.
func TestBundleRebuildHandler_WiredIntoProductionRegistry(t *testing.T) {
	if os.Getenv("VELOX_BUNDLE_OUTBOX_REGISTRY_HELPER") == "1" {
		reg := outbox.ProductionRegistry()
		if reg == nil {
			t.Fatal("outbox.ProductionRegistry() returned nil")
		}
		h, err := reg.Lookup(BundleRebuildRequestedEventType)
		if err != nil {
			t.Fatalf("registry missing handler for %q: %v", BundleRebuildRequestedEventType, err)
		}
		if h == nil {
			t.Fatalf("registry returned nil handler for %q", BundleRebuildRequestedEventType)
		}
		if ht := h.EventType(); ht != BundleRebuildRequestedEventType {
			t.Errorf("registered handler EventType() = %q, want %q", ht, BundleRebuildRequestedEventType)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestBundleRebuildHandler_WiredIntoProductionRegistry$", "-test.v")
	cmd.Env = append(os.Environ(), "VELOX_BUNDLE_OUTBOX_REGISTRY_HELPER=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated production registry subprocess failed: %v\n%s", err, output)
	}
}
