// Package store_test verifies the persistence contract for
// worker_flags.raw_json: it must NOT carry WorkerInfo vocabulary
// (SessionActive, ConnectionStatus, etc.) so the persistence-leak
// vector fixed by workers.ScrubForPersist cannot be reintroduced via
// the revoke path. This file exists as a reviewer-flagged mitigation
// for /api/v1/workers/:worker_id ("sibling leak vector" — see PR
// improvement for the CONNECTED/STALE/DISCONNECTED semantics change).
package store_test

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"velox-server/internal/store"
)

// workerInfoVocabulary is the set of WorkerInfo JSON keys that MUST NOT
// appear in worker_flags.raw_json. If any of these leak into the audit
// blob, a future "harmonization" refactor would re-introduce the
// read-time-derived state leak that ScrubForPersist is meant to prevent
// — but without a matching read-time hydrator on this side (none
// exists and none should exist).
// NOTE: keep this in lockstep with the JSON tags on workers.WorkerInfo
// (DataServer/internal/workers/worker_info.go). Every WorkerInfo field
// MUST appear here unless it is one of the three allowed audit keys
// ({worker_id, revoked, updated_at}). The three stale WorkerInfo keys
// caught only by a future review (worker_name, status, ...) MUST be
// added here the same day the corresponding struct field is added.
var workerInfoVocabulary = []string{
	"worker_name",
	"status",
	"session_active",
	"connection_status",
	"last_heartbeat",
	"first_seen",
	"current_job",
	"worker_group",
	"display_name",
	"ip_address",
	"host",
	"code_version",
	"bundle_version",
	"bundle_hash",
	"protocol_version",
	"engine_version",
	"capabilities",
	"boot_id",
	"boot_ts",
	"readiness",
	"recent_logs",
	"recent_errors",
	"metrics",
	"schedulable",
	"drain",
}

func newWorkersFlagStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	dbPath := t.TempDir() + "/workers_flag_test.db"
	s, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// storeDBField returns the unexported SQLiteStore.db *sql.DB field via
// reflection+unsafe. This is the standard Go pattern for cross-package
// tests accessing unexported state without modifying production code.
// Adding an exported DB() accessor just for tests was the alternative
// weighed and rejected as heavier.
func storeDBField(t *testing.T, s *store.SQLiteStore) *sql.DB {
	t.Helper()
	v := reflect.ValueOf(s).Elem().FieldByName("db")
	if !v.IsValid() {
		t.Fatalf("SQLiteStore.db field not found (struct layout changed?)")
	}
	dbIface := reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem().Interface()
	db, ok := dbIface.(*sql.DB)
	if !ok {
		t.Fatalf("SQLiteStore.db is not *sql.DB; got %T", dbIface)
	}
	return db
}

func readWorkerFlagRawJSON(t *testing.T, s *store.SQLiteStore, workerID string) string {
	t.Helper()
	var raw string
	err := storeDBField(t, s).QueryRow(
		`SELECT raw_json FROM worker_flags WHERE worker_id = ?`, workerID,
	).Scan(&raw)
	if err != nil {
		t.Fatalf("read worker_flags.raw_json for %q: %v", workerID, err)
	}
	return raw
}

// TestSetWorkerRevoked_RawJsonShapeContract pins the contract on
// worker_flags.raw_json after a revoke call: it contains EXACTLY
// {worker_id, revoked, updated_at} and NOTHING from the WorkerInfo
// vocabulary. Any future regression that re-introduces the sibling
// leak vector (e.g. by marshalling a *workers.WorkerInfo into the blob,
// or by "harmonizing" worker_flags.raw_json with workers.raw_json)
// will be caught here.
//
// This test is the load-bearing mitigation for the reviewer-flagged
// "sibling leak vector" — it converts the doc-comment contract on
// SetWorkerRevoked into an enforced runtime guarantee.
func TestSetWorkerRevoked_RawJsonShapeContract(t *testing.T) {
	s := newWorkersFlagStore(t)
	workerID := "worker-shape-contract-1"

	if err := s.SetWorkerRevoked(workerID, true); err != nil {
		t.Fatalf("SetWorkerRevoked(true): %v", err)
	}
	raw := readWorkerFlagRawJSON(t, s, workerID)

	// Parse the blob.
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("worker_flags.raw_json must be valid JSON: %v\nblob=%s", err, raw)
	}

	// (a) Must contain exactly the three expected keys.
	expected := map[string]bool{"worker_id": false, "revoked": false, "updated_at": false}
	for k := range m {
		if _, ok := expected[k]; ok {
			expected[k] = true
		} else {
			t.Errorf("worker_flags.raw_json has unexpected key %q (shape must be exactly {worker_id, revoked, updated_at}); blob=%s", k, raw)
		}
	}
	for k, seen := range expected {
		if !seen {
			t.Errorf("worker_flags.raw_json missing required key %q; blob=%s", k, raw)
		}
	}

	// (b) Parsed-key check: must NOT contain any WorkerInfo vocabulary key.
	for _, forbidden := range workerInfoVocabulary {
		if _, leaked := m[forbidden]; leaked {
			t.Errorf("LEAK VECTOR: worker_flags.raw_json contains WorkerInfo key %q; blob=%s", forbidden, raw)
		}
	}

	// (c) Substring sanity — defense against future blobs like
	// {"worker_id":"...","revoked":false,"session_active":true}.
	for _, forbidden := range workerInfoVocabulary {
		if strings.Contains(raw, `"`+forbidden+`"`) {
			t.Errorf("LEAK VECTOR (substring): worker_flags.raw_json string-literal contains %q; blob=%s", forbidden, raw)
		}
	}
}

// TestSetWorkerRevoked_UnrevokeRoundTripPreservesShape pins that
// UnrevokeWorker writes the same shape, so a revoked→unrevoked→revoked
// sequence never accidentally conjures a WorkerInfo-shaped blob.
func TestSetWorkerRevoked_UnrevokeRoundTripPreservesShape(t *testing.T) {
	s := newWorkersFlagStore(t)
	workerID := "worker-roundtrip-1"

	for _, revoked := range []bool{true, false, true} {
		if err := s.SetWorkerRevoked(workerID, revoked); err != nil {
			t.Fatalf("SetWorkerRevoked(%v): %v", revoked, err)
		}
		blob := readWorkerFlagRawJSON(t, s, workerID)
		for _, forbidden := range workerInfoVocabulary {
			if strings.Contains(blob, `"`+forbidden+`"`) {
				t.Errorf("after SetWorkerRevoked(%v): blob string-literal contains %q; blob=%s", revoked, forbidden, blob)
			}
		}
	}
}
