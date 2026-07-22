// Package artifacts_test contains shared retry-budget fixtures.
// Tests are split into retry_budget_plan_test.go and retry_budget_limits_test.go.
package artifacts_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
	"velox-server/internal/store"
)

const phase5Schema = `
CREATE TABLE jobs (job_id TEXT PRIMARY KEY,status TEXT,revision INTEGER,completed_at TEXT,updated_at TEXT,migrated_at TEXT);
CREATE TABLE artifacts (id TEXT PRIMARY KEY,job_id TEXT,attempt_id INTEGER,type TEXT,storage_provider TEXT,storage_key TEXT,storage_url TEXT,local_path TEXT,sha256 TEXT,size_bytes INTEGER,duration_seconds REAL,duration_ms INTEGER,mime_type TEXT,status TEXT,verified_at TEXT,created_at TEXT);
CREATE TABLE artifact_uploads (upload_id TEXT PRIMARY KEY,artifact_id TEXT,job_id TEXT,attempt_number INTEGER,worker_id TEXT,lease_id TEXT,status TEXT,temporary_storage_key TEXT,expected_size_bytes INTEGER,expected_sha256 TEXT,expected_revision INTEGER,received_size_bytes INTEGER,received_sha256 TEXT,created_at TEXT,expires_at TEXT,completed_at TEXT);
CREATE TABLE outbox_events (aggregate_type TEXT,aggregate_id TEXT,event_type TEXT,payload_json TEXT,status TEXT,available_at TEXT,created_at TEXT);
CREATE TABLE delivery_destinations (destination_id TEXT PRIMARY KEY,provider TEXT,name TEXT,enabled INTEGER DEFAULT 1,created_at TEXT,updated_at TEXT,account_id TEXT,folder_id TEXT,channel_id TEXT,language TEXT,configuration_json TEXT,metadata_json TEXT);
CREATE TABLE job_delivery_plans (job_id TEXT,destination_id TEXT,enabled INTEGER NOT NULL DEFAULT 1,priority INTEGER NOT NULL DEFAULT 0,retry_budget INTEGER NOT NULL DEFAULT 5,metadata_json TEXT NOT NULL DEFAULT '{}',created_at TEXT,updated_at TEXT,PRIMARY KEY (job_id,destination_id));
CREATE TABLE job_deliveries (delivery_id TEXT PRIMARY KEY,artifact_id TEXT,destination_id TEXT,status TEXT DEFAULT 'PENDING',max_attempts INTEGER NOT NULL DEFAULT 5,idempotency_key TEXT,remote_id TEXT,remote_url TEXT,locked_by TEXT,lease_id TEXT,lease_expires_at TEXT,next_attempt_at TEXT,attempt_count INTEGER NOT NULL DEFAULT 0,last_error_code TEXT,last_error_message TEXT,completed_at TEXT,created_at TEXT,updated_at TEXT,UNIQUE (artifact_id,destination_id));
CREATE TABLE tasks (task_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,project_id TEXT NOT NULL DEFAULT '',render_plan_id TEXT NOT NULL DEFAULT '',executor_id TEXT NOT NULL DEFAULT '',executor_version INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'PENDING',priority INTEGER NOT NULL DEFAULT 0,revision INTEGER NOT NULL DEFAULT 0,attempt_count INTEGER NOT NULL DEFAULT 0,worker_id TEXT NOT NULL DEFAULT '',lease_id TEXT NOT NULL DEFAULT '',ready_at TEXT,started_at TEXT,completed_at TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
`

func openPropagationDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(phase5Schema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO delivery_destinations (destination_id,provider,name,enabled,created_at,updated_at) VALUES ('primary','social_gateway','Primary',1,'',''),('secondary','drive','Secondary',1,'','')`); err != nil {
		t.Fatal(err)
	}
	return db
}

type phase5Fixture struct {
	JobID, WorkerID, LeaseID string
	Revision, AttemptNumber  int
	ArtifactID, UploadID     string
}

func seedPhase5Fixture(t *testing.T, db *sql.DB, f phase5Fixture) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO jobs (job_id,status,revision,updated_at,migrated_at) VALUES (?,'RUNNING',?,?,?)`, f.JobID, f.Revision, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifacts (id,job_id,attempt_id,type,storage_provider,status,created_at) VALUES (?,?,?,'render','local','STAGING',?)`, f.ArtifactID, f.JobID, f.AttemptNumber, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads (upload_id,artifact_id,job_id,attempt_number,worker_id,lease_id,status,created_at,expires_at,completed_at) VALUES (?,?,?,?,?,?,'FINALIZING',?,?,NULL)`, f.UploadID, f.ArtifactID, f.JobID, f.AttemptNumber, f.WorkerID, f.LeaseID, now, time.Now().Add(24*time.Hour).UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
}

type phase5Plan struct {
	DestinationID         string
	Priority, RetryBudget int
	Enabled               bool
}

func seedDeliveryPlans(t *testing.T, db *sql.DB, jobID string, plans []phase5Plan) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	for _, p := range plans {
		enabled := 0
		if p.Enabled {
			enabled = 1
		}
		if _, err := db.Exec(`INSERT INTO job_delivery_plans (job_id,destination_id,enabled,priority,retry_budget,metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,'{}',?,?)`, jobID, p.DestinationID, enabled, p.Priority, p.RetryBudget, now, now); err != nil {
			t.Fatal(err)
		}
	}
}
func runFinalize(t *testing.T, db *sql.DB, resolver artifacts.DeliveryPlanResolver, cmd artifacts.FinalizeVerifiedCommand) (*artifacts.SQLiteFinalizeWriter, *sql.DB) {
	t.Helper()
	reader := store.NewSQLiteArtifactReader(db)
	fin := artifacts.NewSQLiteFinalizeWriter(db, reader, resolver)
	if _, err := fin.FinalizeVerified(context.Background(), cmd); err != nil {
		t.Fatalf("FinalizeVerified: %v", err)
	}
	return fin, db
}

type zeroBudgetResolver struct {
	dests []artifacts.DeliveryDestination
}

func (r *zeroBudgetResolver) ResolveDestinations(context.Context, string, string) ([]artifacts.DeliveryDestination, error) {
	return r.dests, nil
}
