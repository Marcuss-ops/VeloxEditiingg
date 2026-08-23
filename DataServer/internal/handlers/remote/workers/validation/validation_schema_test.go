package validation

import (
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"velox-server/internal/store"
)

func TestValidationStoreBootstrapCreatesValidationTable(t *testing.T) {
	t.Parallel()

	db := newValidationTestStore(t)

	var tableName string
	err := db.DB().QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'worker_validations'`).Scan(&tableName)
	require.NoError(t, err)
	require.Equal(t, "worker_validations", tableName)

	columns := validationColumns(t, db)
	for _, column := range []string{"worker_id", "validation_code", "canonical_unit", "exec_start", "validated_at", "failure_reason", "created_at", "updated_at"} {
		require.True(t, columns[column], "bootstrap worker_validations missing %s", column)
	}
}

func TestMigratedValidationSchemaMatchesRepository(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)
	columns := validationColumns(t, db)
	for _, column := range []string{"worker_id", "validation_code", "canonical_unit", "exec_start", "validated_at", "failure_reason", "created_at", "updated_at"} {
		require.True(t, columns[column], "worker_validations missing %s", column)
	}
}

func TestMigrationUpgradesLegacyValidationTable(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "validation.db")
	legacy, err := store.NewSQLiteStoreFromPath(path, false)
	require.NoError(t, err)
	_, err = legacy.DB().Exec(`
CREATE TABLE worker_validations (
  worker_id TEXT PRIMARY KEY,
  validation_code TEXT NOT NULL,
  canonical_unit TEXT,
  valid_from TEXT,
  valid_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE calendar_events (id TEXT PRIMARY KEY);
-- This fixture records migration 094 as applied below, so it must also
-- provide the pre-138 worker runtime table that migration 138 extends.
-- The production migration chain creates this table in 094; this test
-- intentionally creates only the legacy tables it exercises.
CREATE TABLE worker_task_runtime (
  task_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  lease_id TEXT NOT NULL,
  executor_id TEXT NOT NULL,
  executor_version INTEGER NOT NULL DEFAULT 0,
  runtime_status TEXT NOT NULL,
  progress_percent INTEGER NOT NULL DEFAULT 0,
  progress_stage TEXT,
  current_scene INTEGER NOT NULL DEFAULT 0,
  total_scenes INTEGER NOT NULL DEFAULT 0,
  started_at TEXT NOT NULL,
  last_progress_at TEXT,
  cancel_requested_at TEXT,
  updated_at TEXT NOT NULL,
  missing_heartbeats INTEGER NOT NULL DEFAULT 0
);
-- This fixture records migrations through 136 as applied below, so it must
-- also provide the pre-142 publication_states table that migration 142
-- extends (ALTER TABLE + state reclassification). The production chain
-- creates this table in 126; the fixture mirrors that shape so 142 applies
-- exactly as it would on a real upgraded database.
CREATE TABLE publication_states (
  publication_id TEXT PRIMARY KEY,
  job_id         TEXT,
  state          TEXT NOT NULL CHECK (state IN (
    'PENDING', 'WAITING_FOR_RENDER', 'ARTIFACT_BOUND', 'READY',
    'SCHEDULED', 'UPLOADING', 'VIDEO_CREATED', 'METADATA_APPLYING',
    'LOCALIZATIONS_APPLYING', 'VERIFYING', 'PUBLISHED', 'PARTIAL',
    'RETRY_WAIT', 'FAILED', 'CANCELLED'
  )),
  retry_from     TEXT CHECK (retry_from IS NULL OR retry_from IN (
    'WAITING_FOR_RENDER', 'ARTIFACT_BOUND', 'UPLOADING',
    'METADATA_APPLYING', 'LOCALIZATIONS_APPLYING', 'VERIFYING'
  )),
  artifact_id    TEXT,
  remote_id      TEXT,
  remote_url     TEXT,
  revision       INTEGER NOT NULL DEFAULT 0,
  last_error_code TEXT,
  created_at     TEXT NOT NULL,
  updated_at     TEXT NOT NULL
);
-- This fixture records migrations through 136 as applied below, so it must
-- also provide the pre-145 task_attempts table that migration 145 extends
-- (plain ADD COLUMN). The production chain creates this table in 041 and
-- extends it through 144; the fixture mirrors that shape so 145 applies
-- exactly as it would on a real upgraded database.
CREATE TABLE task_attempts (
  id                   TEXT PRIMARY KEY,
  task_id              TEXT NOT NULL,
  job_id               TEXT NOT NULL,
  attempt_number       INTEGER NOT NULL,
  worker_id            TEXT NOT NULL,
  worker_session_id    TEXT NOT NULL DEFAULT '',
  worker_snapshot_id   TEXT NOT NULL DEFAULT '',
  lease_id             TEXT NOT NULL,
  status               TEXT NOT NULL,
  started_at           TEXT,
  completed_at         TEXT,
  error_code           TEXT NOT NULL DEFAULT '',
  error_message        TEXT NOT NULL DEFAULT '',
  report_version       INTEGER NOT NULL DEFAULT 0,
  created_at           TEXT NOT NULL,
  updated_at           TEXT NOT NULL,
  git_sha              TEXT NOT NULL DEFAULT '',
  worker_version       TEXT NOT NULL DEFAULT '',
  engine_version       TEXT NOT NULL DEFAULT '',
  ffmpeg_version       TEXT NOT NULL DEFAULT '',
  config_hash          TEXT NOT NULL DEFAULT '',
  docker_image_digest  TEXT NOT NULL DEFAULT '',
  trace_id             TEXT NOT NULL DEFAULT '',
  span_id              TEXT NOT NULL DEFAULT ''
);
-- This fixture records migrations through 136 as applied below, so it must
-- also provide the pre-146 jobs table that migration 146 extends (plain
-- ADD COLUMN + index). The production chain creates this table in 001 and
-- ALTERs it through 145; the fixture mirrors a compatible shape so 146
-- applies exactly as it would on a real upgraded database.
CREATE TABLE jobs (
  job_id     TEXT PRIMARY KEY,
  status     TEXT,
  created_at TEXT,
  updated_at TEXT
);
-- Migration 156 upgrades these delivery tables in place.  Keep the legacy
-- shape in this sparse upgrade fixture so the full migration chain is
-- exercised just like a production database created before publication IDs.
CREATE TABLE delivery_destinations (
  destination_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  name TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  configuration_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE job_delivery_plans (
  job_id TEXT NOT NULL,
  destination_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  priority INTEGER NOT NULL DEFAULT 0,
  retry_budget INTEGER NOT NULL DEFAULT 5,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (job_id, destination_id)
);
CREATE TABLE job_deliveries (
  delivery_id TEXT PRIMARY KEY,
  artifact_id TEXT NOT NULL DEFAULT '',
  destination_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'PENDING',
  idempotency_key TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  next_attempt_at TEXT
);
-- This fixture records migrations through 136 as applied below, so it must
-- also provide the pre-151 deployment_records table that migration 151
-- extends (ALTER TABLE ADD COLUMN error_message + backfill into
-- worker_deployment_state). The production chain creates this table in 103
-- and makes previous_digest nullable in 134 (baselines without rollback
-- provenance); the fixture mirrors that shape so 151 applies exactly as it
-- would on a real upgraded database.
CREATE TABLE deployment_records (
  deployment_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  previous_digest TEXT CHECK (previous_digest IS NULL OR length(previous_digest) > 0),
  target_digest TEXT NOT NULL CHECK (length(target_digest) > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  applied_by TEXT NOT NULL,
  is_rollback INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);
-- This fixture records migrations through 136 as applied below, so it must
-- also provide the pre-154 creator_forwardings table that migration 154
-- extends (ALTER TABLE ADD COLUMN intake_source). The production chain
-- creates this table in 055 and extends it through 101; the fixture mirrors
-- that shape so 154 applies exactly as it would on a real upgraded database.
CREATE TABLE creator_forwardings (
  forwarding_id      TEXT PRIMARY KEY,
  source_provider    TEXT NOT NULL,
  source_job_id      TEXT NOT NULL,
  source_status      TEXT NOT NULL DEFAULT '',
  target_executor_id TEXT NOT NULL,
  target_job_id      TEXT,
  payload_json       TEXT NOT NULL DEFAULT '',
  payload_sha256     TEXT NOT NULL DEFAULT '',
  status             TEXT NOT NULL DEFAULT 'PENDING',
  attempt_count      INTEGER NOT NULL DEFAULT 0,
  next_attempt_at    TEXT NOT NULL DEFAULT '',
  locked_by          TEXT NOT NULL DEFAULT '',
  lease_id           TEXT NOT NULL DEFAULT '',
  lease_expires_at   TEXT NOT NULL DEFAULT '',
  last_error_code    TEXT NOT NULL DEFAULT '',
  last_error_message TEXT NOT NULL DEFAULT '',
  created_at         TEXT NOT NULL DEFAULT '',
  updated_at         TEXT NOT NULL DEFAULT '',
  forwarded_at       TEXT NOT NULL DEFAULT '',
  poll_attempts      INTEGER NOT NULL DEFAULT 0,
  next_poll_at       TEXT NOT NULL DEFAULT '',
  last_polled_at     TEXT NOT NULL DEFAULT '',
  last_remote_status TEXT NOT NULL DEFAULT '',
  last_error_class   TEXT NOT NULL DEFAULT '',
  external_client_id TEXT
);
`)
	require.NoError(t, err)
	seedMigrationHistory(t, legacy, 136)
	require.NoError(t, legacy.Close())

	migrated, err := store.NewSQLiteStoreFromPath(path, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, migrated.Close()) })

	columns := validationColumns(t, migrated)
	for _, column := range []string{"exec_start", "validated_at", "failure_reason"} {
		require.True(t, columns[column], "upgraded worker_validations missing %s", column)
	}

	var migrationVersion int
	err = migrated.DB().QueryRow(`SELECT version FROM schema_migrations WHERE version = 137`).Scan(&migrationVersion)
	require.NoError(t, err)
	require.Equal(t, 137, migrationVersion)

	// Migration 142 (publication submission identity) must apply on the
	// legacy-upgrade path too — this pins the ALTER TABLE against the
	// fixture-provided pre-142 publication_states shape.
	err = migrated.DB().QueryRow(`SELECT version FROM schema_migrations WHERE version = 142`).Scan(&migrationVersion)
	require.NoError(t, err)
	require.Equal(t, 142, migrationVersion)

	// Migration 145 (attempt render plan) must apply on the legacy-upgrade
	// path too — this pins the ALTER TABLE task_attempts against the
	// fixture-provided pre-145 task_attempts shape.
	err = migrated.DB().QueryRow(`SELECT version FROM schema_migrations WHERE version = 145`).Scan(&migrationVersion)
	require.NoError(t, err)
	require.Equal(t, 145, migrationVersion)

	// Migration 151 (worker deployment read model) must apply on the
	// legacy-upgrade path too — this pins the ALTER TABLE deployment_records
	// + worker_deployment_state backfill against the fixture-provided
	// pre-151 deployment_records shape.
	err = migrated.DB().QueryRow(`SELECT version FROM schema_migrations WHERE version = 151`).Scan(&migrationVersion)
	require.NoError(t, err)
	require.Equal(t, 151, migrationVersion)

	// Migration 154 (forwarding intake source) must apply on the
	// legacy-upgrade path too — this pins the ALTER TABLE creator_forwardings
	// against the fixture-provided pre-154 creator_forwardings shape.
	err = migrated.DB().QueryRow(`SELECT version FROM schema_migrations WHERE version = 154`).Scan(&migrationVersion)
	require.NoError(t, err)
	require.Equal(t, 154, migrationVersion)
}
