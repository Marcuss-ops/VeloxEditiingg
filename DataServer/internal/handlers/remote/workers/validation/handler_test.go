package validation

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"velox-server/internal/config"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/store"
	"velox-server/internal/store/migrations"
	workersreg "velox-server/internal/workers"
)

type fakeValidationRepository struct {
	mu             sync.Mutex
	saveErr        error
	getErr         error
	listErr        error
	status         *ValidationStatus
	all            []map[string]any
	saved          *ValidationReport
	savedWorkerIDs map[string]struct{}
	saveCalls      int
	getCalls       []string
}

func (f *fakeValidationRepository) SaveValidation(report *ValidationReport) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.saveCalls++
	if f.saveErr != nil {
		return f.saveErr
	}
	copy := *report
	f.saved = &copy
	if f.savedWorkerIDs == nil {
		f.savedWorkerIDs = make(map[string]struct{})
	}
	f.savedWorkerIDs[report.WorkerID] = struct{}{}
	return nil
}

func (f *fakeValidationRepository) GetValidation(workerID string) (*ValidationStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getCalls = append(f.getCalls, workerID)
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.status == nil {
		return nil, nil
	}
	status := *f.status
	return &status, nil
}

func (f *fakeValidationRepository) GetAllValidations() ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.all, nil
}

func (f *fakeValidationRepository) callCounts() (saveCalls, getCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveCalls, len(f.getCalls)
}

func (f *fakeValidationRepository) savedIDs() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	ids := make(map[string]struct{}, len(f.savedWorkerIDs))
	for workerID := range f.savedWorkerIDs {
		ids[workerID] = struct{}{}
	}
	return ids
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
	return payload
}

func newValidationRequest(t *testing.T, method, routePath, requestPath string, body string, handler gin.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	router := gin.New()
	if method == http.MethodGet {
		router.GET(routePath, handler)
	} else {
		router.POST(routePath, handler)
	}

	req := httptest.NewRequest(method, requestPath, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func TestInjectedHandlersAreIsolated(t *testing.T) {
	t.Parallel()

	first := &fakeValidationRepository{}
	second := &fakeValidationRepository{}

	firstHandler := NewHandler(first)
	secondHandler := NewHandler(second)

	firstResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, firstHandler.HandleValidationReport())
	secondResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-b","validation_code":"MISSING_UNIT"}`, secondHandler.HandleValidationReport())

	require.Equal(t, http.StatusOK, firstResponse.Code)
	require.Equal(t, http.StatusOK, secondResponse.Code)
	firstPayload := decodeResponse(t, firstResponse)
	secondPayload := decodeResponse(t, secondResponse)
	require.Equal(t, true, firstPayload["ok"])
	require.Equal(t, true, firstPayload["valid"])
	require.Equal(t, "PASS", firstPayload["code"])
	require.Equal(t, false, secondPayload["valid"])
	require.Equal(t, "MISSING_UNIT", secondPayload["code"])
	require.NotNil(t, first.saved)
	require.NotNil(t, second.saved)
	require.Equal(t, "worker-a", first.saved.WorkerID)
	require.Equal(t, "worker-b", second.saved.WorkerID)
	require.Equal(t, "PASS", first.saved.ValidationCode)
	require.Equal(t, "MISSING_UNIT", second.saved.ValidationCode)
}

func TestValidationHandlersHandleConcurrentPOSTRequests(t *testing.T) {
	t.Parallel()

	const requestCount = 32
	repository := &fakeValidationRepository{}
	handler := NewHandler(repository)
	responses := make(chan *httptest.ResponseRecorder, requestCount)

	var wg sync.WaitGroup
	wg.Add(requestCount)
	for i := 0; i < requestCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		go func() {
			defer wg.Done()
			responses <- newValidationRequest(
				t,
				http.MethodPost,
				"/api/workers/validation",
				"/api/workers/validation",
				fmt.Sprintf(`{"worker_id":%q,"validation_code":"PASS"}`, workerID),
				handler.HandleValidationReport(),
			)
		}()
	}
	wg.Wait()
	close(responses)

	for response := range responses {
		require.Equal(t, http.StatusOK, response.Code)
		payload := decodeResponse(t, response)
		require.Equal(t, true, payload["ok"])
		require.Equal(t, true, payload["valid"])
		require.Equal(t, "PASS", payload["code"])
	}

	saveCalls, getCalls := repository.callCounts()
	require.Equal(t, requestCount, saveCalls)
	require.Zero(t, getCalls)

	savedIDs := repository.savedIDs()
	require.Len(t, savedIDs, requestCount)
	for i := 0; i < requestCount; i++ {
		_, ok := savedIDs[fmt.Sprintf("worker-%d", i)]
		require.True(t, ok, "worker-%d was not persisted", i)
	}
}

func TestHandleValidationReportPreservesHTTPContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "invalid json", body: "{", code: http.StatusBadRequest},
		{name: "missing worker", body: `{"validation_code":"PASS"}`, code: http.StatusBadRequest},
		{name: "missing code", body: `{"worker_id":"worker-a"}`, code: http.StatusBadRequest},
		{name: "valid report", body: `{"worker_id":"worker-a","validation_code":"PASS"}`, code: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			response := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", tt.body, NewHandler(&fakeValidationRepository{}).HandleValidationReport())
			require.Equal(t, tt.code, response.Code)
		})
	}
}

func TestHandleValidationReportRejectsMismatchedAuthenticatedWorker(t *testing.T) {
	t.Parallel()

	repository := &fakeValidationRepository{}
	router := gin.New()
	router.POST("/api/workers/validation", func(c *gin.Context) {
		c.Set("authenticated_worker_id", "worker-authenticated")
		NewHandler(repository).HandleValidationReport()(c)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/workers/validation", strings.NewReader(`{"worker_id":"worker-other","validation_code":"PASS"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusForbidden, response.Code)
	require.Empty(t, repository.savedIDs())
}

func TestValidationRouteBindsRealWorkerTokenIdentity(t *testing.T) {
	t.Parallel()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "auth-validation.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	tokenManager := workersreg.NewTokenManager(db)
	token := tokenManager.GenerateToken("worker-authenticated")
	repository := &fakeValidationRepository{}
	router := gin.New()
	router.POST("/api/v1/agent/validation", api.WorkerOrAdminAuthMiddleware(&config.Config{}, tokenManager), NewHandler(repository).HandleValidationReport())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/validation", strings.NewReader(`{"worker_id":"worker-other","validation_code":"PASS"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Empty(t, repository.savedIDs())

	request = httptest.NewRequest(http.MethodPost, "/api/v1/agent/validation", strings.NewReader(`{"worker_id":"worker-authenticated","validation_code":"PASS"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
}

func TestAdminValidationRouteAllowsExplicitRemediation(t *testing.T) {
	t.Parallel()

	repository := &fakeValidationRepository{}
	cfg := &config.Config{Auth: config.AuthConfig{AdminToken: "admin-token"}}
	router := gin.New()
	router.POST("/api/v1/agent/validation", api.WorkerOrAdminAuthMiddleware(cfg, nil), NewHandler(repository).HandleValidationReport())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/validation", strings.NewReader(`{"worker_id":"worker-other","validation_code":"PASS"}`))
	request.Header.Set("Authorization", "Bearer admin-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, map[string]struct{}{"worker-other": {}}, repository.savedIDs())
}

func TestHandleValidationReportReturns500OnRepositorySaveError(t *testing.T) {
	t.Parallel()

	response := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, NewHandler(&fakeValidationRepository{
		saveErr: errors.New("write failed"),
	}).HandleValidationReport())

	payload := decodeResponse(t, response)
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Equal(t, false, payload["ok"])
	require.Equal(t, "failed to persist validation report", payload["error"])
}

func TestGetWorkerValidationHandlerReturnsValidationResults(t *testing.T) {
	t.Parallel()

	notValidated := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-b/validation", "", NewHandler(&fakeValidationRepository{}).GetWorkerValidationHandler())
	require.Equal(t, http.StatusOK, notValidated.Code)
	notValidatedPayload := decodeResponse(t, notValidated)
	require.Equal(t, "worker-b", notValidatedPayload["worker_id"])
	require.Equal(t, false, notValidatedPayload["valid"])
	require.Equal(t, "NOT_VALIDATED", notValidatedPayload["code"])
	require.Equal(t, "Worker has not been validated yet", notValidatedPayload["message"])

	validatedAt := time.Date(2026, time.August, 10, 0, 0, 0, 0, time.UTC)
	validated := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-c/validation", "", NewHandler(&fakeValidationRepository{
		status: &ValidationStatus{
			WorkerID:       "worker-c",
			ValidationCode: "PASS",
			CanonicalUnit:  "velox-worker.service",
			ExecStart:      "/usr/bin/velox-worker",
			ValidatedAt:    validatedAt,
		},
	}).GetWorkerValidationHandler())
	require.Equal(t, http.StatusOK, validated.Code)
	validatedPayload := decodeResponse(t, validated)
	require.Equal(t, "worker-c", validatedPayload["worker_id"])
	require.Equal(t, true, validatedPayload["valid"])
	require.Equal(t, "PASS", validatedPayload["code"])
	require.Equal(t, "velox-worker.service", validatedPayload["canonical_unit"])
	require.Equal(t, "/usr/bin/velox-worker", validatedPayload["exec_start"])
}

func TestGetWorkerValidationHandlerReturnsRepositoryError(t *testing.T) {
	t.Parallel()

	response := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-a/validation", "", NewHandler(&fakeValidationRepository{
		getErr: errors.New("repository unavailable"),
	}).GetWorkerValidationHandler())

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Contains(t, response.Body.String(), "repository unavailable")
}

func TestGetAllValidationsHandlerUsesInjectedRepository(t *testing.T) {
	t.Parallel()

	repository := &fakeValidationRepository{
		all: []map[string]any{{"worker_id": "worker-a", "validation_code": "PASS"}},
	}
	response := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", NewHandler(repository).GetAllValidationsHandler())

	require.Equal(t, http.StatusOK, response.Code)
	payload := decodeResponse(t, response)
	require.Equal(t, true, payload["ok"])
	require.Len(t, payload["validations"], 1)

	errorResponse := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", NewHandler(&fakeValidationRepository{
		listErr: errors.New("list failed"),
	}).GetAllValidationsHandler())
	require.Equal(t, http.StatusInternalServerError, errorResponse.Code)
	require.Contains(t, errorResponse.Body.String(), "list failed")
}

func TestHandlerRejectsMissingRepository(t *testing.T) {
	t.Parallel()

	post := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, NewHandler(nil).HandleValidationReport())
	require.Equal(t, http.StatusServiceUnavailable, post.Code)

	get := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-a/validation", "", NewHandler(nil).GetWorkerValidationHandler())
	require.Equal(t, http.StatusServiceUnavailable, get.Code)

	list := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", NewHandler(nil).GetAllValidationsHandler())
	require.Equal(t, http.StatusServiceUnavailable, list.Code)

}

// newValidationTestStore deliberately starts with migrations disabled so the
// test exercises the store bootstrap DDL directly.
func newValidationTestStore(t *testing.T) *store.SQLiteStore {
	t.Helper()

	db, err := store.NewSQLiteStoreFromPath(filepath.Join(t.TempDir(), "validation.db"), false)
	require.NoError(t, err)
	require.NoError(t, db.CreateValidationTableIfNotExists())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func newMigratedValidationStore(t *testing.T) *store.SQLiteStore {
	t.Helper()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "validation.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

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

func validationColumns(t *testing.T, db *store.SQLiteStore) map[string]bool {
	t.Helper()

	rows, err := db.DB().Query(`PRAGMA table_info(worker_validations)`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })

	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		columns[name] = true
	}
	require.NoError(t, rows.Err())
	return columns
}

func TestMigratedValidationSchemaMatchesRepository(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)
	columns := validationColumns(t, db)
	for _, column := range []string{"worker_id", "validation_code", "canonical_unit", "exec_start", "validated_at", "failure_reason", "created_at", "updated_at"} {
		require.True(t, columns[column], "worker_validations missing %s", column)
	}
}

func seedMigrationHistory(t *testing.T, db *store.SQLiteStore, through int) {
	t.Helper()

	_, err := db.DB().Exec(`CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		checksum TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`)
	require.NoError(t, err)

	entries, err := fs.ReadDir(migrations.SQLiteMigrationsFS(), "sqlite")
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || strings.HasSuffix(entry.Name(), ".down.sql") {
			continue
		}
		var version int
		_, err := fmt.Sscanf(entry.Name(), "%d_", &version)
		require.NoError(t, err)
		if version > through {
			continue
		}
		contents, err := fs.ReadFile(migrations.SQLiteMigrationsFS(), "sqlite/"+entry.Name())
		require.NoError(t, err)
		checksum := fmt.Sprintf("%x", sha256.Sum256(contents))
		_, err = db.DB().Exec(`INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`, version, entry.Name(), checksum, "2026-08-10T00:00:00Z")
		require.NoError(t, err)
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

func TestValidationStoreHandlesConcurrentUpserts(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)
	repository := NewValidationStore(db)
	const writeCount = 24
	var wg sync.WaitGroup
	errorsCh := make(chan error, writeCount)
	wg.Add(writeCount)
	for i := 0; i < writeCount; i++ {
		code := "PASS"
		if i%2 == 0 {
			code = "MISSING_UNIT"
		}
		go func(code string) {
			defer wg.Done()
			errorsCh <- repository.SaveValidation(&ValidationReport{
				WorkerID:       "worker-concurrent",
				ValidationCode: code,
				Timestamp:      "2026-08-10T00:00:00Z",
			})
		}(code)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}

	status, err := repository.GetValidation("worker-concurrent")
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Contains(t, []string{"PASS", "MISSING_UNIT"}, status.ValidationCode)
}

func TestValidationStorePersistsPASSAndFailure(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)

	repository := NewValidationStore(db)
	handler := NewHandler(repository)

	passResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-pass","validation_code":"PASS","timestamp":"2026-08-10T00:00:00Z"}`, handler.HandleValidationReport())
	require.Equal(t, http.StatusOK, passResponse.Code)
	passStatus, err := repository.GetValidation("worker-pass")
	require.NoError(t, err)
	require.NotNil(t, passStatus)
	require.Equal(t, "PASS", passStatus.ValidationCode)
	require.Empty(t, passStatus.FailureReason)

	failureResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-failure","validation_code":"MISSING_UNIT","timestamp":"2026-08-10T00:00:00Z"}`, handler.HandleValidationReport())
	require.Equal(t, http.StatusOK, failureResponse.Code)
	failureStatus, err := repository.GetValidation("worker-failure")
	require.NoError(t, err)
	require.NotNil(t, failureStatus)
	require.Equal(t, "MISSING_UNIT", failureStatus.ValidationCode)
	require.Equal(t, "Canonical unit does not exist", failureStatus.FailureReason)
}

func TestUnknownWorkerReturnsNotValidated(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)

	response := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/never-seen/validation", "", NewHandler(NewValidationStore(db)).GetWorkerValidationHandler())
	payload := decodeResponse(t, response)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "never-seen", payload["worker_id"])
	require.Equal(t, false, payload["valid"])
	require.Equal(t, "NOT_VALIDATED", payload["code"])
}

func TestValidationHandlersReturnSQLiteErrors(t *testing.T) {
	t.Parallel()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "validation.db"))
	require.NoError(t, err)
	repository := NewValidationStore(db)
	handler := NewHandler(repository)
	require.NoError(t, db.Close())

	postResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-closed","validation_code":"PASS"}`, handler.HandleValidationReport())
	postPayload := decodeResponse(t, postResponse)
	require.Equal(t, http.StatusInternalServerError, postResponse.Code)
	require.Equal(t, false, postPayload["ok"])
	require.Equal(t, "failed to persist validation report", postPayload["error"])

	getResponse := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-closed/validation", "", handler.GetWorkerValidationHandler())
	require.Equal(t, http.StatusInternalServerError, getResponse.Code)
	require.Contains(t, getResponse.Body.String(), "database is closed")

	listResponse := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", handler.GetAllValidationsHandler())
	require.Equal(t, http.StatusInternalServerError, listResponse.Code)
	require.Contains(t, listResponse.Body.String(), "database is closed")

}

func TestValidationHandlersHandleConcurrentGET(t *testing.T) {
	t.Parallel()

	const requestCount = 32
	repository := &fakeValidationRepository{status: &ValidationStatus{
		WorkerID:       "worker-a",
		ValidationCode: "PASS",
	}}
	handler := NewHandler(repository)

	var wg sync.WaitGroup
	wg.Add(requestCount)
	responses := make(chan *httptest.ResponseRecorder, requestCount)
	for i := 0; i < requestCount; i++ {
		go func() {
			defer wg.Done()
			responses <- newValidationRequest(
				t,
				http.MethodGet,
				"/api/workers/:id/validation",
				"/api/workers/worker-a/validation",
				"",
				handler.GetWorkerValidationHandler(),
			)
		}()
	}
	wg.Wait()
	close(responses)

	for response := range responses {
		require.Equal(t, http.StatusOK, response.Code)
		payload := decodeResponse(t, response)
		require.Equal(t, true, payload["valid"])
		require.Equal(t, "PASS", payload["code"])
	}
	saveCalls, getCalls := repository.callCounts()
	require.Zero(t, saveCalls)
	require.Equal(t, requestCount, getCalls)

}

func TestValidationStoreTypedNilRepositoryFailsClosed(t *testing.T) {
	t.Parallel()

	var nilStore *ValidationStore
	var repository ValidationRepository = nilStore
	handler := NewHandler(repository)

	post := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, handler.HandleValidationReport())
	require.Equal(t, http.StatusInternalServerError, post.Code)

	get := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-a/validation", "", handler.GetWorkerValidationHandler())
	require.Equal(t, http.StatusInternalServerError, get.Code)

	list := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", handler.GetAllValidationsHandler())
	require.Equal(t, http.StatusInternalServerError, list.Code)

}
