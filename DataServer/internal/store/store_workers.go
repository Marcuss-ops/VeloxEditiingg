package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// UpsertWorker creates or updates a worker record.
// Uses UPSERT (ON CONFLICT DO UPDATE) for idempotent writes.
func (s *SQLiteStore) UpsertWorker(raw []byte) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	workerID := asString(m["worker_id"])
	if workerID == "" {
		return fmt.Errorf("upsert worker: missing worker_id")
	}

	now := time.Now().UTC().Format(time.RFC3339)

	return s.upsertWorkerExec(s.db, m, raw, now)
}

type workerSQLExec interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// EnsureWorkerRecord creates the minimum worker snapshot required before a
// session can be persisted. Registration may issue the session token before
// the first heartbeat arrives; keeping this small bootstrap row here preserves
// the worker_sessions foreign-key/trigger invariant without inventing runtime
// state.
func (s *SQLiteStore) EnsureWorkerRecord(workerID string) error {
	if workerID == "" {
		return fmt.Errorf("ensure worker: missing worker_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`INSERT INTO workers
		(worker_id,worker_name,status,schedulable,drain,raw_json,migrated_at,node_id,node_role)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(worker_id) DO NOTHING`,
		workerID, workerID, "REGISTERING", 0, 0, `{}`, now, workerID, "worker")
	return err
}

func (s *SQLiteStore) upsertWorkerExec(exec workerSQLExec, m map[string]any, raw []byte, now string) error {
	workerID := asString(m["worker_id"])
	sched := boolInt(m["schedulable"])
	drain := boolInt(m["drain"])
	metrics, _ := m["metrics"].(map[string]any)
	metric := func(key string) any { return metrics[key] }
	_, err := exec.Exec(
		`INSERT INTO workers (
			worker_id, worker_name, status, last_heartbeat,
			schedulable, drain, worker_group,
			display_name, ip_address, first_seen, current_job,
			code_version, bundle_version, bundle_hash,
			protocol_version, engine_version,
			node_id, node_role, cluster_id, host_fingerprint, certificate_fingerprint,
			connection_status, connection_reason, session_active, current_task_id,
			active_task_count, task_slots, cpu_utilization_ratio, memory_used_bytes,
			disk_free_bytes, jobs_completed, jobs_failed, connected_at, last_heartbeat_at, updated_at,
			recent_logs, recent_errors, readiness, metrics, capabilities,
			raw_json, migrated_at
		) VALUES (
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET
			worker_name=excluded.worker_name,
			status=excluded.status,
			last_heartbeat=excluded.last_heartbeat,
			schedulable=excluded.schedulable,
			drain=excluded.drain,
			worker_group=excluded.worker_group,
			display_name=excluded.display_name,
			ip_address=excluded.ip_address,
			first_seen=COALESCE(NULLIF(excluded.first_seen, ''), workers.first_seen),
			current_job=excluded.current_job,
			code_version=excluded.code_version,
			bundle_version=excluded.bundle_version,
			bundle_hash=excluded.bundle_hash,
			protocol_version=excluded.protocol_version,
			engine_version=excluded.engine_version,
			node_id=excluded.node_id, node_role=excluded.node_role, cluster_id=excluded.cluster_id,
			host_fingerprint=excluded.host_fingerprint, certificate_fingerprint=excluded.certificate_fingerprint,
			connection_status=excluded.connection_status, connection_reason=excluded.connection_reason,
			session_active=excluded.session_active, current_task_id=excluded.current_task_id,
			active_task_count=excluded.active_task_count, task_slots=excluded.task_slots,
			cpu_utilization_ratio=excluded.cpu_utilization_ratio, memory_used_bytes=excluded.memory_used_bytes,
			disk_free_bytes=excluded.disk_free_bytes, jobs_completed=excluded.jobs_completed,
			jobs_failed=excluded.jobs_failed, connected_at=COALESCE(NULLIF(excluded.connected_at, ''), workers.connected_at),
			last_heartbeat_at=excluded.last_heartbeat_at, updated_at=excluded.updated_at,
			recent_logs=excluded.recent_logs,
			recent_errors=excluded.recent_errors,
			readiness=excluded.readiness,
			metrics=excluded.metrics,
			capabilities=excluded.capabilities,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		workerID,
		asString(m["worker_name"]), asString(m["status"]), asString(m["last_heartbeat"]),
		sched, drain, asString(m["worker_group"]),
		asString(m["display_name"]), asString(m["ip_address"]),
		asString(m["first_seen"]), asString(m["current_job"]),
		asString(m["code_version"]), asString(m["bundle_version"]),
		asString(m["bundle_hash"]), asString(m["protocol_version"]),
		asString(m["engine_version"]),
		asString(m["node_id"]), defaultString(m["node_role"], "worker"), asString(m["cluster_id"]),
		asString(m["host_fingerprint"]), asString(m["certificate_fingerprint"]),
		asString(m["connection_status"]), asString(m["connection_reason"]), boolInt(m["session_active"]),
		defaultString(m["current_task_id"], asString(m["current_job"])), workerActiveTaskCount(m, metric), int64OrDefault(m["task_slots"], int64OrDefault(metric("task_slots"), 1)),
		floatOrMetric(m["cpu_utilization_ratio"], metric("cpu_utilization_ratio")), int64OrDefault(m["memory_used_bytes"], int64Value(metric("memory_used_bytes"))), int64OrDefault(m["disk_free_bytes"], int64Value(metric("disk_free_bytes"))),
		int64Value(m["jobs_completed"]), int64Value(m["jobs_failed"]), asString(m["connected_at"]),
		defaultString(m["last_heartbeat_at"], asString(m["last_heartbeat"])), now,
		jsonString(m["recent_logs"]), jsonString(m["recent_errors"]),
		jsonString(m["readiness"]), jsonString(m["metrics"]), jsonString(m["capabilities"]),
		string(raw), now,
	)
	return err
}

func workerActiveTaskCount(m map[string]any, metric func(string) any) int64 {
	if n := int64Value(m["active_task_count"]); n != 0 {
		return n
	}
	if n := int64Value(metric("active_task_count")); n != 0 {
		return n
	}
	if n := int64Value(metric("active_jobs_count")); n != 0 {
		return n
	}
	if n := int64Value(metric("active_tasks")); n != 0 {
		return n
	}
	metrics, _ := m["metrics"].(map[string]any)
	if items, ok := metrics["active_jobs"].([]any); ok {
		return int64(len(items))
	}
	return 0
}

// GetWorker returns a single worker as a map by ID.
func (s *SQLiteStore) GetWorker(workerID string) (map[string]any, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM workers WHERE worker_id = ?`, workerID).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// DeleteWorker removes a worker record.
func (s *SQLiteStore) DeleteWorker(workerID string) error {
	_, err := s.db.Exec(`DELETE FROM workers WHERE worker_id = ?`, workerID)
	return err
}

// ListWorkers returns all workers as raw JSON maps, ordered by last heartbeat descending.
func (s *SQLiteStore) ListWorkers() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT raw_json FROM workers ORDER BY last_heartbeat DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

// SetWorkerRevoked sets the revoked flag for a worker in worker_flags.
//
// SECURITY / SHAPE CONTRACT — read before "harmonizing" with workers.raw_json:
// The `raw_json` blob here is INTENTIONALLY a separate three-key audit
// shape ({worker_id, revoked, updated_at}), NOT a WorkerInfo copy. WorkerInfo
// carries read-time-hydrated fields (SessionActive, ConnectionStatus) that
// must NEVER be persisted (see workers.ScrubForPersist) — adding them to
// this blob would reintroduce the persistence-leak class fixed by that
// helper, but without a matching read-time hydrator on this side (there is
// none, and none should exist). The shape is locked by
// TestSetWorkerRevoked_RawJsonShapeContract below. If a future change needs
// structured flag metadata beyond the three-key blob, add explicit columns
// to worker_flags — keep raw_json as the audit map it is today.
func (s *SQLiteStore) SetWorkerRevoked(workerID string, revoked bool) error {
	revInt := 0
	if revoked {
		revInt = 1
	}
	now := time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]any{
		"worker_id":  workerID,
		"revoked":    revoked,
		"updated_at": now,
	})
	_, err := s.db.Exec(
		`INSERT INTO worker_flags (worker_id, revoked, quarantined, raw_json, migrated_at)
		 VALUES (?, ?, 0, ?, ?)
		 ON CONFLICT(worker_id) DO UPDATE SET
			revoked=excluded.revoked,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		workerID, revInt, string(raw), now,
	)
	return err
}

// GetRevokedWorkers returns the list of all revoked worker IDs.
func (s *SQLiteStore) GetRevokedWorkers() ([]string, error) {
	rows, err := s.db.Query(`SELECT worker_id FROM worker_flags WHERE revoked = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ReplaceWorkers has been removed. Use individual UpsertWorker + SetWorkerRevoked instead.
// This was a bulk DELETE+re-insert approach that caused unnecessary write amplification.

// jsonString serializes a value to JSON string, or returns "{}"/"[]" for nil.
func jsonString(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// WorkerValidationStatus represents the validation status of a worker
type WorkerValidationStatus struct {
	WorkerID       string    `json:"worker_id"`
	ValidationCode string    `json:"validation_code"`
	CanonicalUnit  string    `json:"canonical_unit"`
	ExecStart      string    `json:"exec_start"`
	ValidatedAt    time.Time `json:"validated_at"`
	FailureReason  string    `json:"failure_reason,omitempty"`
}

// CreateValidationTableIfNotExists creates the worker_validations table
func (s *SQLiteStore) CreateValidationTableIfNotExists() error {
	ddl := `
CREATE TABLE IF NOT EXISTS worker_validations (
  worker_id TEXT PRIMARY KEY,
  validation_code TEXT NOT NULL,
  canonical_unit TEXT,
  exec_start TEXT,
  validated_at TEXT,
  failure_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_worker_validations_code ON worker_validations(validation_code);
`
	_, err := s.db.Exec(ddl)
	return err
}

// SaveWorkerValidation saves or updates a worker's validation status
func (s *SQLiteStore) SaveWorkerValidation(workerID, validationCode, canonicalUnit, execStart string, validatedAt time.Time, failureReason string) error {
	validatedAtStr := validatedAt.UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO worker_validations (worker_id, validation_code, canonical_unit, exec_start, validated_at, failure_reason)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(worker_id) DO UPDATE SET
		   validation_code=excluded.validation_code,
		   canonical_unit=excluded.canonical_unit,
		   exec_start=excluded.exec_start,
		   validated_at=excluded.validated_at,
		   failure_reason=excluded.failure_reason`,
		workerID, validationCode, canonicalUnit, execStart, validatedAtStr, failureReason,
	)
	return err
}

// GetWorkerValidation retrieves the validation status for a worker
func (s *SQLiteStore) GetWorkerValidation(workerID string) (*WorkerValidationStatus, error) {
	row := s.db.QueryRow(
		`SELECT worker_id, validation_code, canonical_unit, exec_start, validated_at, failure_reason
		 FROM worker_validations WHERE worker_id = ?`,
		workerID,
	)
	var status WorkerValidationStatus
	var validatedAtStr string
	err := row.Scan(
		&status.WorkerID, &status.ValidationCode, &status.CanonicalUnit,
		&status.ExecStart, &validatedAtStr, &status.FailureReason,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if validatedAtStr != "" {
		status.ValidatedAt, _ = time.Parse(time.RFC3339, validatedAtStr)
	}
	return &status, nil
}

// GetAllWorkerValidations returns all worker validation statuses
func (s *SQLiteStore) GetAllWorkerValidations() ([]map[string]any, error) {
	rows, err := s.db.Query(
		`SELECT worker_id, validation_code, canonical_unit, exec_start, validated_at, failure_reason
		 FROM worker_validations ORDER BY validated_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var workerID, code, unit, execStart, validatedAt, failureReason string
		if err := rows.Scan(&workerID, &code, &unit, &execStart, &validatedAt, &failureReason); err != nil {
			continue
		}
		out = append(out, map[string]any{
			"worker_id":       workerID,
			"validation_code": code,
			"canonical_unit":  unit,
			"exec_start":      execStart,
			"validated_at":    validatedAt,
			"failure_reason":  failureReason,
			"valid":           code == "PASS",
		})
	}
	return out, nil
}

// WorkersRepository defines the interface for worker persistence.
// The Registry uses this as its single source of truth — no JSON fallback.
type WorkersRepository interface {
	// ListWorkers returns all workers as raw JSON maps.
	ListWorkers() ([]map[string]any, error)
	// GetWorker returns a single worker by ID.
	GetWorker(workerID string) (map[string]any, error)
	// UpsertWorker creates or updates a worker from its raw JSON representation.
	UpsertWorker(raw []byte) error
	// DeleteWorker removes a worker from the active set.
	DeleteWorker(workerID string) error
	// SetRevoked marks a worker as revoked or unrevoked.
	SetRevoked(workerID string, revoked bool) error
	// GetRevoked returns the list of revoked worker IDs.
	GetRevoked() ([]string, error)
}

type SQLiteWorkersRepository struct {
	store *SQLiteStore
}

func NewSQLiteWorkersRepository(store *SQLiteStore) *SQLiteWorkersRepository {
	return &SQLiteWorkersRepository{store: store}
}

func (r *SQLiteWorkersRepository) ListWorkers() ([]map[string]any, error) {
	return r.store.ListWorkers()
}

func (r *SQLiteWorkersRepository) GetWorker(workerID string) (map[string]any, error) {
	return r.store.GetWorker(workerID)
}

func (r *SQLiteWorkersRepository) UpsertWorker(raw []byte) error {
	return r.store.UpsertWorker(raw)
}

func (r *SQLiteWorkersRepository) DeleteWorker(workerID string) error {
	return r.store.DeleteWorker(workerID)
}

func (r *SQLiteWorkersRepository) SetRevoked(workerID string, revoked bool) error {
	return r.store.SetWorkerRevoked(workerID, revoked)
}

func (r *SQLiteWorkersRepository) GetRevoked() ([]string, error) {
	return r.store.GetRevokedWorkers()
}
