package store

import (
	"database/sql"
	"time"
)

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
