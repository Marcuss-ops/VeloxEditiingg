package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ── Delivery Target ──

// DeliveryTarget represents a resolved delivery destination created at enqueue time.
type DeliveryTarget struct {
	ID            int    `json:"id"`
	JobID         string `json:"job_id"`
	TargetType    string `json:"target_type"`
	Status        string `json:"status"`
	Config        string `json:"config"`
	Result        string `json:"result"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
	AttemptCount  int    `json:"attempt_count"`
	LastAttemptAt string `json:"last_attempt_at,omitempty"`
}// DeliveryTargetConfig holds the resolved parameters for a delivery target.
type DeliveryTargetConfig struct {
	// YouTube fields
	ChannelID   string `json:"channel_id,omitempty"`
	ChannelName string `json:"channel_name,omitempty"`
	GroupName   string `json:"group_name,omitempty"`   // e.g. "amish"
	Language    string `json:"language,omitempty"`      // e.g. "it", "en"
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Privacy     string `json:"privacy,omitempty"`
	JobRunID    string `json:"job_run_id,omitempty"`

	// Drive fields
	FolderID   string `json:"folder_id,omitempty"`
	FolderName string `json:"folder_name,omitempty"`
	Subfolder  string `json:"subfolder,omitempty"`
	VideoName  string `json:"video_name,omitempty"`
	ProjectID  string `json:"project_id,omitempty"`
}

// DeliveryTargetResult holds the outcome of a delivery attempt.
type DeliveryTargetResult struct {
	Success    bool   `json:"success"`
	URL        string `json:"url,omitempty"`
	VideoID    string `json:"video_id,omitempty"`
	WebViewLink string `json:"web_view_link,omitempty"`
	FolderLink  string `json:"folder_link,omitempty"`
	Error      string `json:"error,omitempty"`
}

// InsertDeliveryTarget creates a new delivery target.
func (s *SQLiteStore) InsertDeliveryTarget(target *DeliveryTarget) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	if target.CreatedAt == "" {
		target.CreatedAt = now
	}
	if target.UpdatedAt == "" {
		target.UpdatedAt = now
	}
	if target.Status == "" {
		target.Status = "pending"
	}
	if target.Config == "" {
		target.Config = "{}"
	}
	if target.Result == "" {
		target.Result = "{}"
	}

	result, err := s.db.Exec(
		`INSERT INTO delivery_targets (job_id, target_type, status, config, result, created_at, updated_at, attempt_count, last_attempt_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		target.JobID, target.TargetType, target.Status, target.Config, target.Result,
		target.CreatedAt, target.UpdatedAt, target.AttemptCount, toNullString(target.LastAttemptAt),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// GetDeliveryTargetsByJob returns all delivery targets for a job.
func (s *SQLiteStore) GetDeliveryTargetsByJob(jobID string) ([]DeliveryTarget, error) {
	rows, err := s.db.Query(
		`SELECT id, job_id, target_type, status, config, result,
		        created_at, updated_at, attempt_count, COALESCE(last_attempt_at, '')
		 FROM delivery_targets WHERE job_id = ? ORDER BY id`,
		jobID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []DeliveryTarget
	for rows.Next() {
		var t DeliveryTarget
		if err := rows.Scan(&t.ID, &t.JobID, &t.TargetType, &t.Status,
			&t.Config, &t.Result, &t.CreatedAt, &t.UpdatedAt,
			&t.AttemptCount, &t.LastAttemptAt); err != nil {
			continue
		}
		targets = append(targets, t)
	}
	return targets, rows.Err()
}

// GetDeliveryTarget returns a single delivery target by ID.
func (s *SQLiteStore) GetDeliveryTarget(id int) (*DeliveryTarget, error) {
	row := s.db.QueryRow(
		`SELECT id, job_id, target_type, status, config, result,
		        created_at, updated_at, attempt_count, COALESCE(last_attempt_at, '')
		 FROM delivery_targets WHERE id = ?`, id,
	)
	var t DeliveryTarget
	err := row.Scan(&t.ID, &t.JobID, &t.TargetType, &t.Status,
		&t.Config, &t.Result, &t.CreatedAt, &t.UpdatedAt,
		&t.AttemptCount, &t.LastAttemptAt)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// UpdateDeliveryTargetResult updates the status and result of a delivery target.
func (s *SQLiteStore) UpdateDeliveryTargetResult(id int, status, resultJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE delivery_targets
		 SET status = ?, result = ?, updated_at = ?, attempt_count = attempt_count + 1, last_attempt_at = ?
		 WHERE id = ?`,
		status, resultJSON, now, now, id,
	)
	return err
}

// ── Delivery Attempt ──

// DeliveryAttempt records a single delivery attempt.
type DeliveryAttempt struct {
	ID               int    `json:"id"`
	DeliveryTargetID int    `json:"delivery_target_id"`
	AttemptNumber    int    `json:"attempt_number"`
	Status           string `json:"status"`
	Result           string `json:"result"`
	StartedAt        string `json:"started_at,omitempty"`
	CompletedAt      string `json:"completed_at,omitempty"`
	ErrorMessage     string `json:"error_message,omitempty"`
	WorkerID         string `json:"worker_id,omitempty"`
}

// InsertDeliveryAttempt creates a new delivery attempt record.
func (s *SQLiteStore) InsertDeliveryAttempt(attempt *DeliveryAttempt) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	if attempt.StartedAt == "" {
		attempt.StartedAt = now
	}
	if attempt.Status == "" {
		attempt.Status = "scheduled"
	}
	if attempt.Result == "" {
		attempt.Result = "{}"
	}

	result, err := s.db.Exec(
		`INSERT INTO delivery_attempts (delivery_target_id, attempt_number, status, result, started_at, completed_at, error_message, worker_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		attempt.DeliveryTargetID, attempt.AttemptNumber, attempt.Status, attempt.Result,
		attempt.StartedAt, toNullString(attempt.CompletedAt),
		toNullString(attempt.ErrorMessage), toNullString(attempt.WorkerID),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// UpdateDeliveryAttempt updates a delivery attempt with completion data.
func (s *SQLiteStore) UpdateDeliveryAttempt(id int, status, resultJSON, errorMsg string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE delivery_attempts
		 SET status = ?, result = ?, completed_at = ?, error_message = ?
		 WHERE id = ?`,
		status, resultJSON, now, toNullString(errorMsg), id,
	)
	return err
}

// GetDeliveryAttemptsByTarget returns all attempts for a delivery target.
func (s *SQLiteStore) GetDeliveryAttemptsByTarget(targetID int) ([]DeliveryAttempt, error) {
	rows, err := s.db.Query(
		`SELECT id, delivery_target_id, attempt_number, status, result,
		        COALESCE(started_at, ''), COALESCE(completed_at, ''),
		        COALESCE(error_message, ''), COALESCE(worker_id, '')
		 FROM delivery_attempts WHERE delivery_target_id = ? ORDER BY attempt_number DESC`,
		targetID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []DeliveryAttempt
	for rows.Next() {
		var a DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.DeliveryTargetID, &a.AttemptNumber, &a.Status,
			&a.Result, &a.StartedAt, &a.CompletedAt, &a.ErrorMessage, &a.WorkerID); err != nil {
			continue
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

// ── Helpers ──

func toNullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// ParseTargetConfig parses a delivery target's config JSON into a typed struct.
func ParseTargetConfig(configJSON string) (*DeliveryTargetConfig, error) {
	if configJSON == "" {
		return &DeliveryTargetConfig{}, nil
	}
	var cfg DeliveryTargetConfig
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, fmt.Errorf("invalid target config: %w", err)
	}
	return &cfg, nil
}

// MustTargetConfigJSON marshals a config to JSON, panicking on error.
func MustTargetConfigJSON(cfg *DeliveryTargetConfig) string {
	if cfg == nil {
		return "{}"
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// MustTargetResultJSON marshals a result to JSON, panicking on error.
func MustTargetResultJSON(result *DeliveryTargetResult) string {
	if result == nil {
		return "{}"
	}
	b, err := json.Marshal(result)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ── CAS Job Transition ──

// TransitionJobStatus atomically transitions a job from expectedStatus to newStatus
// using optimistic locking via the revision column.
// Returns (newRevision, error). Error is non-nil if the transition fails.
func (s *SQLiteStore) TransitionJobStatus(ctx context.Context, jobID string, expectedStatus, newStatus string, revision int) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	newRevision := revision + 1

	result, err := s.db.Exec(
		`UPDATE jobs
		 SET status = ?, revision = ?, updated_at = ?
		 WHERE job_id = ? AND status = ? AND revision = ?`,
		newStatus, newRevision, now, jobID, expectedStatus, revision,
	)
	if err != nil {
		return 0, fmt.Errorf("transition exec: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("transition rows affected: %w", err)
	}
	if affected == 0 {
		return 0, fmt.Errorf("transition conflict: job %s not in status %s with revision %d", jobID, expectedStatus, revision)
	}

	return newRevision, nil
}

// UpdateJobSupplementary updates non-CAS fields on a job after a successful transition.
func (s *SQLiteStore) UpdateJobSupplementary(jobID string, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}

	setClauses := []string{}
	args := []interface{}{}

	for key, value := range fields {
		switch key {
		case "completed_at", "last_error", "error_message", "failed_at", "failed_by",
			"lease_id", "lease_expiry", "assigned_to", "claimed_by":
			setClauses = append(setClauses, key+" = ?")
			args = append(args, value)
		}
	}

	if len(setClauses) == 0 {
		return nil
	}

	query := "UPDATE jobs SET "
	for i, clause := range setClauses {
		if i > 0 {
			query += ", "
		}
		query += clause
	}
	query += " WHERE job_id = ?"
	args = append(args, jobID)

	_, err := s.db.Exec(query, args...)
	return err
}

// GetJobRevision returns the current revision of a job.
func (s *SQLiteStore) GetJobRevision(jobID string) (int, error) {
	var revision int
	err := s.db.QueryRow(`SELECT revision FROM jobs WHERE job_id=?`, jobID).Scan(&revision)
	if err != nil {
		return 0, err
	}
	return revision, nil
}
