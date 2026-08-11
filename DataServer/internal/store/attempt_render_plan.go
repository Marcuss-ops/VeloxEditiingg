package store

// attempt_render_plan.go: compiled render plan identity on task_attempts
// (Fase D). The master RenderPlanCompiler stamps plan_version + plan_sha256
// + the canonical plan JSON at claim time; these helpers persist and read
// that identity. plan_version=0 / empty plan_sha256 means "no compiled plan".

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// UpsertRenderPlan stamps the compiled render plan identity on an attempt.
// It is idempotent (last writer wins) and intentionally NOT versioned: the
// plan is compiled once at claim time from the immutable task spec payload,
// so repeated stamps of the same attempt converge to the same document.
func (r *SQLiteTaskAttemptRepository) UpsertRenderPlan(ctx context.Context, attemptID string, planVersion int, planSHA256, planJSON string) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("attempt repository: store not initialized")
	}
	if strings.TrimSpace(attemptID) == "" {
		return fmt.Errorf("attempt repository: attempt_id is required")
	}
	if planVersion <= 0 {
		return fmt.Errorf("attempt repository: plan_version must be positive")
	}
	if strings.TrimSpace(planSHA256) == "" {
		return fmt.Errorf("attempt repository: plan_sha256 is required")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE task_attempts SET plan_version = ?, plan_sha256 = ?, render_plan_json = ?, updated_at = ? WHERE id = ?`,
		planVersion, planSHA256, planJSON, now, attemptID,
	)
	if err != nil {
		return fmt.Errorf("attempt render plan: persist: %w", err)
	}
	affected, err := readRowsAffected(result, "attempt render plan: persist")
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("attempt render plan: persist attempt %q: %w", attemptID, ErrTransitionConflict)
	}
	return nil
}

// GetRenderPlan returns the compiled render plan identity for an attempt:
// plan_version, plan_sha256 and the canonical plan JSON. Returns
// (0, "", "", nil) when the attempt has no plan stamped.
func (r *SQLiteTaskAttemptRepository) GetRenderPlan(ctx context.Context, attemptID string) (int, string, string, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return 0, "", "", fmt.Errorf("attempt repository: store not initialized")
	}
	if strings.TrimSpace(attemptID) == "" {
		return 0, "", "", fmt.Errorf("attempt repository: attempt_id is required")
	}
	var version int
	var planSHA256, planJSON string
	err := r.store.db.QueryRowContext(ctx,
		`SELECT plan_version, plan_sha256, render_plan_json FROM task_attempts WHERE id = ?`,
		attemptID,
	).Scan(&version, &planSHA256, &planJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", "", nil
	}
	if err != nil {
		return 0, "", "", fmt.Errorf("attempt render plan: read: %w", err)
	}
	return version, planSHA256, planJSON, nil
}
