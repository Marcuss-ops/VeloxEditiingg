package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// StaleExecutionReconciler is the typed application maintenance surface.
// SQL is intentionally confined to this store package; the admin command
// only handles flags, serialization, and process I/O.
type StaleExecutionReconciler struct {
	db                 *sql.DB
	tasks              *SQLiteTaskRepository
	jobs               *SQLiteJobRepository
	workerOfflineAfter time.Duration
}

func NewStaleExecutionReconciler(s *SQLiteStore) (*StaleExecutionReconciler, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("stale execution reconciler: store is not initialized")
	}
	return &StaleExecutionReconciler{
		db: s.db, tasks: NewSQLiteTaskRepository(s), jobs: NewSQLiteJobRepository(s),
		workerOfflineAfter: 10 * time.Minute,
	}, nil
}

// Scan is SELECT-only and deterministic. It never writes state or audit rows.
func (r *StaleExecutionReconciler) Scan(ctx context.Context, now time.Time, limit int) ([]StaleExecutionFinding, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 500
	}
	// Scan each category independently, then interleave the results. This
	// preserves the caller's global limit without allowing an early category
	// (for example, a large expired-lease backlog) to starve later categories.
	scanners := []func(context.Context, time.Time, int, []StaleExecutionFinding) ([]StaleExecutionFinding, error){
		r.scanExpiredLeases, r.scanOrphanTasks, r.scanCommittedArtifactDrift,
		r.scanUnconfirmedSpool, r.scanOfflineWorkers, r.scanOrphanAttempts,
	}
	perCategory := make([][]StaleExecutionFinding, len(scanners))
	for i, scan := range scanners {
		var scanErr error
		perCategory[i], scanErr = scan(ctx, now, limit, nil)
		if scanErr != nil {
			return nil, scanErr
		}
	}
	findings := make([]StaleExecutionFinding, 0, limit)
	for offset := 0; len(findings) < limit; offset++ {
		added := false
		for category := range perCategory {
			if offset >= len(perCategory[category]) {
				continue
			}
			findings = append(findings, perCategory[category][offset])
			added = true
			if len(findings) == limit {
				break
			}
		}
		if !added {
			break
		}
	}
	return findings, nil
}

func (r *StaleExecutionReconciler) Reconcile(ctx context.Context, now time.Time, limit int, apply bool, actor string) (StaleExecutionReport, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "velox-admin"
	}
	findings, err := r.Scan(ctx, now, limit)
	if err != nil {
		return StaleExecutionReport{}, err
	}
	report := StaleExecutionReport{GeneratedAt: now.UTC().Format(time.RFC3339Nano), Mode: "dry-run", Findings: findings}
	if !apply {
		return report, nil
	}
	report.Mode = "apply"
	for _, finding := range findings {
		changed, err := r.applyFinding(ctx, finding, actor, now)
		if err != nil {
			return report, fmt.Errorf("apply %s/%s: %w", finding.Category, finding.ResourceID, err)
		}
		if changed {
			report.Applied = append(report.Applied, finding)
		} else {
			report.Skipped++
		}
	}
	return report, nil
}
