package store

import (
	"context"
	"database/sql"
	"fmt"
)

// WorkerCapacityRow is the lease-store projection used by scheduling and
// operator read paths. ActiveSlots counts only current LEASED/RUNNING tasks
// with a non-empty lease identity and a non-expired lease. Legacy rows
// without a lease identity or expiry are deliberately excluded.
type WorkerCapacityRow struct {
	WorkerID    string
	ActiveSlots int
}

// WorkerPhaseCapacityRow extends WorkerCapacityRow with per-phase active counts.
// The classification is based on executor_id prefixes: render_batch → render,
// asset_prefetch → prefetch, artifact_publish → publisher.
type WorkerPhaseCapacityRow struct {
	WorkerCapacityRow
	ActiveRender    int
	ActivePrefetch  int
	ActivePublisher int
}

// GetWorkerCapacity returns the authoritative active lease count for one
// worker. The lease store is the only source for ActiveSlots; heartbeat
// counters are deliberately not consulted. maxSlots is supplied by the
// worker capability boundary and is combined by the registry.
func (s *SQLiteStore) GetWorkerCapacity(ctx context.Context, workerID string, nowRFC3339 string) (WorkerCapacityRow, error) {
	if s == nil || s.db == nil {
		return WorkerCapacityRow{WorkerID: workerID}, fmt.Errorf("worker capacity: store not initialized")
	}
	if workerID == "" || nowRFC3339 == "" {
		return WorkerCapacityRow{WorkerID: workerID}, fmt.Errorf("worker capacity: worker_id and now are required")
	}
	var active int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM tasks
		 WHERE worker_id = ?
		   AND worker_id <> ''
		   AND lease_id IS NOT NULL AND lease_id <> ''
		   AND status IN ('LEASED', 'RUNNING')
		   AND lease_expires_at IS NOT NULL
		   AND lease_expires_at <> ''
		   AND julianday(lease_expires_at) >= julianday(?)`, workerID, nowRFC3339).Scan(&active)
	if err != nil {
		if err == sql.ErrNoRows {
			return WorkerCapacityRow{WorkerID: workerID}, nil
		}
		return WorkerCapacityRow{WorkerID: workerID}, fmt.Errorf("worker capacity query: %w", err)
	}
	return WorkerCapacityRow{WorkerID: workerID, ActiveSlots: active}, nil
}

// GetWorkerCapacities bulk-loads authoritative active lease counts for a
// worker set using one grouped query. Missing workers are returned as zero.
func (s *SQLiteStore) GetWorkerCapacities(ctx context.Context, workerIDs []string, nowRFC3339 string) (map[string]WorkerCapacityRow, error) {
	out := make(map[string]WorkerCapacityRow, len(workerIDs))
	if len(workerIDs) == 0 {
		return out, nil
	}
	if s == nil || s.db == nil {
		return out, fmt.Errorf("worker capacities: store not initialized")
	}
	if nowRFC3339 == "" {
		return out, fmt.Errorf("worker capacities: now is required")
	}
	args := make([]interface{}, 0, len(workerIDs)+1)
	placeholders := make([]byte, 0, len(workerIDs)*2)
	for i, id := range workerIDs {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	args = append(args, nowRFC3339)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT worker_id, COUNT(*)
		  FROM tasks
		 WHERE worker_id IN (%s)
		   AND worker_id <> ''
		   AND lease_id IS NOT NULL AND lease_id <> ''
		   AND status IN ('LEASED', 'RUNNING')
		   AND lease_expires_at IS NOT NULL
		   AND lease_expires_at <> ''
		   AND julianday(lease_expires_at) >= julianday(?)
		 GROUP BY worker_id`, string(placeholders)), args...)
	if err != nil {
		return out, fmt.Errorf("worker capacities query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row WorkerCapacityRow
		if err := rows.Scan(&row.WorkerID, &row.ActiveSlots); err != nil {
			return out, fmt.Errorf("worker capacities scan: %w", err)
		}
		out[row.WorkerID] = row
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("worker capacities rows: %w", err)
	}
	for _, id := range workerIDs {
		if _, ok := out[id]; !ok {
			out[id] = WorkerCapacityRow{WorkerID: id}
		}
	}
	return out, nil
}

// GetWorkerPhaseCapacity returns per-phase active lease counts for a worker.
// Tasks are classified by executor_id prefix: render_batch → render,
// asset_prefetch → prefetch, artifact_publish → publisher.
func (s *SQLiteStore) GetWorkerPhaseCapacity(ctx context.Context, workerID string, nowRFC3339 string) (WorkerPhaseCapacityRow, error) {
	out := WorkerPhaseCapacityRow{
		WorkerCapacityRow: WorkerCapacityRow{WorkerID: workerID},
	}
	if s == nil || s.db == nil {
		return out, fmt.Errorf("worker phase capacity: store not initialized")
	}
	if workerID == "" || nowRFC3339 == "" {
		return out, fmt.Errorf("worker phase capacity: worker_id and now are required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT executor_id, COUNT(*)
		  FROM tasks
		 WHERE worker_id = ?
		   AND worker_id <> ''
		   AND lease_id IS NOT NULL AND lease_id <> ''
		   AND status IN ('LEASED', 'RUNNING')
		   AND lease_expires_at IS NOT NULL
		   AND lease_expires_at <> ''
		   AND julianday(lease_expires_at) >= julianday(?)
		 GROUP BY executor_id`, workerID, nowRFC3339)
	if err != nil {
		return out, fmt.Errorf("worker phase capacity query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var executorID string
		var count int
		if err := rows.Scan(&executorID, &count); err != nil {
			return out, fmt.Errorf("worker phase capacity scan: %w", err)
		}
		out.ActiveSlots += count
		switch {
		case executorID == "render_batch" || len(executorID) > 12 && executorID[:12] == "render_batch@":
			out.ActiveRender += count
		case executorID == "asset_prefetch" || len(executorID) > 14 && executorID[:14] == "asset_prefetch@":
			out.ActivePrefetch += count
		case executorID == "artifact_publish" || len(executorID) > 16 && executorID[:16] == "artifact_publish@":
			out.ActivePublisher += count
		default:
			// Unknown executor → count as render (conservative fallback)
			out.ActiveRender += count
		}
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("worker phase capacity rows: %w", err)
	}
	return out, nil
}

// GetWorkerPhaseCapacities bulk-loads per-phase active lease counts for a
// worker set using one grouped query. Missing workers are returned as zero.
func (s *SQLiteStore) GetWorkerPhaseCapacities(ctx context.Context, workerIDs []string, nowRFC3339 string) (map[string]WorkerPhaseCapacityRow, error) {
	out := make(map[string]WorkerPhaseCapacityRow, len(workerIDs))
	if len(workerIDs) == 0 {
		return out, nil
	}
	if s == nil || s.db == nil {
		return out, fmt.Errorf("worker phase capacities: store not initialized")
	}
	if nowRFC3339 == "" {
		return out, fmt.Errorf("worker phase capacities: now is required")
	}
	args := make([]interface{}, 0, len(workerIDs)+1)
	placeholders := make([]byte, 0, len(workerIDs)*2)
	for i, id := range workerIDs {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	args = append(args, nowRFC3339)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT worker_id, executor_id, COUNT(*)
		  FROM tasks
		 WHERE worker_id IN (%s)
		   AND worker_id <> ''
		   AND lease_id IS NOT NULL AND lease_id <> ''
		   AND status IN ('LEASED', 'RUNNING')
		   AND lease_expires_at IS NOT NULL
		   AND lease_expires_at <> ''
		   AND julianday(lease_expires_at) >= julianday(?)
		 GROUP BY worker_id, executor_id`, string(placeholders)), args...)
	if err != nil {
		return out, fmt.Errorf("worker phase capacities query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var workerID, executorID string
		var count int
		if err := rows.Scan(&workerID, &executorID, &count); err != nil {
			return out, fmt.Errorf("worker phase capacities scan: %w", err)
		}
		row := out[workerID]
		row.WorkerID = workerID
		row.ActiveSlots += count
		switch {
		case executorID == "render_batch" || len(executorID) > 12 && executorID[:12] == "render_batch@":
			row.ActiveRender += count
		case executorID == "asset_prefetch" || len(executorID) > 14 && executorID[:14] == "asset_prefetch@":
			row.ActivePrefetch += count
		case executorID == "artifact_publish" || len(executorID) > 16 && executorID[:16] == "artifact_publish@":
			row.ActivePublisher += count
		default:
			row.ActiveRender += count
		}
		out[workerID] = row
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("worker phase capacities rows: %w", err)
	}
	for _, id := range workerIDs {
		if _, ok := out[id]; !ok {
			out[id] = WorkerPhaseCapacityRow{WorkerCapacityRow: WorkerCapacityRow{WorkerID: id}}
		}
	}
	return out, nil
}
