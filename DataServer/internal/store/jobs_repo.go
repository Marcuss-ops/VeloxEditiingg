package store

import "context"

// JobsRepository exposes the minimal job read operations needed by HTTP handlers.
type JobsRepository interface {
	ListJobs(ctx context.Context, limit int) ([]map[string]any, error)
	GetJob(ctx context.Context, jobID string) (map[string]any, error)
	JobCounts(ctx context.Context) (map[string]int64, error)
}

type SQLiteJobsRepository struct {
	store *SQLiteStore
}

func NewSQLiteJobsRepository(store *SQLiteStore) *SQLiteJobsRepository {
	return &SQLiteJobsRepository{store: store}
}

func (r *SQLiteJobsRepository) ListJobs(ctx context.Context, limit int) ([]map[string]any, error) {
	return r.store.ListJobs(ctx, limit)
}

func (r *SQLiteJobsRepository) GetJob(ctx context.Context, jobID string) (map[string]any, error) {
	return r.store.GetJob(ctx, jobID)
}

func (r *SQLiteJobsRepository) JobCounts(ctx context.Context) (map[string]int64, error) {
	return r.store.JobCounts(ctx)
}
