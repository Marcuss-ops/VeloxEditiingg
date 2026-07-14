package performance

import "context"

// Repository is the persistence contract for performance baselines.
type Repository interface {
	// Upsert inserts a new baseline or replaces an existing baseline with
	// the same (workload_key, git_sha, config_hash, worker_class) tuple.
	// On success the input baseline is mutated: BaselineID is generated if
	// empty and CreatedAt is set to the current UTC time if zero.
	Upsert(ctx context.Context, baseline *Baseline) error

	// Get returns the baseline matching the unique tuple, or (nil, nil)
	// when no baseline exists.
	Get(ctx context.Context, workloadKey, gitSHA, configHash, workerClass string) (*Baseline, error)

	// ListByWorkloadKey returns all baselines for a given workload class,
	// ordered by created_at descending.
	ListByWorkloadKey(ctx context.Context, workloadKey string) ([]Baseline, error)

	// ListByGitSHA returns all baselines for a given git SHA, ordered by
	// created_at descending.
	ListByGitSHA(ctx context.Context, gitSHA string) ([]Baseline, error)
}
