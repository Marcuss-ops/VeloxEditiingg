// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes

package store

import "velox-server/internal/repository"

// repository_workers.go owns the WorkersRepository interface and the
// SQLiteWorkersRepository adapter. The registry uses this interface as
// its single source of truth — no JSON fallback. Callers that want to
// opt out of direct SQLiteStore dependency go through this adapter;
// methods are intentionally thin wrappers (one-line forwarders) so the
// SQL truth of the snapshot file is the only persistence surface that
// has to stay in lockstep with the schema.

// WorkersRepository is re-exported from the repository leaf package.
type WorkersRepository = repository.WorkersRepository

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
