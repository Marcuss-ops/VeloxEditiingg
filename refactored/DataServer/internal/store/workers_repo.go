package store

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
