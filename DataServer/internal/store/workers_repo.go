package store

// WorkersRepository exposes the worker read operations needed by HTTP handlers.
type WorkersRepository interface {
	ListWorkers() ([]map[string]any, error)
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
