// Package repository is the leaf home for the narrow persistence contracts
// (repository interfaces + their pure-data parameter/record types) that were
// previously declared inside internal/store. Consumers that only need a
// contract — not a SQLite implementation — import this package and therefore
// do not inherit store's domain dependency graph. The SQLite implementations
// remain in internal/store, which re-exports these symbols for backwards
// compatibility.
package repository

import "os"

// BlobStore is the storage abstraction for artifact blobs.
type BlobStore interface {
	// StagingPath returns a unique path in the staging area. The caller
	// writes the upload bytes to this path, then calls PromoteToFinal.
	StagingPath(jobID, artifactID, extension string) (string, error)

	// FinalPath returns the canonical storage_key for a verified artifact.
	// The key is deterministic from the artifact's identity so retries
	// produce the same path (idempotent move).
	FinalPath(jobID, artifactID, extension string) string

	// PromoteToFinal moves a staged file to its final canonical location.
	// Returns the storage_key (relative path) on success.
	PromoteToFinal(stagingPath, finalPath string) (string, error)

	// PromoteDurable atomically promotes a staged file to finalPath with
	// the durability guarantees the artifact spec requires (flush →
	// fsync → close → atomic rename → best-effort directory fsync) and
	// removes the staging file on success. A concurrently-promoted
	// identical blob (staging already gone, final already present) is
	// tolerated as an idempotent success. Returns finalPath.
	PromoteDurable(stagingPath, finalPath string) (string, error)

	// OpenStagedWrite creates (or truncates) a staged file at path for
	// writing, creating parent directories first. The caller streams
	// bytes and is responsible for Sync/Close and failure cleanup.
	OpenStagedWrite(path string) (*os.File, error)

	// OpenStagedRead opens a staged file for reading (chunk assembly).
	OpenStagedRead(path string) (*os.File, error)

	// RemoveStaging cleanup a staged file on failure.
	RemoveStaging(path string) error

	// ReadFinal opens the final file for reading (providers use this).
	ReadFinal(storageKey string) (*os.File, error)

	// StagingDir returns the staging root path (for reconciliation).
	StagingDir() string

	// FinalDir returns the final storage root path (for reconciliation).
	FinalDir() string
}

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

// DBTelemetry is the narrow persistence-observability seam. The store owns
// when measurements are taken; the metrics package owns how they are exposed.
type DBTelemetry interface {
	ObserveDBTransaction(waitMS, transactionMS float64, busy, busyTimeout, retried bool, writeOps, readOps uint64)
	ObserveDBStats(openConnections, inUse, idle, waitCount int64, waitDurationMS float64)
	RecordDBOperation(write bool)
	RecordDBRetry()
}
