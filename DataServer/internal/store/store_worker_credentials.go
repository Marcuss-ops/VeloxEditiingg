// Package store / store_worker_credentials.go — worker_credentials persistence.
// Extracted from store_worker_control.go: the persistent identity hashes
// (worker_credentials table) + the SHA-256 credential helper.
package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
)

// ---------- worker_credentials (persistent identity) ----------

// SetWorkerCredential stores or updates a worker's credential hash.
func (s *SQLiteStore) SetWorkerCredential(workerID, credentialHash string) error {
	now := nowRFC3339()
	_, err := s.db.Exec(
		`INSERT INTO worker_credentials (worker_id, credential_hash, created_at, rotated_at)
		 VALUES (?, ?, ?, NULL)
		 ON CONFLICT(worker_id) DO UPDATE SET
		   credential_hash = excluded.credential_hash,
		   rotated_at = ?`,
		workerID, credentialHash, now, now,
	)
	return err
}

// ValidateWorkerCredential checks if a credential hash matches the stored one.
func (s *SQLiteStore) ValidateWorkerCredential(workerID, credentialHash string) (bool, error) {
	var stored string
	err := s.db.QueryRow(
		`SELECT credential_hash FROM worker_credentials WHERE worker_id = ?`, workerID,
	).Scan(&stored)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stored == credentialHash, nil
}

// HasWorkerCredential returns true if a credential already exists for this worker.
func (s *SQLiteStore) HasWorkerCredential(workerID string) (bool, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM worker_credentials WHERE worker_id = ?`, workerID,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// HashCredential creates a SHA-256 hex digest of a credential string.
func HashCredential(credential string) string {
	h := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(h[:])
}
