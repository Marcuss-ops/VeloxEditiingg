package store

// store_m2mkeys.go: re-export + delegation shim for the m2mkeys leaf
// package (internal/m2mkeys), which owns the typed CRUD over the M2M
// credential tables (m2m_api_keys + m2m_audit_log). The SQL moved out of
// this god-package into the leaf; the *SQLiteStore methods below stay as
// thin forwarders so existing callers (handlers/server/pipeline + api)
// keep the store.M2M* surface unchanged.

import (
	"context"

	"velox-server/internal/m2mkeys"
)

// M2MAPIKey / M2MQuotas / M2MAuditEntry are re-exported from the leaf.
type M2MAPIKey = m2mkeys.M2MAPIKey
type M2MQuotas = m2mkeys.M2MQuotas
type M2MAuditEntry = m2mkeys.M2MAuditEntry

// Pure helpers re-exported from the leaf.
var (
	GenerateM2MSecret = m2mkeys.GenerateM2MSecret
	HashM2MSecret     = m2mkeys.HashM2MSecret
	M2MSecretMatches  = m2mkeys.M2MSecretMatches
)

// InsertM2MAPIKey stamps a new key row.
func (s *SQLiteStore) InsertM2MAPIKey(ctx context.Context, k M2MAPIKey) error {
	return m2mkeys.InsertM2MAPIKey(ctx, s.db, k)
}

// GetM2MAPIKeyByClientID returns the typed key row for client_id.
func (s *SQLiteStore) GetM2MAPIKeyByClientID(ctx context.Context, clientID string) (*M2MAPIKey, error) {
	return m2mkeys.GetM2MAPIKeyByClientID(ctx, s.db, clientID)
}

// GetActiveM2MAPIKeyBySecretHash finds an active key by secret hash.
func (s *SQLiteStore) GetActiveM2MAPIKeyBySecretHash(ctx context.Context, secretHash string) (*M2MAPIKey, error) {
	return m2mkeys.GetActiveM2MAPIKeyBySecretHash(ctx, s.db, secretHash)
}

// ListM2MAPIKeys returns all keys ordered by created_at DESC.
func (s *SQLiteStore) ListM2MAPIKeys(ctx context.Context) ([]M2MAPIKey, error) {
	return m2mkeys.ListM2MAPIKeys(ctx, s.db)
}

// DisableM2MAPIKey is the supported revocation path.
func (s *SQLiteStore) DisableM2MAPIKey(ctx context.Context, clientID string) error {
	return m2mkeys.DisableM2MAPIKey(ctx, s.db, clientID)
}

// TouchM2MAPIKeyLastUsed updates the row's last_used_at to "now".
func (s *SQLiteStore) TouchM2MAPIKeyLastUsed(ctx context.Context, clientID string) error {
	return m2mkeys.TouchM2MAPIKeyLastUsed(ctx, s.db, clientID)
}

// AppendM2MAuditLog writes one best-effort audit row.
func (s *SQLiteStore) AppendM2MAuditLog(ctx context.Context, e M2MAuditEntry) error {
	return m2mkeys.AppendM2MAuditLog(ctx, s.db, e)
}

// ListM2MAuditLog returns latest-first audit entries.
func (s *SQLiteStore) ListM2MAuditLog(ctx context.Context, clientID string, limit int) ([]M2MAuditEntry, error) {
	return m2mkeys.ListM2MAuditLog(ctx, s.db, clientID, limit)
}
