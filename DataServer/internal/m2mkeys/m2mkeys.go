// Package m2mkeys / m2mkeys.go: typed CRUD over the M2M credential
// tables introduced by migration 101 (m2m_api_keys + m2m_audit_log).
//
// This leaf package is the single writer of these tables; the middleware
// (handlers/server/pipeline/m2m_auth.go) and the admin endpoints
// (handlers/server/api/admin_m2m_keys.go) both go through it (via the
// internal/store re-export shim). Putting all SQL behind the package-level
// functions avoids the typical drift where one caller writes raw SQL and the
// next caller's expected column shape has subtly drifted.
//
// Two design notes:
//
//   - secrets NEVER round-trip from the DB — only the SHA-256 hash is
//     persisted. Lookups by plaintext are necessarily a hash-then-
//     match: callers compute sha256(secret) and pass the hex digest
//     here. This avoids the DB ever holding plaintext secrets (and
//     protects against log-scrape leaks on backup/restore paths).
//
//   - audit rows reference m2m_api_keys via FK with ON DELETE
//     RESTRICT — a careless DELETE that would orphan a client's
//     history is rejected at the DB level. Soft-disable
//     (is_active=0) is the supported revocation path.
//
// Layout of this package:
//   - m2mkeys.go           — types + m2m_api_keys CRUD + row scanning.
//   - m2mkeys_audit.go     — m2m_audit_log append/list.
//   - m2mkeys_crypto.go    — secret generation/hashing/constant-time match.
//   - m2mkeys_effective.go — per-key effective limits + shared helpers.
package m2mkeys

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// M2MAPIKey is the typed projection of an m2m_api_keys row.
//
// secret_hash is the hex-encoded SHA-256 of the plaintext secret at
// creation time. Callers never see plaintext after creation (the
// admin POST returns plaintext ONCE; the DB only ever holds the
// hash).
type M2MAPIKey struct {
	ClientID       string
	SecretHash     string
	Scopes         []string // parsed from the comma-separated DB column
	IsActive       bool
	Description    string
	RateLimitRPS   int // 0 → cfg.M2M.DefaultRPS
	RateLimitBurst int // 0 → cfg.M2M.DefaultBurst
	Quotas         M2MQuotas
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LastUsedAt     sql.NullTime
}

// M2MQuotas caps the per-request resource usage for a single client.
// Zero values mean "use the cfg.M2M defaults" — the middleware
// substitutes the config value at enforcement time.
type M2MQuotas struct {
	MaxScenes         int
	MaxTotalDurationS float64
}

// M2MAuditEntry is one row of m2m_audit_log. The idem_key_hash
// column is the 12-byte-prefix SHA-256 of the original
// idempotency_key; the original key is NOT recorded because it can
// carry client PII (e.g., embedded customer IDs, file paths).
type M2MAuditEntry struct {
	ID                   int64
	ClientID             string
	IdemKeyHash          string
	Method               string
	Path                 string
	StatusCode           int
	Scope                string
	SceneCount           int
	TotalDurationSeconds float64
	IPAddress            string
	RejectReason         sql.NullString
	CreatedAt            time.Time
}

// =====================================================================
// CRUD: m2m_api_keys
// =====================================================================

// InsertM2MAPIKey stamps a new key row. The caller supplies the
// already-computed secret_hash (hex-encoded SHA-256). Plaintext is
// not stored.
func InsertM2MAPIKey(ctx context.Context, db *sql.DB, k M2MAPIKey) error {
	if strings.TrimSpace(k.ClientID) == "" {
		return errors.New("m2mkeys: InsertM2MAPIKey: ClientID is required")
	}
	if strings.TrimSpace(k.SecretHash) == "" {
		return errors.New("m2mkeys: InsertM2MAPIKey: SecretHash is required")
	}
	if len(k.Scopes) == 0 {
		k.Scopes = []string{"jobs.submit"}
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO m2m_api_keys (
			client_id, secret_hash, scopes, is_active, description,
			rate_limit_rps, rate_limit_burst,
			quota_max_scenes, quota_max_total_secs,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
	`,
		strings.TrimSpace(k.ClientID),
		strings.ToLower(strings.TrimSpace(k.SecretHash)),
		strings.Join(k.Scopes, ","),
		boolToInt(k.IsActive),
		strings.TrimSpace(k.Description),
		k.RateLimitRPS,
		k.RateLimitBurst,
		k.Quotas.MaxScenes,
		k.Quotas.MaxTotalDurationS,
	)
	if err != nil {
		return fmt.Errorf("m2mkeys: InsertM2MAPIKey: %w", err)
	}
	return nil
}

// GetM2MAPIKeyByClientID returns the typed key row for client_id (or
// nil + nil if no row exists).
func GetM2MAPIKeyByClientID(ctx context.Context, db *sql.DB, clientID string) (*M2MAPIKey, error) {
	row := db.QueryRowContext(ctx, `
		SELECT
			client_id, secret_hash, scopes, is_active, description,
			rate_limit_rps, rate_limit_burst,
			quota_max_scenes, quota_max_total_secs,
			created_at, updated_at, last_used_at
		FROM m2m_api_keys WHERE client_id = ?
	`, clientID)
	return scanM2MAPIKey(row)
}

// GetActiveM2MAPIKeyBySecretHash finds a key whose stored hash
// matches secretHash AND IsActive=1. Used by the middleware on
// every request. The hash comparison is done in Go (not SQL) so the
// DB never accumulates unused indexes specifically for secret lookup
// (avoiding both a write-amplification path on insert and a
// catastrophic leak vector if the DB is dumped).
func GetActiveM2MAPIKeyBySecretHash(ctx context.Context, db *sql.DB, secretHash string) (*M2MAPIKey, error) {
	secretHash = strings.ToLower(strings.TrimSpace(secretHash))
	if secretHash == "" {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT
			client_id, secret_hash, scopes, is_active, description,
			rate_limit_rps, rate_limit_burst,
			quota_max_scenes, quota_max_total_secs,
			created_at, updated_at, last_used_at
		FROM m2m_api_keys
		WHERE is_active = 1 AND secret_hash = ?
	`, secretHash)
	if err != nil {
		return nil, fmt.Errorf("m2mkeys: GetActiveM2MAPIKeyBySecretHash: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		return scanM2MAPIKey(rows)
	}
	return nil, rows.Err()
}

// ListM2MAPIKeys returns all rows ordered by created_at DESC. Used
// by the admin GET endpoint. limit/cursor not implemented yet (the
// admin client is operator-grade, expected to operate on
// dozens-of-rows scale).
func ListM2MAPIKeys(ctx context.Context, db *sql.DB) ([]M2MAPIKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			client_id, secret_hash, scopes, is_active, description,
			rate_limit_rps, rate_limit_burst,
			quota_max_scenes, quota_max_total_secs,
			created_at, updated_at, last_used_at
		FROM m2m_api_keys
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("m2mkeys: ListM2MAPIKeys: %w", err)
	}
	defer rows.Close()

	var out []M2MAPIKey
	for rows.Next() {
		k, err := scanM2MAPIKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *k)
	}
	return out, rows.Err()
}

// DisableM2MAPIKey is the supported revocation path. It sets
// is_active=0 so the middleware rejects subsequent requests with
// the same secret; the row is preserved for audit trail integrity.
// Returns nil if the row was found and disabled; sql.ErrNoRows if
// no row matches.
func DisableM2MAPIKey(ctx context.Context, db *sql.DB, clientID string) error {
	res, err := db.ExecContext(ctx, `
		UPDATE m2m_api_keys
		SET is_active = 0, updated_at = datetime('now')
		WHERE client_id = ?
	`, strings.TrimSpace(clientID))
	if err != nil {
		return fmt.Errorf("m2mkeys: DisableM2MAPIKey: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("m2mkeys: DisableM2MAPIKey: rowsAffected: %w", err)
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// TouchM2MAPIKeyLastUsed updates the row's last_used_at to "now".
// Called by the middleware after a successful auth, but NOT on
// every request — only on accept (status < 400) to avoid hammering
// the DB with updates on rejected traffic. Best-effort: errors are
// logged by the caller but do NOT fail the request.
func TouchM2MAPIKeyLastUsed(ctx context.Context, db *sql.DB, clientID string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE m2m_api_keys SET last_used_at = datetime('now')
		WHERE client_id = ?
	`, strings.TrimSpace(clientID))
	if err != nil {
		return fmt.Errorf("m2mkeys: TouchM2MAPIKeyLastUsed: %w", err)
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanM2MAPIKey(row rowScanner) (*M2MAPIKey, error) {
	var (
		clientID, secretHash, scopes, description string
		isActive                                  int
		rateRPS, rateBurst, maxScenes             int
		maxDur                                    float64
		createdAt, updatedAt                      string
		lastUsed                                  sql.NullTime
	)
	if err := row.Scan(
		&clientID, &secretHash, &scopes, &isActive, &description,
		&rateRPS, &rateBurst, &maxScenes, &maxDur,
		&createdAt, &updatedAt, &lastUsed,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("m2mkeys: scanM2MAPIKey: %w", err)
	}
	createdT, err := parseSQLiteTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("m2mkeys: scanM2MAPIKey created_at: %w", err)
	}
	updatedT, err := parseSQLiteTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("m2mkeys: scanM2MAPIKey updated_at: %w", err)
	}
	return &M2MAPIKey{
		ClientID:       clientID,
		SecretHash:     secretHash,
		Scopes:         splitCSV(scopes),
		IsActive:       isActive == 1,
		Description:    description,
		RateLimitRPS:   rateRPS,
		RateLimitBurst: rateBurst,
		Quotas:         M2MQuotas{MaxScenes: maxScenes, MaxTotalDurationS: maxDur},
		CreatedAt:      createdT,
		UpdatedAt:      updatedT,
		LastUsedAt:     lastUsed,
	}, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
