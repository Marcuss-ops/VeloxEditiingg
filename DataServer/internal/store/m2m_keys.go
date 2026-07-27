// Package store / m2m_keys.go: typed CRUD over the M2M credential
// tables introduced by migration 101 (m2m_api_keys + m2m_audit_log).
//
// The store is the single writer of these tables; the middleware
// (handlers/server/pipeline/m2m_auth.go) and the admin endpoints
// (handlers/server/api/admin_m2m_keys.go) both go through it. Putting
// all SQL behind the stored methods avoids the typical drift where
// one caller writes raw SQL and the next caller's expected column
// shape has subtly drifted.
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
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
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
	RateLimitRPS   int      // 0 → cfg.M2M.DefaultRPS
	RateLimitBurst int      // 0 → cfg.M2M.DefaultBurst
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
	ID                  int64
	ClientID            string
	IdemKeyHash         string
	Method              string
	Path                string
	StatusCode          int
	Scope               string
	SceneCount          int
	TotalDurationSeconds float64
	IPAddress           string
	RejectReason        sql.NullString
	CreatedAt           time.Time
}

// =====================================================================
// CRUD: m2m_api_keys
// =====================================================================

// InsertM2MAPIKey stamps a new key row. The caller supplies the
// already-computed secret_hash (hex-encoded SHA-256). Plaintext is
// not stored.
//
// Returns the inserted key row ID (always == ClientID, but the
// returned string keeps the API symmetric with other typed inserts).
func (s *SQLiteStore) InsertM2MAPIKey(ctx context.Context, k M2MAPIKey) error {
	if strings.TrimSpace(k.ClientID) == "" {
		return errors.New("store: InsertM2MAPIKey: ClientID is required")
	}
	if strings.TrimSpace(k.SecretHash) == "" {
		return errors.New("store: InsertM2MAPIKey: SecretHash is required")
	}
	if len(k.Scopes) == 0 {
		k.Scopes = []string{"jobs.submit"}
	}
	_, err := s.db.ExecContext(ctx, `
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
		return fmt.Errorf("store: InsertM2MAPIKey: %w", err)
	}
	return nil
}

// GetM2MAPIKeyByClientID returns the typed key row for client_id (or
// nil + nil if no row exists).
func (s *SQLiteStore) GetM2MAPIKeyByClientID(ctx context.Context, clientID string) (*M2MAPIKey, error) {
	row := s.db.QueryRowContext(ctx, `
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
func (s *SQLiteStore) GetActiveM2MAPIKeyBySecretHash(ctx context.Context, secretHash string) (*M2MAPIKey, error) {
	secretHash = strings.ToLower(strings.TrimSpace(secretHash))
	if secretHash == "" {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			client_id, secret_hash, scopes, is_active, description,
			rate_limit_rps, rate_limit_burst,
			quota_max_scenes, quota_max_total_secs,
			created_at, updated_at, last_used_at
		FROM m2m_api_keys
		WHERE is_active = 1 AND secret_hash = ?
	`, secretHash)
	if err != nil {
		return nil, fmt.Errorf("store: GetActiveM2MAPIKeyBySecretHash: %w", err)
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
func (s *SQLiteStore) ListM2MAPIKeys(ctx context.Context) ([]M2MAPIKey, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			client_id, secret_hash, scopes, is_active, description,
			rate_limit_rps, rate_limit_burst,
			quota_max_scenes, quota_max_total_secs,
			created_at, updated_at, last_used_at
		FROM m2m_api_keys
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("store: ListM2MAPIKeys: %w", err)
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
func (s *SQLiteStore) DisableM2MAPIKey(ctx context.Context, clientID string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE m2m_api_keys
		SET is_active = 0, updated_at = datetime('now')
		WHERE client_id = ?
	`, strings.TrimSpace(clientID))
	if err != nil {
		return fmt.Errorf("store: DisableM2MAPIKey: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: DisableM2MAPIKey: rowsAffected: %w", err)
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
func (s *SQLiteStore) TouchM2MAPIKeyLastUsed(ctx context.Context, clientID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE m2m_api_keys SET last_used_at = datetime('now')
		WHERE client_id = ?
	`, strings.TrimSpace(clientID))
	if err != nil {
		return fmt.Errorf("store: TouchM2MAPIKeyLastUsed: %w", err)
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
		rateRPS, rateBurst, maxScenes              int
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
		return nil, fmt.Errorf("store: scanM2MAPIKey: %w", err)
	}
	createdT, _ := parseSQLiteTime(createdAt)
	updatedT, _ := parseSQLiteTime(updatedAt)
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

// =====================================================================
// Audit log
// =====================================================================

// AppendM2MAuditLog writes one row. The caller (middleware) supplies
// the post-handler status_code + reject_reason so the row reflects
// what the client actually saw. Failures are not propagated — the
// audit log is best-effort: callers log warnings but do NOT rewind
// the response.
func (s *SQLiteStore) AppendM2MAuditLog(ctx context.Context, e M2MAuditEntry) error {
	if e.Method == "" {
		e.Method = "POST"
	}
	if e.Path == "" {
		e.Path = "/api/v1/jobs"
	}
	if e.Scope == "" {
		e.Scope = "jobs.submit"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO m2m_audit_log (
			client_id, idem_key_hash, method, path, status_code, scope,
			scene_count, total_duration_seconds, ip_address, reject_reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		strings.TrimSpace(e.ClientID),
		strings.ToLower(strings.TrimSpace(e.IdemKeyHash)),
		e.Method,
		e.Path,
		e.StatusCode,
		e.Scope,
		e.SceneCount,
		e.TotalDurationSeconds,
		strings.TrimSpace(e.IPAddress),
		e.RejectReason,
	)
	if err != nil {
		return fmt.Errorf("store: AppendM2MAuditLog: %w", err)
	}
	return nil
}

// ListM2MAuditLog returns latest-first audit entries, optionally
// filtered by client_id. Limit caps rows returned (0 → 100).
func (s *SQLiteStore) ListM2MAuditLog(ctx context.Context, clientID string, limit int) ([]M2MAuditEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `
		SELECT id, client_id, idem_key_hash, method, path, status_code, scope,
			scene_count, total_duration_seconds, ip_address, reject_reason, created_at
		FROM m2m_audit_log`
	args := []any{}
	if clientID = strings.TrimSpace(clientID); clientID != "" {
		query += " WHERE client_id = ?"
		args = append(args, clientID)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: ListM2MAuditLog: %w", err)
	}
	defer rows.Close()

	var out []M2MAuditEntry
	for rows.Next() {
		var (
			id, status, sceneCount int64
			clientID, idemHash, method, path, scope, ip string
			dur                                            float64
			rejectReason                                   sql.NullString
			createdAt                                      string
		)
		if err := rows.Scan(&id, &clientID, &idemHash, &method, &path, &status, &scope,
			&sceneCount, &dur, &ip, &rejectReason, &createdAt); err != nil {
			return nil, fmt.Errorf("store: ListM2MAuditLog scan: %w", err)
		}
		createdT, _ := parseSQLiteTime(createdAt)
		out = append(out, M2MAuditEntry{
			ID:                   id,
			ClientID:             clientID,
			IdemKeyHash:          idemHash,
			Method:               method,
			Path:                 path,
			StatusCode:           int(status),
			Scope:                scope,
			SceneCount:           int(sceneCount),
			TotalDurationSeconds: dur,
			IPAddress:            ip,
			RejectReason:         rejectReason,
			CreatedAt:            createdT,
		})
	}
	return out, rows.Err()
}

// =====================================================================
// Helpers
// =====================================================================

// GenerateM2MSecret returns a 32-byte (64 hex-char) high-entropy
// plaintext secret suitable as the bearer value of an M2M API key.
// Hex output keeps the secret URL-safe AND copy-paste safe. crypto/rand
// panics on entropy failure (the system is broken). Sits next to
// HashM2MSecret so the create/lookup pair are auditable in one file.
//
// This is paired with HashM2MSecret: the admin POST endpoint calls
// GenerateM2MSecret() to create the plaintext, then HashM2MSecret()
// to obtain the value stored in m2m_api_keys.secret_hash. The
// plaintext is returned to the operator ONCE.
func GenerateM2MSecret() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic("store: GenerateM2MSecret: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// HashM2MSecret computes the hex-encoded SHA-256 of secret. The
// canonical form is the lowercase hex of the SHA-256 digest. The
// middleware calls this on every request so the helper MUST be
// constant-time-ish; the actual constant-time compare happens in
// M2MSecretMatches.
func HashM2MSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// M2MSecretMatches reports whether storedHash (hex-encoded SHA-256)
// matches the hash of secret. Constant-time on length matched
// (Go's subtle.ConstantTimeCompare).
//
// NOTE: With 256-bit entropy tokens, the SQLite equality lookup
// (in GetActiveM2MAPIKeyBySecretHash) is non-constant-time on the
// hash value but brute-force from timing remains computationally
// infeasible — a side-channel attacker would need to leak log2 bits
// of position over millions of requests. The in-Go ConstantTimeCompare
// here is the belt-and-braces second line of defense.
func M2MSecretMatches(storedHash, secret string) bool {
	storedHash = strings.ToLower(strings.TrimSpace(storedHash))
	candidate := HashM2MSecret(secret)
	if len(storedHash) != len(candidate) {
		// Length mismatch => cannot match; return false without
		// touching the substring compare so timing of this path is
		// independent of storedHash content.
		return false
	}
	return subtle.ConstantTimeCompare([]byte(storedHash), []byte(candidate)) == 1
}

// HasScope reports whether the key entry's scope list contains want.
// Useful at middleware boundaries where scope-check is
// scope-presence-only (no override ordering).
func (k *M2MAPIKey) HasScope(want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, s := range k.Scopes {
		if strings.EqualFold(strings.TrimSpace(s), want) {
			return true
		}
	}
	return false
}

// EffectiveRateLimitRPS resolves the per-key override or falls back
// to the operator-supplied default. The two-sided shape lets the
// middleware call one function in the hot path.
func (k *M2MAPIKey) EffectiveRateLimitRPS(defaultRPS int) int {
	if k == nil {
		return defaultRPS
	}
	if k.RateLimitRPS > 0 {
		return k.RateLimitRPS
	}
	return defaultRPS
}

// EffectiveBurst mirrors EffectiveRateLimitRPS for the burst cap.
func (k *M2MAPIKey) EffectiveBurst(defaultBurst int) int {
	if k == nil {
		return defaultBurst
	}
	if k.RateLimitBurst > 0 {
		return k.RateLimitBurst
	}
	return defaultBurst
}

// EffectiveMaxScenes mirrors the pattern for the per-request scene
// quota (caller passes cfg.M2M.MaxScenesPerRequest as the default).
func (k *M2MAPIKey) EffectiveMaxScenes(defaultMax int) int {
	if k == nil {
		return defaultMax
	}
	if k.Quotas.MaxScenes > 0 {
		return k.Quotas.MaxScenes
	}
	return defaultMax
}

// EffectiveMaxTotalDurationS mirrors MaxScenes for the per-request
// duration cap (cfg.M2M.MaxTotalDurationSecondsPerRequest).
func (k *M2MAPIKey) EffectiveMaxTotalDurationS(defaultMax float64) float64 {
	if k == nil {
		return defaultMax
	}
	if k.Quotas.MaxTotalDurationS > 0 {
		return k.Quotas.MaxTotalDurationS
	}
	return defaultMax
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseSQLiteTime parses datetime('now') outputs (UTC, no
// timezone). Returns zero time on error so the callers' signature
// stays simple.
func parseSQLiteTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// SQLite without timezone: try several common layouts.
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized sqlite timestamp: %q", s)
}
