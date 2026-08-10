// Package store / store_delivery_destinations.go
//
// CRUD for delivery_destinations (reusable, per-provider configuration).
// Split out of store_deliveries.go.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ── Destination CRUD ─────────────────────────────────────────────────────────

// InsertDeliveryDestination persists a delivery destination (idempotent
// on destination_id via INSERT OR IGNORE so retries are safe).
func (s *SQLiteStore) InsertDeliveryDestination(dest *DeliveryDestination) error {
	s.observeDBOperation(true)
	if dest.DestinationID == "" || dest.Provider == "" {
		return fmt.Errorf("store: InsertDeliveryDestination: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if dest.CreatedAt == "" {
		dest.CreatedAt = now
	}
	if dest.UpdatedAt == "" {
		dest.UpdatedAt = now
	}
	if dest.ConfigurationJSON == "" {
		dest.ConfigurationJSON = "{}"
	}
	enabled := 0
	if dest.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO delivery_destinations
	 (destination_id, provider, external_destination_id, folder_id, name,
	  enabled, configuration_json, created_at, updated_at)
	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dest.DestinationID, dest.Provider,
		nullIfEmpty(dest.ExternalDestinationID),
		nullIfEmpty(dest.FolderID),
		dest.Name, enabled, dest.ConfigurationJSON,
		dest.CreatedAt, dest.UpdatedAt,
	)
	return err
}

// ListDeliveryDestinations returns all enabled destinations, optionally
// filtered by provider. Returns at most `limit` rows (zero means default).
//
// Opaque-mode SQL (Residuo 2 of YouTube → Social closure + migration 091):
// the legacy account_id / channel_id / language columns have been dropped
// from the delivery_destinations table. ExternalDestinationID is the
// canonical opaque reference (renamed from social_destination_id by
// migration 092, Residuo 4).
func (s *SQLiteStore) ListDeliveryDestinations(provider string, limit int) ([]DeliveryDestination, error) {
	s.observeDBOperation(false)
	if limit <= 0 {
		limit = 200
	}
	query := `SELECT destination_id, provider, COALESCE(external_destination_id, ''),
	                 COALESCE(folder_id, ''),
	                 COALESCE(name, ''),
	                 enabled, COALESCE(configuration_json, ''),
	                 created_at, updated_at
	          FROM delivery_destinations`
	args := []interface{}{}
	if provider != "" {
		query += ` WHERE provider = ? AND enabled = 1`
		args = append(args, provider)
	} else {
		query += ` WHERE enabled = 1`
	}
	query += ` ORDER BY created_at ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeliveryDestination
	for rows.Next() {
		var d DeliveryDestination
		var enabledInt int
		if err := rows.Scan(&d.DestinationID, &d.Provider, &d.ExternalDestinationID,
			&d.FolderID,
			&d.Name,
			&enabledInt, &d.ConfigurationJSON,
			&d.CreatedAt, &d.UpdatedAt); err != nil {
			continue
		}
		d.Enabled = enabledInt != 0
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDeliveryDestinationByExternalID returns a destination by its
// opaque InstaEdit identifier. This is the lookup path used by the
// InstaEdit BFF when creating a job, because the BFF only knows the
// external_destination_id, not the internal Velox destination_id.
func (s *SQLiteStore) GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*DeliveryDestination, error) {
	s.observeDBOperation(false)
	row := s.db.QueryRowContext(ctx,
		`SELECT destination_id, provider, COALESCE(external_destination_id, ''),
		        COALESCE(folder_id, ''),
		        COALESCE(name, ''),
		        enabled, COALESCE(configuration_json, ''),
		        created_at, updated_at
		 FROM delivery_destinations WHERE external_destination_id = ?`, externalID)
	var d DeliveryDestination
	var enabledInt int
	err := row.Scan(&d.DestinationID, &d.Provider, &d.ExternalDestinationID,
		&d.FolderID,
		&d.Name,
		&enabledInt, &d.ConfigurationJSON,
		&d.CreatedAt, &d.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrDeliveryNoRow
	}
	if err != nil {
		return nil, err
	}
	d.Enabled = enabledInt != 0
	return &d, nil
}

// DeliveryDestinationStatus is the 3-state verdict returned by
// BatchDeliveryDestinationsStatus. It exists so the handler-layer
// pre-flight can distinguish the two failure modes that §0.3.4
// item 4 of the runbook splits (Velox-side enabled=false vs.
// an upstream InstaEdit-side verdict) — the previous 2-state
// batched helper collapsed both into a single `false` and made
// operator dashboards unable to disambiguate the remediation.
type DeliveryDestinationStatus int

const (
	// DeliveryDestinationNotFound indicates the destination_id is
	// not present in delivery_destinations. The producer picked
	// (or fabricated) an unknown id; handler emits 422
	// target_error_code=DESTINATION_NOT_FOUND.
	DeliveryDestinationNotFound DeliveryDestinationStatus = iota
	// DeliveryDestinationDisabled indicates the row exists but
	// enabled = 0. Handler emits 422
	// target_error_code=BLOCKED_VELOX_DISABLED — distinct from
	// the catalog-sourced BLOCKED_NO_PUBLISHABLE_CHANNEL.
	DeliveryDestinationDisabled
	// DeliveryDestinationEnabled indicates the row exists and
	// enabled = 1. Handler allows the enqueue.
	DeliveryDestinationEnabled
)

// String returns the canonical wire-format name for use in JSON
// envelopes. Stable contract: the lowercase strings are the
// accepted values for `details[].status` in the §0.3.4 split
// response.
func (s DeliveryDestinationStatus) String() string {
	switch s {
	case DeliveryDestinationNotFound:
		return "not_found"
	case DeliveryDestinationDisabled:
		return "disabled"
	case DeliveryDestinationEnabled:
		return "enabled"
	default:
		return "unknown"
	}
}

// BatchDeliveryDestinationsStatus replaces the previous 2-state
// BatchDeliveryDestinationsExistAndEnabled helper. Each
// destination_id is looked up exactly once and bucketed into
// NOT_FOUND / DISABLED / ENABLED. The 2-state helper collapsed
// "row missing" and "row present-but-disabled" into the same
// `false`, which made it impossible for the POST /api/v1/jobs
// pre-flight to surface a distinct target_error_code for the
// §0.3.4 item 4 split (BLOCKED_VELOX_DISABLED for Velox-side
// enabled=false vs. catalog-sourced BLOCKED_NO_PUBLISHABLE_CHANNEL).
//
// Inputs are deduplicated + trimmed before SQL. IDs the caller
// supplied but the table did not return are explicitly bucketed
// as NOT_FOUND (rather than identically mapped via map presence)
// so call sites can distinguish the buckets with a single map
// lookup per id.
//
// Used by HTTP handlers (SubmitJob, future CreatorPush
// counterpart) that need to verify delivery_plan destinations
// BEFORE delegating to the resolver. The reason this lookup
// lives OUTSIDE the enqueue package is the file's
// "intentionally self-contained (no store import)" contract in
// DataServer/internal/jobs/enqueue/delivery_plan_validator.go,
// which keeps shape-only validation in the enqueue package and
// pushes existence + enabled checks up to the consumer that
// already owns a store handle.
//
// Multi-id SQL: SQLite's SQLITE_MAX_VARIABLE_NUMBER defaults to
// 999 (compile-time constant), so the helper chunks requests
// into batches of 500 placeholders — well under the floor
// across SQLite builds (3.32+ defaults to 32766, but we keep
// the chunk conservative).
//
// Errors:
//   - sql driver failure during QueryContext → wrapped error
//     returned, caller should fail-closed (handler surfaces 500
//     store_failure).
//   - rows.Scan / rows.Err failures → wrapped error returned.
//
// Empty input: returns an empty map and nil error; no SQL is
// issued.
//
// Mirrors the policy of the in-tx `validateDeliveryDestinationTx`
// (DataServer/internal/store/atomic_job_task.go) so the
// handler-side pre-check and the atomic write agree on the
// existence + enabled invariant.
func (s *SQLiteStore) BatchDeliveryDestinationsStatus(ctx context.Context, ids []string) (map[string]DeliveryDestinationStatus, error) {
	s.observeDBOperation(false)
	out := make(map[string]DeliveryDestinationStatus, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	// Deduplicate + trim. SQLite's IN clause tolerates duplicates
	// (slower) but the de-dup here keeps the placeholder set minimal
	// for the common case where a plan has repeated destinations.
	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, id := range ids {
		tid := strings.TrimSpace(id)
		if tid == "" {
			continue
		}
		if _, dup := seen[tid]; dup {
			continue
		}
		seen[tid] = struct{}{}
		unique = append(unique, tid)
	}
	if len(unique) == 0 {
		return out, nil
	}

	// First pass: collect (destination_id, enabled) for every id
	// the caller supplied, regardless of enabled. This is what
	// makes the 3-state verdict possible: enabling filtering on
	// `enabled = 1` here (as the 2-state helper did) would drop
	// the rows we need to bucket as DISABLED.
	const chunkSize = 500
	for start := 0; start < len(unique); start += chunkSize {
		end := start + chunkSize
		if end > len(unique) {
			end = len(unique)
		}
		chunk := unique[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		// NOTE: no `AND enabled = 1` filter. We bucket into
		// DISABLED based on the enabled column in the result set.
		query := `SELECT destination_id, enabled FROM delivery_destinations` +
			` WHERE destination_id IN (` + placeholders + `)`
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("store: BatchDeliveryDestinationsStatus: query: %w", err)
		}
		for rows.Next() {
			var id string
			var enabledInt int
			if err := rows.Scan(&id, &enabledInt); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: BatchDeliveryDestinationsStatus: scan: %w", err)
			}
			if enabledInt == 1 {
				out[id] = DeliveryDestinationEnabled
			} else {
				out[id] = DeliveryDestinationDisabled
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("store: BatchDeliveryDestinationsStatus: rows: %w", err)
		}
	}

	// Populate NotFound for every unique id NOT in the result set
	// so the caller can distinguish the three buckets with a
	// single map lookup per id.
	for _, id := range unique {
		if _, ok := out[id]; !ok {
			out[id] = DeliveryDestinationNotFound
		}
	}
	return out, nil
}

// BatchDeliveryDestinations returns a point-in-time local registry snapshot
// for the requested destination IDs. It deliberately returns rows regardless
// of enabled state so callers can distinguish a disabled member from a
// missing member without issuing per-member queries. IDs absent from the
// registry are omitted from the result map.
func (s *SQLiteStore) BatchDeliveryDestinations(ctx context.Context, ids []string) (map[string]*DeliveryDestination, error) {
	s.observeDBOperation(false)
	out := make(map[string]*DeliveryDestination, len(ids))
	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, rawID := range ids {
		id := strings.TrimSpace(rawID)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return out, nil
	}

	// Keep every chunk inside one read transaction. SQLite establishes the
	// read snapshot on the first SELECT and holds it until commit, so a large
	// group cannot observe mixed enabled/configuration state if a destination
	// changes while the snapshot is being collected.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("store: BatchDeliveryDestinations: begin snapshot: %w", err)
	}
	defer tx.Rollback()

	const chunkSize = 500
	for start := 0; start < len(unique); start += chunkSize {
		end := start + chunkSize
		if end > len(unique) {
			end = len(unique)
		}
		chunk := unique[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		query := `SELECT destination_id, provider, COALESCE(external_destination_id, ''),
		                 COALESCE(folder_id, ''), COALESCE(name, ''), enabled,
		                 COALESCE(configuration_json, ''), created_at, updated_at
		          FROM delivery_destinations WHERE destination_id IN (` + placeholders + `)`
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("store: BatchDeliveryDestinations: query: %w", err)
		}
		for rows.Next() {
			var d DeliveryDestination
			var enabledInt int
			if err := rows.Scan(&d.DestinationID, &d.Provider, &d.ExternalDestinationID,
				&d.FolderID, &d.Name, &enabledInt, &d.ConfigurationJSON,
				&d.CreatedAt, &d.UpdatedAt); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: BatchDeliveryDestinations: scan: %w", err)
			}
			d.Enabled = enabledInt != 0
			copy := d
			out[d.DestinationID] = &copy
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: BatchDeliveryDestinations: rows: %w", err)
		}
		rows.Close()
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: BatchDeliveryDestinations: commit snapshot: %w", err)
	}
	return out, nil
}

// GetDeliveryDestination returns a single destination by id, or
// ErrDeliveryNoRow when missing (sql.ErrNoRows is normalized).
//
// Opaque-mode SQL (Residuo 2 of YouTube → Social closure + migration 091):
// the legacy account_id / channel_id / language columns have been dropped
// from the delivery_destinations table. ExternalDestinationID is the
// canonical opaque reference (renamed from social_destination_id by
// migration 092, Residuo 4).
func (s *SQLiteStore) GetDeliveryDestination(ctx context.Context, destID string) (*DeliveryDestination, error) {
	s.observeDBOperation(false)
	row := s.db.QueryRowContext(ctx,
		`SELECT destination_id, provider, COALESCE(external_destination_id, ''),
		        COALESCE(folder_id, ''),
		        COALESCE(name, ''),
		        enabled, COALESCE(configuration_json, ''),
		        created_at, updated_at
		 FROM delivery_destinations WHERE destination_id = ?`, destID)
	var d DeliveryDestination
	var enabledInt int
	err := row.Scan(&d.DestinationID, &d.Provider, &d.ExternalDestinationID,
		&d.FolderID,
		&d.Name,
		&enabledInt, &d.ConfigurationJSON,
		&d.CreatedAt, &d.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrDeliveryNoRow
	}
	if err != nil {
		return nil, err
	}
	d.Enabled = enabledInt != 0
	return &d, nil
}
