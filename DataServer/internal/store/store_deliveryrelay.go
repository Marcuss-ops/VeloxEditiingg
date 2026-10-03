// Package store / store_deliveryrelay.go
//
// Store-side implementation of the deliverystore.ProviderTimingSeeder
// cross-domain seam plus the canonical deliveryrelay session store used by
// the Drive stream relay. The SQL for drive_relay_sessions lives here because
// internal/store is the only unrestricted SQL gateway
// (scripts/ci/ratchet-sql.sh); providers must couple to rows, not SQL.
// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Read-only:    no
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"velox-server/internal/store/deliveryrelay"
)

// NewDeliveryRelaySessionStore exposes the canonical drive_relay_sessions
// persistence to bootstrap wiring.
func NewDeliveryRelaySessionStore(db *sql.DB) *deliveryrelay.SessionStore {
	return deliveryrelay.New(db)
}

// SeedDeliveryProviderTiming implements deliverystore.ProviderTimingSeeder.
// The store leaves (deliverystore) stay out of the delivery_attempts read
// side; this store-side seeder runs inside the caller's transaction and
// merges the upload timing fields into the latest attempt's result JSON. The
// transaction arrives as the generic any transport asserted back to *sql.Tx
// here; a foreign transaction type fails closed.
func (s *SQLiteStore) SeedDeliveryProviderTiming(ctx context.Context, txAny any, deliveryID string, uploadNetworkMS, uploadLocalBufferMS int64) error {
	tx, ok := txAny.(*sql.Tx)
	if !ok || tx == nil {
		return fmt.Errorf("store: SeedDeliveryProviderTiming requires a *sql.Tx transaction")
	}
	var attemptID int64
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT id, COALESCE(result, '{}') FROM delivery_attempts WHERE delivery_id=? ORDER BY id DESC LIMIT 1`,
		deliveryID).Scan(&attemptID, &raw)
	if err != nil {
		return fmt.Errorf("load latest delivery attempt timing result: %w", err)
	}
	result := make(map[string]json.RawMessage)
	if json.Unmarshal([]byte(raw), &result) != nil {
		result = make(map[string]json.RawMessage)
	}
	if result == nil {
		result = make(map[string]json.RawMessage)
	}
	meta, err := json.Marshal(map[string]int64{
		"upload_network_ms":      uploadNetworkMS,
		"upload_local_buffer_ms": uploadLocalBufferMS,
	})
	if err != nil {
		return fmt.Errorf("marshal delivery provider timing: %w", err)
	}
	result["provider_meta"] = meta
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal delivery attempt result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_attempts SET result=? WHERE id=? AND delivery_id=?`, string(encoded), attemptID, deliveryID); err != nil {
		return fmt.Errorf("persist delivery provider timing: %w", err)
	}
	return nil
}
