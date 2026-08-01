package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// UpsertSyncedDeliveryDestination creates or refreshes a destination discovered
// from InstaEdit. The external destination ID is opaque and authoritative.
//
// Unlike InsertDeliveryDestination (operator/bootstrap idempotency via INSERT
// OR IGNORE), this method intentionally updates enabled/name/configuration on
// conflict so a channel disabled or re-authorized in InstaEdit is reflected in
// Velox on the next catalog refresh.
func (s *SQLiteStore) UpsertSyncedDeliveryDestination(ctx context.Context, dest DeliveryDestination) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store: UpsertSyncedDeliveryDestination: store not configured")
	}
	if strings.TrimSpace(dest.DestinationID) == "" {
		return fmt.Errorf("store: UpsertSyncedDeliveryDestination: destination_id is required")
	}
	if strings.TrimSpace(dest.Provider) == "" {
		return fmt.Errorf("store: UpsertSyncedDeliveryDestination: provider is required")
	}
	if strings.TrimSpace(dest.ExternalDestinationID) == "" {
		return fmt.Errorf("store: UpsertSyncedDeliveryDestination: external_destination_id is required")
	}
	if strings.TrimSpace(dest.ConfigurationJSON) == "" {
		dest.ConfigurationJSON = "{}"
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if dest.CreatedAt == "" {
		dest.CreatedAt = now
	}
	dest.UpdatedAt = now
	enabled := 0
	if dest.Enabled {
		enabled = 1
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO delivery_destinations
			(destination_id, provider, external_destination_id, folder_id, name,
			 enabled, configuration_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(destination_id) DO UPDATE SET
			provider = excluded.provider,
			external_destination_id = excluded.external_destination_id,
			name = excluded.name,
			enabled = excluded.enabled,
			configuration_json = excluded.configuration_json,
			updated_at = excluded.updated_at`,
		dest.DestinationID,
		dest.Provider,
		dest.ExternalDestinationID,
		nullIfEmpty(dest.FolderID),
		dest.Name,
		enabled,
		dest.ConfigurationJSON,
		dest.CreatedAt,
		dest.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: UpsertSyncedDeliveryDestination: %w", err)
	}
	return nil
}

// SyncSyncedDeliveryDestinations atomically upserts one complete catalog
// snapshot and disables stale catalog-managed destinations. The transaction
// prevents a partial refresh from leaving a mixture of new and old channel
// state when one row fails during synchronization.
func (s *SQLiteStore) SyncSyncedDeliveryDestinations(ctx context.Context, destinations []DeliveryDestination) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store: SyncSyncedDeliveryDestinations: store not configured")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: SyncSyncedDeliveryDestinations: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	ids := make([]string, 0, len(destinations))
	for _, dest := range destinations {
		if strings.TrimSpace(dest.DestinationID) == "" || strings.TrimSpace(dest.Provider) == "" || strings.TrimSpace(dest.ExternalDestinationID) == "" {
			return fmt.Errorf("store: SyncSyncedDeliveryDestinations: destination_id, provider, and external_destination_id are required")
		}
		if strings.TrimSpace(dest.ConfigurationJSON) == "" {
			dest.ConfigurationJSON = "{}"
		}
		createdAt := dest.CreatedAt
		if createdAt == "" {
			createdAt = now
		}
		enabled := 0
		if dest.Enabled {
			enabled = 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO delivery_destinations
				(destination_id, provider, external_destination_id, folder_id, name,
				 enabled, configuration_json, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(destination_id) DO UPDATE SET
				provider = excluded.provider,
				external_destination_id = excluded.external_destination_id,
				name = excluded.name,
				enabled = excluded.enabled,
				configuration_json = excluded.configuration_json,
				updated_at = excluded.updated_at`,
			dest.DestinationID, dest.Provider, dest.ExternalDestinationID,
			nullIfEmpty(dest.FolderID), dest.Name, enabled, dest.ConfigurationJSON,
			createdAt, now); err != nil {
			return fmt.Errorf("store: SyncSyncedDeliveryDestinations: upsert: %w", err)
		}
		ids = append(ids, strings.TrimSpace(dest.DestinationID))
	}

	args := []any{now}
	query := `UPDATE delivery_destinations
		SET enabled = 0, updated_at = ?
		WHERE provider = 'social_gateway'
		  AND json_extract(configuration_json, '$.source') = 'instaedit_catalog'`
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i := range ids {
			placeholders[i] = "?"
			args = append(args, ids[i])
		}
		query += " AND destination_id NOT IN (" + strings.Join(placeholders, ",") + ")"
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("store: SyncSyncedDeliveryDestinations: disable stale: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: SyncSyncedDeliveryDestinations: commit: %w", err)
	}
	return nil
}

// DisableMissingSyncedDeliveryDestinations disables catalog-managed social
// destinations that were not returned by the latest InstaEdit catalog.
// Historical delivery rows remain intact; only future submissions are
// prevented from selecting a stale channel.
func (s *SQLiteStore) DisableMissingSyncedDeliveryDestinations(ctx context.Context, destinationIDs []string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store: DisableMissingSyncedDeliveryDestinations: store not configured")
	}
	args := make([]any, 0, len(destinationIDs))
	placeholders := make([]string, 0, len(destinationIDs))
	for _, id := range destinationIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}

	query := `UPDATE delivery_destinations
		SET enabled = 0, updated_at = ?
		WHERE provider = 'social_gateway'
		  AND json_extract(configuration_json, '$.source') = 'instaedit_catalog'`
	args = append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)
	if len(placeholders) > 0 {
		query += " AND destination_id NOT IN (" + strings.Join(placeholders, ",") + ")"
	}
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("store: DisableMissingSyncedDeliveryDestinations: %w", err)
	}
	return nil
}
