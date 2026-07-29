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
