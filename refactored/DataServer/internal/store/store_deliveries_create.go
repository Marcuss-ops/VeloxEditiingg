// Package store / store_deliveries_create.go
//
// Methods for inserting PENDING job_deliveries when an artifact becomes READY.
// Bridges the legacy delivery_targets (created at enqueue time) to the new
// job_deliveries × delivery_destinations model that the DeliveryRunner consumes.
package store

import (
	"context"
	"fmt"
	"log"
	"time"
)

// ErrNoDeliveryDestinations is returned when no enabled destination matches.
var ErrNoDeliveryDestinations = fmt.Errorf("deliveries: no matching destination found")

// InsertJobDeliveriesForArtifact creates PENDING job_delivery rows for a
// READY artifact by matching the artifact's job's delivery_targets (legacy)
// against enabled delivery_destinations.
//
// For each legacy delivery_target of type "drive" or "youtube" that is still
// in "pending"/"scheduled" status, this method finds or expects a matching
// delivery_destination and creates a PENDING job_delivery row so the
// DeliveryRunner can claim and dispatch it.
//
// Returns the number of job_deliveries created.
func (s *SQLiteStore) InsertJobDeliveriesForArtifact(ctx context.Context, artifactID, jobID string) (int, error) {
	if artifactID == "" || jobID == "" {
		return 0, fmt.Errorf("store: InsertJobDeliveriesForArtifact: missing artifact_id or job_id")
	}

	// Read legacy delivery_targets for this job.
	targets, err := s.GetDeliveryTargetsByJob(jobID)
	if err != nil {
		return 0, fmt.Errorf("store: get delivery targets for %s: %w", jobID, err)
	}
	if len(targets) == 0 {
		return 0, nil // no targets configured
	}

	// Read enabled delivery_destinations for provider matching.
	destinations, err := s.ListDeliveryDestinations("", 200)
	if err != nil {
		return 0, fmt.Errorf("store: list delivery destinations: %w", err)
	}

	created := 0
	now := time.Now().UTC().Format(time.RFC3339)

	for _, tgt := range targets {
		if tgt.Status != "pending" && tgt.Status != "scheduled" {
			continue
		}

		// Map provider name from target type.
		provider := tgt.TargetType
		if provider == "" {
			continue
		}

		// Find or use the matching delivery_destination.
		destID := s.matchDestination(provider, tgt.Config, destinations)
		if destID == "" {
			log.Printf("[DELIVERY] No enabled delivery_destination for %s target %d (provider=%s) — skipping auto-insert",
				jobID, tgt.ID, provider)
			continue
		}

		// Generate a stable delivery ID from artifact + destination.
		deliveryID := fmt.Sprintf("del_%s_%s", artifactID, destID)

		// Check if job_delivery already exists (idempotency).
		existing, err := s.GetJobDelivery(ctx, deliveryID)
		if err == nil && existing != nil {
			continue // already created
		}

		jd := &JobDelivery{
			DeliveryID:             deliveryID,
			ArtifactID:             artifactID,
			DestinationID:          destID,
			LegacyDeliveryTargetID: int64(tgt.ID),
			Status:                 "PENDING",
			IdempotencyKey:         fmt.Sprintf("%s-%s", artifactID, destID),
			CreatedAt:              now,
			UpdatedAt:              now,
		}

		if err := s.InsertJobDelivery(jd); err != nil {
			log.Printf("[DELIVERY] Failed to insert job_delivery for %s target %d: %v", jobID, tgt.ID, err)
			continue
		}

		log.Printf("[DELIVERY] Created job_delivery %s for artifact %s → destination %s (provider=%s)",
			deliveryID, artifactID, destID, provider)
		created++
	}

	return created, nil
}

// GetJobDelivery retrieves a single job_delivery by ID.
func (s *SQLiteStore) GetJobDelivery(ctx context.Context, deliveryID string) (*JobDelivery, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT delivery_id, artifact_id, destination_id,
		        COALESCE(legacy_delivery_target_id, 0), status,
		        COALESCE(idempotency_key,''), COALESCE(remote_id,''),
		        COALESCE(remote_url,''),
		        created_at, updated_at
		 FROM job_deliveries WHERE delivery_id = ?`, deliveryID)
	var jd JobDelivery
	var legacyID interface{}
	var idempotencyKey, remoteID, remoteURL string
	err := row.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
		&legacyID, &jd.Status, &idempotencyKey, &remoteID,
		&remoteURL, &jd.CreatedAt, &jd.UpdatedAt)
	if err != nil {
		return nil, err
	}
	jd.IdempotencyKey = idempotencyKey
	jd.RemoteID = remoteID
	jd.RemoteURL = remoteURL
	if legacyID, ok := legacyID.(int64); ok {
		jd.LegacyDeliveryTargetID = legacyID
	}
	return &jd, nil
}

// matchDestination finds an enabled delivery_destination matching a provider
// and its config (by folder_id for drive, channel_id for youtube).
// Returns empty string if no match.
func (s *SQLiteStore) matchDestination(provider, configJSON string, destinations []DeliveryDestination) string {
	if configJSON == "" {
		// Fallback: find first enabled destination for this provider.
		for _, d := range destinations {
			if d.Provider == provider && d.Enabled {
				return d.DestinationID
			}
		}
		return ""
	}

	cfg, err := ParseTargetConfig(configJSON)
	if err != nil {
		// Fallback: find first enabled destination for this provider.
		for _, d := range destinations {
			if d.Provider == provider && d.Enabled {
				return d.DestinationID
			}
		}
		return ""
	}

	for _, d := range destinations {
		if !d.Enabled || d.Provider != provider {
			continue
		}
		switch provider {
		case "drive":
			if d.FolderID != "" && d.FolderID == cfg.FolderID {
				return d.DestinationID
			}
		case "youtube":
			if d.ChannelID != "" && d.ChannelID == cfg.ChannelID {
				return d.DestinationID
			}
			if d.Language != "" && d.Language == cfg.Language {
				return d.DestinationID
			}
		}
	}

	// Fallback: first enabled destination for this provider.
	for _, d := range destinations {
		if d.Provider == provider && d.Enabled {
			return d.DestinationID
		}
	}

	return ""
}
