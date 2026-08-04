package workercache

import (
	"encoding/json"
	"log"
	"time"
)

// CleanerAssetMetadata enriches a cache entry with the semantic information
// that is not part of the local cache index. The master snapshot and worker
// callers may provide this metadata when they know the asset's role or the
// number of future job references.
type CleanerAssetMetadata struct {
	Role                 string
	FutureReferenceCount int
}

// CleanerAuditEvent is emitted once for every cache-entry decision made by a
// cleaner pass. AssetKey is the canonical Drive/file cache key; Lease is the
// active job lease when one exists. With multiple leases, Lease is one
// representative job ID and ActiveLeaseCount is the authoritative total.
// FutureReferenceCount is optional metadata and is zero when the caller has
// no richer reference projection.
type CleanerAuditEvent struct {
	Event                string    `json:"event"`
	AssetKey             string    `json:"asset_key"`
	Role                 string    `json:"role"`
	Decision             string    `json:"decision"`
	Reason               string    `json:"reason"`
	Lease                string    `json:"lease"`
	ActiveLeaseCount     int       `json:"active_lease_count"`
	FutureReferenceCount int       `json:"future_reference_count"`
	Timestamp            time.Time `json:"timestamp"`
	SizeBytes            int64     `json:"size_bytes"`
	LastUsedAt           time.Time `json:"last_used_at"`
}

// CleanerAuditLogger receives structured decisions from Cleanup and
// CleanupWithPolicy. A function type keeps the hook lightweight and allows
// workers, tests, and operators to choose their own structured sink.
type CleanerAuditLogger func(CleanerAuditEvent)

const cleanerAuditEventName = "CACHE_EVICTION_DECISION"

func cleanerAuditMetadata(assetKey string, metadata map[string]CleanerAssetMetadata) (string, int) {
	role := "unknown"
	futureRefs := 0
	if metadata != nil {
		if m, ok := metadata[assetKey]; ok {
			if m.Role != "" {
				role = m.Role
			}
			if m.FutureReferenceCount > 0 {
				futureRefs = m.FutureReferenceCount
			}
		}
	}
	return role, futureRefs
}

func emitCleanerAudit(logger CleanerAuditLogger, entry Entry, metadata map[string]CleanerAssetMetadata, decision, reason string, at time.Time) {
	role, futureRefs := cleanerAuditMetadata(entry.DriveFileID, metadata)
	event := CleanerAuditEvent{
		Event:                cleanerAuditEventName,
		AssetKey:             entry.DriveFileID,
		Role:                 role,
		Decision:             decision,
		Reason:               reason,
		Lease:                entry.ActiveJobID,
		ActiveLeaseCount:     entry.ActiveLeaseCount,
		FutureReferenceCount: futureRefs,
		Timestamp:            at.UTC(),
		SizeBytes:            entry.SizeBytes,
		LastUsedAt:           entry.LastUsedAt.UTC(),
	}
	if logger != nil {
		logger(event)
		return
	}
	// The default sink is structured JSON so every production decision remains
	// observable even when the caller uses the legacy API without injection.
	payload, err := json.Marshal(event)
	if err == nil {
		log.Printf("%s", payload)
	}
}
