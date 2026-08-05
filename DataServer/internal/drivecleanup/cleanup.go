// Package drivecleanup contains the reviewed, fail-closed maintenance flow for
// removing duplicate Google Drive delivery objects.
package drivecleanup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/audittrail"
	"velox-server/internal/store"
)

// DriveClient is deliberately narrower than the production Drive service so
// the cleanup flow can be tested without network access.
type DriveClient interface {
	GetFileMetadata(context.Context, string) (*FileMetadata, error)
	DeleteFile(context.Context, string) error
}

// FileMetadata is the only remote state needed by the safety check.
type FileMetadata struct {
	ID      string `json:"id"`
	Trashed bool   `json:"trashed,omitempty"`
}

// Manifest is the persisted operator input. Records are deterministic; the
// envelope timestamp identifies when the snapshot was generated.
type Manifest struct {
	GeneratedAt string                          `json:"generated_at"`
	Mode        string                          `json:"mode"`
	Records     []store.DuplicateDeliveryRecord `json:"records"`
}

type Result struct {
	Mode             string `json:"mode"`
	Records          int    `json:"records"`
	CanonicalChecked int    `json:"canonical_checked"`
	Deleted          int    `json:"deleted"`
	AlreadyAbsent    int    `json:"already_absent"`
	Skipped          int    `json:"skipped"`
}

// ParseManifest validates the safety-critical identity fields before any
// remote operation is possible.
func ParseManifest(raw []byte) (Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse duplicate manifest: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	if strings.TrimSpace(manifest.GeneratedAt) == "" {
		return errors.New("manifest generated_at is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, manifest.GeneratedAt); err != nil {
		return fmt.Errorf("manifest generated_at must be RFC3339: %w", err)
	}
	seenDuplicates := make(map[string]struct{}, len(manifest.Records))
	seenCorrect := make(map[string]struct{}, len(manifest.Records))
	seenDeliveries := make(map[string]struct{}, len(manifest.Records))
	for i, record := range manifest.Records {
		correctID := strings.TrimSpace(record.DriveFileIDCorrect)
		duplicateID := strings.TrimSpace(record.DriveFileIDDuplicate)
		if correctID == "" || duplicateID == "" {
			return fmt.Errorf("manifest record %d: both correct and duplicate Drive IDs are required", i)
		}
		if correctID != record.DriveFileIDCorrect || duplicateID != record.DriveFileIDDuplicate {
			return fmt.Errorf("manifest record %d: Drive IDs must not contain surrounding whitespace", i)
		}
		if correctID == duplicateID {

			return fmt.Errorf("manifest record %d: correct and duplicate Drive IDs must differ", i)
		}
		if record.CanonicalDeliveryID == record.DeliveryID {
			return fmt.Errorf("manifest record %d: canonical and duplicate delivery IDs must differ", i)
		}
		if _, exists := seenDeliveries[record.DeliveryID]; exists {
			return fmt.Errorf("manifest record %d: delivery ID %q appears more than once", i, record.DeliveryID)
		}
		seenDeliveries[record.DeliveryID] = struct{}{}
		if _, exists := seenDuplicates[duplicateID]; exists {
			return fmt.Errorf("manifest record %d: duplicate Drive ID %q appears more than once", i, duplicateID)
		}
		if _, exists := seenCorrect[duplicateID]; exists {
			return fmt.Errorf("manifest record %d: duplicate Drive ID %q is canonical in another record", i, duplicateID)
		}
		if _, exists := seenDuplicates[correctID]; exists {
			return fmt.Errorf("manifest record %d: correct Drive ID %q is already marked duplicate", i, correctID)
		}
		seenDuplicates[duplicateID] = struct{}{}
		seenCorrect[correctID] = struct{}{}
		if strings.TrimSpace(record.DeliveryID) == "" || strings.TrimSpace(record.ArtifactID) == "" || strings.TrimSpace(record.DestinationID) == "" || strings.TrimSpace(record.JobID) == "" || strings.TrimSpace(record.CanonicalDeliveryID) == "" || strings.TrimSpace(record.DuplicateStatus) == "" {
			return fmt.Errorf("manifest record %d: job, canonical delivery, delivery, artifact, and destination identities are required", i)
		}
		for field, value := range map[string]string{"created_at": record.CreatedAt, "duplicate_created_at": record.DuplicateCreatedAt} {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("manifest record %d: %s is required", i, field)
			}
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				return fmt.Errorf("manifest record %d: %s must be RFC3339: %w", i, field, err)
			}
		}
	}
	return nil
}

// Apply executes a manifest. Both modes verify that the canonical Drive file
// exists and is not trashed; dry-run never deletes a remote object. Apply then
// deletes only the duplicate ID. A Drive 404 during deletion is treated as
// already complete.
func Apply(ctx context.Context, client DriveClient, audit auditRepository, verifier manifestVerifier, manifest Manifest, dryRun bool, actor string, now time.Time) (Result, error) {
	if err := validateManifest(manifest); err != nil {
		return Result{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "velox-admin"
	}
	if client == nil && len(manifest.Records) > 0 {
		return Result{}, errors.New("drive duplicate cleanup: Drive client is required for canonical verification")
	}
	if audit == nil {
		return Result{}, errors.New("drive duplicate cleanup: audit repository is required")
	}
	if verifier == nil && len(manifest.Records) > 0 {
		return Result{}, errors.New("drive duplicate cleanup: store manifest verifier is required")
	}
	result := Result{Mode: "dry-run", Records: len(manifest.Records)}
	if !dryRun {
		result.Mode = "apply"
	}
	for _, record := range manifest.Records {
		if verifier != nil {
			if err := verifier.VerifyDuplicateDeliveryRecord(ctx, record); err != nil {
				return result, fmt.Errorf("verify manifest record delivery %q against database: %w", record.DeliveryID, err)
			}
		}
		if !dryRun {
			if err := appendAudit(ctx, audit, record, actor, now, "DRIVE_DUPLICATE_CLEANUP_PLANNED", "apply", map[string]string{"manifest_generated_at": manifest.GeneratedAt, "mode": "apply"}); err != nil {
				return result, err
			}
		}
		metadata, err := client.GetFileMetadata(ctx, record.DriveFileIDCorrect)
		if err != nil {
			// Canonical absence or any verification error is fail-closed:
			// never remove the duplicate when the backup cannot be proven.
			return result, fmt.Errorf("verify canonical Drive file %q for delivery %q: %w", record.DriveFileIDCorrect, record.DeliveryID, err)
		}
		if metadata == nil || strings.TrimSpace(metadata.ID) != strings.TrimSpace(record.DriveFileIDCorrect) {
			return result, fmt.Errorf("verify canonical Drive file %q for delivery %q: metadata ID mismatch", record.DriveFileIDCorrect, record.DeliveryID)
		}
		if metadata.Trashed {
			return result, fmt.Errorf("verify canonical Drive file %q for delivery %q: canonical file is trashed", record.DriveFileIDCorrect, record.DeliveryID)
		}
		duplicateMetadata, duplicateErr := client.GetFileMetadata(ctx, record.DriveFileIDDuplicate)
		if duplicateErr != nil && !isNotFound(duplicateErr) {
			return result, fmt.Errorf("verify duplicate Drive file %q for delivery %q: %w", record.DriveFileIDDuplicate, record.DeliveryID, duplicateErr)
		}
		if duplicateErr == nil && duplicateMetadata == nil {
			return result, fmt.Errorf("verify duplicate Drive file %q for delivery %q: metadata unavailable without a not-found error", record.DriveFileIDDuplicate, record.DeliveryID)
		}
		if duplicateMetadata != nil && strings.TrimSpace(duplicateMetadata.ID) != strings.TrimSpace(record.DriveFileIDDuplicate) {
			return result, fmt.Errorf("verify duplicate Drive file %q for delivery %q: metadata ID mismatch", record.DriveFileIDDuplicate, record.DeliveryID)
		}
		result.CanonicalChecked++
		if dryRun {
			if err := appendAudit(ctx, audit, record, actor, now, "DRIVE_DUPLICATE_CLEANUP_PLANNED", "dry-run-verified", map[string]string{"manifest_generated_at": manifest.GeneratedAt, "mode": "dry-run"}); err != nil {
				return result, err
			}
			result.Skipped++
			continue
		}

		err = client.DeleteFile(ctx, record.DriveFileIDDuplicate)
		outcome := "deleted"
		if err != nil {
			if !isNotFound(err) {
				return result, fmt.Errorf("delete duplicate Drive file %q for delivery %q: %w", record.DriveFileIDDuplicate, record.DeliveryID, err)
			}
			outcome = "already_absent"
			result.AlreadyAbsent++
		} else {
			result.Deleted++
		}
		if err := appendAudit(ctx, audit, record, actor, now, "DRIVE_DUPLICATE_DELETED", outcome, map[string]string{"manifest_generated_at": manifest.GeneratedAt, "mode": "apply"}); err != nil {
			return result, err
		}
	}
	return result, nil
}

type auditRepository interface {
	AppendAuditEventIdempotent(context.Context, audittrail.Event) error
}

type manifestVerifier interface {
	VerifyDuplicateDeliveryRecord(context.Context, store.DuplicateDeliveryRecord) error
}

func appendAudit(ctx context.Context, audit auditRepository, record store.DuplicateDeliveryRecord, actor string, now time.Time, action, outcome string, extra map[string]string) error {
	metadata := map[string]any{
		"job_id":                  record.JobID,
		"artifact_id":             record.ArtifactID,
		"delivery_id":             record.DeliveryID,
		"canonical_delivery_id":   record.CanonicalDeliveryID,
		"drive_file_id_correct":   record.DriveFileIDCorrect,
		"drive_file_id_duplicate": record.DriveFileIDDuplicate,
		"destination_id":          record.DestinationID,
		"created_at":              record.CreatedAt,
		"duplicate_created_at":    record.DuplicateCreatedAt,
		"duplicate_status":        record.DuplicateStatus,
		"manifest_action_outcome": outcome,
		"operation_timestamp":     now.UTC().Format(time.RFC3339Nano),
	}
	for key, value := range extra {
		metadata[key] = value
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	identityMode := extra["mode"]
	manifestGeneratedAt := extra["manifest_generated_at"]
	hash := sha256.Sum256([]byte(action + "\x00" + outcome + "\x00" + identityMode + "\x00" + manifestGeneratedAt + "\x00" + record.DeliveryID + "\x00" + record.DriveFileIDDuplicate))
	return audit.AppendAuditEventIdempotent(ctx, audittrail.Event{
		ID:           "drive-duplicate-" + hex.EncodeToString(hash[:]),
		OccurredAt:   now.UTC(),
		ActorType:    "operator",
		ActorID:      actor,
		Action:       action,
		ResourceType: "drive_file",
		ResourceID:   record.DriveFileIDDuplicate,
		BeforeHash:   hashText(record.DriveFileIDDuplicate),
		AfterHash:    hashText(outcome),
		MetadataJSON: string(encoded),
	})
}

func hashText(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func isNotFound(err error) bool {
	var statusErr interface{ HTTPStatus() int }
	if errors.As(err, &statusErr) {
		return statusErr.HTTPStatus() == 404
	}
	// Keep the cleanup fail-closed for arbitrary errors; only accept the
	// explicit status-bearing Drive API error as idempotent absence.
	return false
}
