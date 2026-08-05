package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// DuplicateDeliveryRecord identifies one remote delivery ID that appears more
// than once for the same destination. The first-created delivery is reported
// as the canonical row; no remote object is deleted by this package.
type DuplicateDeliveryRecord struct {
	JobID                string `json:"job_id"`
	ArtifactID           string `json:"artifact_id"`
	DeliveryID           string `json:"delivery_id"`
	DriveFileIDCorrect   string `json:"drive_file_id_correct"`
	DriveFileIDDuplicate string `json:"drive_file_id_duplicate"`
	DestinationID        string `json:"destination_id"`
	CreatedAt            string `json:"created_at"`
	DuplicateCreatedAt   string `json:"duplicate_created_at"`
	CanonicalDeliveryID  string `json:"canonical_delivery_id"`
	DuplicateStatus      string `json:"duplicate_status"`
}

type duplicateDeliveryRow struct {
	jobID, artifactID, deliveryID string
	remoteID, destinationID       string
	createdAt, status             string
}

// VerifyDuplicateDeliveryRecord re-reads the canonical and duplicate delivery
// rows before a remote deletion. It intentionally lives behind the store API so
// cleanup cannot act on a stale or operator-edited manifest without proving
// that every identity and timestamp still matches the current database state.
func (s *SQLiteStore) VerifyDuplicateDeliveryRecord(ctx context.Context, record DuplicateDeliveryRecord) error {
	var (
		jobID, canonicalDeliveryID, canonicalRemoteID, canonicalDestinationID, canonicalCreatedAt           string
		duplicateDeliveryID, duplicateRemoteID, duplicateDestinationID, duplicateCreatedAt, duplicateStatus string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT a.job_id,
		       canonical.delivery_id, COALESCE(canonical.remote_id,''), canonical.destination_id, canonical.created_at,
		       duplicate.delivery_id, COALESCE(duplicate.remote_id,''), duplicate.destination_id, duplicate.created_at, duplicate.status
		FROM job_deliveries duplicate
		JOIN artifacts a ON a.id = duplicate.artifact_id
		JOIN delivery_destinations dd ON dd.destination_id = duplicate.destination_id
		JOIN job_deliveries canonical
		  ON canonical.delivery_id = ?
		 AND canonical.artifact_id = duplicate.artifact_id
		 AND canonical.destination_id = duplicate.destination_id
		WHERE duplicate.delivery_id = ?
		  AND duplicate.artifact_id = ?
		  AND duplicate.destination_id = ?
		  AND LOWER(COALESCE(dd.provider,'')) = 'drive'
		  AND duplicate.status = 'SUCCEEDED'
		  AND canonical.status = 'SUCCEEDED'
		  AND TRIM(COALESCE(canonical.remote_id,'')) <> ''
		  AND TRIM(COALESCE(duplicate.remote_id,'')) <> ''
		  AND TRIM(canonical.remote_id) <> TRIM(duplicate.remote_id)`,
		record.CanonicalDeliveryID, record.DeliveryID, record.ArtifactID, record.DestinationID,
	).Scan(
		&jobID,
		&canonicalDeliveryID, &canonicalRemoteID, &canonicalDestinationID, &canonicalCreatedAt,
		&duplicateDeliveryID, &duplicateRemoteID, &duplicateDestinationID, &duplicateCreatedAt, &duplicateStatus,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("duplicate manifest delivery rows are missing or no longer share artifact/destination")
		}
		return fmt.Errorf("verify duplicate manifest delivery rows: %w", err)
	}
	checks := []struct {
		name, got, want string
	}{
		{"job_id", jobID, record.JobID},
		{"artifact_id", record.ArtifactID, record.ArtifactID},
		{"canonical_delivery_id", canonicalDeliveryID, record.CanonicalDeliveryID},
		{"canonical_remote_id", canonicalRemoteID, record.DriveFileIDCorrect},
		{"canonical_destination_id", canonicalDestinationID, record.DestinationID},
		{"canonical_created_at", canonicalCreatedAt, record.CreatedAt},
		{"duplicate_delivery_id", duplicateDeliveryID, record.DeliveryID},
		{"duplicate_remote_id", duplicateRemoteID, record.DriveFileIDDuplicate},
		{"duplicate_destination_id", duplicateDestinationID, record.DestinationID},
		{"duplicate_created_at", duplicateCreatedAt, record.DuplicateCreatedAt},
		{"duplicate_status", duplicateStatus, record.DuplicateStatus},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.got) != strings.TrimSpace(check.want) {
			return fmt.Errorf("duplicate manifest %s mismatch: got %q want %q", check.name, check.got, check.want)
		}
	}
	return nil
}

// BuildDuplicateDeliveryManifest returns deterministic, audit-ready duplicate
// candidates. It groups by (artifact_id, destination_id), keeps the earliest row
// as canonical, and reports later rows as duplicates. This is intentionally
// dry-run only: callers must verify the canonical remote object before any
// separate deletion operation.
func (s *SQLiteStore) BuildDuplicateDeliveryManifest(ctx context.Context) ([]DuplicateDeliveryRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.job_id, jd.artifact_id, jd.delivery_id,
		       jd.remote_id, jd.destination_id, jd.created_at, jd.status
		FROM job_deliveries jd
		JOIN artifacts a ON a.id = jd.artifact_id
		JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id
		WHERE LOWER(COALESCE(dd.provider, '')) = 'drive'
		  AND jd.status = 'SUCCEEDED'
		  AND TRIM(COALESCE(jd.remote_id, '')) <> ''
		ORDER BY jd.destination_id ASC, jd.remote_id ASC, jd.created_at ASC, jd.delivery_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	groups := make(map[string][]duplicateDeliveryRow)
	for rows.Next() {
		var row duplicateDeliveryRow
		if err := rows.Scan(&row.jobID, &row.artifactID, &row.deliveryID, &row.remoteID, &row.destinationID, &row.createdAt, &row.status); err != nil {
			return nil, err
		}
		// A duplicate physical upload is multiple remote IDs for the same
		// artifact and destination. Group by that logical delivery intent,
		// not by remote ID, because the IDs are precisely what may differ.
		key := strings.TrimSpace(row.artifactID) + "\x00" + strings.TrimSpace(row.destinationID)
		groups[key] = append(groups[key], row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]DuplicateDeliveryRecord, 0)
	for _, group := range groups {
		if len(group) < 2 {
			continue
		}
		sort.SliceStable(group, func(i, j int) bool {
			if group[i].createdAt != group[j].createdAt {
				return group[i].createdAt < group[j].createdAt
			}
			return group[i].deliveryID < group[j].deliveryID
		})
		canonical := group[0]
		for _, duplicate := range group[1:] {
			out = append(out, DuplicateDeliveryRecord{
				JobID:                duplicate.jobID,
				ArtifactID:           duplicate.artifactID,
				DeliveryID:           duplicate.deliveryID,
				DriveFileIDCorrect:   canonical.remoteID,
				DriveFileIDDuplicate: duplicate.remoteID,
				DestinationID:        duplicate.destinationID,
				CreatedAt:            canonical.createdAt,
				DuplicateCreatedAt:   duplicate.createdAt,
				CanonicalDeliveryID:  canonical.deliveryID,
				DuplicateStatus:      duplicate.status,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DestinationID != out[j].DestinationID {
			return out[i].DestinationID < out[j].DestinationID
		}
		if out[i].DriveFileIDDuplicate != out[j].DriveFileIDDuplicate {
			return out[i].DriveFileIDDuplicate < out[j].DriveFileIDDuplicate
		}
		return out[i].DeliveryID < out[j].DeliveryID
	})
	return out, nil
}
