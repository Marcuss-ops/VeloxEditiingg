package store

import (
	"context"
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

// BuildDuplicateDeliveryManifest returns deterministic, audit-ready duplicate
// candidates. It groups by (destination_id, remote_id), keeps the earliest row
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
