package deliverystore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// GetDeliveryPlanMetadata returns the immutable per-destination metadata
// snapshot associated with an artifact's job delivery plan. Missing metadata
// is represented as an empty JSON object so providers can safely apply their
// defaults.
func (w *SQLiteDeliveryStore) GetDeliveryPlanMetadata(ctx context.Context, artifactID, publicationID, destinationID string) (string, error) {
	w.observeDBOperation(false)
	if strings.TrimSpace(artifactID) == "" || strings.TrimSpace(destinationID) == "" {
		return "", fmt.Errorf("store: GetDeliveryPlanMetadata: artifact_id and destination_id are required")
	}
	var metadata string
	err := w.db.QueryRowContext(ctx, `
		SELECT COALESCE(jdp.metadata_json, '{}')
		FROM job_delivery_plans jdp
		JOIN artifacts a ON a.job_id = jdp.job_id
		WHERE a.id = ? AND COALESCE(jdp.publication_id,'') = ?
		  AND jdp.destination_id = ? AND jdp.enabled = 1`, artifactID, strings.TrimSpace(publicationID), destinationID).Scan(&metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return "{}", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: GetDeliveryPlanMetadata: %w", err)
	}
	if strings.TrimSpace(metadata) == "" {
		return "{}", nil
	}
	return metadata, nil
}
