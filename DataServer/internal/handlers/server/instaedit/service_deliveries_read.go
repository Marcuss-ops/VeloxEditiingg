package instaedit

import "context"

func (s *Service) loadDeliveries(ctx context.Context, jobID string) ([]deliveryResponse, error) {
	rows, err := s.jobs.ListJobDeliveriesByJob(jobID)
	if err != nil {
		return nil, err
	}
	out := make([]deliveryResponse, 0, len(rows))
	for _, row := range rows {
		dest, err := s.jobs.GetDeliveryDestination(ctx, row.DestinationID)
		if err != nil {
			return nil, err
		}
		externalID := ""
		if dest != nil {
			externalID = dest.ExternalDestinationID
		}
		out = append(out, deliveryResponse{
			ExternalDestinationID: externalID,
			SocialDeliveryID:      row.DeliveryID,
			Status:                row.Status,
			Phase:                 row.Status,
			Attempt:               row.AttemptCount,
			NextRetryAt:           row.NextAttemptAt,
			LastErrorCode:         row.LastError,
			LastErrorMessage:      row.LastErrorMessage,
			RetryFrom:             row.Status,
			PlatformMediaID:       row.RemoteID,
			PlatformURL:           row.RemoteURL,
		})
	}
	return out, nil
}
