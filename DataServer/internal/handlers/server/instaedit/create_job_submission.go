package instaedit

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/creatorflow"
)

// submitCreateJob preserves the creatorflow and enqueue submission branches
// and returns the result plus the duplicate marker used by the response.
func (s *Service) submitCreateJob(ctx context.Context, cmd CreateJobCmd, payload map[string]any) (map[string]interface{}, bool, error) {
	var result map[string]interface{}
	duplicate := false
	if s.submission != nil {
		if strings.TrimSpace(cmd.IdempotencyKey) == "" {
			return nil, false, fmt.Errorf("%w: idempotency_key is required", ErrInvalidPayload)
		}
		sourceID := fmt.Sprintf("workspace:%d:%s", cmd.WorkspaceID, strings.TrimSpace(cmd.IdempotencyKey))
		// A2-2 adapter-boundary canonicalization: Prepare() stamps the
		// identity/execution-metadata keys HERE, at intake, so the resolver
		// receives an already-canonical payload. Submit() still re-normalizes
		// defensively (idempotent), but the boundary call is the contract.
		submission := (&creatorflow.CanonicalJobSubmission{
			ContractVersion:  cmd.ContractVersion,
			WorkspaceID:      cmd.WorkspaceID,
			IntakeSource:     creatorflow.IntakeSourceInstaedit,
			SourceProvider:   "instaedit_bff",
			SourceJobID:      sourceID,
			TargetExecutorID: "scene.composite.v1",
			Payload:          payload,
		}).Prepare()
		resolved, submitErr := s.submission.Submit(ctx, *submission)
		if submitErr != nil {
			return nil, false, submitErr
		}
		if resolved == nil || resolved.Response == nil {
			return nil, false, errors.New("job submission returned nil result")
		}
		result = resolved.Response
		if created, ok := result["created"].(bool); ok {
			duplicate = !created
		}
	} else {
		var err error
		result, err = s.enqueuer.Enqueue(ctx, payload, cmd.WorkspaceID)
		if err != nil {
			return nil, false, err
		}
		if result == nil {
			return nil, false, errors.New("enqueue returned nil result")
		}
	}
	return result, duplicate, nil
}
