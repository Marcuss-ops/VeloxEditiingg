package creatorflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"velox-shared/publication"
)

// CanonicalJobSubmission is the application boundary shared by the M2M
// intake and the InstaEdit control-plane intake. HTTP handlers only adapt
// their wire/authentication concerns into this value; persistence and
// idempotency remain owned by the resolver below.
type CanonicalJobSubmission struct {
	ContractVersion  string
	WorkspaceID      int64
	ExternalClientID string
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	Payload          map[string]interface{}
	DeliveryPlan     map[string]interface{}
	PublicationSpecs []publication.Spec
}

// JobSubmissionService is the single production Job+Task submission path.
// The resolver performs the durable idempotency check and the atomic
// forwarding/job/task write.
type JobSubmissionService struct {
	resolver *Resolver
}

func NewJobSubmissionService(resolver *Resolver) *JobSubmissionService {
	if resolver == nil {
		return nil
	}
	return &JobSubmissionService{resolver: resolver}
}

func (s *JobSubmissionService) Submit(ctx context.Context, req CanonicalJobSubmission) (*ResolveOutput, error) {
	if s == nil || s.resolver == nil {
		return nil, fmt.Errorf("job submission service is not configured")
	}
	if req.ContractVersion != "" && req.ContractVersion != "velox.job.v1" {
		return nil, fmt.Errorf("unsupported contract_version: %s", req.ContractVersion)
	}
	if strings.TrimSpace(req.SourceProvider) == "" {
		return nil, fmt.Errorf("source_provider is required")
	}
	if strings.TrimSpace(req.SourceJobID) == "" {
		return nil, fmt.Errorf("idempotency_key is required")
	}
	if req.Payload == nil {
		return nil, fmt.Errorf("payload is required")
	}
	// These fields are generated at ingress by older adapters. They are
	// execution metadata, not request content; retaining them in the
	// resolver hash would turn an identical retry into a false idempotency
	// conflict because timestamps/UUIDs change between attempts.
	identityHash := sha256.Sum256([]byte(req.SourceProvider + ":" + req.SourceJobID + ":" + req.TargetExecutorID))
	stableIdentity := "submission_" + hex.EncodeToString(identityHash[:8])
	req.Payload["job_id"] = stableIdentity
	req.Payload["job_run_id"] = "run_" + stableIdentity
	req.Payload["correlation_id"] = "corr_" + stableIdentity
	// Fixed timestamps keep the payload hash a function of the canonical
	// request, never of the wall clock at which a retry arrived.
	req.Payload["created_at"] = "1970-01-01T00:00:00Z"
	req.Payload["updated_at"] = "1970-01-01T00:00:00Z"
	if req.DeliveryPlan != nil {
		req.Payload["delivery_plan"] = req.DeliveryPlan["delivery_plan"]
		if req.DeliveryPlan["delivery_plan"] == nil {
			req.Payload["delivery_plan"] = req.DeliveryPlan
		}
	}
	return s.resolver.Resolve(ctx, ResolveRequest{
		WorkspaceID:      req.WorkspaceID,
		ExternalClientID: req.ExternalClientID,
		SourceProvider:   req.SourceProvider,
		SourceJobID:      req.SourceJobID,
		TargetExecutorID: req.TargetExecutorID,
		Payload:          req.Payload,
		DeliveryPlan:     req.DeliveryPlan,
		PublicationSpecs: req.PublicationSpecs,
	})
}
