package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-shared/contract"

	"velox-server/internal/costmodel"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// Domain errors. The HTTP adapter maps these to status codes.
var (
	ErrNotFound            = errors.New("not found")
	ErrBadRequest          = errors.New("bad request")
	ErrInvalidPayload      = errors.New("invalid payload")
	ErrDestinationUnknown  = errors.New("destination unknown")
	ErrDestinationDisabled = errors.New("destination disabled")
)

// storeReader is the narrow persistence surface the InstaEdit service
// needs for reads. It is satisfied by *store.SQLiteStore.
type storeReader interface {
	ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error)
	GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error)
	ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error)
	GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error)
	GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*store.DeliveryDestination, error)
	ListJobDeliveriesByJob(jobID string) ([]store.JobDelivery, error)
	GetDeliveryDestination(ctx context.Context, destID string) (*store.DeliveryDestination, error)
}

// JobGateway is the port for job persistence and delivery metadata.
type JobGateway interface {
	ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error)
	GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error)
	Cancel(ctx context.Context, jobID string, reason string, revision int) error
	GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*store.DeliveryDestination, error)
	GetDeliveryDestination(ctx context.Context, destID string) (*store.DeliveryDestination, error)
	ListJobDeliveriesByJob(jobID string) ([]store.JobDelivery, error)
}

// WorkerReader is the port for worker snapshots.
type WorkerReader interface {
	ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error)
	GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error)
}

// AssetReader is the port for workspace-scoped assets.
type AssetReader interface {
	GetByIDAndWorkspace(ctx context.Context, assetID string, workspaceID int64) (*store.AssetRecord, error)
}

// JobEnqueuer is the port for enqueueing new jobs.
type JobEnqueuer interface {
	Enqueue(ctx context.Context, payload map[string]any, workspaceID int64) (map[string]any, error)
}

// Service is the InstaEdit BFF application layer. It owns validation,
// payload canonicalisation, workspace isolation, and error semantics.
type Service struct {
	jobs           JobGateway
	workers        WorkerReader
	assets         AssetReader
	enqueuer       JobEnqueuer
	submission     *creatorflow.JobSubmissionService
	deliveryEvents interface {
		ApplyInstaEditDeliveryEvent(context.Context, store.InstaEditDeliveryEvent) (bool, error)
	}
}

// NewService wires the service to the supplied ports.
func NewService(jobs JobGateway, workers WorkerReader, assets AssetReader, enqueuer JobEnqueuer) *Service {
	return &Service{
		jobs:     jobs,
		workers:  workers,
		assets:   assets,
		enqueuer: enqueuer,
	}
}

// NewServiceFromSQLite is a convenience constructor for the production
// composition root. It adapts the concrete SQLite/enqueuer types to the
// service's consumer-owned ports.
func NewServiceFromSQLite(sqlite *store.SQLiteStore, jobsRepo jobs.Repository, assets store.AssetRepository, enq *enqueue.Enqueuer, resolvers ...*creatorflow.Resolver) *Service {
	s := NewService(
		&sqliteJobGateway{storeReader: sqlite, jobs: jobsRepo},
		sqlite,
		assets,
		&enqueuerAdapter{enq: enq},
	)
	if len(resolvers) > 0 {
		s.submission = creatorflow.NewJobSubmissionService(resolvers[0])
	}
	s.deliveryEvents = sqlite
	return s
}

func (s *Service) ApplyDeliveryEvent(ctx context.Context, event store.InstaEditDeliveryEvent) (bool, error) {
	if s == nil || s.deliveryEvents == nil {
		return false, fmt.Errorf("delivery callback persistence is not configured")
	}
	return s.deliveryEvents.ApplyInstaEditDeliveryEvent(ctx, event)
}

// sqliteJobGateway adapts a storeReader and a jobs.Repository into a
// JobGateway. The service never sees the concrete SQLite types.
type sqliteJobGateway struct {
	storeReader
	jobs jobs.Repository
}

func (g *sqliteJobGateway) Cancel(ctx context.Context, jobID string, reason string, revision int) error {
	return g.jobs.Cancel(ctx, jobID, reason, revision)
}

// enqueuerAdapter adapts *enqueue.Enqueuer to the JobEnqueuer port.
type enqueuerAdapter struct {
	enq *enqueue.Enqueuer
}

func (a *enqueuerAdapter) Enqueue(ctx context.Context, payload map[string]any, workspaceID int64) (map[string]any, error) {
	var opts []enqueue.EnqueueOption
	if workspaceID != 0 {
		opts = append(opts, enqueue.WithWorkspaceID(workspaceID))
	}
	return a.enq.Enqueue(ctx, payload, costmodel.JobRequirements{}, opts...)
}

// CreateJob validates the request, builds a canonical payload, and
// enqueues a new job scoped to the command's workspace.
func (s *Service) CreateJob(ctx context.Context, cmd CreateJobCmd) (*jobResponse, error) {
	if strings.TrimSpace(cmd.ProjectID) == "" {
		return nil, fmt.Errorf("%w: project_id is required", ErrInvalidPayload)
	}
	if !cmd.RenderOnly && len(cmd.Destinations) == 0 {
		return nil, fmt.Errorf("%w: delivery_plan.destinations is required unless render_only=true", ErrInvalidPayload)
	}

	var renderSpec map[string]any
	if len(cmd.RenderSpec) > 0 {
		if err := json.Unmarshal(cmd.RenderSpec, &renderSpec); err != nil {
			return nil, fmt.Errorf("%w: invalid render_spec JSON: %v", ErrBadRequest, err)
		}
	} else {
		renderSpec = map[string]any{}
	}

	if err := contract.StrictValidatePayload(renderSpec); err != nil {
		return nil, fmt.Errorf("%w: invalid render_spec: %v", ErrInvalidPayload, err)
	}
	// The resolver consumes the canonical worker projection. Preserve the
	// scene list in its stable JSON form so the completion gate and the
	// renderer see the same scene snapshot on every retry.
	if _, present := renderSpec["scenes_json"]; !present {
		if scenes, present := renderSpec["scenes"]; present {
			encoded, marshalErr := json.Marshal(scenes)
			if marshalErr != nil {
				return nil, fmt.Errorf("%w: invalid scenes: %v", ErrInvalidPayload, marshalErr)
			}
			renderSpec["scenes_json"] = string(encoded)
		}
	}

	deliveryPlan := make([]map[string]any, 0, len(cmd.Destinations))
	for i, d := range cmd.Destinations {
		externalID := strings.TrimSpace(d.ExternalDestinationID)
		if externalID == "" {
			return nil, fmt.Errorf("%w: destination[%d].external_destination_id is required", ErrInvalidPayload, i)
		}
		dest, err := s.jobs.GetDeliveryDestinationByExternalID(ctx, externalID)
		if err != nil {
			return nil, err
		}
		if dest == nil {
			return nil, fmt.Errorf("%w: %s", ErrDestinationUnknown, externalID)
		}
		if !dest.Enabled {
			return nil, fmt.Errorf("%w: %s", ErrDestinationDisabled, externalID)
		}

		metadata := map[string]any{}
		if len(d.Metadata) > 0 {
			if err := json.Unmarshal(d.Metadata, &metadata); err != nil {
				return nil, fmt.Errorf("%w: invalid metadata for destination[%d]: %v", ErrInvalidPayload, i, err)
			}
		}

		deliveryPlan = append(deliveryPlan, map[string]any{
			"destination_id": dest.DestinationID,
			"priority":       i,
			"retry_budget":   contract.DefaultDeliveryRetryBudget,
			"metadata":       metadata,
		})
	}

	if _, ok := renderSpec["video_name"]; !ok {
		renderSpec["video_name"] = cmd.ProjectID
	}
	renderSpec["delivery_plan"] = deliveryPlan
	if cmd.RenderOnly {
		renderSpec["render_only"] = true
	}

	typedPayload := contract.NewJobPayloadV2(renderSpec)
	payload, err := typedPayload.ToMap()
	if err != nil {
		return nil, fmt.Errorf("build canonical payload: %w", err)
	}
	payload["project_id"] = cmd.ProjectID
	// This adapter submits a fully assembled render request (unlike the
	// remote-engine polling path), so mark the canonical handoff complete
	// for the resolver's completion gate. The enqueue normalizer still owns
	// the persisted worker lifecycle status.
	payload["status"] = "completed"

	var result map[string]interface{}
	duplicate := false
	if s.submission != nil {
		if strings.TrimSpace(cmd.IdempotencyKey) == "" {
			return nil, fmt.Errorf("%w: idempotency_key is required", ErrInvalidPayload)
		}
		sourceID := fmt.Sprintf("workspace:%d:%s", cmd.WorkspaceID, strings.TrimSpace(cmd.IdempotencyKey))
		resolved, submitErr := s.submission.Submit(ctx, creatorflow.CanonicalJobSubmission{
			ContractVersion:  cmd.ContractVersion,
			WorkspaceID:      cmd.WorkspaceID,
			SourceProvider:   "instaedit_bff",
			SourceJobID:      sourceID,
			TargetExecutorID: "scene.composite.v1",
			Payload:          payload,
		})
		if submitErr != nil {
			return nil, submitErr
		}
		if resolved == nil || resolved.Response == nil {
			return nil, errors.New("job submission returned nil result")
		}
		result = resolved.Response
		if created, ok := result["created"].(bool); ok {
			duplicate = !created
		}
	} else {
		result, err = s.enqueuer.Enqueue(ctx, payload, cmd.WorkspaceID)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, errors.New("enqueue returned nil result")
		}
	}

	jobID := asString(result["job_id"])
	if jobID == "" {
		return nil, errors.New("enqueue result missing job_id")
	}

	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, cmd.WorkspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("job created but not found")
	}
	j := mapJob(row, cmd.WorkspaceID)
	j.Duplicate = duplicate
	return &j, nil
}

// --- Mapping helpers -------------------------------------------------------
