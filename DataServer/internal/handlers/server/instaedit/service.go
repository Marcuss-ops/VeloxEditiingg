package instaedit

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/costmodel"
	"velox-server/internal/creatorflow"
	"velox-server/internal/deliverystore"
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
	GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*deliverystore.DeliveryDestination, error)
	ListJobDeliveriesByJob(jobID string) ([]deliverystore.JobDelivery, error)
	GetDeliveryDestination(ctx context.Context, destID string) (*deliverystore.DeliveryDestination, error)
}

// JobGateway is the port for job persistence and delivery metadata.
type JobGateway interface {
	ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error)
	GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error)
	Cancel(ctx context.Context, jobID string, reason string, revision int) error
	GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*deliverystore.DeliveryDestination, error)
	GetDeliveryDestination(ctx context.Context, destID string) (*deliverystore.DeliveryDestination, error)
	ListJobDeliveriesByJob(jobID string) ([]deliverystore.JobDelivery, error)
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
	submission     *creatorflow.CanonicalJobSubmitter
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
		s.submission = creatorflow.NewCanonicalJobSubmitter(resolvers[0])
	}
	s.deliveryEvents = sqlite
	return s
}

// WithIntakeSourceRecorder wires the canonical submitter's intake-source
// telemetry sink. Production passes velmetrics.NewIntakeSourceSink(); nil
// is a noop (the submitter then records nothing).
func (s *Service) WithIntakeSourceRecorder(recorder creatorflow.IntakeSourceRecorder) *Service {
	if s != nil && s.submission != nil {
		s.submission.WithIntakeSourceRecorder(recorder)
	}
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

// CreateJob orchestrates validation, canonical payload construction,
// submission and the final workspace-scoped response lookup.
func (s *Service) CreateJob(ctx context.Context, cmd CreateJobCmd) (*jobResponse, error) {
	renderSpec, err := validateCreateJobCommand(cmd)
	if err != nil {
		return nil, err
	}
	payload, err := s.buildCreateJobPayload(ctx, cmd, renderSpec)
	if err != nil {
		return nil, err
	}
	result, duplicate, err := s.submitCreateJob(ctx, cmd, payload)
	if err != nil {
		return nil, err
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
