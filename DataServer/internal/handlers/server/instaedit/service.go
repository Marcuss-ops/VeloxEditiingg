package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-shared/contract"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// defaultDeliveryRetryBudget is the retry budget stamped on each
// delivery-plan entry when the caller does not override it.
const defaultDeliveryRetryBudget = 5

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
	jobs     JobGateway
	workers  WorkerReader
	assets   AssetReader
	enqueuer JobEnqueuer
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
func NewServiceFromSQLite(sqlite *store.SQLiteStore, jobsRepo jobs.Repository, assets store.AssetRepository, enq *enqueue.Enqueuer) *Service {
	return NewService(
		&sqliteJobGateway{storeReader: sqlite, jobs: jobsRepo},
		sqlite,
		assets,
		&enqueuerAdapter{enq: enq},
	)
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

// ListJobs returns the jobs visible to the workspace.
func (s *Service) ListJobs(ctx context.Context, workspaceID int64, limit int) ([]jobResponse, error) {
	rows, err := s.jobs.ListJobsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	resp := make([]jobResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, mapJob(row, workspaceID))
	}
	return resp, nil
}

// CreateJob validates the request, builds a canonical payload, and
// enqueues a new job scoped to the command's workspace.
func (s *Service) CreateJob(ctx context.Context, cmd CreateJobCmd) (*jobResponse, error) {
	if strings.TrimSpace(cmd.ProjectID) == "" {
		return nil, fmt.Errorf("%w: project_id is required", ErrInvalidPayload)
	}
	if len(cmd.Destinations) == 0 {
		return nil, fmt.Errorf("%w: delivery_plan.destinations is required", ErrInvalidPayload)
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
			"retry_budget":   defaultDeliveryRetryBudget,
			"metadata":       metadata,
		})
	}

	if _, ok := renderSpec["video_name"]; !ok {
		renderSpec["video_name"] = cmd.ProjectID
	}
	renderSpec["delivery_plan"] = deliveryPlan

	typedPayload := contract.NewJobPayloadV2(renderSpec)
	payload, err := typedPayload.ToMap()
	if err != nil {
		return nil, fmt.Errorf("build canonical payload: %w", err)
	}
	payload["project_id"] = cmd.ProjectID

	result, err := s.enqueuer.Enqueue(ctx, payload, cmd.WorkspaceID)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("enqueue returned nil result")
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
	return &j, nil
}

// GetJob returns a job together with its deliveries.
func (s *Service) GetJob(ctx context.Context, workspaceID int64, jobID string) (*jobDetailResponse, error) {
	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, workspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	deliveries, err := s.loadDeliveries(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return &jobDetailResponse{
		Job:        mapJob(row, workspaceID),
		Deliveries: deliveries,
	}, nil
}

// GetJobDeliveries returns only the deliveries for a job.
func (s *Service) GetJobDeliveries(ctx context.Context, workspaceID int64, jobID string) ([]deliveryResponse, error) {
	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, workspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	return s.loadDeliveries(ctx, jobID)
}

// CancelJob cancels a job after verifying workspace ownership.
func (s *Service) CancelJob(ctx context.Context, workspaceID int64, jobID string) error {
	row, err := s.jobs.GetJobByWorkspace(ctx, jobID, workspaceID)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	return s.jobs.Cancel(ctx, jobID, "cancelled via InstaEdit BFF", 0)
}

// ListWorkers returns workers visible to the workspace.
func (s *Service) ListWorkers(ctx context.Context, workspaceID int64) ([]workerResponse, error) {
	rows, err := s.workers.ListWorkersByWorkspace(workspaceID)
	if err != nil {
		return nil, err
	}
	resp := make([]workerResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, mapWorker(row, workspaceID))
	}
	return resp, nil
}

// GetWorker returns a single worker snapshot.
func (s *Service) GetWorker(ctx context.Context, workspaceID int64, workerID string) (*workerResponse, error) {
	row, err := s.workers.GetWorkerByWorkspace(workerID, workspaceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: worker %s", ErrNotFound, workerID)
	}
	w := mapWorker(row, workspaceID)
	return &w, nil
}

// GetAsset returns a single workspace-scoped asset.
func (s *Service) GetAsset(ctx context.Context, workspaceID int64, assetID string) (*assetResponse, error) {
	asset, err := s.assets.GetByIDAndWorkspace(ctx, assetID, workspaceID)
	if err != nil {
		return nil, err
	}
	if asset == nil {
		return nil, fmt.Errorf("%w: asset %s", ErrNotFound, assetID)
	}
	a := mapAsset(asset, workspaceID)
	return &a, nil
}

// loadDeliveries loads and maps the deliveries for a job.
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
			PlatformMediaID:       row.RemoteID,
			PlatformURL:           row.RemoteURL,
		})
	}
	return out, nil
}

// --- Mapping helpers -------------------------------------------------------

func mapJob(row map[string]any, workspaceID int64) jobResponse {
	j := jobResponse{
		ID:           asString(row["job_id"]),
		WorkspaceID:  workspaceID,
		ProjectID:    asString(row["project_id"]),
		RenderStatus: asString(row["status"]),
		CreatedAt:    parseTime(row["created_at"]),
		UpdatedAt:    parseTime(row["updated_at"]),
	}
	if j.RenderStatus == "" {
		j.RenderStatus = "PENDING"
	}
	return j
}

func mapWorker(row map[string]any, workspaceID int64) workerResponse {
	w := workerResponse{
		ID:          asString(row["worker_id"]),
		WorkspaceID: workspaceID,
		Status:      asString(row["status"]),
	}
	if w.ID == "" {
		w.ID = asString(row["id"])
	}
	if metrics, ok := row["metrics"].(map[string]any); ok {
		if cpu, ok := metrics["cpu_count"].(int64); ok {
			w.CPU = int(cpu)
		} else if cpu, ok := metrics["cpu_count"].(float64); ok {
			w.CPU = int(cpu)
		}
		if ram, ok := metrics["ram_bytes"].(int64); ok {
			w.RAMMB = int(ram / 1024 / 1024)
		} else if ram, ok := metrics["ram_bytes"].(float64); ok {
			w.RAMMB = int(ram / 1024 / 1024)
		}
		if disk, ok := metrics["disk_free_bytes"].(int64); ok {
			w.DiskGB = int(disk / 1024 / 1024 / 1024)
		} else if disk, ok := metrics["disk_free_bytes"].(float64); ok {
			w.DiskGB = int(disk / 1024 / 1024 / 1024)
		}
		w.GPU = asString(metrics["gpu"])
	}
	return w
}

func mapAsset(a *store.AssetRecord, workspaceID int64) assetResponse {
	return assetResponse{
		ID:          a.AssetID,
		WorkspaceID: workspaceID,
		SHA256:      a.SHA256,
		SizeBytes:   a.SizeBytes,
		MimeType:    a.MimeType,
		DownloadURL: "",
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	if s, ok := v.([]byte); ok {
		return string(s)
	}
	return fmt.Sprintf("%v", v)
}

func parseTime(v any) time.Time {
	s := asString(v)
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}
