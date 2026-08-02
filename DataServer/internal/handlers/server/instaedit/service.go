package instaedit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-shared/publication"

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

	videoName, _ := renderSpec["video_name"].(string)
	if strings.TrimSpace(videoName) == "" {
		videoName = cmd.ProjectID
		renderSpec["video_name"] = videoName
	}
	deliveryPlan := make([]map[string]any, 0, len(cmd.Destinations))
	publicationSpecs := make([]publication.Spec, 0, len(cmd.Destinations))
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
		publicationID := strings.TrimSpace(d.PublicationID)
		if publicationID == "" {
			publicationID = defaultPublicationID(cmd.IdempotencyKey, externalID)
		}
		metadata["publication_id"] = publicationID

		deliveryPlan = append(deliveryPlan, map[string]any{
			"destination_id": dest.DestinationID,
			"priority":       i,
			"retry_budget":   contract.DefaultDeliveryRetryBudget,
			"metadata":       metadata,
		})
		retryBudget := contract.DefaultDeliveryRetryBudget
		publicationSpecs = append(publicationSpecs, publication.Spec{
			Version:       publication.Version,
			PublicationID: publicationID,
			OutputRef:     publication.OutputRef{ArtifactRole: "final_video"},
			Language:      stringValue(metadata["language"]),
			Metadata:      publicationMetadataFromMap(metadata, videoName),
			Destinations: []publication.Destination{{
				DestinationID: dest.DestinationID,
				Priority:      i,
				RetryBudget:   &retryBudget,
			}},
		})
	}

	renderSpec["delivery_plan"] = deliveryPlan

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
			PublicationSpecs: publicationSpecs,
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
		Job:        mapJobWithDeliveries(row, workspaceID, deliveries),
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
	j.PublicationStatus = "pending"
	j.OverallStatus = strings.ToLower(j.RenderStatus)
	return j
}

func mapJobWithDeliveries(row map[string]any, workspaceID int64, deliveries []deliveryResponse) jobResponse {
	j := mapJob(row, workspaceID)
	if len(deliveries) == 0 {
		return j
	}
	allSucceeded := true
	anyFailed := false
	anyActive := false
	for _, d := range deliveries {
		switch strings.ToUpper(d.Status) {
		case "SUCCEEDED", "PUBLISHED", "COMPLETED":
		default:
			allSucceeded = false
		}
		switch strings.ToUpper(d.Status) {
		case "FAILED", "BLOCKED_AUTH", "CANCELLED":
			anyFailed = true
		case "RUNNING", "RETRY_WAIT", "CLAIMED", "PENDING":
			anyActive = true
		}
	}
	switch {
	case allSucceeded:
		j.PublicationStatus = "published"
	case anyFailed:
		j.PublicationStatus = "failed"
	case anyActive:
		j.PublicationStatus = "publishing"
	default:
		j.PublicationStatus = "pending"
	}
	if strings.EqualFold(j.RenderStatus, "succeeded") || strings.EqualFold(j.RenderStatus, "completed") {
		j.OverallStatus = j.PublicationStatus
	} else {
		j.OverallStatus = strings.ToLower(j.RenderStatus)
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

func defaultPublicationID(idempotencyKey, externalDestinationID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(idempotencyKey) + "\x00" + strings.TrimSpace(externalDestinationID)))
	return "pub_" + hex.EncodeToString(sum[:8])
}

func stringValue(value any) string {
	if value, ok := value.(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func publicationMetadataFromMap(metadata map[string]any, videoName string) publication.Metadata {
	title := stringValue(metadata["title"])
	if title == "" {
		title = videoName
	}
	privacy := stringValue(metadata["final_privacy"])
	if privacy == "" {
		privacy = stringValue(metadata["privacy_status"])
	}
	if privacy == "" {
		privacy = stringValue(metadata["privacy"])
	}
	return publication.Metadata{
		Title:       title,
		Description: stringValue(metadata["description"]),
		Tags:        stringSlice(metadata["tags"]),
		CategoryID:  stringValue(metadata["category_id"]),
		Privacy:     privacy,
		PublishAt:   stringValue(metadata["publish_at"]),
	}
}

func stringSlice(value any) []string {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if item := stringValue(value); item != "" {
				out = append(out, item)
			}
		}
		return out
	default:
		return nil
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
