package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"velox-shared/contract"

	"velox-server/internal/store"
)

// --- In-memory port mocks -------------------------------------------------

type memoryJobGateway struct {
	jobs                     []map[string]any
	getByID                  map[string]map[string]any
	deliveries               []store.JobDelivery
	destinations             map[string]*store.DeliveryDestination
	cancelled                []string
	cancelErr                error
	listJobDeliveriesErr     error
	getDeliveryDestinationErr error
}

func (m *memoryJobGateway) ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error) {
	return m.jobs, nil
}

func (m *memoryJobGateway) GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error) {
	if row, ok := m.getByID[jobID]; ok {
		return row, nil
	}
	return nil, nil
}

func (m *memoryJobGateway) Cancel(ctx context.Context, jobID string, reason string, revision int) error {
	if m.cancelErr != nil {
		return m.cancelErr
	}
	m.cancelled = append(m.cancelled, jobID)
	return nil
}

func (m *memoryJobGateway) GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*store.DeliveryDestination, error) {
	return m.destinations[externalID], nil
}

func (m *memoryJobGateway) ListJobDeliveriesByJob(jobID string) ([]store.JobDelivery, error) {
	if m.listJobDeliveriesErr != nil {
		return nil, m.listJobDeliveriesErr
	}
	return m.deliveries, nil
}

func (m *memoryJobGateway) GetDeliveryDestination(ctx context.Context, destID string) (*store.DeliveryDestination, error) {
	if m.getDeliveryDestinationErr != nil {
		return nil, m.getDeliveryDestinationErr
	}
	for _, d := range m.destinations {
		if d.DestinationID == destID {
			return d, nil
		}
	}
	return nil, nil
}

type memoryWorkerReader struct {
	workers []map[string]any
	worker  map[string]any
}

func (m *memoryWorkerReader) ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error) {
	return m.workers, nil
}

func (m *memoryWorkerReader) GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error) {
	return m.worker, nil
}

type memoryAssetReader struct {
	asset *store.AssetRecord
}

func (m *memoryAssetReader) GetByIDAndWorkspace(ctx context.Context, assetID string, workspaceID int64) (*store.AssetRecord, error) {
	return m.asset, nil
}

type memoryEnqueuer struct {
	result map[string]any
	err    error
	last   map[string]any
}

func (m *memoryEnqueuer) Enqueue(ctx context.Context, payload map[string]any, workspaceID int64) (map[string]any, error) {
	m.last = payload
	if m.err != nil {
		return nil, m.err
	}
	return m.result, nil
}

// --- Tests -----------------------------------------------------------------

func TestService_ListJobs_MapsRows(t *testing.T) {
	jobs := &memoryJobGateway{
		jobs: []map[string]any{
			{"job_id": "job-1", "status": "PENDING", "project_id": "p-1"},
		},
	}
	svc := NewService(jobs, nil, nil, nil)
	resp, err := svc.ListJobs(context.Background(), 45, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp) != 1 {
		t.Fatalf("expected 1 job, got %d", len(resp))
	}
	if resp[0].ID != "job-1" || resp[0].WorkspaceID != 45 {
		t.Fatalf("unexpected job: %+v", resp[0])
	}
}

func TestService_GetJob_NotFound(t *testing.T) {
	jobs := &memoryJobGateway{getByID: map[string]map[string]any{}}
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.GetJob(context.Background(), 45, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestService_CreateJob_RequiresProjectID(t *testing.T) {
	svc := NewService(nil, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID: 45,
		Destinations:  []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
	})
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestService_CreateJob_RequiresDestinations(t *testing.T) {
	svc := NewService(nil, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID: 45,
		ProjectID:   "proj-1",
	})
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestService_CreateJob_UnknownDestination(t *testing.T) {
	jobs := &memoryJobGateway{destinations: map[string]*store.DeliveryDestination{}}
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID:  45,
		ProjectID:    "proj-1",
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-unknown"}},
	})
	if !errors.Is(err, ErrDestinationUnknown) {
		t.Fatalf("expected ErrDestinationUnknown, got %v", err)
	}
}

func TestService_CreateJob_DisabledDestination(t *testing.T) {
	jobs := &memoryJobGateway{
		destinations: map[string]*store.DeliveryDestination{
			"ext-disabled": {DestinationID: "d-1", ExternalDestinationID: "ext-disabled", Enabled: false},
		},
	}
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID:  45,
		ProjectID:    "proj-1",
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-disabled"}},
	})
	if !errors.Is(err, ErrDestinationDisabled) {
		t.Fatalf("expected ErrDestinationDisabled, got %v", err)
	}
}

func TestService_CreateJob_Success(t *testing.T) {
	jobs := &memoryJobGateway{
		destinations: map[string]*store.DeliveryDestination{
			"ext-1": {DestinationID: "d-1", ExternalDestinationID: "ext-1", Enabled: true},
		},
		getByID: map[string]map[string]any{
			"job-abc": {"job_id": "job-abc", "status": "PENDING", "project_id": "proj-1"},
		},
	}
	enq := &memoryEnqueuer{result: map[string]any{"job_id": "job-abc"}}
	svc := NewService(jobs, nil, nil, enq)
	resp, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID:  45,
		ProjectID:    "proj-1",
		RenderSpec:   json.RawMessage(`{"video_name":"Test","scenes":[]}`),
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ID != "job-abc" {
		t.Fatalf("expected job-abc, got %s", resp.ID)
	}
	if enq.last == nil {
		t.Fatal("expected payload to be enqueued")
	}
	if enq.last["project_id"] != "proj-1" {
		t.Fatalf("expected project_id to be preserved, got %v", enq.last["project_id"])
	}
	plan, ok := enq.last["delivery_plan"].([]map[string]any)
	if !ok || len(plan) != 1 {
		t.Fatalf("expected one delivery plan entry, got %v", enq.last["delivery_plan"])
	}
	if plan[0]["retry_budget"] != contract.DefaultDeliveryRetryBudget {
		t.Fatalf("expected retry_budget %d, got %v", contract.DefaultDeliveryRetryBudget, plan[0]["retry_budget"])
	}
}

func TestService_CreateJob_InvalidRenderSpecJSON(t *testing.T) {
	svc := NewService(nil, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID:  45,
		ProjectID:    "proj-1",
		RenderSpec:   json.RawMessage(`not-json`),
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
	})
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got %v", err)
	}
}

func TestService_CreateJob_LegacyAlias(t *testing.T) {
	svc := NewService(nil, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID:  45,
		ProjectID:    "proj-1",
		RenderSpec:   json.RawMessage(`{"video_name":"x","voiceover_path":"/a.mp3"}`),
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
	})
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestService_CancelJob_NotFound(t *testing.T) {
	jobs := &memoryJobGateway{getByID: map[string]map[string]any{}}
	svc := NewService(jobs, nil, nil, nil)
	err := svc.CancelJob(context.Background(), 45, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestService_CancelJob_Success(t *testing.T) {
	jobs := &memoryJobGateway{
		getByID: map[string]map[string]any{"job-1": {"job_id": "job-1"}},
	}
	svc := NewService(jobs, nil, nil, nil)
	if err := svc.CancelJob(context.Background(), 45, "job-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(jobs.cancelled) != 1 || jobs.cancelled[0] != "job-1" {
		t.Fatalf("expected job-1 to be cancelled, got %v", jobs.cancelled)
	}
}

func TestService_GetAsset_NotFound(t *testing.T) {
	assets := &memoryAssetReader{asset: nil}
	svc := NewService(nil, nil, assets, nil)
	_, err := svc.GetAsset(context.Background(), 45, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestService_GetAsset_Success(t *testing.T) {
	assets := &memoryAssetReader{asset: &store.AssetRecord{AssetID: "a-1", SHA256: "deadbeef", SizeBytes: 42, MimeType: "video/mp4"}}
	svc := NewService(nil, nil, assets, nil)
	resp, err := svc.GetAsset(context.Background(), 45, "a-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ID != "a-1" || resp.WorkspaceID != 45 {
		t.Fatalf("unexpected asset: %+v", resp)
	}
}

func TestService_ListWorkers(t *testing.T) {
	workers := &memoryWorkerReader{workers: []map[string]any{{"worker_id": "w-1", "status": "READY"}}}
	svc := NewService(nil, workers, nil, nil)
	resp, err := svc.ListWorkers(context.Background(), 45)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp) != 1 || resp[0].ID != "w-1" {
		t.Fatalf("unexpected workers: %+v", resp)
	}
}

func TestService_GetJob_DeliveryLoadFailure_PropagatesError(t *testing.T) {
	want := errors.New("db: connection lost")
	jobs := &memoryJobGateway{
		getByID:              map[string]map[string]any{"job-1": {"job_id": "job-1", "status": "PENDING", "project_id": "p-1"}},
		listJobDeliveriesErr: want,
	}
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.GetJob(context.Background(), 45, "job-1")
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestService_GetJob_DestinationLookupFailure_PropagatesError(t *testing.T) {
	want := errors.New("db: destination lookup failed")
	jobs := &memoryJobGateway{
		getByID: map[string]map[string]any{"job-1": {"job_id": "job-1", "status": "PENDING", "project_id": "p-1"}},
		deliveries: []store.JobDelivery{{DeliveryID: "d-1", DestinationID: "dest-1"}},
		getDeliveryDestinationErr: want,
	}
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.GetJob(context.Background(), 45, "job-1")
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestService_GetJobDeliveries_Failure_PropagatesError(t *testing.T) {
	want := errors.New("db: connection lost")
	jobs := &memoryJobGateway{
		getByID:              map[string]map[string]any{"job-1": {"job_id": "job-1", "status": "PENDING", "project_id": "p-1"}},
		listJobDeliveriesErr: want,
	}
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.GetJobDeliveries(context.Background(), 45, "job-1")
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}
