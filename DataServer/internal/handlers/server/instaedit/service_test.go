package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"velox-shared/contract"

	"velox-server/internal/deliverystore"
	"velox-server/internal/store"
)

// --- In-memory port mocks -------------------------------------------------

type memoryJobGateway struct {
	jobs                      []map[string]any
	getByID                   map[string]map[string]any
	deliveries                []deliverystore.JobDelivery
	destinations              map[string]*deliverystore.DeliveryDestination
	cancelled                 []string
	cancelReason              string
	cancelRevision            int
	getWorkspaceID            int64
	getJobID                  string
	cancelErr                 error
	listJobsErr               error
	listJobDeliveriesErr      error
	getDeliveryDestinationErr error
	getDestinationErr         error
}

func (m *memoryJobGateway) ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error) {
	if m.listJobsErr != nil {
		return nil, m.listJobsErr
	}
	return m.jobs, nil
}

func (m *memoryJobGateway) GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error) {
	m.getWorkspaceID = workspaceID
	m.getJobID = jobID
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
	m.cancelReason = reason
	m.cancelRevision = revision
	return nil
}

func (m *memoryJobGateway) GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*deliverystore.DeliveryDestination, error) {
	if m.getDestinationErr != nil {
		return nil, m.getDestinationErr
	}
	return m.destinations[externalID], nil
}

func (m *memoryJobGateway) ListJobDeliveriesByJob(jobID string) ([]deliverystore.JobDelivery, error) {
	if m.listJobDeliveriesErr != nil {
		return nil, m.listJobDeliveriesErr
	}
	return m.deliveries, nil
}

func (m *memoryJobGateway) GetDeliveryDestination(ctx context.Context, destID string) (*deliverystore.DeliveryDestination, error) {
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
	listErr error
	getErr  error
}

func (m *memoryWorkerReader) ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.workers, nil
}

func (m *memoryWorkerReader) GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.worker, nil
}

type memoryAssetReader struct {
	asset *store.AssetRecord
	err   error
}

func (m *memoryAssetReader) GetByIDAndWorkspace(ctx context.Context, assetID string, workspaceID int64) (*store.AssetRecord, error) {
	if m.err != nil {
		return nil, m.err
	}
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
		WorkspaceID:  45,
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
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
	jobs := &memoryJobGateway{destinations: map[string]*deliverystore.DeliveryDestination{}}
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

func TestService_CreateJob_StoreMissingDestinationMapsToDomainError(t *testing.T) {
	jobs := &memoryJobGateway{destinations: map[string]*deliverystore.DeliveryDestination{}}
	// Use a gateway variant that reproduces SQLite's normalized missing-row error.
	jobs.getDestinationErr = store.ErrDeliveryNoRow
	svc := NewService(jobs, nil, nil, nil)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID:  45,
		ProjectID:    "proj-1",
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-missing"}},
	})
	if !errors.Is(err, ErrDestinationUnknown) {
		t.Fatalf("expected ErrDestinationUnknown, got %v", err)
	}
}

func TestService_CreateJob_DisabledDestination(t *testing.T) {
	jobs := &memoryJobGateway{
		destinations: map[string]*deliverystore.DeliveryDestination{
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
		destinations: map[string]*deliverystore.DeliveryDestination{
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
	if enq.last["project_id"] != "proj-1" || enq.last["status"] != string(contract.InputAssemblyCompleted) {
		t.Fatalf("expected canonical project/status fields, got project_id=%v status=%v", enq.last["project_id"], enq.last["status"])
	}
	if _, present := enq.last["render_only"]; present {
		t.Fatalf("normal payload unexpectedly contains render_only: %#v", enq.last["render_only"])
	}
	if enq.last["video_name"] != "Test" {
		t.Fatalf("expected video_name Test, got %v", enq.last["video_name"])
	}
	if scenesJSON, ok := enq.last["scenes_json"].(string); !ok || scenesJSON != "[]" {
		t.Fatalf("expected scenes_json [] string, got %#v", enq.last["scenes_json"])
	}
	plan, ok := enq.last["delivery_plan"].([]map[string]any)
	if !ok || len(plan) != 1 {
		t.Fatalf("expected one delivery plan entry, got %v", enq.last["delivery_plan"])
	}
	if plan[0]["retry_budget"] != contract.DefaultDeliveryRetryBudget {
		t.Fatalf("expected retry_budget %d, got %v", contract.DefaultDeliveryRetryBudget, plan[0]["retry_budget"])
	}
}

func TestService_CreateJob_PropagatesScheduleAndPublicationTarget(t *testing.T) {
	jobs := &memoryJobGateway{
		destinations: map[string]*deliverystore.DeliveryDestination{
			"ext-1": {DestinationID: "d-1", ExternalDestinationID: "ext-1", Enabled: true},
		},
		getByID: map[string]map[string]any{
			"job-target": {"job_id": "job-target", "status": "PENDING", "project_id": "proj-1"},
		},
	}
	enq := &memoryEnqueuer{result: map[string]any{"job_id": "job-target"}}
	svc := NewService(jobs, nil, nil, enq)
	publishAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID: 45,
		ProjectID:   "proj-1",
		RenderSpec:  json.RawMessage(`{"video_name":"Test","scenes":[]}`),
		Destinations: []CreateDestinationCmd{{
			ExternalDestinationID: "ext-1",
		}},
		PublishAt: publishAt,
		Target: &PublicationTargetCmd{
			Type: "group", GroupID: 12, GroupName: "Social IT",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	plan, ok := enq.last["delivery_plan"].([]map[string]any)
	if !ok || len(plan) != 1 {
		t.Fatalf("expected one delivery plan entry, got %#v", enq.last["delivery_plan"])
	}
	metadata, ok := plan[0]["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map, got %#v", plan[0]["metadata"])
	}
	if metadata["publish_at"] != publishAt || metadata["group_name"] != "Social IT" || metadata["group_id"] != int64(12) {
		t.Fatalf("publication metadata not propagated: %#v", metadata)
	}
}

func TestService_CreateJob_PropagatesPublicationBundle(t *testing.T) {
	jobs := &memoryJobGateway{
		destinations: map[string]*deliverystore.DeliveryDestination{
			"ext-it": {DestinationID: "d-it", ExternalDestinationID: "ext-it", Enabled: true},
		},
		getByID: map[string]map[string]any{
			"job-boxe": {"job_id": "job-boxe", "status": "PENDING", "project_id": "proj-boxe"},
		},
	}
	enq := &memoryEnqueuer{result: map[string]any{"job_id": "job-boxe"}}
	svc := NewService(jobs, nil, nil, enq)
	publications := json.RawMessage(`[{"publication_id":"boxe-it","output_ref":{"variant_id":"it"},"language":"it","metadata":{"publish_at":"2030-01-01T12:00:00Z"},"destinations":[{"destination_id":"ext-it"}],"provider_options":{"voiceover":{"asset_id":"vo-it","language":"it"}}},{"publication_id":"boxe-en","output_ref":{"variant_id":"en"},"language":"en","metadata":{"publish_at":"2030-01-02T12:00:00Z"},"destinations":[{"destination_id":"ext-it"}],"provider_options":{"voiceover":{"asset_id":"vo-en","language":"en"}}}]`)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID: 45,
		ProjectID:   "proj-boxe",
		RenderSpec:  json.RawMessage(`{"video_name":"Boxe","scenes":[]}`),
		// Compatibility clients may still send the legacy top-level plan;
		// publications must remain authoritative for language fan-out.
		Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-it"}},
		Publications: publications,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	items, ok := enq.last["publications"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("publication bundle not propagated: %#v", enq.last["publications"])
	}
	for index, expected := range []string{"boxe-it", "boxe-en"} {
		item, itemOK := items[index].(map[string]any)
		if !itemOK || item["publication_id"] != expected {
			t.Fatalf("unexpected publication item: %#v", items[index])
		}
	}
	plan, ok := enq.last["delivery_plan"].([]map[string]any)
	if !ok || len(plan) != 2 {
		t.Fatalf("expected derived delivery plan, got %#v", enq.last["delivery_plan"])
	}
	for index, expected := range []struct {
		publicationID, language, variant, publishAt string
	}{
		{"boxe-it", "it", "it", "2030-01-01T12:00:00Z"},
		{"boxe-en", "en", "en", "2030-01-02T12:00:00Z"},
	} {
		metadata, metadataOK := plan[index]["metadata"].(map[string]any)
		if !metadataOK || metadata["publication_id"] != expected.publicationID || metadata["language"] != expected.language || metadata["output_variant_id"] != expected.variant || metadata["publish_at"] != expected.publishAt {
			t.Fatalf("publication routing metadata not propagated: %#v", plan[index]["metadata"])
		}
		if plan[index]["publication_id"] != expected.publicationID {
			t.Fatalf("publication id not propagated to delivery plan: %#v", plan[index])
		}
	}
}

func TestService_CreateJob_RenderOnlyBuildsCanonicalPayloadWithoutDestinations(t *testing.T) {
	jobs := &memoryJobGateway{
		getByID: map[string]map[string]any{
			"job-render-only": {"job_id": "job-render-only", "status": "PENDING", "project_id": "proj-render"},
		},
	}
	enq := &memoryEnqueuer{result: map[string]any{"job_id": "job-render-only"}}
	svc := NewService(jobs, nil, nil, enq)
	_, err := svc.CreateJob(context.Background(), CreateJobCmd{
		WorkspaceID: 45,
		ProjectID:   "proj-render",
		RenderOnly:  true,
		RenderSpec:  json.RawMessage(`{"scenes":[]}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enq.last["status"] != string(contract.InputAssemblyCompleted) || enq.last["render_only"] != true {
		t.Fatalf("expected completed render-only payload, got status=%v render_only=%v", enq.last["status"], enq.last["render_only"])
	}
	plan, ok := enq.last["delivery_plan"].([]map[string]any)
	if !ok || len(plan) != 0 {
		t.Fatalf("expected empty delivery plan, got %#v", enq.last["delivery_plan"])
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
	if jobs.getWorkspaceID != 45 || jobs.getJobID != "job-1" {
		t.Fatalf("expected ownership lookup for workspace 45/job-1, got workspace=%d job=%q", jobs.getWorkspaceID, jobs.getJobID)
	}
	if jobs.cancelReason != "cancelled via InstaEdit BFF" || jobs.cancelRevision != 0 {
		t.Fatalf("unexpected cancel arguments: reason=%q revision=%d", jobs.cancelReason, jobs.cancelRevision)
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
		getByID:                   map[string]map[string]any{"job-1": {"job_id": "job-1", "status": "PENDING", "project_id": "p-1"}},
		deliveries:                []deliverystore.JobDelivery{{DeliveryID: "d-1", DestinationID: "dest-1"}},
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
