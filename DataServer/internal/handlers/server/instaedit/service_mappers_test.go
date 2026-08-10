package instaedit

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

func TestMapJob_DefaultsAndFields(t *testing.T) {
	created := "2026-08-10T12:00:00Z"
	updated := "2026-08-10T12:05:00Z"

	got := mapJob(map[string]any{
		"job_id":     []byte("job-1"),
		"project_id": "project-1",
		"created_at": created,
		"updated_at": updated,
	}, 45)

	if got.ID != "job-1" || got.WorkspaceID != 45 || got.ProjectID != "project-1" {
		t.Fatalf("unexpected job identity: %+v", got)
	}
	if got.RenderStatus != "PENDING" || got.PublicationStatus != "pending" || got.OverallStatus != "pending" {
		t.Fatalf("unexpected default statuses: %+v", got)
	}
	if !got.CreatedAt.Equal(time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)) || !got.UpdatedAt.Equal(time.Date(2026, 8, 10, 12, 5, 0, 0, time.UTC)) {
		t.Fatalf("unexpected timestamps: created=%v updated=%v", got.CreatedAt, got.UpdatedAt)
	}
}

func TestMapJobWithDeliveries_PublicationStates(t *testing.T) {
	tests := []struct {
		name         string
		renderStatus string
		statuses     []string
		publication  string
		overall      string
	}{
		{name: "all succeeded", renderStatus: "SUCCEEDED", statuses: []string{"SUCCEEDED", "PUBLISHED", "COMPLETED"}, publication: "published", overall: "published"},
		{name: "failed wins", renderStatus: "SUCCEEDED", statuses: []string{"SUCCEEDED", "FAILED"}, publication: "failed", overall: "failed"},
		{name: "blocked auth is failed", renderStatus: "COMPLETED", statuses: []string{"BLOCKED_AUTH"}, publication: "failed", overall: "failed"},
		{name: "cancelled is failed", renderStatus: "COMPLETED", statuses: []string{"CANCELLED"}, publication: "failed", overall: "failed"},
		{name: "active is publishing", renderStatus: "COMPLETED", statuses: []string{"PENDING", "RUNNING"}, publication: "publishing", overall: "publishing"},
		{name: "retry wait is publishing", renderStatus: "COMPLETED", statuses: []string{"RETRY_WAIT"}, publication: "publishing", overall: "publishing"},
		{name: "claimed is publishing", renderStatus: "COMPLETED", statuses: []string{"CLAIMED"}, publication: "publishing", overall: "publishing"},
		{name: "unknown is pending", renderStatus: "COMPLETED", statuses: []string{"UNKNOWN"}, publication: "pending", overall: "pending"},
		{name: "render not complete keeps render status", renderStatus: "RUNNING", statuses: []string{"SUCCEEDED"}, publication: "published", overall: "running"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deliveries := make([]deliveryResponse, 0, len(tt.statuses))
			for _, status := range tt.statuses {
				deliveries = append(deliveries, deliveryResponse{Status: status})
			}
			got := mapJobWithDeliveries(map[string]any{"job_id": "job-1", "status": tt.renderStatus}, 45, deliveries)
			if got.PublicationStatus != tt.publication || got.OverallStatus != tt.overall {
				t.Fatalf("statuses = publication=%q overall=%q, want publication=%q overall=%q", got.PublicationStatus, got.OverallStatus, tt.publication, tt.overall)
			}
		})
	}
}

func TestMapWorker_FallbacksAndNumericConversions(t *testing.T) {
	tests := []struct {
		name       string
		row        map[string]any
		wantID     string
		wantStatus string
		wantCPU    int
		wantRAMMB  int
		wantDiskGB int
	}{
		{
			name: "canonical fields and int64 metrics",
			row: map[string]any{
				"worker_id":         "worker-canonical",
				"connection_status": "READY",
				"status":            "STALE",
				"metrics": map[string]any{
					"cpu_count":       int64(8),
					"ram_bytes":       int64(4 * 1024 * 1024 * 1024),
					"disk_free_bytes": int64(12 * 1024 * 1024 * 1024),
					"gpu":             "NVIDIA",
				},
			},
			wantID: "worker-canonical", wantStatus: "READY", wantCPU: 8, wantRAMMB: 4096, wantDiskGB: 12,
		},
		{
			name: "legacy fields and float64 metrics",
			row: map[string]any{
				"id":     "worker-legacy",
				"status": "DISCONNECTED",
				"metrics": map[string]any{
					"cpu_count":       float64(4),
					"ram_bytes":       float64(2 * 1024 * 1024 * 1024),
					"disk_free_bytes": float64(7 * 1024 * 1024 * 1024),
				},
			},
			wantID: "worker-legacy", wantStatus: "DISCONNECTED", wantCPU: 4, wantRAMMB: 2048, wantDiskGB: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapWorker(tt.row, 45)
			if got.ID != tt.wantID || got.Status != tt.wantStatus || got.WorkspaceID != 45 {
				t.Fatalf("worker identity/status = %+v", got)
			}
			if got.CPU != tt.wantCPU || got.RAMMB != tt.wantRAMMB || got.DiskGB != tt.wantDiskGB {
				t.Fatalf("worker metrics = cpu=%d ram=%d disk=%d, want cpu=%d ram=%d disk=%d", got.CPU, got.RAMMB, got.DiskGB, tt.wantCPU, tt.wantRAMMB, tt.wantDiskGB)
			}
		})
	}
}

func TestMapWorker_EmptyCanonicalValuesFallBackToLegacy(t *testing.T) {
	got := mapWorker(map[string]any{
		"worker_id":         "",
		"id":                "legacy-id",
		"connection_status": "",
		"status":            "READY",
	}, 9)
	if got.ID != "legacy-id" || got.Status != "READY" {
		t.Fatalf("legacy fallback not applied: %+v", got)
	}
}

func TestMapAsset_MapsAllFieldsAndWorkspace(t *testing.T) {
	got := mapAsset(&store.AssetRecord{
		AssetID:   "asset-1",
		SHA256:    "abc123",
		SizeBytes: 987,
		MimeType:  "video/mp4",
	}, 73)
	want := assetResponse{ID: "asset-1", WorkspaceID: 73, SHA256: "abc123", SizeBytes: 987, MimeType: "video/mp4"}
	if got != want {
		t.Fatalf("asset = %+v, want %+v", got, want)
	}
}

func TestMappingHelpers_StringAndTimeConversions(t *testing.T) {
	if got := asString(nil); got != "" {
		t.Fatalf("asString(nil) = %q", got)
	}
	if got := asString([]byte("bytes")); got != "bytes" {
		t.Fatalf("asString([]byte) = %q", got)
	}
	if got := asString(42); got != "42" {
		t.Fatalf("asString(int) = %q", got)
	}
	if got := parseTime("not-a-time"); !got.IsZero() {
		t.Fatalf("parseTime(invalid) = %v, want zero", got)
	}
	if got := parseTime(""); !got.IsZero() {
		t.Fatalf("parseTime(empty) = %v, want zero", got)
	}
}

func TestMapJobWithDeliveries_EmptyKeepsBaseMapping(t *testing.T) {
	got := mapJobWithDeliveries(map[string]any{"job_id": "job-1", "status": "SUCCEEDED"}, 45, nil)
	if got.PublicationStatus != "pending" || got.OverallStatus != "succeeded" {
		t.Fatalf("empty deliveries = publication=%q overall=%q", got.PublicationStatus, got.OverallStatus)
	}
}

type recordingJobsRepository struct {
	cancelID       string
	cancelReason   string
	cancelRevision int
	cancelErr      error
}

func (r *recordingJobsRepository) Get(context.Context, string) (*jobs.Job, error) { return nil, nil }
func (r *recordingJobsRepository) List(context.Context, jobs.Filter) ([]jobs.Job, error) {
	return nil, nil
}
func (r *recordingJobsRepository) Counts(context.Context) (jobs.Counts, error) { return nil, nil }
func (r *recordingJobsRepository) SetStatus(context.Context, string, jobs.Status, jobs.Status) error {
	return nil
}
func (r *recordingJobsRepository) Fail(context.Context, string, string) error { return nil }
func (r *recordingJobsRepository) Cancel(_ context.Context, id, reason string, revision int) error {
	if r.cancelErr != nil {
		return r.cancelErr
	}
	r.cancelID = id
	r.cancelReason = reason
	r.cancelRevision = revision
	return nil
}
func (r *recordingJobsRepository) Delete(context.Context, string) error { return nil }

func TestSQLiteJobGateway_CancelDelegatesArguments(t *testing.T) {
	repo := &recordingJobsRepository{}
	gateway := &sqliteJobGateway{jobs: repo}
	if err := gateway.Cancel(context.Background(), "job-1", "cancel reason", 7); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.cancelID != "job-1" || repo.cancelReason != "cancel reason" || repo.cancelRevision != 7 {
		t.Fatalf("delegated cancel = id=%q reason=%q revision=%d", repo.cancelID, repo.cancelReason, repo.cancelRevision)
	}
}

func TestSQLiteJobGateway_CancelPropagatesErrors(t *testing.T) {
	want := errors.New("repository unavailable")
	gateway := &sqliteJobGateway{jobs: &recordingJobsRepository{cancelErr: want}}
	if err := gateway.Cancel(context.Background(), "job-1", "cancel reason", 7); !errors.Is(err, want) {
		t.Fatalf("expected repository error %v, got %v", want, err)
	}
}

func TestService_ReadPortsPropagateErrors(t *testing.T) {
	want := errors.New("read failed")
	ctx := context.Background()

	t.Run("jobs", func(t *testing.T) {
		_, err := NewService(&memoryJobGateway{listJobsErr: want}, nil, nil, nil).ListJobs(ctx, 45, 10)
		if !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	})
	t.Run("workers list", func(t *testing.T) {
		_, err := NewService(nil, &memoryWorkerReader{listErr: want}, nil, nil).ListWorkers(ctx, 45)
		if !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	})
	t.Run("worker get", func(t *testing.T) {
		_, err := NewService(nil, &memoryWorkerReader{getErr: want}, nil, nil).GetWorker(ctx, 45, "worker-1")
		if !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	})
	t.Run("asset", func(t *testing.T) {
		_, err := NewService(nil, nil, &memoryAssetReader{err: want}, nil).GetAsset(ctx, 45, "asset-1")
		if !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	})
}

func TestAdapterContracts(t *testing.T) {
	var _ JobEnqueuer = (*enqueuerAdapter)(nil)
	var _ JobGateway = (*sqliteJobGateway)(nil)
}
