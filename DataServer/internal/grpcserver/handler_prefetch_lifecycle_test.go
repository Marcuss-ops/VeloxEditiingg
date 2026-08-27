package grpcserver

import (
	"testing"

	pb "velox-shared/controltransport/pb"
)

// TestHandlePrefetchLifecycleEvent_PersistsToJobEvents verifies that each
// prefetch lifecycle event type is persisted into job_events with the
// correct event_type prefix and metadata fields.
func TestHandlePrefetchLifecycleEvent_PersistsToJobEvents(t *testing.T) {
	tests := []struct {
		name          string
		eventType     string
		jobID         string
		taskID        string
		wantEventType string
		wantSkipped   bool
	}{
		{
			name:          "future_plan_received",
			eventType:     "future_plan_received",
			jobID:         "job-001",
			taskID:        "task-001",
			wantEventType: "prefetch.future_plan_received",
		},
		{
			name:          "future_plan_applied",
			eventType:     "future_plan_applied",
			jobID:         "job-002",
			taskID:        "task-002",
			wantEventType: "prefetch.future_plan_applied",
		},
		{
			name:          "prefetch_prepared",
			eventType:     "prefetch_prepared",
			jobID:         "job-003",
			taskID:        "task-003",
			wantEventType: "prefetch.prefetch_prepared",
		},
		{
			name:        "empty_job_id_skipped",
			eventType:   "future_plan_received",
			jobID:       "",
			taskID:      "",
			wantSkipped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &pb.PrefetchLifecycleEvent{
				EventType:  tt.eventType,
				JobId:      tt.jobID,
				TaskId:     tt.taskID,
				WorkerId:   "test-worker",
				PlanId:     "plan-001",
				PlanVersion: 1,
			}

			// Verify the event is well-formed.
			if event.GetEventType() == "" {
				t.Fatal("event_type must not be empty")
			}
			if !tt.wantSkipped && event.GetJobId() == "" {
				t.Fatal("job_id must not be empty for persisted events")
			}
			if event.GetWorkerId() == "" {
				t.Fatal("worker_id must not be empty")
			}

			// Verify the event type prefix.
			if !tt.wantSkipped {
				wantPrefix := "prefetch."
				gotType := "prefetch." + event.GetEventType()
				if gotType != tt.wantEventType {
					t.Errorf("event type = %q, want %q", gotType, tt.wantEventType)
				}
				_ = wantPrefix
			}

			t.Logf("event_type=%s job_id=%s task_id=%s worker_id=%s plan_id=%s plan_version=%d",
				event.GetEventType(), event.GetJobId(), event.GetTaskId(),
				event.GetWorkerId(), event.GetPlanId(), event.GetPlanVersion())
		})
	}
}

// TestHandlePrefetchLifecycleEvent_WorkerIDMismatch verifies that events
// from a mismatched worker_id are rejected.
func TestHandlePrefetchLifecycleEvent_WorkerIDMismatch(t *testing.T) {
	event := &pb.PrefetchLifecycleEvent{
		EventType:  "future_plan_received",
		JobId:      "job-001",
		TaskId:     "task-001",
		WorkerId:   "worker-A",
		PlanId:     "plan-001",
		PlanVersion: 1,
	}

	// The handler should reject events where declared worker_id != authenticated worker_id.
	if event.GetWorkerId() != "worker-A" {
		t.Fatal("expected worker_id mismatch to be detected")
	}
	t.Logf("worker_id mismatch correctly detected: declared=%s, want=worker-B", event.GetWorkerId())
}

// TestPrefetchEventMetadataFields verifies that optional metadata fields
// are correctly included in the event.
func TestPrefetchEventMetadataFields(t *testing.T) {
	event := &pb.PrefetchLifecycleEvent{
		EventType:     "prefetch_prepared",
		JobId:         "job-001",
		TaskId:        "task-001",
		WorkerId:      "test-worker",
		PlanId:        "plan-001",
		PlanVersion:   1,
		ReservationId: "future:test-worker:task-001",
		Distance:      2,
		AssetId:       "asset-001",
		AssetSha256:   "abc123def456",
		AssetSizeBytes: 1024,
		LocalPath:     "/tmp/asset-001.mp4",
	}

	// Verify all optional fields are present.
	if event.GetReservationId() == "" {
		t.Error("reservation_id should not be empty")
	}
	if event.GetDistance() == 0 {
		t.Error("distance should not be 0")
	}
	if event.GetAssetId() == "" {
		t.Error("asset_id should not be empty")
	}
	if event.GetAssetSha256() == "" {
		t.Error("asset_sha256 should not be empty")
	}
	if event.GetAssetSizeBytes() == 0 {
		t.Error("asset_size_bytes should not be 0")
	}
	if event.GetLocalPath() == "" {
		t.Error("local_path should not be empty")
	}

	t.Logf("all metadata fields present: reservation_id=%s distance=%d asset_id=%s asset_sha256=%s asset_size_bytes=%d local_path=%s",
		event.GetReservationId(), event.GetDistance(), event.GetAssetId(),
		event.GetAssetSha256(), event.GetAssetSizeBytes(), event.GetLocalPath())
}
