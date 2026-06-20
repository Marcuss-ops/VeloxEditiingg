package jobs

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestToQueueItemNilInput(t *testing.T) {
	if got := ToQueueItem(nil); got != nil {
		t.Errorf("expected nil for nil input, got %+v", got)
	}
}

func TestToQueueItemDualAliasing(t *testing.T) {
	// WorkerID must be aliased to BOTH WorkerName AND AssignedTo.
	j := &Job{
		ID:       "job-1",
		Status:   StatusRunning,
		WorkerID: "worker-7",
	}
	q := ToQueueItem(j)
	if q.JobID != "job-1" {
		t.Errorf("JobID=%q, want %q", q.JobID, "job-1")
	}
	if q.Status != StatusRunning {
		t.Errorf("Status=%v, want %v", q.Status, StatusRunning)
	}
	if q.WorkerName != "worker-7" {
		t.Errorf("WorkerName=%q, want %q (dual-aliasing with AssignedTo)", q.WorkerName, "worker-7")
	}
	if q.AssignedTo != "worker-7" {
		t.Errorf("AssignedTo=%q, want %q (dual-aliasing with WorkerName)", q.AssignedTo, "worker-7")
	}
	if q.LeaseID != "" {
		t.Errorf("LeaseID=%q, want \"\" (no domain LeaseID set)", q.LeaseID)
	}
}

func TestToQueueItemAttemptsAlias(t *testing.T) {
	j := &Job{ID: "job-a", Attempts: 3, MaxRetries: 5}
	q := ToQueueItem(j)
	if q.RetryCount != 3 {
		t.Errorf("RetryCount=%d, want 3 (= Attempts)", q.RetryCount)
	}
	if q.Attempt != 3 {
		t.Errorf("Attempt=%d, want 3 (= Attempts)", q.Attempt)
	}
	if q.MaxRetries != 5 {
		t.Errorf("MaxRetries=%d, want 5", q.MaxRetries)
	}
}

func TestToPayloadMapLeaseIDInjection(t *testing.T) {
	// LeaseID non-empty → injected
	j := &Job{ID: "job-1", RunID: "run-1", LeaseID: "lease-abc"}
	m := ToPayloadMap(j)
	if m["lease_id"] != "lease-abc" {
		t.Errorf("lease_id=%v, want lease-abc", m["lease_id"])
	}
	for k, want := range map[string]string{
		"job_id":     "job-1",
		"job_run_id": "run-1",
		"run_id":     "run-1",
	} {
		if m[k] != want {
			t.Errorf("%s=%v, want %s", k, m[k], want)
		}
	}

	// LeaseID empty → lease_id key NOT present
	j2 := &Job{ID: "job-2", RunID: "run-2"}
	m2 := ToPayloadMap(j2)
	if _, exists := m2["lease_id"]; exists {
		t.Errorf("lease_id should be absent for empty LeaseID, got=%v", m2["lease_id"])
	}
	// status always present
	if _, exists := m2["status"]; !exists {
		t.Errorf("status key missing from payload map")
	}
}

func TestToFlatMapBlankFieldsPresent(t *testing.T) {
	// HTTP consumers depend on these keys being PRESENT even when zero-valued.
	j := &Job{ID: "job-1", Status: StatusPending}
	m := ToFlatMap(j)
	blankKeys := []string{"claimed_by", "claimed_at", "last_error", "error_message"}
	for _, key := range blankKeys {
		if _, exists := m[key]; !exists {
			t.Errorf("flat map missing blank key %q (HTTP consumer expectation)", key)
		}
	}
	if m["lease_expiry"] != nil {
		t.Errorf("lease_expiry=%v, want nil", m["lease_expiry"])
	}
	if m["assigned_to"] != "" {
		t.Errorf("assigned_to=%v, want \"\" (empty WorkerID)", m["assigned_to"])
	}
	if m["worker_name"] != "" {
		t.Errorf("worker_name=%v, want \"\" (empty WorkerID)", m["worker_name"])
	}
}

func TestFlatMapPayloadMergeNoOverride(t *testing.T) {
	// Payload keys must NOT override top-level fields like job_id/status.
	const raw = `{"job_id":"FROM-PAYLOAD","extra":"val","status":"FROM-PAYLOAD"}`
	j := &Job{ID: "top-id", Status: StatusPending, Payload: raw}
	m := ToFlatMap(j)
	if m["job_id"] != "top-id" {
		t.Errorf("top-level job_id should NOT be overridden by payload; got %v want top-id", m["job_id"])
	}
	if m["status"] != "PENDING" {
		t.Errorf("top-level status should NOT be overridden by payload; got %v want PENDING", m["status"])
	}
	if m["extra"] != "val" {
		t.Errorf("payload-extra=%v, want val", m["extra"])
	}
}

func TestFormatStatsCounts(t *testing.T) {
	c := Counts{
		StatusPending: 5,
		StatusRunning: 2,
	}
	m := FormatStats(c)
	if len(m) != 2 {
		t.Fatalf("expected 2 keys, got %d (%v)", len(m), m)
	}
	// Keys are string-cast of jobs.Status — verify exact case mapped.
	if _, ok := m[string(StatusPending)]; !ok {
		t.Errorf("missing key %q (StatusPending string form)", string(StatusPending))
	}
	if _, ok := m[string(StatusRunning)]; !ok {
		t.Errorf("missing key %q (StatusRunning string form)", string(StatusRunning))
	}
}

// TestToQueueItemTimeFieldsPassThrough locks the legacy behavior of
// passing time.Time values through interface{} boxing (json.Marshal then
// renders zero time as "0001-01-01T00:00:00Z"). A future "improve time
// formatting" change must first update this test, otherwise wire format
// silently drifts from the boxed-zero rendering to something else.
func TestToQueueItemTimeFieldsPassThrough(t *testing.T) {
	j := &Job{
		ID:          "t",
		CreatedAt:   time.Time{}, // zero
		UpdatedAt:   time.Time{},
		StartedAt:   time.Time{},
		CompletedAt: time.Time{},
	}
	q := ToQueueItem(j)
	// TIME FIELDS MUST be non-nil in the result (boxed through interface{}).
	// omitempty does NOT drop non-nil interface{} values, so the boxed zero
	// is rendered as "0001-01-01T00:00:00Z" in JSON — matching legacy.
	for name, val := range map[string]interface{}{
		"CreatedAt":   q.CreatedAt,
		"UpdatedAt":   q.UpdatedAt,
		"StartedAt":   q.StartedAt,
		"CompletedAt": q.CompletedAt,
	} {
		if val == nil {
			t.Errorf("%s should be non-nil (boxed through interface{}), got nil", name)
		}
		if _, ok := val.(time.Time); !ok {
			t.Errorf("%s should hold a time.Time value, got %T", name, val)
		}
	}
}

// TestFormatStatsWireShapeSnapshot locks the JSON wire shape produced by
// FormatStats so the Phase 2 sweep of queue.QueryService.Stats cannot
// silently drift. The legacy Stats() uses string-cast of jobs.Status which
// yields uppercase passthrough keys (matches pre-Batch-3 wire format).
func TestFormatStatsWireShapeSnapshot(t *testing.T) {
	c := Counts{
		StatusPending:   5,
		StatusRunning:   2,
		StatusSucceeded: 10,
		StatusFailed:    1,
		StatusCancelled: 3,
	}
	m := FormatStats(c)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	// Verify expected uppercase keys present (jobs.Status values are
	// uppercase — StatusPending = "PENDING", StatusRunning = "RUNNING", etc.).
	for _, key := range []string{
		`"PENDING":5`,
		`"RUNNING":2`,
		`"SUCCEEDED":10`,
		`"FAILED":1`,
		`"CANCELLED":3`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("stats wire shape invariant missing: %q\nfull output: %s", key, got)
		}
	}
}

func TestParsePayloadJSONEdgeCases(t *testing.T) {
	for _, raw := range []string{"", "{}", "not-json", "{\"x\":null}"} {
		got := parsePayloadJSON(raw)
		// Acceptable: empty map OR map with x→null (both behave the same)
		if raw == "" || raw == "{}" || raw == "not-json" {
			if len(got) != 0 {
				t.Errorf("parsePayloadJSON(%q): expected empty map, got %v", raw, got)
			}
		}
	}
	got := parsePayloadJSON(`{"foo":"bar","n":42,"nested":{"k":"v"}}`)
	if got["foo"] != "bar" {
		t.Errorf("foo=%v, want bar", got["foo"])
	}
	if _, ok := got["nested"]; !ok {
		t.Errorf("missing nested key")
	}
}

// TestToQueueItemWireShapeSnapshot locks the JSON wire shape produced by
// ToQueueItem so the Phase 2 sweep (which removes queue.domainJobToQueueJob)
// cannot silently drift. If this test ever fails, either ToQueueItem drifted
// OR the expectations here must be updated to match an EXPLICIT, reviewed
// wire-format change.
//
// NOTE: We deliberately test only NON-TIME fields here. QueueItem's time
// fields are interface{}-typed; a zero time.Time{} gets boxed into a non-nil
// interface{}, which json.Marshal then renders as "0001-01-01T00:00:00Z" —
// omitempty does NOT drop non-nil interface{} values. Locking time formatting
// here would be brittle; we instead test those fields separately by direct
// struct comparison (MockFieldMarshal) or rely on integration tests.
func TestToQueueItemWireShapeSnapshot(t *testing.T) {
	j := &Job{
		ID:        "snap-1",
		Status:    StatusRunning,
		WorkerID:  "worker-X",
		Attempts:  2,
		MaxRetries: 5,
		RunID:     "run-X",
		VideoName: "video.mp4",
		ProjectID: "proj-1",
		LeaseID:   "lease-X",
	}
	q := ToQueueItem(j)
	raw, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	for _, key := range []string{
		`"job_id":"snap-1"`,
		`"status":"RUNNING"`,
		`"video_name":"video.mp4"`,
		`"project_id":"proj-1"`,
		`"worker_name":"worker-X"`, // dual-aliasing critical invariant
		`"assigned_to":"worker-X"`, // dual-aliasing critical invariant
		`"lease_id":"lease-X"`,
		`"retry_count":2`,
		`"attempt":2`,
		`"max_retries":5`,
		`"run_id":"run-X"`,
		`"job_run_id":"run-X"`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("wire shape invariant missing: %q\nfull output: %s", key, got)
		}
	}
}

// TestToPayloadMapWireShapeSnapshot locks the JSON wire shape produced by
// ToPayloadMap (mirrors queue.QueryService.GetJobPayload).
func TestToPayloadMapWireShapeSnapshot(t *testing.T) {
	// Case 1: LeaseID non-empty → injected. Payload merges.
	j := &Job{
		ID:       "snap-payload-1",
		RunID:    "run-payload-1",
		LeaseID:  "lease-payload-1",
		Status:   StatusRunning,
		VideoName: "video.mp4",
		ProjectID: "proj-1",
		Payload:  `{"custom_field":"custom_value","nested":{"k":"v"}}`,
	}
	m := ToPayloadMap(j)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	for _, key := range []string{
		`"job_id":"snap-payload-1"`,
		`"job_run_id":"run-payload-1"`,
		`"run_id":"run-payload-1"`,
		`"status":"RUNNING"`,
		`"video_name":"video.mp4"`,
		`"project_id":"proj-1"`,
		`"lease_id":"lease-payload-1"`,
		`"custom_field":"custom_value"`, // payload must be merged
	} {
		if !strings.Contains(got, key) {
			t.Errorf("payload map invariant missing: %q\nfull output: %s", key, got)
		}
	}

	// Case 2: LeaseID empty → lease_id key NOT present.
	j2 := &Job{ID: "snap-payload-2", RunID: "run-payload-2"}
	m2 := ToPayloadMap(j2)
	if _, exists := m2["lease_id"]; exists {
		t.Errorf("lease_id should be absent for empty LeaseID, got %v", m2["lease_id"])
	}
}

// TestToFlatMapWireShapeSnapshot locks the JSON wire shape produced by
// ToFlatMap (mirrors queue.QueryService.GetJobAsMap).
//
// Maps are JSON-marshaled with ALL keys present — blank strings become ""
// and nil becomes null. We assert that all blank-string keys are emitted
// as literal "" (HTTP consumer expectation).
func TestToFlatMapWireShapeSnapshot(t *testing.T) {
	j := &Job{
		ID:         "snap-flat",
		Status:     StatusPending,
		WorkerID:   "worker-F",
		Attempts:   1,
		MaxRetries: 3,
		RunID:      "run-F",
		VideoName:  "flat.mp4",
		ProjectID:  "proj-F",
		LeaseID:    "lease-F",
		Payload:    `{"custom":"val"}`,
	}
	m := ToFlatMap(j)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	for _, key := range []string{
		`"job_id":"snap-flat"`,
		`"status":"PENDING"`,
		`"video_name":"flat.mp4"`,
		`"project_id":"proj-F"`,
		`"assigned_to":"worker-F"`,
		`"worker_name":"worker-F"`,
		`"lease_id":"lease-F"`,
		`"retry_count":1`,
		`"attempt":1`,
		`"max_retries":3`,
		`"run_id":"run-F"`,
		`"job_run_id":"run-F"`,
		`"custom":"val"`, // payload-merge (no override of top-level)
	} {
		if !strings.Contains(got, key) {
			t.Errorf("flat map invariant missing: %q\nfull output: %s", key, got)
		}
	}
	// Blank-string keys MUST be present literally (HTTP consumer expectation).
	for _, key := range []string{
		`"claimed_by":""`,
		`"claimed_at":""`,
		`"last_error":""`,
		`"error_message":""`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("flat map blank-string key invariant missing: %q\nfull output: %s", key, got)
		}
	}
	// lease_expiry is nil interface{} → marshaled as null.
	if !strings.Contains(got, `"lease_expiry":null`) {
		t.Errorf("flat map lease_expiry null invariant missing\nfull output: %s", got)
	}
}
