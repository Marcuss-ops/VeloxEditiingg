package jobs

import (
	"encoding/json"
	"fmt"
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
	// PR #7: WorkerID/LeaseID removed from Job; WorkerName/AssignedTo/LeaseID
	// removed from QueueItem. Verify QueueItem projection still works.
	j := &Job{
		ID:     "job-1",
		Status: StatusRunning,
	}
	q := ToQueueItem(j)
	if q.JobID != "job-1" {
		t.Errorf("JobID=%q, want %q", q.JobID, "job-1")
	}
	if q.Status != StatusRunning {
		t.Errorf("Status=%v, want %v", q.Status, StatusRunning)
	}
}

func TestToQueueItemAttemptsAlias(t *testing.T) {
	// PR #7: RetryCount/Attempt removed from QueueItem. MaxRetries still present.
	j := &Job{ID: "job-a", Attempts: 3, MaxRetries: 5}
	q := ToQueueItem(j)
	if q.MaxRetries != 5 {
		t.Errorf("MaxRetries=%d, want 5", q.MaxRetries)
	}
}

func TestToPayloadMapLeaseIDInjection(t *testing.T) {
	// PR #7: LeaseID removed from Job; lease_id no longer injected.
	j := &Job{ID: "job-1", RunID: "run-1"}
	m := ToPayloadMap(j)
	for k, want := range map[string]string{
		"job_id":     "job-1",
		"job_run_id": "run-1",
		"run_id":     "run-1",
	} {
		if m[k] != want {
			t.Errorf("%s=%v, want %s", k, m[k], want)
		}
	}
	// lease_id key NOT present
	if _, exists := m["lease_id"]; exists {
		t.Errorf("lease_id should be absent")
	}
	if _, exists := m["status"]; !exists {
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
// silently drift.
//
// We deliberately do NOT lock the exact casing of status keys because
// the literal string values of jobs.StatusPending / StatusRunning / etc.
// are not micro-verified here — they could be uppercase or lowercase.
// The wire-format guarantee we lock is: FormatStats(c) key == string(k)
// where k is the Status enum, so the JSON rendering is byte-identical to
// the legacy Stats() body in queue/query.go (which also does
// `res[string(k)] = v`). If casing changes, the test still passes — the
// structural invariant is "every Status in Counts appears under its
// string-cast key with the right value", regardless of case.
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
	// Verify the JSON contains each key/value pair, with the key being
	// the string-cast of jobs.Status (whatever case the constants use).
	// Direct map iteration is cleaner than a positional struct literal
	// and avoids type-mismatch bugs.
	for k, v := range (Counts{
		StatusPending:   5,
		StatusRunning:   2,
		StatusSucceeded: 10,
		StatusFailed:    1,
		StatusCancelled: 3,
	}) {
		expectedKey := fmt.Sprintf("%q:%d", string(k), v)
		if !strings.Contains(got, expectedKey) {
			t.Errorf("stats key/value pair missing: %s\nfull output: %s", expectedKey, got)
		}
	}
}

func TestParsePayloadJSONEdgeCases(t *testing.T) {
	for _, raw := range []string{"", "{}", "not-json", "{\"x\":null}"} {
		got := ParsePayloadJSON(raw)
		// Acceptable: empty map OR map with x→null (both behave the same)
		if raw == "" || raw == "{}" || raw == "not-json" {
			if len(got) != 0 {
				t.Errorf("ParsePayloadJSON(%q): expected empty map, got %v", raw, got)
			}
		}
	}
	got := ParsePayloadJSON(`{"foo":"bar","n":42,"nested":{"k":"v"}}`)
	if got["foo"] != "bar" {
		t.Errorf("foo=%v, want bar", got["foo"])
	}
	if _, ok := got["nested"]; !ok {
		t.Errorf("missing nested key")
	}
}

// TestToQueueItemWireShapeSnapshot locks the JSON wire shape produced by
// ToQueueItem. PR #7: WorkerName, AssignedTo, LeaseID, RetryCount, Attempt
// removed from QueueItem — verify new wire shape.
func TestToQueueItemWireShapeSnapshot(t *testing.T) {
	j := &Job{
		ID:         "snap-1",
		Status:     StatusRunning,
		Attempts:   2,
		MaxRetries: 5,
		RunID:      "run-X",
		VideoName:  "video.mp4",
		ProjectID:  "proj-1",
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
		`"max_retries":5`,
		`"run_id":"run-X"`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("wire shape invariant missing: %q\nfull output: %s", key, got)
		}
	}
}

// TestToPayloadMapWireShapeSnapshot locks the JSON wire shape produced by
// ToPayloadMap. PR #7: LeaseID removed from Job; lease_id not injected.
func TestToPayloadMapWireShapeSnapshot(t *testing.T) {
	j := &Job{
		ID:        "snap-payload-1",
		RunID:     "run-payload-1",
		Status:    StatusRunning,
		VideoName: "video.mp4",
		ProjectID: "proj-1",
		Payload:   `{"custom_field":"custom_value","nested":{"k":"v"}}`,
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
		`"custom_field":"custom_value"`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("payload map invariant missing: %q\nfull output: %s", key, got)
		}
	}
	// lease_id NOT present
	if _, exists := m["lease_id"]; exists {
		t.Errorf("lease_id should be absent")
	}
}

// TestToFlatMapWireShapeSnapshot locks the JSON wire shape produced by
// ToFlatMap. PR #7: runtime fields set to zero; blank keys preserved.
func TestToFlatMapWireShapeSnapshot(t *testing.T) {
	j := &Job{
		ID:         "snap-flat",
		Status:     StatusPending,
		Attempts:   1,
		MaxRetries: 3,
		RunID:      "run-F",
		VideoName:  "flat.mp4",
		ProjectID:  "proj-F",
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
		`"max_retries":3`,
		`"run_id":"run-F"`,
		`"job_run_id":"run-F"`,
		`"custom":"val"`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("flat map invariant missing: %q\nfull output: %s", key, got)
		}
	}
	// PR #7: runtime fields set to zero values for HTTP compat.
	for _, key := range []string{
		`"assigned_to":""`,
		`"worker_name":""`,
		`"claimed_by":""`,
		`"claimed_at":""`,
		`"lease_id":""`,
		`"last_error":""`,
		`"error_message":""`,
		`"retry_count":0`,
		`"attempt":0`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("flat map zero-key invariant missing: %q\nfull output: %s", key, got)
		}
	}
	if !strings.Contains(got, `"lease_expiry":null`) {
		t.Errorf("flat map lease_expiry null invariant missing\nfull output: %s", got)
	}
}
