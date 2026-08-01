package enqueue

import (
	"context"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/routing"
)

// =====================================================================
// Idempotency / routing tests
// =====================================================================
//
// Verifies the deterministic forwarding-key contract:
//   - DeriveForwardingJobID produces the same job_id for the same key
//     (idempotency on re-routing) and distinct job_ids for distinct
//     keys (no collision across the forwarding-keyspace).
//   - EnqueueWithForwardingKey persists the deterministic id AND
//     re-enqueueing with the same payload + same forwarding key
//     must surface the same job_id (replay-safe under retry).

// TestDeriveForwardingJobID_Idempotency verifies that the deterministic
// forwarding key always produces the same job ID.
func TestDeriveForwardingJobID_Idempotency(t *testing.T) {
	t.Parallel()
	key := "remote_engine:creator-job-123:scene.composite.v1"
	id1 := DeriveForwardingJobID(key)
	id2 := DeriveForwardingJobID(key)
	if id1 != id2 {
		t.Errorf("same key should produce same ID: %q != %q", id1, id2)
	}
	if id1 == "" {
		t.Error("job ID should not be empty")
	}
	if !strings.HasPrefix(id1, "job_") {
		t.Errorf("job ID should start with job_: %q", id1)
	}
}

func TestDeriveForwardingJobID_DifferentKeys(t *testing.T) {
	t.Parallel()
	id1 := DeriveForwardingJobID("openai:job-1:scene.composite.v1")
	id2 := DeriveForwardingJobID("openai:job-2:scene.composite.v1")
	if id1 == id2 {
		t.Errorf("different keys should produce different IDs: both are %q", id1)
	}
}

// TestEnqueueWithForwardingKey verifies that when a payload carries
// _internal_forwarding_key, the job_id is deterministic.
func TestValidateForwardingIdentity(t *testing.T) {
	t.Parallel()

	validKey := routing.FormatForwardingKey("remote_engine", "creator-forward-1", "scene.composite.v1").String()
	cases := []struct {
		name    string
		value   interface{}
		wantErr string
	}{
		{name: "valid", value: validKey},
		{name: "empty", value: "", wantErr: "required"},
		{name: "wrong_type", value: 42, wantErr: "must be a string"},
		{name: "incomplete", value: "remote_engine:creator-forward-1", wantErr: "must contain"},
		{name: "non_canonical_escape", value: "remote%engine:creator-forward-1:scene.composite.v1", wantErr: "not canonical"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]interface{}{routing.KeyForwardingKey: tc.value}
			got, present, err := validateForwardingIdentity(payload)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateForwardingIdentity: %v", err)
				}
				if !present || got != validKey {
					t.Fatalf("identity = (%q, %t), want (%q, true)", got, present, validKey)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
			if !present {
				t.Fatal("present = false, want true for an explicitly supplied key")
			}
		})
	}

	got, present, err := validateForwardingIdentity(map[string]interface{}{})
	if err != nil || present || got != "" {
		t.Fatalf("missing key = (%q, %t, %v), want (\"\", false, nil)", got, present, err)
	}
}

func TestEnqueueWithForwardingKey(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":             "Forwarded Video",
		"script_text":            "forwarded script",
		routing.KeyForwardingKey: "remote_engine:creator-forward-1:scene.composite.v1",
		"scenes": []interface{}{
			map[string]interface{}{"scene": "intro", "voiceover": "v1"},
		},
		"voiceover_paths": []string{"/tmp/v-forward.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
	}

	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobID, _ := response["job_id"].(string)
	expected := DeriveForwardingJobID("remote_engine:creator-forward-1:scene.composite.v1")
	if jobID != expected {
		t.Errorf("forwarding job_id = %q, want deterministic %q", jobID, expected)
	}

	// Second enqueue with same forwarding key should be idempotent.
	response2, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue (retry): %v", err)
	}
	jobID2, _ := response2["job_id"].(string)
	if jobID2 != jobID {
		t.Errorf("retry job_id = %q, want same %q", jobID2, jobID)
	}
}
