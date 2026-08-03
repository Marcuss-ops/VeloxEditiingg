package grpcserver

import (
	"encoding/json"
	"fmt"
	"testing"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestProjectPayloadForWorker_VersionMatrix(t *testing.T) {
	canonical := map[string]interface{}{
		"payload_contract_version": contract.PayloadContractVersionCanonical,
		"video_name":               "Version matrix",
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"asset_id":    "clip-1",
					"url":         "velox-asset://clip-1",
					"duration_ms": 5000,
				},
				"duration_seconds": 5.0,
			},
		},
	}
	before := payloadJSONForTest(canonical)

	for _, tc := range []struct {
		name      string
		executorV int
	}{
		{name: "unknown", executorV: 0},
		{name: "legacy", executorV: 1},
		{name: "canonical", executorV: 2},
		{name: "newer-canonical", executorV: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := projectPayloadForWorker(canonical, tc.executorV)
			if err != nil {
				t.Fatalf("projectPayloadForWorker: %v", err)
			}
			if _, err := structpb.NewStruct(wire); err != nil {
				t.Fatalf("structpb.NewStruct: %v", err)
			}
			gotVersion, ok := wire["payload_contract_version"].(float64)
			if !ok {
				t.Fatalf("payload_contract_version type = %T, want JSON number", wire["payload_contract_version"])
			}
			assertNoForbiddenWorkerKeys(t, wire)
			if int(gotVersion) != contract.PayloadContractVersionCanonical {
				t.Fatalf("payload_contract_version = %v, want %d", gotVersion, contract.PayloadContractVersionCanonical)
			}
		})
	}

	if got := payloadJSONForTest(canonical); got != before {
		t.Fatalf("canonical payload mutated by worker projection:\nbefore=%s\nafter=%s", before, got)
	}
}

func TestSendClaimedTaskOffer_UsesWorkerSpecificPayloadContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		executorV int
	}{
		{name: "unknown-worker", executorV: 0},
		{name: "legacy-worker", executorV: 1},
		{name: "canonical-worker", executorV: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]interface{}{
				"payload_contract_version": contract.PayloadContractVersionCanonical,
				"video_name":               "Task offer",
				"scenes": []interface{}{
					map[string]interface{}{
						"clip": map[string]interface{}{
							"asset_id":    "clip-1",
							"url":         "velox-asset://clip-1",
							"duration_ms": 5000,
						},
						"duration_seconds": 5.0,
					},
				},
			}
			original := payloadJSONForTest(payload)
			sess := &workerSession{workerID: "worker-1", sendCh: make(chan *outboundMessage, 1)}
			h := &Handler{}
			tws := &taskgraph.TaskWithSpec{Task: taskgraph.Task{
				ID: "task-1", JobID: "job-1", ExecutorID: "scene.composite.v1", ExecutorVersion: tc.executorV,
			}, SpecPayload: payload}
			attempt := &taskattempts.TaskAttempt{ID: "attempt-1", AttemptNumber: 1}

			h.sendClaimedTaskOffer(t.Context(), sess, tws, attempt, "lease-1")
			out := <-sess.sendCh
			if out == nil || out.Envelope == nil || out.Envelope.GetTaskOffer() == nil {
				t.Fatal("sendClaimedTaskOffer did not enqueue a TaskOffer")
			}
			wire := out.Envelope.GetTaskOffer().GetTaskSpec().AsMap()
			assertNoForbiddenWorkerKeys(t, wire)
			if got := wire["payload_contract_version"]; got != float64(contract.PayloadContractVersionCanonical) {
				t.Fatalf("TaskOffer payload_contract_version = %v, want %v", got, float64(contract.PayloadContractVersionCanonical))
			}
			if got := payloadJSONForTest(payload); got != original {
				t.Fatalf("canonical payload mutated by TaskOffer projection:\nbefore=%s\nafter=%s", original, got)
			}
		})
	}
}

func assertNoForbiddenWorkerKeys(t *testing.T, payload map[string]interface{}) {
	t.Helper()
	forbidden := map[string]struct{}{}
	for _, key := range []string{
		"voiceover_paths", "clip_link", "image_link", "local_path", "bindings",
		"project_id", "render_spec", "delivery_plan", "publications", "publication_specs",
		"metadata", "title", "description", "privacy_status", "destination_id", "destination_ids",
	} {
		forbidden[key] = struct{}{}
	}
	var walk func(interface{}, string)
	walk = func(value interface{}, path string) {
		switch typed := value.(type) {
		case map[string]interface{}:
			for key, child := range typed {
				if _, blocked := forbidden[key]; blocked {
					t.Errorf("forbidden worker key %q at %s", key, path)
				}
				walk(child, path+"."+key)
			}
		case []interface{}:
			for index, child := range typed {
				walk(child, fmt.Sprintf("%s[%d]", path, index))
			}
		}
	}
	walk(payload, "payload")
}

func payloadJSONForTest(input map[string]interface{}) string {
	encoded, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
