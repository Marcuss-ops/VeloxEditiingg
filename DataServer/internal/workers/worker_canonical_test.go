package workers

import (
	"encoding/json"
	"reflect"
	"testing"

	"velox-shared/identity"
)

func TestWorkerInfoIsCanonicalWorkerAlias(t *testing.T) {
	var canonical Worker
	var compatibility WorkerInfo
	if reflect.TypeOf(canonical) != reflect.TypeOf(compatibility) {
		t.Fatalf("WorkerInfo must alias Worker: canonical=%T compatibility=%T", canonical, compatibility)
	}
}

func TestWorkerJSONKeepsTypedWorkerIDAsString(t *testing.T) {
	worker := Worker{WorkerID: identity.ParseWorkerID("worker-json-001")}
	encoded, err := json.Marshal(worker)
	if err != nil {
		t.Fatalf("marshal Worker: %v", err)
	}
	var wire map[string]interface{}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("unmarshal Worker JSON: %v", err)
	}
	if got, ok := wire["worker_id"].(string); !ok || got != "worker-json-001" {
		t.Fatalf("worker_id JSON = %#v, want string %q", wire["worker_id"], "worker-json-001")
	}
}
