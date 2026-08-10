package observability

import "testing"

type identityWorkerReader struct{}

func (identityWorkerReader) ListWorkers() ([]map[string]any, error) { return nil, nil }
func (identityWorkerReader) GetWorker(string) (map[string]any, error) {
	return map[string]any{
		"worker_id":   "host_57_129_132_133",
		"worker_name": "velox-worker-01",
	}, nil
}

func TestWorkerDisplayNameUsesCanonicalRegistryName(t *testing.T) {
	svc := &Service{workers: identityWorkerReader{}}
	if got := svc.workerDisplayName("host_57_129_132_133"); got != "velox-worker-01" {
		t.Fatalf("worker display name = %q, want canonical worker_name", got)
	}
}

func TestWorkerDisplayNameDoesNotRewriteWorkerID(t *testing.T) {
	svc := &Service{workers: identityWorkerReader{}}
	if got := svc.workerDisplayName("host_57_129_132_133"); got == "host_57_129_132_133" {
		t.Fatal("worker display name must come from worker_name, not rewrite worker_id")
	}
}
