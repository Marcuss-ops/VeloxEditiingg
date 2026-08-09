package doctor

import (
	"context"
	"strings"
	"testing"

	"velox-worker-agent/pkg/config"
)

func TestProductionImageValidatorRejectsMutableReference(t *testing.T) {
	t.Setenv("VELOX_WORKER_IMAGE", "ghcr.io/example/velox-worker:v1.2.28-canonical")
	r := (&ProductionImageValidator{}).Run(context.Background(), config.DefaultConfig("/tmp/velox"))
	if r.Status != StatusFail {
		t.Fatalf("mutable image reference status=%s detail=%s", r.Status, r.Detail)
	}
}

func TestProductionDigestValidatorRequiresMasterEvidence(t *testing.T) {
	t.Setenv("VELOX_WORKER_IMAGE", "ghcr.io/example/velox-worker@sha256:"+strings.Repeat("a", 64))
	t.Setenv("VELOX_MASTER_DESIRED_IMAGE", "")
	r := (&ProductionDigestValidator{}).Run(context.Background(), config.DefaultConfig("/tmp/velox"))
	if r.Status != StatusFail {
		t.Fatalf("missing desired digest status=%s detail=%s", r.Status, r.Detail)
	}
}

func TestRunProductionTreatsWarningAsFailure(t *testing.T) {
	var buf strings.Builder
	_, err := RunProduction(context.Background(), config.DefaultConfig("/tmp/velox"), []Validator{
		warningValidator{},
	}, &buf)
	if err == nil {
		t.Fatal("production doctor accepted WARN")
	}
	if !strings.Contains(buf.String(), `"verdict": "NOT_READY"`) {
		t.Fatalf("report did not fail closed: %s", buf.String())
	}
}

type warningValidator struct{}

func (warningValidator) ID() string { return "test.warning" }
func (warningValidator) Run(context.Context, *config.WorkerConfig) Result {
	return warn("test.warning", "not independently verified")
}
