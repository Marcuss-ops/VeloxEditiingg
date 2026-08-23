package fleet

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"velox-server/internal/store"
)

func TestUpdate_ForwardRowTransitions_SUCCEEDED(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = validImageRef()
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err != nil {
		t.Fatalf("happy path Execute returned err %v", err)
	}
	// Single PENDING forward row → marked SUCCEEDED.
	if len(st.insertedRows) != 1 {
		t.Fatalf("expected 1 forward row, got %d", len(st.insertedRows))
	}
	if st.markedStatuses[st.insertedRows[0].DeploymentID] != store.DeployStatusSucceeded {
		t.Errorf("forward row not marked SUCCEEDED: %+v", st.markedStatuses)
	}
}
func TestUpdate_ForwardRowTransitions_FAILED(t *testing.T) {
	backend, st := stubBackends(t)
	st.cosignErr = errors.New("bad sig")
	e := NewUpdateExecutor(backend)
	_ = e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if len(st.insertedRows) < 2 {
		t.Fatalf("forward+rollback: expected 2 rows, got %d", len(st.insertedRows))
	}
	if st.markedStatuses[st.insertedRows[0].DeploymentID] != store.DeployStatusFailed {
		t.Errorf("forward row not marked FAILED: %+v", st.markedStatuses)
	}
}
func TestClassifyDeploymentError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil is no error", nil, ""},
		{"digest mismatch sentinel", fmt.Errorf("%w: expected=X observed=Y", ErrDigestMismatch), DeploymentErrorCodeDigestMismatch},
		{"digest mismatch re-wrapped", fmt.Errorf("verify_digest: %w", ErrDigestMismatch), DeploymentErrorCodeDigestMismatch},
		{"drain wait prefix", errors.New("update: drain wait: worker busy"), DeploymentErrorCodeDrainTimeout},
		{"waiting_ready prefix", errors.New("waiting_ready: worker did not reconnect on a NEW authenticated session within budget"), DeploymentErrorCodeReadyTimeout},
		{"activate image prefix", errors.New("activate image: docker pull failed: network unreachable"), DeploymentErrorCodeDeployCommandFailed},
		{"cosign prefix", errors.New("cosign: invalid signature"), DeploymentErrorCodeDeployCommandFailed},
		{"container_running prefix", fmt.Errorf("container_running: %w", ErrContainerUnhealthy), DeploymentErrorCodeRestartFailed},
		{"health_ready prefix", errors.New("health_ready: curl exit 7"), DeploymentErrorCodeRestartFailed},
		{"ssh transport signature", errors.New("health_ready: ssh: connect to host port 22: connection refused"), DeploymentErrorCodeSSHFailed},
		{"smoke sentinel", fmt.Errorf("%w: ffmpeg rc=1", ErrSmokeFailed), DeploymentErrorCodeSmokeFailed},
		{"drive missing sentinel", ErrDriveDeliveryMissing, DeploymentErrorCodeDriveFailed},
		{"drive size sentinel", ErrDriveDeliverySize, DeploymentErrorCodeDriveFailed},
		{"rollback cascade prefix", errors.New("rollback activate previous_digest: network unreachable"), DeploymentErrorCodeRollbackFailed},
		{"unknown defaults to deploy command", errors.New("mystery failure"), DeploymentErrorCodeDeployCommandFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyDeploymentError(tc.err); got != tc.want {
				t.Errorf("classifyDeploymentError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
