package fleet

import (
	"context"
	"errors"
	"fmt"
)

// CanonicalWorkerRuntimePreflight is the read-only gate for release updates.
// It checks the same fixed names used by SSHWorkerDockerClient and the
// activation helper, so a legacy/partially-installed host is rejected before
// the update executor changes drain state.
type CanonicalWorkerRuntimePreflight struct {
	SSH BackendSSHClient
}

var _ BackendRuntimePreflight = (*CanonicalWorkerRuntimePreflight)(nil)

var canonicalWorkerRuntimePreflightCommands = []string{
	"test -r /etc/velox-worker/worker.env",
	"test -r /opt/velox-worker/compose.yml",
	"test -x /usr/local/sbin/velox-worker-activate-image",
	"systemctl cat velox-worker.service",
	"docker compose --project-name velox-worker --file /opt/velox-worker/compose.yml config --quiet",
	"docker inspect --format '{{.State.Running}}' velox-worker",
	"curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready",
}

func (p *CanonicalWorkerRuntimePreflight) Check(ctx context.Context, workerID string) error {
	if p == nil || p.SSH == nil {
		return errors.New("canonical runtime preflight: ssh client not wired")
	}
	if workerID == "" {
		return errors.New("canonical runtime preflight: worker id empty")
	}
	for _, command := range canonicalWorkerRuntimePreflightCommands {
		if _, err := p.SSH.Run(ctx, workerID, command); err != nil {
			return fmt.Errorf("canonical runtime contract failed (%s): %w", command, err)
		}
	}
	return nil
}

// CanonicalWorkerRuntimePreflightCommand is exposed only for contract tests
// and diagnostics; production callers should use Check.
func CanonicalWorkerRuntimePreflightCommand() string {
	return fmt.Sprintf("%v", canonicalWorkerRuntimePreflightCommands)
}
