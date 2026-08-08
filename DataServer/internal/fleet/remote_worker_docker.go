package fleet

import (
	"context"
	"fmt"

	"velox-server/internal/deploy"
)

// SSHWorkerDockerClient is the canonical worker container adapter. The
// worker owns one fixed Compose project/container; the worker id is used only
// to select the SSH target in WorkerRegistry.
type SSHWorkerDockerClient struct {
	SSH BackendSSHClient
}

var _ BackendDockerClient = (*SSHWorkerDockerClient)(nil)

func (c *SSHWorkerDockerClient) ActivateImage(ctx context.Context, workerID, imageRef string) (string, error) {
	if c == nil || c.SSH == nil {
		return "", fmt.Errorf("ssh docker: ssh client not wired")
	}
	if err := deploy.ValidateImageRef(imageRef); err != nil {
		return "", fmt.Errorf("ssh docker: invalid image ref: %w", err)
	}
	// The fixed, root-owned helper performs the worker-side transaction:
	// docker pull, atomic VELOX_WORKER_IMAGE replacement, systemd restart,
	// container image assertion, and /health/ready validation. Only the
	// validated immutable ref is placed in this fixed command.
	return c.SSH.Run(ctx, workerID, "sudo -n /usr/local/sbin/velox-worker-activate-image "+imageRef)
}

func (c *SSHWorkerDockerClient) ContainerRunning(ctx context.Context, workerID string) (bool, error) {
	if c == nil || c.SSH == nil {
		return false, fmt.Errorf("ssh docker: ssh client not wired")
	}
	out, err := c.SSH.Run(ctx, workerID, "docker inspect --format '{{.State.Running}}' velox-worker")
	return trim(out) == "true", err
}
