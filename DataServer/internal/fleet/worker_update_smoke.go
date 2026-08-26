package fleet

import (
	"context"
	"fmt"
)

// WorkerUpdateSmokeRunner validates the worker image locally after restart.
// It deliberately has no Drive or socialediting dependency.
type WorkerUpdateSmokeRunner struct{ SSH BackendSSHClient }

func NewWorkerUpdateSmokeRunner(ssh BackendSSHClient) *WorkerUpdateSmokeRunner {
	return &WorkerUpdateSmokeRunner{SSH: ssh}
}

func (r *WorkerUpdateSmokeRunner) RunLevelD(ctx context.Context, workerID string) (string, error) {
	if r == nil || r.SSH == nil {
		return "", fmt.Errorf("worker update smoke: ssh runner not wired")
	}
	const command = "ffmpeg -hide_banner -loglevel error -f lavfi -i color=c=black:s=16x16:r=1 -t 1 -f null -"
	if _, err := r.SSH.Run(ctx, workerID, command); err != nil {
		return "", fmt.Errorf("worker-local ffmpeg render: %w", err)
	}
	return "worker-local-render-smoke", nil
}

var _ BackendSmokeRunner = (*WorkerUpdateSmokeRunner)(nil)
