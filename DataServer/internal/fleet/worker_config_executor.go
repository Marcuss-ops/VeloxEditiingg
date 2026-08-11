package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"velox-server/internal/store"
)

// WorkerConfigPayload is the allowlisted worker.env surface exposed through
// the Master. Arbitrary environment mutation is intentionally forbidden.
type WorkerConfigPayload struct {
	AudioMixStrategy string `json:"audio_mix_strategy,omitempty"`
	AudioMixProfile  *int   `json:"audio_mix_profile,omitempty"`
}

// WorkerConfigExecutor updates the worker through the root-owned helper
// installed by prepare-host.sh. The SSH client never receives an arbitrary
// shell script or an operator-supplied path.
type WorkerConfigExecutor struct{ SSH BackendSSHClient }

func NewWorkerConfigExecutor(ssh BackendSSHClient) *WorkerConfigExecutor {
	return &WorkerConfigExecutor{SSH: ssh}
}

func (e *WorkerConfigExecutor) ValidateProductionBackends() error {
	if e == nil || e.SSH == nil {
		return errors.New("worker config SSH client is not wired")
	}
	return nil
}

func (e *WorkerConfigExecutor) Execute(ctx context.Context, op *store.Operation) error {
	if err := e.ValidateProductionBackends(); err != nil {
		return fmt.Errorf("worker config: %w", err)
	}
	if op == nil || op.WorkerID == "" {
		return errors.New("worker config: worker_id is required")
	}
	var payload WorkerConfigPayload
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return fmt.Errorf("worker config: invalid payload: %w", err)
	}
	if payload.AudioMixStrategy == "" && payload.AudioMixProfile == nil {
		return errors.New("worker config: no supported settings requested")
	}
	command := "sudo -n /usr/local/sbin/velox-worker-set-config"
	if payload.AudioMixStrategy != "" {
		if payload.AudioMixStrategy != "legacy" && payload.AudioMixStrategy != "optimized" && payload.AudioMixStrategy != "auto" {
			return fmt.Errorf("worker config: invalid audio_mix_strategy %q", payload.AudioMixStrategy)
		}
		command += " --audio-mix-strategy " + payload.AudioMixStrategy
	}
	if payload.AudioMixProfile != nil {
		if *payload.AudioMixProfile != 0 && *payload.AudioMixProfile != 1 {
			return fmt.Errorf("worker config: invalid audio_mix_profile %d", *payload.AudioMixProfile)
		}
		command += fmt.Sprintf(" --audio-mix-profile %d", *payload.AudioMixProfile)
	}
	if _, err := e.SSH.Run(ctx, op.WorkerID, command); err != nil {
		return fmt.Errorf("worker config: apply helper: %w", err)
	}
	return nil
}
