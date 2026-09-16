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
	// FMP4StreamProfile opens (1) or closes (0) the fragmented-MP4 admission
	// gate (VELOX_FMP4_STREAM_PROFILE) on an ALREADY-INSTALLED worker. It is
	// the canonical rollout path: deploy/runtime/worker.env.example only
	// seeds fresh hosts, and hand-editing worker.env is forbidden by
	// docs/operations/worker-rollout-paths.md §5. It is an admission gate,
	// never a job selector: jobs must still arrive with
	// output.profile_id=velox-h264-fmp4-stream-v1.
	FMP4StreamProfile *int `json:"fmp4_stream_profile,omitempty"`
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
	if payload.AudioMixStrategy == "" && payload.AudioMixProfile == nil && payload.FMP4StreamProfile == nil {
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
	if payload.FMP4StreamProfile != nil {
		if *payload.FMP4StreamProfile != 0 && *payload.FMP4StreamProfile != 1 {
			return fmt.Errorf("worker config: invalid fmp4_stream_profile %d", *payload.FMP4StreamProfile)
		}
		command += fmt.Sprintf(" --fmp4-stream-profile %d", *payload.FMP4StreamProfile)
	}
	if _, err := e.SSH.Run(ctx, op.WorkerID, command); err != nil {
		return fmt.Errorf("worker config: apply helper: %w", err)
	}
	return nil
}
