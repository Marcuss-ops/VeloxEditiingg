package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/store"
)

func (e *UpdateExecutor) releaseOwnedDrain(ctx context.Context, workerID string, owned bool) error {
	if !owned {
		return nil
	}
	if e.backend.Registry == nil {
		return errors.New("update: registry gater not wired (cannot release drain)")
	}
	if err := e.backend.Registry.SetDrainMode(ctx, workerID, false); err != nil {
		return err
	}
	return nil
}

func (e *UpdateExecutor) parsePayload(op *store.Operation) (string, string, error) {
	if len(op.Payload) == 0 || string(op.Payload) == "{}" {
		return "", "", errors.New("update: payload empty (target_digest required)")
	}
	var p UpdatePayload
	if err := json.Unmarshal(op.Payload, &p); err != nil {
		return "", "", fmt.Errorf("update: payload parse: %w", err)
	}
	if p.TargetDigest == "" {
		return "", "", errors.New("update: target_digest missing")
	}
	return strings.TrimSpace(p.TargetDigest), strings.TrimSpace(p.PreviousDigest), nil
}

// waitForIdle polls the registry until BOTH authoritative DRAINING
// conditions hold before the executor proceeds to DEPLOYING:
//
//  1. the registry read model reflects drain=true (IsDrained), and
//  2. active_tasks == 0 (IsActiveJobsZero).
//
// A drain() call that returns nil without the worker entering
// DRAINING must never advance the rollout — the flag, not the
// SetDrainMode return value, is the source of truth. If either
// condition is unsatisfied within the poll budget the rollout
// fails closed and the executor-owned drain is released by the
// caller.
func (e *UpdateExecutor) waitForIdle(ctx context.Context, workerID string) error {
	if e.backend.Registry == nil {
		// Defensive: callers should pass a wired gater, but
		// a missing dependency surfaces the failure explicitly.
		return errors.New("update: registry gater not wired (cannot confirm drain)")
	}
	drainTimeout := e.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = timeoutActiveJobsIdle
	}
	deadline := time.Now().Add(drainTimeout)
	pollTicker := time.NewTicker(time.Second)
	defer pollTicker.Stop()
	for {
		if e.backend.Registry.IsDrained(ctx, workerID) && e.backend.Registry.IsActiveJobsZero(ctx, workerID) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("worker did not reach DRAINING (drain=true and active_tasks=0) within budget")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait_for_idle: ctx cancelled: %w", ctx.Err())
		case <-pollTicker.C:
		}
	}
}
