package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

const (
	runtimeFinalizeRequestKey = "runtime_finalize_request"
	runtimeFinalizeStatusKey  = "runtime_finalize_status"
	runtimeFinalizeErrorKey   = "runtime_finalize_error"
	runtimeFinalizeQueued     = "QUEUED"
	runtimeFinalizeRunning    = "RUNNING"
	runtimeFinalizeSucceeded  = "SUCCEEDED"
	runtimeFinalizeFailed     = "FAILED"
	runtimeFinalizePoll       = time.Second
	runtimeFinalizeBatch      = 16
)

func isNilInterface(value interface{}) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}

// RuntimeStockWindowPreparer is the expensive Master-owned part of FINALIZE.
// It runs after the request has been durably accepted, never in the HTTP
// handler's request context.
type RuntimeStockWindowPreparer interface {
	PrepareDriveStockWindows(context.Context, string, []map[string]interface{}) (string, string, error)
}

// RuntimeTaskUpdateNotifier refreshes the worker-scoped future asset plan only
// after the final render plan and its runtime bindings have been committed.
type RuntimeTaskUpdateNotifier interface {
	NotifyTaskUpdated(context.Context, string)
}

type RuntimeFinalizationProcessor struct {
	queue    taskgraph.RuntimeFinalizationQueue
	specs    taskgraph.TaskSpecStore
	assets   RuntimeStockWindowPreparer
	notifier RuntimeTaskUpdateNotifier
}

// NewRuntimeFinalizationProcessor fails bootstrap when the persistent queue,
// real asset preparer or post-commit planner hook is missing. A production
// FINALIZE must never be accepted into an in-memory-only/noop path.
func NewRuntimeFinalizationProcessor(
	repository taskgraph.Repository,
	assets RuntimeStockWindowPreparer,
	notifier RuntimeTaskUpdateNotifier,
) (*RuntimeFinalizationProcessor, error) {
	if isNilInterface(repository) {
		return nil, errors.New("runtime finalizer: task repository is required")
	}
	queue, ok := repository.(taskgraph.RuntimeFinalizationQueue)
	if !ok || isNilInterface(queue) {
		return nil, errors.New("runtime finalizer: durable queue is not configured")
	}
	specs, ok := repository.(taskgraph.TaskSpecStore)
	if !ok || isNilInterface(specs) {
		return nil, errors.New("runtime finalizer: task spec store is not configured")
	}
	if isNilInterface(assets) {
		return nil, errors.New("runtime finalizer: stock window preparer is not configured")
	}
	if isNilInterface(notifier) {
		return nil, errors.New("runtime finalizer: task update notifier is not configured")
	}
	return &RuntimeFinalizationProcessor{queue: queue, specs: specs, assets: assets, notifier: notifier}, nil
}

// Run replays queued and interrupted FINALIZE work. Queue state and request
// bytes live in task_specs, so restarting the Master does not lose accepted
// work. Per-job materialization failures are persisted for GET polling and do
// not stop unrelated finalizations.
func (p *RuntimeFinalizationProcessor) Run(ctx context.Context) error {
	if p == nil || p.queue == nil || p.specs == nil || p.assets == nil || p.notifier == nil {
		return errors.New("runtime finalizer: processor is not configured")
	}
	if err := p.queue.RecoverInterruptedRuntimeFinalizations(ctx); err != nil {
		return fmt.Errorf("runtime finalizer: recover interrupted work: %w", err)
	}
	ticker := time.NewTicker(runtimeFinalizePoll)
	defer ticker.Stop()
	for {
		candidates, err := p.queue.ListRuntimeFinalizationCandidates(ctx, runtimeFinalizeBatch)
		if err != nil {
			return fmt.Errorf("runtime finalizer: list queue: %w", err)
		}
		for _, candidate := range candidates {
			if err := ctx.Err(); err != nil {
				return nil
			}
			if err := p.processOne(ctx, candidate); err != nil {
				return err
			}
		}
		if len(candidates) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *RuntimeFinalizationProcessor) processOne(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate) error {
	current, err := p.specs.GetTaskSpecPayload(ctx, candidate.TaskID)
	if err != nil {
		return fmt.Errorf("runtime finalizer: read task %s: %w", candidate.TaskID, err)
	}
	state, _ := current[runtimeFinalizeStatusKey].(string)
	if state != runtimeFinalizeQueued && state != runtimeFinalizeRunning {
		return nil
	}
	if _, err := p.specs.MergeTaskSpecPayload(ctx, candidate.TaskID, map[string]interface{}{runtimeFinalizeStatusKey: runtimeFinalizeRunning}); err != nil {
		return fmt.Errorf("runtime finalizer: claim task %s: %w", candidate.TaskID, err)
	}
	current, err = p.specs.GetTaskSpecPayload(ctx, candidate.TaskID)
	if err != nil {
		return fmt.Errorf("runtime finalizer: reread task %s: %w", candidate.TaskID, err)
	}
	requestValue, ok := current[runtimeFinalizeRequestKey]
	if !ok || requestValue == nil {
		return p.fail(candidate, errors.New("durable finalize request is missing"))
	}
	encoded, err := json.Marshal(requestValue)
	if err != nil {
		return p.fail(candidate, fmt.Errorf("encode durable finalize request: %w", err))
	}
	var req FinalizeJobRequest
	if err := json.Unmarshal(encoded, &req); err != nil {
		return p.fail(candidate, fmt.Errorf("decode durable finalize request: %w", err))
	}
	planJSON, _ := current[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	patch, err := buildRuntimeFinalizePatch(ctx, planJSON, req, current, p.assets)
	if err != nil {
		return p.fail(candidate, err)
	}
	if _, err := p.specs.MergeTaskSpecPayload(ctx, candidate.TaskID, patch); err != nil {
		return fmt.Errorf("runtime finalizer: commit task %s: %w", candidate.TaskID, err)
	}
	p.notifier.NotifyTaskUpdated(ctx, candidate.JobID)
	log.Printf("[PIPELINE] runtime FINALIZE materialized job_id=%s task_id=%s", candidate.JobID, candidate.TaskID)
	return nil
}

func (p *RuntimeFinalizationProcessor) fail(candidate taskgraph.RuntimeFinalizationCandidate, err error) error {
	message := strings.TrimSpace(err.Error())
	if len(message) > 2048 {
		message = message[:2048]
	}
	if _, mergeErr := p.specs.MergeTaskSpecPayload(context.Background(), candidate.TaskID, map[string]interface{}{
		runtimeFinalizeStatusKey: runtimeFinalizeFailed,
		runtimeFinalizeErrorKey:  message,
		"runtime_assets_pending": true,
	}); mergeErr != nil {
		return fmt.Errorf("runtime finalizer: persist failure for task %s (%v): %w", candidate.TaskID, err, mergeErr)
	}
	log.Printf("[PIPELINE] runtime FINALIZE failed job_id=%s task_id=%s error=%q", candidate.JobID, candidate.TaskID, message)
	return nil
}

func buildRuntimeFinalizePatch(
	ctx context.Context,
	planJSON string,
	req FinalizeJobRequest,
	current map[string]interface{},
	assets RuntimeStockWindowPreparer,
) (map[string]interface{}, error) {
	bindings := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	var err error
	bindings, err = appendRuntimeAssetBindings(bindings, req.RuntimePayload["runtime_assets"])
	if err == nil {
		bindings, err = appendRuntimeAssetBindings(bindings, current["runtime_assets"])
	}
	if storedPayload, ok := current["runtime_payload"].(map[string]interface{}); ok && err == nil {
		bindings, err = appendRuntimeAssetBindings(bindings, storedPayload["runtime_assets"])
	}
	if err != nil {
		return nil, fmt.Errorf("invalid runtime assets: %w", err)
	}
	preparedStockPlan := compiledPlanHasDriveStock(planJSON, bindings)
	finalRuntimeAssets := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	if preparedStockPlan {
		if isNilInterface(assets) {
			return nil, errors.New("stock window preparation capability is unavailable")
		}
		preparedPlan, preparedHash, prepareErr := assets.PrepareDriveStockWindows(ctx, planJSON, bindings)
		if prepareErr != nil {
			return nil, fmt.Errorf("stock window materialization: %w", prepareErr)
		}
		finalRuntimeAssets = removePreparedStockBindings(bindings)
		if req.RuntimePayload == nil {
			req.RuntimePayload = make(map[string]interface{})
		}
		req.RuntimePayload["runtime_assets"] = finalRuntimeAssets
		req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON] = preparedPlan
		req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA] = preparedHash
	}
	patch := make(map[string]interface{}, len(req.RuntimePayload)+8)
	for key, value := range req.RuntimePayload {
		if key == "runtime_assets_pending" || key == runtimeFinalizeStatusKey || key == runtimeFinalizeRequestKey || key == runtimeFinalizeErrorKey {
			continue
		}
		patch[key] = value
	}
	if preparedStockPlan {
		patch["runtime_assets"] = finalRuntimeAssets
		patch[contract.PayloadKeyCompiledRenderPlanJSON] = req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON]
		patch[contract.PayloadKeyCompiledRenderPlanSHA] = req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA]
	} else if len(req.RuntimeAssets) > 0 {
		patch["runtime_assets"] = req.RuntimeAssets
	}
	patch["runtime_assets_pending"] = false
	patch["runtime_finalize_idempotency_key"] = req.IdempotencyKey
	patch[runtimeFinalizeStatusKey] = runtimeFinalizeSucceeded
	patch[runtimeFinalizeRequestKey] = nil
	patch[runtimeFinalizeErrorKey] = nil
	return patch, nil
}
