package pipeline

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

const (
	runtimeFinalizeRequestKey         = "runtime_finalize_request"
	runtimeFinalizeStatusKey          = "runtime_finalize_status"
	runtimeFinalizeErrorKey           = "runtime_finalize_error"
	runtimePrepareTaskIDsKey          = "runtime_prepare_task_ids"
	runtimePrepareTasksKey            = "runtime_prepare_window_tasks"
	runtimePrepareWindowReuseKey      = "runtime_prepare_window_reuse"
	runtimeFinalizeQueued             = "QUEUED"
	runtimeFinalizeRunning            = "RUNNING"
	runtimeFinalizeSucceeded          = "SUCCEEDED"
	runtimeFinalizeFailed             = "FAILED"
	runtimeFinalizeWaitingPreparation = "WAITING_PREPARATION"
	runtimeFinalizePoll               = time.Second
	runtimeFinalizeBatch              = 16
	runtimeFinalizeConcurrency        = 4
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

type RuntimeStockWindowTaskPlanner interface {
	PlanDriveStockWindowTasks(string, []map[string]interface{}) ([]contract.VideoPrepareWindowTaskV1, error)
}

// RuntimePreparedWindowCache exposes fragments already registered on this
// Master. FINALIZE consults it before publishing new preparation tasks so a
// window prepared for another job or another worker is reused instead of
// re-encoded.
type RuntimePreparedWindowCache interface {
	LookupPreparedWindowAsset(context.Context, string) (*voiceoverassets.PreparedWindowAssetBinding, error)
}

type RuntimePreparedWindowPlanRewriter interface {
	RewriteDriveStockPlanFromWorkerResults(string, []map[string]interface{}, map[string]voiceoverassets.PreparedWindowAssetBinding) (string, string, []map[string]interface{}, error)
}

type RuntimeTaskGraphExpander interface {
	Extend(context.Context, string, []taskgraph.ExpandedTask, []string, map[string]interface{}) ([]taskgraph.Task, error)
}

// RuntimeTaskUpdateNotifier refreshes the worker-scoped future asset plan only
// after the final render plan and its runtime bindings have been committed.
type RuntimeTaskUpdateNotifier interface {
	NotifyTaskUpdated(context.Context, string)
}

type RuntimeFinalizationProcessor struct {
	queue     taskgraph.RuntimeFinalizationQueue
	specs     taskgraph.TaskSpecStore
	finalizer taskgraph.RuntimeTaskFinalizer
	expander  RuntimeTaskGraphExpander
	evidence  taskattempts.SuccessfulSegmentTimingReader
	assets    RuntimeStockWindowPreparer
	notifier  RuntimeTaskUpdateNotifier
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
	finalizer, ok := repository.(taskgraph.RuntimeTaskFinalizer)
	if !ok || isNilInterface(finalizer) {
		return nil, errors.New("runtime finalizer: atomic task executor update is not configured")
	}
	if isNilInterface(assets) {
		return nil, errors.New("runtime finalizer: stock window preparer is not configured")
	}
	if isNilInterface(notifier) {
		return nil, errors.New("runtime finalizer: task update notifier is not configured")
	}
	expander, err := taskgraph.NewExpandService(repository)
	if err != nil {
		return nil, fmt.Errorf("runtime finalizer: task graph expansion is not configured: %w", err)
	}
	if _, ok := repository.(taskgraph.AtomicTaskGraphExtender); !ok {
		return nil, errors.New("runtime finalizer: atomic task graph extension is not configured")
	}
	evidence, ok := repository.(taskattempts.SuccessfulSegmentTimingReader)
	if !ok || isNilInterface(evidence) {
		return nil, errors.New("runtime finalizer: successful segment evidence reader is not configured")
	}
	return &RuntimeFinalizationProcessor{queue: queue, specs: specs, finalizer: finalizer, expander: expander, evidence: evidence, assets: assets, notifier: notifier}, nil
}

// Run replays queued and interrupted FINALIZE work. Queue state and request
// bytes live in task_specs, so restarting the Master does not lose accepted
// work. Per-job materialization failures are persisted for GET polling and do
// not stop unrelated finalizations.
func (p *RuntimeFinalizationProcessor) Run(ctx context.Context) error {
	if p == nil || p.queue == nil || p.specs == nil || p.finalizer == nil || p.assets == nil || p.notifier == nil {
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
		if err := p.processBatch(ctx, candidates); err != nil {
			return err
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

func (p *RuntimeFinalizationProcessor) processBatch(ctx context.Context, candidates []taskgraph.RuntimeFinalizationCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	workers := runtimeFinalizeConcurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(candidates) {
		workers = len(candidates)
	}
	queue := make(chan taskgraph.RuntimeFinalizationCandidate)
	errCh := make(chan error, len(candidates))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for candidate := range queue {
				if err := ctx.Err(); err != nil {
					return
				}
				if err := p.processOne(ctx, candidate); err != nil {
					errCh <- err
				}
			}
		}()
	}
send:
	for _, candidate := range candidates {
		select {
		case <-ctx.Done():
			break send
		case queue <- candidate:
		}
	}
	close(queue)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

func (p *RuntimeFinalizationProcessor) processOne(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate) error {
	claimed, err := p.queue.ClaimRuntimeFinalization(ctx, candidate.TaskID)
	if err != nil {
		return fmt.Errorf("runtime finalizer: claim task %s: %w", candidate.TaskID, err)
	}
	if !claimed {
		return nil
	}
	current, err := p.specs.GetTaskSpecPayload(ctx, candidate.TaskID)
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
	bindings, err := runtimeFinalizeBindings(req, current)
	if err != nil {
		return p.fail(candidate, fmt.Errorf("invalid runtime assets: %w", err))
	}
	if rawTasks, waiting := current[runtimePrepareTasksKey]; waiting {
		patch, err := p.completeWorkerPreparations(ctx, candidate, planJSON, req, current, bindings, rawTasks)
		if err != nil {
			return p.fail(candidate, err)
		}
		return p.commitFinalization(ctx, candidate, patch)
	}
	if planner, ok := p.assets.(RuntimeStockWindowTaskPlanner); ok && strings.TrimSpace(planJSON) != "" {
		preparations, planErr := planner.PlanDriveStockWindowTasks(planJSON, bindings)
		if planErr != nil {
			return p.fail(candidate, fmt.Errorf("plan worker stock-window preparation: %w", planErr))
		}
		if len(preparations) > 0 {
			reuse, misses, reuseErr := p.reusablePreparations(ctx, preparations)
			if reuseErr != nil {
				return p.fail(candidate, reuseErr)
			}
			if len(misses) == 0 {
				patch, err := p.rewritePreparedWindowPlan(ctx, candidate, planJSON, req, current, bindings, reuse)
				if err != nil {
					return p.fail(candidate, err)
				}
				return p.commitFinalization(ctx, candidate, patch)
			}
			if err := p.publishWorkerPreparations(ctx, candidate, bindings, misses, reuse); err != nil {
				return p.fail(candidate, err)
			}
			return nil
		}
	}
	// Once worker preparation is wired, the control plane must never
	// materialize Drive stock bytes as a fallback: an empty preparation plan
	// with remaining stock is a contract violation, not an invitation to
	// download on the Master.
	allowMasterStock := true
	if _, ok := p.assets.(RuntimeStockWindowTaskPlanner); ok {
		allowMasterStock = false
	}
	patch, err := buildRuntimeFinalizePatch(ctx, candidate.JobID, planJSON, req, current, p.assets, allowMasterStock)
	if err != nil {
		return p.fail(candidate, err)
	}
	return p.commitFinalization(ctx, candidate, patch)
}

func (p *RuntimeFinalizationProcessor) commitFinalization(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, patch map[string]interface{}) error {
	current, err := p.specs.GetTaskSpecPayload(ctx, candidate.TaskID)
	if err != nil {
		return fmt.Errorf("runtime finalizer: reread task %s before commit: %w", candidate.TaskID, err)
	}
	finalPayload := make(map[string]interface{}, len(current)+len(patch))
	for key, value := range current {
		finalPayload[key] = value
	}
	for key, value := range patch {
		if value == nil {
			delete(finalPayload, key)
		} else {
			finalPayload[key] = value
		}
	}
	executorID := contract.RequiredVideoAssemblyExecutor(finalPayload)
	if executorID == "" {
		return p.fail(candidate, errors.New("finalized render plan has no valid video assembly executor"))
	}
	if _, err := p.finalizer.FinalizeTaskSpec(ctx, candidate.TaskID, executorID, patch); err != nil {
		return fmt.Errorf("runtime finalizer: commit task %s: %w", candidate.TaskID, err)
	}
	p.notifier.NotifyTaskUpdated(ctx, candidate.JobID)
	log.Printf("[PIPELINE] runtime FINALIZE materialized job_id=%s task_id=%s", candidate.JobID, candidate.TaskID)
	return nil
}

func runtimeFinalizeBindings(req FinalizeJobRequest, current map[string]interface{}) ([]map[string]interface{}, error) {
	bindings := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	var err error
	bindings, err = appendRuntimeAssetBindings(bindings, req.RuntimePayload["runtime_assets"])
	if err == nil {
		bindings, err = appendRuntimeAssetBindings(bindings, current["runtime_assets"])
	}
	if storedPayload, ok := current["runtime_payload"].(map[string]interface{}); ok && err == nil {
		bindings, err = appendRuntimeAssetBindings(bindings, storedPayload["runtime_assets"])
	}
	return bindings, err
}

func (p *RuntimeFinalizationProcessor) reusablePreparations(ctx context.Context, preparations []contract.VideoPrepareWindowTaskV1) (map[string]voiceoverassets.PreparedWindowAssetBinding, []contract.VideoPrepareWindowTaskV1, error) {
	reuse := make(map[string]voiceoverassets.PreparedWindowAssetBinding, len(preparations))
	cache, ok := p.assets.(RuntimePreparedWindowCache)
	if !ok || isNilInterface(cache) {
		return reuse, preparations, nil
	}
	misses := make([]contract.VideoPrepareWindowTaskV1, 0, len(preparations))
	for _, preparation := range preparations {
		// A source whose SHA is not yet certified cannot be looked up by
		// content identity; it must go through worker identity discovery.
		if preparation.Request.SourceIdentityDiscovery {
			misses = append(misses, preparation)
			continue
		}
		binding, err := cache.LookupPreparedWindowAsset(ctx, preparation.Identity)
		if err != nil {
			return nil, nil, fmt.Errorf("runtime finalizer: lookup certified fragment %s: %w", preparation.Identity, err)
		}
		if binding == nil {
			misses = append(misses, preparation)
			continue
		}
		if strings.TrimSpace(binding.AssetID) == "" || !validPreparedWindowBinding(*binding) {
			return nil, nil, fmt.Errorf("runtime finalizer: cached fragment %s has an invalid binding", preparation.Identity)
		}
		reuse[preparation.Identity] = *binding
	}
	return reuse, misses, nil
}

func validPreparedWindowBinding(binding voiceoverassets.PreparedWindowAssetBinding) bool {
	if binding.SizeBytes <= 0 || len(binding.SHA256) != 64 || strings.ToLower(binding.SHA256) != binding.SHA256 {
		return false
	}
	_, err := hex.DecodeString(binding.SHA256)
	return err == nil
}

func (p *RuntimeFinalizationProcessor) publishWorkerPreparations(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, bindings []map[string]interface{}, preparations []contract.VideoPrepareWindowTaskV1, reuse map[string]voiceoverassets.PreparedWindowAssetBinding) error {
	if p.expander == nil {
		return errors.New("runtime finalizer: atomic task graph extension is unavailable")
	}
	additions := make([]taskgraph.ExpandedTask, 0, len(preparations))
	dependencies := make([]string, 0, len(preparations))
	identityToTask := make(map[string]string, len(preparations))
	for _, preparation := range preparations {
		if preparation.Identity == "" || len(preparation.Identity) < 24 {
			return errors.New("runtime finalizer: worker preparation identity is invalid")
		}
		taskID := candidate.TaskID + "-prep-" + preparation.Identity[:24]
		identityToTask[preparation.Identity] = taskID
		dependencies = append(dependencies, taskID)
		additions = append(additions, taskgraph.ExpandedTask{
			TaskID: taskID, ExecutorID: contract.VideoPrepareWindowExecutorID, ExecutorVer: contract.VideoPrepareWindowVersion,
			Payload: map[string]interface{}{
				contract.VideoPrepareWindowPayloadKey: preparation.Request,
				"runtime_assets":                      bindings,
				"runtime_prepare_identity":            preparation.Identity,
			},
		})
	}
	patch := map[string]interface{}{
		runtimeFinalizeStatusKey: runtimeFinalizeWaitingPreparation,
		runtimePrepareTaskIDsKey: dependencies,
		runtimePrepareTasksKey:   identityToTask,
		"runtime_assets":         bindings,
		"runtime_assets_pending": true,
	}
	if len(reuse) > 0 {
		patch[runtimePrepareWindowReuseKey] = reuse
	}
	if _, err := p.expander.Extend(ctx, candidate.TaskID, additions, dependencies, patch); err != nil {
		return fmt.Errorf("runtime finalizer: publish worker preparation DAG: %w", err)
	}
	p.notifier.NotifyTaskUpdated(ctx, candidate.JobID)
	log.Printf("[PIPELINE] runtime FINALIZE published preparation DAG job_id=%s task_id=%s tasks=%d", candidate.JobID, candidate.TaskID, len(additions))
	return nil
}

func (p *RuntimeFinalizationProcessor) completeWorkerPreparations(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, planJSON string, req FinalizeJobRequest, current map[string]interface{}, bindings []map[string]interface{}, rawTasks interface{}) (map[string]interface{}, error) {
	planner, planOK := p.assets.(RuntimeStockWindowTaskPlanner)
	_, rewriteOK := p.assets.(RuntimePreparedWindowPlanRewriter)
	if !planOK || !rewriteOK || isNilInterface(p.evidence) {
		return nil, errors.New("runtime finalizer: worker preparation result recovery is not configured")
	}
	encoded, err := json.Marshal(rawTasks)
	if err != nil {
		return nil, fmt.Errorf("decode worker preparation task map: %w", err)
	}
	var identityToTask map[string]string
	if err := json.Unmarshal(encoded, &identityToTask); err != nil || len(identityToTask) == 0 {
		return nil, errors.New("runtime finalizer: worker preparation task map is invalid")
	}
	preparations, err := planner.PlanDriveStockWindowTasks(planJSON, bindings)
	if err != nil {
		return nil, fmt.Errorf("rebuild worker preparation plan: %w", err)
	}
	byIdentity := make(map[string]contract.VideoPrepareWindowTaskV1, len(preparations))
	taskIDs := make([]string, 0, len(identityToTask))
	taskToIdentity := make(map[string]string, len(identityToTask))
	for _, preparation := range preparations {
		byIdentity[preparation.Identity] = preparation
	}
	for identity, taskID := range identityToTask {
		if _, ok := byIdentity[identity]; !ok || taskID == "" {
			return nil, fmt.Errorf("runtime finalizer: preparation %q no longer matches immutable plan", identity)
		}
		taskIDs = append(taskIDs, taskID)
		taskToIdentity[taskID] = identity
	}
	segmentEvidence, err := p.evidence.ListSuccessfulSegmentTimings(ctx, taskIDs)
	if err != nil {
		return nil, fmt.Errorf("runtime finalizer: read preparation evidence: %w", err)
	}
	results := make(map[string]voiceoverassets.PreparedWindowAssetBinding, len(identityToTask))
	for _, segment := range segmentEvidence {
		identity := taskToIdentity[segment.TaskID]
		preparation, known := byIdentity[identity]
		if !known || segment.SourceType != contract.VideoPrepareWindowExecutorID || segment.Status != "succeeded" {
			continue
		}
		var response struct {
			contract.VideoPrepareWindowCertificateV1
			RegisteredAssetID string `json:"registered_asset_id"`
		}
		if err := json.Unmarshal([]byte(segment.MetadataJSON), &response); err != nil {
			return nil, fmt.Errorf("runtime finalizer: decode certificate for %s: %w", segment.TaskID, err)
		}
		certificate := response.VideoPrepareWindowCertificateV1
		if err := certificate.ValidateFor(preparation.Request); err != nil {
			return nil, fmt.Errorf("runtime finalizer: invalid certificate for %s: %w", segment.TaskID, err)
		}
		if response.RegisteredAssetID == "" {
			return nil, fmt.Errorf("runtime finalizer: prepared window %s was not registered on Master", identity)
		}
		results[identity] = voiceoverassets.PreparedWindowAssetBinding{
			AssetID: response.RegisteredAssetID, SHA256: certificate.OutputSHA256, SizeBytes: certificate.OutputSizeBytes,
		}
	}
	reuse, err := decodePreparedWindowReuse(current[runtimePrepareWindowReuseKey])
	if err != nil {
		return nil, err
	}
	for identity, binding := range reuse {
		if _, planned := byIdentity[identity]; !planned {
			return nil, fmt.Errorf("runtime finalizer: reused preparation %q is no longer part of the immutable plan", identity)
		}
		if strings.TrimSpace(binding.AssetID) == "" || !validPreparedWindowBinding(binding) {
			return nil, fmt.Errorf("runtime finalizer: reused preparation %q has an invalid binding", identity)
		}
		results[identity] = binding
	}
	if len(results) != len(byIdentity) {
		return nil, fmt.Errorf("runtime finalizer: recovered %d of %d prepared windows", len(results), len(byIdentity))
	}
	return p.rewritePreparedWindowPlan(ctx, candidate, planJSON, req, current, bindings, results)
}

func decodePreparedWindowReuse(raw interface{}) (map[string]voiceoverassets.PreparedWindowAssetBinding, error) {
	out := make(map[string]voiceoverassets.PreparedWindowAssetBinding)
	if raw == nil {
		return out, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("runtime finalizer: encode reused preparation bindings: %w", err)
	}
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, fmt.Errorf("runtime finalizer: decode reused preparation bindings: %w", err)
	}
	return out, nil
}

func (p *RuntimeFinalizationProcessor) rewritePreparedWindowPlan(
	ctx context.Context,
	candidate taskgraph.RuntimeFinalizationCandidate,
	planJSON string,
	req FinalizeJobRequest,
	current map[string]interface{},
	bindings []map[string]interface{},
	results map[string]voiceoverassets.PreparedWindowAssetBinding,
) (map[string]interface{}, error) {
	rewriter, ok := p.assets.(RuntimePreparedWindowPlanRewriter)
	if !ok || isNilInterface(rewriter) {
		return nil, errors.New("runtime finalizer: prepared-window plan rewriter is not configured")
	}
	plan, planHash, retainedBindings, err := rewriter.RewriteDriveStockPlanFromWorkerResults(planJSON, bindings, results)
	if err != nil {
		return nil, fmt.Errorf("runtime finalizer: bind prepared windows into assembly plan: %w", err)
	}
	if req.RuntimePayload == nil {
		req.RuntimePayload = make(map[string]interface{})
	}
	req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON] = plan
	req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
	patch, err := buildRuntimeFinalizePatch(ctx, candidate.JobID, plan, req, current, p.assets, false)
	if err != nil {
		return nil, err
	}
	patch[contract.PayloadKeyCompiledRenderPlanJSON] = plan
	patch[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
	patch["runtime_assets"] = retainedBindings
	patch[runtimePrepareTaskIDsKey] = nil
	patch[runtimePrepareTasksKey] = nil
	patch[runtimePrepareWindowReuseKey] = nil
	return patch, nil
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
	jobID string,
	planJSON string,
	req FinalizeJobRequest,
	current map[string]interface{},
	assets RuntimeStockWindowPreparer,
	allowStockPreparation bool,
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
	var compiledPlan contract.CompiledRenderPlanV2
	if strings.TrimSpace(planJSON) != "" {
		if err := json.Unmarshal([]byte(planJSON), &compiledPlan); err != nil {
			return nil, fmt.Errorf("decode compiled render plan: %w", err)
		}
	}
	preparedStockPlan := compiledPlanHasDriveStock(planJSON, bindings)
	finalRuntimeAssets := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	var stockPoolSnapshot map[string]interface{}
	if compiledPlan.StockPool != nil {
		finalRuntimeAssets, stockPoolSnapshot, err = snapshotRuntimeStockPool(&compiledPlan, jobID, bindings)
		if err != nil {
			return nil, fmt.Errorf("persist stock pool selection: %w", err)
		}
		if req.RuntimePayload == nil {
			req.RuntimePayload = make(map[string]interface{})
		}
		req.RuntimePayload["runtime_assets"] = finalRuntimeAssets
		req.RuntimePayload["runtime_stock_pool_snapshot"] = stockPoolSnapshot
	}
	if preparedStockPlan {
		if !allowStockPreparation {
			return nil, errors.New("master stock-window preparation is disabled; the worker preparation plan produced no tasks")
		}
		if isNilInterface(assets) {
			return nil, errors.New("stock window preparation capability is unavailable")
		}
		preparedPlan, preparedHash, prepareErr := assets.PrepareDriveStockWindows(ctx, planJSON, bindings)
		if prepareErr != nil {
			return nil, fmt.Errorf("stock window materialization: %w", prepareErr)
		}
		finalRuntimeAssets, err = retainPlanSourceBindings(preparedPlan, bindings)
		if err != nil {
			return nil, fmt.Errorf("prune materialized video sources: %w", err)
		}
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
	} else if compiledPlan.StockPool != nil {
		patch["runtime_assets"] = finalRuntimeAssets
		patch["runtime_stock_pool_snapshot"] = stockPoolSnapshot
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
