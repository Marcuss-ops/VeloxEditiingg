package pipeline

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	"strconv"
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
	runtimeSourceCertTaskIDsKey       = "runtime_source_certification_task_ids"
	runtimeSourceCertTasksKey         = "runtime_source_certification_tasks"
	runtimeFinalizeQueued             = "QUEUED"
	runtimeFinalizeRunning            = "RUNNING"
	runtimeFinalizeSucceeded          = "SUCCEEDED"
	runtimeFinalizeFailed             = "FAILED"
	runtimeFinalizeRetryWait          = "RETRY_WAIT"
	runtimeFinalizeWaitingPreparation = "WAITING_PREPARATION"
	runtimeFinalizePoll               = 100 * time.Millisecond
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

type RuntimeStockWindowTaskPlanner interface {
	PlanDriveStockWindowTasks(string, []map[string]interface{}) ([]contract.VideoPrepareWindowTaskV1, error)
}

type RuntimeSourceCertificationTaskPlanner interface {
	PlanVideoSourceCertificationTasks(string, []map[string]interface{}) ([]contract.VideoSourceCertificationTaskV1, error)
}

// RuntimePreparedWindowCache exposes fragments already registered on this
// Master. FINALIZE consults it before publishing new preparation tasks so a
// window prepared for another job or another worker is reused instead of
// re-encoded.
type RuntimePreparedWindowCache interface {
	LookupPreparedWindowAsset(context.Context, string) (*voiceoverassets.PreparedWindowAssetBinding, error)
}

type RuntimePreparedWindowBatchCache interface {
	LookupPreparedWindowAssets(context.Context, []string) (map[string]voiceoverassets.PreparedWindowAssetBinding, error)
}

type RuntimePreparedWindowPlanRewriter interface {
	RewriteDriveStockPlanFromWorkerResults(string, []map[string]interface{}, map[string]voiceoverassets.PreparedWindowAssetBinding) (string, string, []map[string]interface{}, error)
}

type RuntimeSourceCertificateEnricher interface {
	EnrichPlanWithSourceCertificates(context.Context, string, []map[string]interface{}) (string, error)
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
	failures  taskgraph.RuntimeFinalizationFailures
	expander  RuntimeTaskGraphExpander
	evidence  taskattempts.SuccessfulSegmentTimingReader
	assets    interface{}
	notifier  RuntimeTaskUpdateNotifier
}

// NewRuntimeFinalizationProcessor fails bootstrap when the persistent queue,
// worker stock-window planner or post-commit hook is missing. A production
// FINALIZE must never be accepted into an in-memory-only/noop path.
func NewRuntimeFinalizationProcessor(
	repository taskgraph.Repository,
	assets RuntimeStockWindowTaskPlanner,
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
	failures, ok := repository.(taskgraph.RuntimeFinalizationFailures)
	if !ok || isNilInterface(failures) {
		return nil, errors.New("runtime finalizer: atomic failure reconciliation is not configured")
	}
	if isNilInterface(assets) {
		return nil, errors.New("runtime finalizer: worker stock-window planner is not configured")
	}
	if planner, ok := interface{}(assets).(RuntimeSourceCertificationTaskPlanner); !ok || isNilInterface(planner) {
		return nil, errors.New("runtime finalizer: immutable source certification planner is not configured")
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
	return &RuntimeFinalizationProcessor{queue: queue, specs: specs, finalizer: finalizer, failures: failures, expander: expander, evidence: evidence, assets: assets, notifier: notifier}, nil
}

// Run replays queued and interrupted FINALIZE work. Queue state and request
// bytes live in task_specs, so restarting the Master does not lose accepted
// work. Per-job materialization failures are persisted for GET polling and do
// not stop unrelated finalizations.
func (p *RuntimeFinalizationProcessor) Run(ctx context.Context) error {
	if p == nil || p.queue == nil || p.specs == nil || p.finalizer == nil || p.failures == nil || p.assets == nil || p.notifier == nil {
		return errors.New("runtime finalizer: processor is not configured")
	}
	if err := p.queue.RecoverInterruptedRuntimeFinalizations(ctx); err != nil {
		return fmt.Errorf("runtime finalizer: recover interrupted work: %w", err)
	}
	ticker := time.NewTicker(runtimeFinalizePoll)
	defer ticker.Stop()
	for {
		if _, err := p.failures.ReconcileFailedRuntimePreparations(ctx, runtimeFinalizeBatch); err != nil {
			return fmt.Errorf("runtime finalizer: reconcile failed preparations: %w", err)
		}
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
		return p.fail(ctx, candidate, errors.New("durable finalize request is missing"))
	}
	encoded, err := json.Marshal(requestValue)
	if err != nil {
		return p.fail(ctx, candidate, fmt.Errorf("encode durable finalize request: %w", err))
	}
	var req FinalizeJobRequest
	if err := json.Unmarshal(encoded, &req); err != nil {
		return p.fail(ctx, candidate, fmt.Errorf("decode durable finalize request: %w", err))
	}
	planJSON, _ := current[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	bindings, err := runtimeFinalizeBindings(req, current)
	if err != nil {
		return p.fail(ctx, candidate, fmt.Errorf("invalid runtime assets: %w", err))
	}
	if rawTasks, waiting := current[runtimePrepareTasksKey]; waiting {
		patch, err := p.completeWorkerPreparations(ctx, candidate, planJSON, req, current, bindings, rawTasks)
		if err != nil {
			return p.fail(ctx, candidate, err)
		}
		return p.commitFinalization(ctx, candidate, patch)
	}
	if rawTasks, waiting := current[runtimeSourceCertTasksKey]; waiting {
		patch, err := p.completeWorkerSourceCertifications(ctx, candidate, planJSON, req, current, bindings, rawTasks)
		if err != nil {
			return p.fail(ctx, candidate, err)
		}
		return p.commitFinalization(ctx, candidate, patch)
	}
	if enricher, ok := p.assets.(interface {
		EnrichPlanWithSourceCertificates(context.Context, string, []map[string]interface{}) (string, error)
	}); ok && strings.TrimSpace(planJSON) != "" {
		enriched, enrichErr := enricher.EnrichPlanWithSourceCertificates(ctx, planJSON, bindings)
		if enrichErr != nil {
			return p.fail(ctx, candidate, fmt.Errorf("resolve source certificates: %w", enrichErr))
		}
		if enriched != planJSON {
			planJSON = enriched
			if req.RuntimePayload == nil {
				req.RuntimePayload = make(map[string]interface{})
			}
			req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
			req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA] = contract.HashCompiledPlanV2([]byte(planJSON))
		}
	}
	var stockPoolSnapshot interface{}
	if compiledPlanHasStockPool(planJSON) {
		expandedPlan, selectedBindings, snapshot, expandErr := expandRuntimeStockPoolPlan(planJSON, candidate.JobID, bindings)
		if expandErr != nil {
			return p.fail(ctx, candidate, fmt.Errorf("resolve stock pool plan: %w", expandErr))
		}
		planJSON, bindings = expandedPlan, selectedBindings
		stockPoolSnapshot = snapshot
		if req.RuntimePayload == nil {
			req.RuntimePayload = make(map[string]interface{})
		}
		planHash := contract.HashCompiledPlanV2([]byte(planJSON))
		req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
		req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
		req.RuntimePayload["runtime_assets"] = bindings
		req.RuntimePayload["runtime_stock_pool_snapshot"] = snapshot
	}
	if planner, ok := p.assets.(RuntimeSourceCertificationTaskPlanner); ok && strings.TrimSpace(planJSON) != "" {
		certifications, planErr := planner.PlanVideoSourceCertificationTasks(planJSON, bindings)
		if planErr != nil {
			return p.fail(ctx, candidate, fmt.Errorf("plan immutable video source certification: %w", planErr))
		}
		if len(certifications) > 0 {
			planHash := contract.HashCompiledPlanV2([]byte(planJSON))
			if err := p.publishSourceCertifications(ctx, candidate, bindings, certifications, planJSON, planHash, stockPoolSnapshot); err != nil {
				return p.fail(ctx, candidate, err)
			}
			return nil
		}
	}
	if planner, ok := p.assets.(RuntimeStockWindowTaskPlanner); ok && strings.TrimSpace(planJSON) != "" {
		preparations, planErr := planner.PlanDriveStockWindowTasks(planJSON, bindings)
		if planErr != nil {
			return p.fail(ctx, candidate, fmt.Errorf("plan worker stock-window preparation: %w", planErr))
		}
		if len(preparations) > 0 {
			reuse, misses, reuseErr := p.reusablePreparations(ctx, preparations)
			if reuseErr != nil {
				return p.fail(ctx, candidate, reuseErr)
			}
			if len(misses) == 0 {
				patch, err := p.rewritePreparedWindowPlan(ctx, candidate, planJSON, req, current, bindings, reuse)
				if err != nil {
					return p.fail(ctx, candidate, err)
				}
				return p.commitFinalization(ctx, candidate, patch)
			}
			planHash := contract.HashCompiledPlanV2([]byte(planJSON))
			if err := p.publishWorkerPreparations(ctx, candidate, bindings, misses, reuse, planJSON, planHash, stockPoolSnapshot); err != nil {
				return p.fail(ctx, candidate, err)
			}
			return nil
		}
	}
	// Drive source bytes are worker-owned. FINALIZE may publish only plans
	// whose stock windows have already been certified by worker preparation.
	patch, err := buildRuntimeFinalizePatch(ctx, candidate.JobID, planJSON, req, current, p.assets)
	if err != nil {
		return p.fail(ctx, candidate, err)
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
		return p.fail(ctx, candidate, errors.New("finalized render plan has no valid video assembly executor"))
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
	if cache, ok := p.assets.(RuntimePreparedWindowBatchCache); ok && !isNilInterface(cache) {
		identities := make([]string, 0, len(preparations))
		for _, preparation := range preparations {
			if !preparation.Request.SourceIdentityDiscovery {
				identities = append(identities, preparation.Identity)
			}
		}
		cached, err := cache.LookupPreparedWindowAssets(ctx, identities)
		if err != nil {
			return nil, nil, fmt.Errorf("runtime finalizer: batch lookup certified fragments: %w", err)
		}
		misses := make([]contract.VideoPrepareWindowTaskV1, 0, len(preparations))
		for _, preparation := range preparations {
			if preparation.Request.SourceIdentityDiscovery {
				misses = append(misses, preparation)
				continue
			}
			binding, ok := cached[preparation.Identity]
			if !ok {
				misses = append(misses, preparation)
				continue
			}
			if strings.TrimSpace(binding.AssetID) == "" || !validPreparedWindowBinding(binding) {
				return nil, nil, fmt.Errorf("runtime finalizer: cached fragment %s has an invalid binding", preparation.Identity)
			}
			reuse[preparation.Identity] = binding
		}
		return reuse, misses, nil
	}
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

func (p *RuntimeFinalizationProcessor) publishSourceCertifications(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, bindings []map[string]interface{}, tasks []contract.VideoSourceCertificationTaskV1, planJSON, planHash string, stockPoolSnapshot interface{}) error {
	if p.expander == nil {
		return errors.New("runtime finalizer: atomic task graph extension is unavailable")
	}
	additions := make([]taskgraph.ExpandedTask, 0, len(tasks))
	dependencies := make([]string, 0, len(tasks))
	identityToTask := make(map[string]string, len(tasks))
	for _, task := range tasks {
		if task.Identity == "" || len(task.Identity) < 24 || len(task.AssetIDs) == 0 {
			return errors.New("runtime finalizer: source certification identity is invalid")
		}
		if err := task.Request.Validate(); err != nil {
			return fmt.Errorf("runtime finalizer: invalid source certification request: %w", err)
		}
		canonicalIdentity, err := contract.VideoSourceCertificationIdentity(task.Request)
		if err != nil || canonicalIdentity != task.Identity {
			return errors.New("runtime finalizer: source certification identity does not match request")
		}
		taskID := candidate.TaskID + "-source-" + strings.TrimPrefix(task.Identity, "source:")[:24]
		if strings.HasPrefix(task.Identity, "discovery:") {
			taskID = candidate.TaskID + "-source-discovery-" + strings.TrimPrefix(task.Identity, "discovery:")[:24]
		}
		identityToTask[task.Identity] = taskID
		dependencies = append(dependencies, taskID)
		additions = append(additions, taskgraph.ExpandedTask{
			TaskID: taskID, ExecutorID: contract.VideoSourceCertificationExecutorID, ExecutorVer: contract.VideoSourceCertificationVersion,
			Payload: map[string]interface{}{
				contract.VideoSourceCertificationPayloadKey: task.Request,
				"runtime_assets":                        runtimeBindingsForSource(bindings, task.Request.Source),
				"runtime_source_certification_identity": task.Identity,
			},
		})
	}
	patch := map[string]interface{}{
		runtimeFinalizeStatusKey:    runtimeFinalizeWaitingPreparation,
		runtimePrepareTaskIDsKey:    dependencies,
		runtimeSourceCertTaskIDsKey: dependencies,
		runtimeSourceCertTasksKey:   identityToTask,
		"runtime_assets":            bindings,
		"runtime_assets_pending":    true,
	}
	if planJSON != "" {
		patch[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
	}
	if planHash != "" {
		patch[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
	}
	if stockPoolSnapshot != nil {
		patch["runtime_stock_pool_snapshot"] = stockPoolSnapshot
	}
	if _, err := p.expander.Extend(ctx, candidate.TaskID, additions, dependencies, patch); err != nil {
		return fmt.Errorf("runtime finalizer: publish source certification DAG: %w", err)
	}
	p.notifier.NotifyTaskUpdated(ctx, candidate.JobID)
	log.Printf("[PIPELINE] runtime FINALIZE published source certification DAG job_id=%s task_id=%s sources=%d", candidate.JobID, candidate.TaskID, len(additions))
	return nil
}

func (p *RuntimeFinalizationProcessor) completeWorkerSourceCertifications(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, planJSON string, req FinalizeJobRequest, current map[string]interface{}, bindings []map[string]interface{}, rawTasks interface{}) (map[string]interface{}, error) {
	planner, ok := p.assets.(RuntimeSourceCertificationTaskPlanner)
	if !ok || isNilInterface(planner) || isNilInterface(p.evidence) {
		return nil, errors.New("runtime finalizer: source certification recovery is not configured")
	}
	encoded, err := json.Marshal(rawTasks)
	if err != nil {
		return nil, fmt.Errorf("decode source certification task map: %w", err)
	}
	var identityToTask map[string]string
	if err := json.Unmarshal(encoded, &identityToTask); err != nil || len(identityToTask) == 0 {
		return nil, errors.New("runtime finalizer: source certification task map is invalid")
	}
	tasks, err := planner.PlanVideoSourceCertificationTasks(planJSON, bindings)
	if err != nil {
		return nil, fmt.Errorf("rebuild source certification plan: %w", err)
	}
	byIdentity := make(map[string]contract.VideoSourceCertificationTaskV1, len(tasks))
	taskIDs := make([]string, 0, len(identityToTask))
	taskToIdentity := make(map[string]string, len(identityToTask))
	for _, task := range tasks {
		byIdentity[task.Identity] = task
	}
	for identity, taskID := range identityToTask {
		if _, exists := byIdentity[identity]; !exists || taskID == "" {
			return nil, fmt.Errorf("runtime finalizer: source certification %q no longer matches immutable plan", identity)
		}
		taskIDs = append(taskIDs, taskID)
		taskToIdentity[taskID] = identity
	}
	segmentEvidence, err := p.evidence.ListSuccessfulSegmentTimings(ctx, taskIDs)
	if err != nil {
		return nil, fmt.Errorf("runtime finalizer: read source certification evidence: %w", err)
	}
	certificates := make(map[string]contract.VideoSourceCertificateV1, len(identityToTask))
	for _, segment := range segmentEvidence {
		identity := taskToIdentity[segment.TaskID]
		task, exists := byIdentity[identity]
		if !exists || segment.SourceType != contract.VideoSourceCertificationExecutorID || segment.Status != "succeeded" || segment.MetadataJSON == "" {
			continue
		}
		var certificate contract.VideoSourceCertificateV1
		if err := json.Unmarshal([]byte(segment.MetadataJSON), &certificate); err != nil {
			return nil, fmt.Errorf("runtime finalizer: decode source certificate for %s: %w", segment.TaskID, err)
		}
		if err := certificate.Validate(); err != nil || certificate.SHA256 == "" || certificate.SizeBytes != task.Request.Source.SizeBytes || certificate.ProfileID != task.Request.OutputProfileID || certificate.PreparationRevision != task.Request.OutputProfileRevision || (task.Request.Source.SHA256 != "" && certificate.SHA256 != task.Request.Source.SHA256) {
			return nil, fmt.Errorf("runtime finalizer: invalid full-source certificate for %s", segment.TaskID)
		}
		if certificate.Chunks != nil {
			if err := certificate.Chunks.Validate(certificate.SizeBytes); err != nil {
				return nil, fmt.Errorf("runtime finalizer: invalid authenticated source blocks for %s: %w", segment.TaskID, err)
			}
		}
		certificates[identity] = certificate
	}
	if len(certificates) != len(byIdentity) {
		return nil, fmt.Errorf("runtime finalizer: recovered %d of %d source certificates", len(certificates), len(byIdentity))
	}
	certifiedBindings, err := bindSourceCertificateIdentities(bindings, byIdentity, certificates)
	if err != nil {
		return nil, err
	}
	enricher, ok := p.assets.(RuntimeSourceCertificateEnricher)
	if !ok || isNilInterface(enricher) {
		return nil, errors.New("runtime finalizer: source certificate plan enricher is not configured")
	}
	enriched, err := enricher.EnrichPlanWithSourceCertificates(ctx, planJSON, certifiedBindings)
	if err != nil {
		return nil, fmt.Errorf("runtime finalizer: project certified sources into plan: %w", err)
	}
	remaining, err := planner.PlanVideoSourceCertificationTasks(enriched, certifiedBindings)
	if err != nil || len(remaining) != 0 {
		if err != nil {
			return nil, fmt.Errorf("runtime finalizer: validate certified plan: %w", err)
		}
		return nil, errors.New("runtime finalizer: source certificates did not resolve all required sources")
	}
	if req.RuntimePayload == nil {
		req.RuntimePayload = make(map[string]interface{})
	}
	planHash := contract.HashCompiledPlanV2([]byte(enriched))
	req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON] = enriched
	req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
	req.RuntimePayload["runtime_assets"] = certifiedBindings
	req.RuntimeAssets = certifiedBindings
	patch, err := buildRuntimeFinalizePatch(ctx, candidate.JobID, enriched, req, current, p.assets)
	if err != nil {
		return nil, err
	}
	patch[contract.PayloadKeyCompiledRenderPlanJSON] = enriched
	patch[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
	patch["runtime_assets"] = certifiedBindings
	patch[runtimePrepareTaskIDsKey] = nil
	patch[runtimePrepareTasksKey] = nil
	patch[runtimeSourceCertTaskIDsKey] = nil
	patch[runtimeSourceCertTasksKey] = nil
	return patch, nil
}

func bindSourceCertificateIdentities(bindings []map[string]interface{}, tasks map[string]contract.VideoSourceCertificationTaskV1, certificates map[string]contract.VideoSourceCertificateV1) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, len(bindings))
	for i, binding := range bindings {
		copyBinding := make(map[string]interface{}, len(binding)+2)
		for key, value := range binding {
			copyBinding[key] = value
		}
		for identity, task := range tasks {
			certificate, exists := certificates[identity]
			if !exists {
				continue
			}
			assetID := runtimeBindingString(copyBinding, "asset_id", "id")
			driveID := runtimeBindingString(copyBinding, "drive_file_id")
			matches := false
			for _, candidate := range task.AssetIDs {
				matches = matches || candidate == assetID
			}
			matches = matches || (task.Request.Source.DriveFileID != "" && task.Request.Source.DriveFileID == driveID)
			if !matches {
				continue
			}
			declaredSHA := runtimeBindingString(copyBinding, "source_sha256", "sha256", "asset_sha256")
			declaredSize := runtimeBindingInt64(copyBinding, "size_bytes")
			if (declaredSHA != "" && !strings.EqualFold(declaredSHA, certificate.SHA256)) || (declaredSize > 0 && declaredSize != certificate.SizeBytes) {
				return nil, fmt.Errorf("runtime source binding conflicts with certificate for %s", assetID)
			}
			copyBinding["source_sha256"] = certificate.SHA256
			copyBinding["size_bytes"] = certificate.SizeBytes
		}
		out[i] = copyBinding
	}
	return out, nil
}

func validPreparedWindowBinding(binding voiceoverassets.PreparedWindowAssetBinding) bool {
	if binding.SizeBytes <= 0 || len(binding.SHA256) != 64 || strings.ToLower(binding.SHA256) != binding.SHA256 || len(binding.ExtradataSHA256) != 64 || strings.ToLower(binding.ExtradataSHA256) != binding.ExtradataSHA256 {
		return false
	}
	_, err := hex.DecodeString(binding.SHA256)
	if err != nil {
		return false
	}
	_, err = hex.DecodeString(binding.ExtradataSHA256)
	return err == nil
}

func (p *RuntimeFinalizationProcessor) publishWorkerPreparations(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, bindings []map[string]interface{}, preparations []contract.VideoPrepareWindowTaskV1, reuse map[string]voiceoverassets.PreparedWindowAssetBinding, planJSON, planHash string, stockPoolSnapshot interface{}) error {
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
				"runtime_assets":                      runtimeBindingsForSource(bindings, preparation.Request.Source),
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
	if planJSON != "" {
		patch[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
	}
	if planHash != "" {
		patch[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
	}
	if stockPoolSnapshot != nil {
		patch["runtime_stock_pool_snapshot"] = stockPoolSnapshot
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

// runtimeBindingsForSource keeps preparation tasks self-contained and avoids
// making each window task prefetch every asset in the final assembly plan.
func runtimeBindingsForSource(bindings []map[string]interface{}, source contract.AssetRefV2) []map[string]interface{} {
	for _, binding := range bindings {
		id, _ := binding["asset_id"].(string)
		if id == "" {
			id, _ = binding["drive_file_id"].(string)
		}
		if id == source.AssetID || (source.DriveFileID != "" && id == source.DriveFileID) {
			return []map[string]interface{}{binding}
		}
	}
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
	sourceCertificates := make(map[string]contract.VideoSourceCertificateV1, len(identityToTask))
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
		if certificate.SourceCertificate != nil {
			sourceCertificate := *certificate.SourceCertificate
			if err := sourceCertificate.Validate(); err != nil || sourceCertificate.SHA256 != certificate.SourceSHA256 || sourceCertificate.SizeBytes != certificate.SourceSizeBytes {
				return nil, fmt.Errorf("runtime finalizer: invalid source certificate for %s", segment.TaskID)
			}
			sourceCertificates[identity] = sourceCertificate
		}
		results[identity] = voiceoverassets.PreparedWindowAssetBinding{
			AssetID: response.RegisteredAssetID, SHA256: certificate.OutputSHA256, SizeBytes: certificate.OutputSizeBytes, ExtradataSHA256: certificate.OutputEvidence.ExtradataSHA256,
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
	// A preparation result may also carry a complete source certificate. Bind
	// the discovered immutable identity into this FINALIZE request and prefer
	// direct source assembly when every source window is now certified. The
	// legacy fragment binding remains the recovery path for non-certifiable input.
	if len(sourceCertificates) > 0 {
		if enricher, ok := p.assets.(RuntimeSourceCertificateEnricher); ok && !isNilInterface(enricher) {
			certifiedBindings := bindPreparedSourceIdentities(bindings, byIdentity, sourceCertificates)
			enriched, enrichErr := enricher.EnrichPlanWithSourceCertificates(ctx, planJSON, certifiedBindings)
			if enrichErr != nil {
				return nil, fmt.Errorf("runtime finalizer: reuse newly certified source: %w", enrichErr)
			}
			pending, planErr := planner.PlanDriveStockWindowTasks(enriched, certifiedBindings)
			if planErr != nil {
				return nil, fmt.Errorf("runtime finalizer: validate direct certified plan: %w", planErr)
			}
			if len(pending) == 0 {
				if req.RuntimePayload == nil {
					req.RuntimePayload = make(map[string]interface{})
				}
				planHash := contract.HashCompiledPlanV2([]byte(enriched))
				req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanJSON] = enriched
				req.RuntimePayload[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
				req.RuntimePayload["runtime_assets"] = certifiedBindings
				req.RuntimeAssets = certifiedBindings
				patch, patchErr := buildRuntimeFinalizePatch(ctx, candidate.JobID, enriched, req, current, p.assets)
				if patchErr != nil {
					return nil, patchErr
				}
				patch[contract.PayloadKeyCompiledRenderPlanJSON] = enriched
				patch[contract.PayloadKeyCompiledRenderPlanSHA] = planHash
				patch["runtime_assets"] = certifiedBindings
				patch[runtimePrepareTaskIDsKey] = nil
				patch[runtimePrepareTasksKey] = nil
				patch[runtimePrepareWindowReuseKey] = nil
				return patch, nil
			}
		}
	}
	return p.rewritePreparedWindowPlan(ctx, candidate, planJSON, req, current, bindings, results)
}

func bindPreparedSourceIdentities(
	bindings []map[string]interface{},
	preparations map[string]contract.VideoPrepareWindowTaskV1,
	certificates map[string]contract.VideoSourceCertificateV1,
) []map[string]interface{} {
	out := make([]map[string]interface{}, len(bindings))
	for i, binding := range bindings {
		copyBinding := make(map[string]interface{}, len(binding)+2)
		for key, value := range binding {
			copyBinding[key] = value
		}
		for identity, sourceCertificate := range certificates {
			preparation, ok := preparations[identity]
			if !ok {
				continue
			}
			source := preparation.Request.Source
			assetID := runtimeBindingString(copyBinding, "asset_id", "id")
			driveID := runtimeBindingString(copyBinding, "drive_file_id")
			if assetID != source.AssetID && (source.DriveFileID == "" || driveID != source.DriveFileID) {
				continue
			}
			declaredSHA := runtimeBindingString(copyBinding, "source_sha256", "sha256", "asset_sha256")
			declaredSize := runtimeBindingInt64(copyBinding, "size_bytes")
			if declaredSHA != "" && !strings.EqualFold(declaredSHA, sourceCertificate.SHA256) {
				continue
			}
			if declaredSize > 0 && declaredSize != sourceCertificate.SizeBytes {
				continue
			}
			copyBinding["source_sha256"] = sourceCertificate.SHA256
			copyBinding["size_bytes"] = sourceCertificate.SizeBytes
		}
		out[i] = copyBinding
	}
	return out
}

func runtimeBindingString(binding map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := binding[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func runtimeBindingInt64(binding map[string]interface{}, key string) int64 {
	switch value := binding[key].(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case float64:
		if value == float64(int64(value)) {
			return int64(value)
		}
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed
	}
	return 0
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
	patch, err := buildRuntimeFinalizePatch(ctx, candidate.JobID, plan, req, current, p.assets)
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

func (p *RuntimeFinalizationProcessor) fail(ctx context.Context, candidate taskgraph.RuntimeFinalizationCandidate, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 2048 {
		message = message[:2048]
	}
	if p.failures == nil {
		return fmt.Errorf("runtime finalizer: atomic failure writer unavailable for task %s: %w", candidate.TaskID, err)
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) {
		scheduled, retryErr := p.failures.ScheduleRuntimeFinalizationRetry(ctx, candidate.TaskID, message)
		if retryErr != nil {
			return fmt.Errorf("runtime finalizer: schedule retry for task %s: %w", candidate.TaskID, retryErr)
		}
		if scheduled {
			log.Printf("[PIPELINE] runtime FINALIZE retry scheduled job_id=%s task_id=%s", candidate.JobID, candidate.TaskID)
			return nil
		}
	}
	if failErr := p.failures.FailRuntimeFinalization(ctx, candidate.TaskID, "RUNTIME_FINALIZE_FAILED", message); failErr != nil {
		return fmt.Errorf("runtime finalizer: persist failure for task %s: %w", candidate.TaskID, errors.Join(err, failErr))
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
	assets interface{},
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
	finalRuntimeAssets := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	if compiledPlan.StockPool != nil {
		return nil, errors.New("runtime finalizer: unresolved stock_pool reached dispatch; worker stock preparation was not planned")
	}
	preparedStockPlan := compiledPlanHasDriveStock(planJSON, finalRuntimeAssets)
	if preparedStockPlan {
		planner, ok := assets.(RuntimeStockWindowTaskPlanner)
		if !ok || isNilInterface(planner) {
			return nil, errors.New("runtime finalizer: worker stock-window planner is unavailable; refusing Master-side source download")
		}
		pending, planErr := planner.PlanDriveStockWindowTasks(planJSON, bindings)
		if planErr != nil {
			return nil, fmt.Errorf("runtime finalizer: check worker stock-window plan: %w", planErr)
		}
		if len(pending) > 0 {
			return nil, errors.New("runtime finalizer: unresolved stock windows reached dispatch; refusing Master-side source download")
		}
	}
	patch := make(map[string]interface{}, len(req.RuntimePayload)+8)
	for key, value := range req.RuntimePayload {
		if key == "runtime_assets_pending" || key == runtimeFinalizeStatusKey || key == runtimeFinalizeRequestKey || key == runtimeFinalizeErrorKey || key == "runtime_finalize_error_code" || key == "runtime_finalize_retry_at" || key == "runtime_finalize_retry_count" {
			continue
		}
		patch[key] = value
	}
	if len(req.RuntimeAssets) > 0 {
		patch["runtime_assets"] = req.RuntimeAssets
	}
	patch["runtime_assets_pending"] = false
	patch["runtime_finalize_idempotency_key"] = req.IdempotencyKey
	patch[runtimeFinalizeStatusKey] = runtimeFinalizeSucceeded
	patch[runtimeFinalizeRequestKey] = nil
	patch[runtimeFinalizeErrorKey] = nil
	patch["runtime_finalize_error_code"] = nil
	patch["runtime_finalize_retry_at"] = nil
	patch["runtime_finalize_retry_count"] = nil
	return patch, nil
}
