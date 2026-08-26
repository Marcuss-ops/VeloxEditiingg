package executors

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
)

// render_batch_observability.go owns the render_batch@1 structured-log and
// telemetry surface: the per-phase observability wrapper, its begin/finish/
// failure lifecycle and the error-detail redaction helpers. It never touches
// ffmpeg or the compiled plan — it only records what each phase did.

type renderBatchPhase struct {
	stage   string
	started time.Time
	handle  *telemetry.EventHandle
}

type renderBatchObservability struct {
	logger           executor.Logger
	recorder         *telemetry.EventRecorder
	planSHA          string
	timelineSHA      string
	timelineRevision int64
	metrics          map[string]interface{}
	rawMetrics       *telemetry.RawExecutionMetrics
}

func newRenderBatchObservability(execCtx executor.ExecutionContext, planSHA string) *renderBatchObservability {
	obs := &renderBatchObservability{
		planSHA: planSHA,
		metrics: make(map[string]interface{}),
	}
	if execCtx != nil {
		obs.logger = execCtx.Logger()
		obs.recorder = recorderFromExecutionContext(execCtx)
	}
	return obs
}

func compiledPlanSHA(spec executor.TaskSpec) string {
	sha, _ := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	return strings.TrimSpace(sha)
}

func (o *renderBatchObservability) ensureRawMetrics() {
	if o == nil {
		return
	}
	if o.rawMetrics == nil {
		o.rawMetrics = &telemetry.RawExecutionMetrics{}
	}
}

func (o *renderBatchObservability) mergeRawMetrics(raw *telemetry.RawExecutionMetrics) {
	if o == nil || raw == nil {
		return
	}
	o.ensureRawMetrics()
	mergeRawFFmpegMetrics(o.rawMetrics, raw)
}

func (o *renderBatchObservability) identityFields() map[string]interface{} {
	fields := make(map[string]interface{}, 2)
	if o.planSHA != "" {
		fields["plan_sha256"] = o.planSHA
	}
	if o.timelineSHA != "" {
		fields["timeline_sha256"] = o.timelineSHA
	}
	if o.timelineRevision > 0 {
		fields["timeline_revision"] = o.timelineRevision
	}
	return fields
}

func (o *renderBatchObservability) info(event string, fields map[string]interface{}) {
	if o == nil || o.logger == nil {
		return
	}
	merged := o.identityFields()
	for key, value := range fields {
		merged[key] = value
	}
	o.logger.Info(event, merged)
}

func (o *renderBatchObservability) logFailure(stage, code string, _ error) {
	if o == nil {
		return
	}
	fields := map[string]interface{}{"stage": stage, "error_code": code}
	// Do not pass err to the logger: asset errors can contain worker-local
	// paths. The stable code and identity fields are the structured contract.
	o.error("render_batch.failed", fields)
}

func (o *renderBatchObservability) error(event string, fields map[string]interface{}) {
	if o == nil || o.logger == nil {
		return
	}
	merged := o.identityFields()
	for key, value := range fields {
		merged[key] = value
	}
	o.logger.Error(event, nil, merged)
}

func (o *renderBatchObservability) begin(stage, component, action string) *renderBatchPhase {
	if o == nil {
		return nil
	}
	o.info("render_batch."+stage+".started", map[string]interface{}{"stage": stage})
	phase := &renderBatchPhase{stage: stage, started: time.Now()}
	if o.recorder == nil {
		return phase
	}
	spec, ok := telemetry.LookupCanonicalPhaseSpec(component, action)
	if !ok {
		o.info("render_batch.telemetry_unregistered", map[string]interface{}{"stage": stage})
		return phase
	}
	metadata := o.identityFields()
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return phase
	}
	phase.handle = o.recorder.Start(telemetry.EventSpec{
		Origin: spec.Origin, Scope: spec.Scope, Component: spec.Component,
		Action: spec.Action, Phase: spec.Phase, EventType: spec.EventType,
		SchemaVersion: telemetry.SchemaVersion, MetadataJSON: string(encoded),
	})
	return phase
}

func (o *renderBatchObservability) finish(phase *renderBatchPhase, status, code string, err error) {
	if o == nil || phase == nil {
		return
	}
	duration := time.Since(phase.started).Milliseconds()
	fields := o.identityFields()
	fields["stage"] = phase.stage
	fields["duration_ms"] = duration
	fields["status"] = status
	if code != "" {
		fields["error_code"] = code
	}
	if status == telemetry.StatusFailed {
		o.error("render_batch."+phase.stage+".failed", fields)
	} else {
		o.info("render_batch."+phase.stage+".completed", fields)
	}
	switch phase.stage {
	case "validation":
		o.metrics["render_plan_validate_ms"] = duration
	case "asset_resolution":
		o.metrics["compiled_asset_resolve_ms"] = duration
	case "visual_render":
		o.metrics["visual_execute_ms"] = duration
	case "final_mux":
		o.metrics["final_mux_ms"] = duration
	}
	if phase.handle == nil {
		return
	}
	metadata := o.identityFields()
	for key, value := range fields {
		metadata[key] = value
	}
	encoded, marshalErr := json.Marshal(metadata)
	if marshalErr == nil {
		phase.handle.SetMetadataJSON(string(encoded))
	}
	if status == telemetry.StatusFailed {
		phase.handle.Abort(code, code)
	} else {
		phase.handle.CompleteWith(0, 0, 0, telemetry.StatusOK, "", "")
	}
}

func (o *renderBatchObservability) failure(started time.Time, code string, err error) executor.ExecutionResult {
	detail := safeRenderBatchErrorDetail(code, err)
	if o == nil {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: detail, StartedAt: started, CompletedAt: time.Now().UTC()}
	}
	o.logFailure("execution", code, err)
	if o.rawMetrics != nil {
		o.rawMetrics.WallClockSeconds = time.Since(started).Seconds()
	}
	return executor.ExecutionResult{
		Status: "failed", ErrorCode: code, ErrorDetail: detail,
		RawMetrics: o.rawMetrics, Metrics: o.metrics,
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}
}

func safeRenderBatchErrorDetail(code string, err error) string {
	if err == nil {
		return code
	}
	parts := strings.Fields(err.Error())
	for i, part := range parts {
		if strings.ContainsAny(part, "/\\\\") || strings.Contains(part, ".mp4") || strings.Contains(part, ".asset") || isSHA256Token(part) {
			parts[i] = "<redacted>"
		}
	}
	if len(parts) == 0 {
		return code
	}
	return code + ": " + strings.Join(parts, " ")
}

func isSHA256Token(value string) bool {
	value = strings.Trim(value, "=:,;()[]{}\\\"")
	if separator := strings.LastIndexAny(value, "=:"); separator >= 0 {
		value = value[separator+1:]
	}
	value = strings.Trim(value, "=:,;()[]{}\\\"")
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}
