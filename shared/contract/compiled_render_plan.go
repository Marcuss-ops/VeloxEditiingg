package contract

// Wire keys for the master-compiled render plan (Fase D) delivered inside
// the TaskOffer payload at claim time.
//
// The master RenderPlanCompiler produces the canonical CompiledRenderPlan
// (plan_version / job_id / attempt_id / duration_ms / media_contract /
// segments[] / audio[] / assets[] — DataServer/internal/renderplan) and
// stamps SHA256(canonical JSON) on task_attempts.plan_sha256. At claim the
// placement pipeline ALSO embeds the canonical document in the offer so the
// worker's batch FFmpeg path can consume the compiled segments instead of
// re-deriving a timeline from raw scenes. No local paths ever travel here;
// assets stay asset_id + sha256 references resolved worker-side.
const (
	// PayloadKeyCompiledRenderPlanJSON carries the canonical
	// CompiledRenderPlan JSON document as a string.
	PayloadKeyCompiledRenderPlanJSON = "compiled_render_plan_json"

	// PayloadKeyCompiledRenderPlanSHA carries SHA256(canonical JSON) — the
	// same value persisted on task_attempts.plan_sha256, closing the
	// job→attempt→plan_version→plan_sha256→renderer_version→artifact_sha256
	// determinism chain on the wire.
	PayloadKeyCompiledRenderPlanSHA = "compiled_render_plan_sha256"
)
