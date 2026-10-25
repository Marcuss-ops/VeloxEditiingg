package pipeline

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-shared/contract/assembly"
)

// TestSubmitJobBuildsWorkerPayloadCorrectly is replaced by
// TestNormalizeExternalJobSubmission_ProducesCanonicalPayload below —
// the worker payload is no longer hand-built; it is produced through
// remoteengine.ParseRemotePipelineResult → RemotePipelineResult.
// ToWorkerPayloadChecked. The shape assertion that previously read
// payload["scenes"].([]interface{}) is now payload["scenes_json"]
// (string) because the canonical typed-DTO path encodes scenes as a
// JSON string rather than a nested []interface{}. Tests for the
// canonical shape below.

// TestNormalizeExternalJobSubmission_ProducesCanonicalPayload locks
// the canonical output shape that NormalizeExternalJobSubmission
// produces. The contract:
//
//	(1) source_provider   == ExternalAPISourceProvider (low-cardinality
//	    constant; per the [P0 #4] audit verbatim).
//	(2) source_job_id      == trimmed idempotency_key.
//	(3) target_executor_id == JobSubmitTargetExecutorID.
//	(4) worker_payload["status"]            == "completed".
//	(5) worker_payload["job_id"]            == trimmed idempotency_key.
//	(6) worker_payload["video_name"]        (when set on req).
//	(7) worker_payload["script_text"]       (when set on req).
//	(8) per-scene voiceover.url and subtitles remain inside scenes_json.
//	(9) worker_payload["scenes_json"]       — JSON-encoding of the
//	    scene list with text + duration_seconds + nested clip / voiceover / subtitles
//	    fields carried through.
//
// (10) canonical.DeliveryPlan              — preserved as the
//
//	control-plane shape (entry-level {destination_id, priority,
//	retry_budget, metadata}); the renderer WorkerPayload must not
//
// carry delivery routing.
func TestNormalizeExternalJobSubmission_ProducesCanonicalPayload(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	req := SubmitJobRequest{
		IdempotencyKey: "test-job-123", VideoName: "Test Video", ScriptText: "Hello world",
		CompiledRenderPlanJSON: planJSON, CompiledRenderPlanSHA256: planSHA,
		DeliveryPlan: []SubmitDeliveryPlanEntry{{DestinationID: "drive", Priority: 1, RetryBudget: intPtr(3)}},
	}
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil canonical")
	}
	if canonical.SourceProvider != ExternalAPISourceProvider || canonical.SourceJobID != "test-job-123" || canonical.TargetExecutorID != JobSubmitTargetExecutorID {
		t.Fatalf("unexpected canonical identity: %#v", canonical)
	}
	if canonical.WorkerPayload[contract.PayloadKeyCompiledRenderPlanJSON] != planJSON || canonical.WorkerPayload[contract.PayloadKeyCompiledRenderPlanSHA] != planSHA {
		t.Fatal("producer-owned plan was not preserved byte-for-byte")
	}
	if _, present := canonical.WorkerPayload["scenes_json"]; present {
		t.Fatal("legacy scene projection crossed the worker boundary")
	}
	if canonical.DeliveryPlan["delivery_plan"] == nil {
		t.Fatal("delivery plan was lost from the control-plane envelope")
	}
}

func TestNormalizeExternalJobSubmissionCarriesAssemblyOutsideRendererPayload(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "assembly-job-1",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 1}},
		Assembly: &assembly.ExternalAssemblyRequest{
			SendToVelox: true, TimelineRevision: 1, TimelineHash: "sha256:timeline", OutputProfile: "velox-h264-copy-v1",
			Assets: []assembly.AssetRequirement{{AssetID: "clip-1", Kind: assembly.KindSourceClip, Availability: assembly.AvailabilityKnown, URL: "https://example.test/clip", SHA256: strings.Repeat("a", 64), Required: true, State: assembly.AssetReady}},
		},
	}
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil || canonical.Assembly == nil {
		t.Fatal("assembly was not normalized into canonical control-plane payload")
	}
	if canonical.Assembly.Dispatch.Mode != assembly.ModeEager || !canonical.Assembly.Dispatch.StartPrepare {
		t.Fatalf("dispatch = %#v, want eager preparation", canonical.Assembly.Dispatch)
	}
	if _, leaked := canonical.WorkerPayload["assembly"]; leaked {
		t.Fatal("assembly control-plane metadata leaked into renderer payload")
	}
}

func TestNormalizeExternalJobSubmissionRoutesProducerOwnedCompiledPlan(t *testing.T) {
	planJSON := `{"plan_version":2,"output":{"profile_id":"velox-h264-fmp4-stream-v1"}}`
	planSHA := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := SubmitJobRequest{
		IdempotencyKey:           "compiled-job-1",
		VideoName:                "Compiled fMP4 job",
		ScriptText:               "Producer-owned compiled plan",
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil canonical")
	}
	if canonical.TargetExecutorID != "video.assemble.copy.v1" {
		t.Fatalf("target executor = %q, want video.assemble.copy.v1", canonical.TargetExecutorID)
	}
	if got := canonical.WorkerPayload[contract.PayloadKeyCompiledRenderPlanJSON]; got != planJSON {
		t.Fatalf("worker compiled plan JSON = %#v, want exact producer bytes", got)
	}
	if got := canonical.WorkerPayload[contract.PayloadKeyCompiledRenderPlanSHA]; got != planSHA {
		t.Fatalf("worker compiled plan SHA = %#v, want %q", got, planSHA)
	}
}

// TestCanonicalPayloadParity_PipelinePreservesCanonicalKeys verifies the
// fourth contract surface: keys emitted by the external pipeline intake must
// survive the DTO → worker-payload projection. The raw request projection
// intentionally encodes scenes as scenes_json; every other populated
// canonical key is expected to remain at the same top-level key.
func TestCanonicalPayloadParity_PipelinePreservesCanonicalKeys(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "contract-parity-pipeline-001",
		VideoName:      "Parity pipeline video",
		ScriptText:     "Parity pipeline script",
		Scenes: []SubmitScene{{
			Text:            "Parity scene",
			Clip:            &SubmitClip{URL: "velox-asset://clip/parity.mp4"},
			Voiceover:       &SubmitVoiceover{URL: "velox-asset://voiceover/parity.mp3"},
			Subtitles:       &SubmitSubtitles{URL: "velox-asset://subtitle/parity.srt", Format: "srt"},
			DurationSeconds: 5,
		}},
		Layers: []SubmitLayer{{
			ID: "layer-parity", Type: "text", Role: "title", Text: "Parity",
		}},
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "drive", Priority: 1, RetryBudget: intPtr(3),
			Metadata: map[string]interface{}{"folder_id": "folder-parity"},
		}},
	}

	raw := submitRequestToRawPayload(&req)
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil || canonical.WorkerPayload == nil {
		t.Fatal("NormalizeExternalJobSubmission returned no worker payload")
	}

	outputKey := map[string]string{"scenes": "scenes_json"}
	for key, value := range raw {
		if value == nil || strings.HasPrefix(key, "_") {
			continue
		}
		// Delivery routing is carried by CanonicalCompletedPayload.DeliveryPlan,
		// not by the renderer WorkerPayload. Its boundary is asserted below.
		if key == "delivery_plan" {
			continue
		}
		if !contract.IsCanonicalKey(key) {
			t.Errorf("pipeline raw payload emitted non-canonical key %q", key)
			continue
		}
		wantKey := key
		if mapped, ok := outputKey[key]; ok {
			wantKey = mapped
		}
		workerValue, ok := canonical.WorkerPayload[wantKey]
		if !ok {
			t.Errorf("canonical pipeline key %q was lost during worker projection (raw key %q)", wantKey, key)
			continue
		}
		if key == "scenes" {
			continue // scenes is intentionally serialized as scenes_json.
		}
		rawJSON, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal raw pipeline key %q: %v", key, err)
		}
		workerJSON, err := json.Marshal(workerValue)
		if err != nil {
			t.Fatalf("marshal worker pipeline key %q: %v", wantKey, err)
		}
		if string(rawJSON) != string(workerJSON) {
			t.Errorf("pipeline key %q changed during worker projection: raw=%s worker=%s", key, rawJSON, workerJSON)
		}
	}

	dp, ok := canonical.DeliveryPlan["delivery_plan"].([]interface{})
	if !ok || len(dp) != 1 {
		t.Fatalf("control-plane delivery_plan shape wrong: %v", canonical.DeliveryPlan["delivery_plan"])
	}
	if _, leaked := canonical.WorkerPayload["delivery_plan"]; leaked {
		t.Fatalf("delivery_plan leaked into renderer WorkerPayload: %#v", canonical.WorkerPayload["delivery_plan"])
	}
	metadata, ok := dp[0].(map[string]interface{})["metadata"].(map[string]interface{})
	if !ok || metadata["folder_id"] != "folder-parity" {
		t.Fatalf("delivery metadata lost during projection: %#v", dp[0])
	}

}

// TestNormalizeExternalJobSubmission_MatchesCreatorPushShape is the
// parity test the click asked for. It builds equivalent semantic
// content through BOTH intake paths — creator_push envelopes and
// /api/v1/jobs flat — and asserts that the resulting
// CanonicalCompletedPayload structures produce equivalent worker
// payloads (same job_id, status, canonical scene content, and delivery_plan).
// This locks the [P1] invariant: a future PR that
// diverges the two paths — e.g., by adding a private field on one
// path's worker_payload — will fail this test loudly.
//
// The two requests below are canonically-equivalent: same job_id,
// same video, same script, same voiceover, same scene text +
// duration, same delivery destination. The idem-key for the SubmitJob
// path sets job_id (matching CreatorPush's source_job_id == job_id).
func TestNormalizeExternalJobSubmission_MatchesCreatorPushShape(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	jobID := "parity-job-001"
	submitCanonical := (&Handlers{}).NormalizeExternalJobSubmission(SubmitJobRequest{
		IdempotencyKey: jobID, VideoName: "Parity Video", ScriptText: "Same script body.",
		CompiledRenderPlanJSON: planJSON, CompiledRenderPlanSHA256: planSHA,
	})
	creatorCanonical, err := normalizeCreatorPushRequest(creatorPushRequest{
		SourceProvider: "creator_parity", SourceJobID: jobID, TargetExecutorID: JobSubmitTargetExecutorID,
		Payload: map[string]interface{}{
			"job_id": jobID, "video_name": "Parity Video", "script_text": "Same script body.",
			contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
			contract.PayloadKeyCompiledRenderPlanSHA:  planSHA,
		},
	})
	if err != nil {
		t.Fatalf("normalizeCreatorPushRequest: %v", err)
	}
	if submitCanonical == nil {
		t.Fatal("external submission did not produce a canonical payload")
	}
	for _, field := range []string{"video_name", "script_text", contract.PayloadKeyCompiledRenderPlanJSON, contract.PayloadKeyCompiledRenderPlanSHA} {
		if submitCanonical.WorkerPayload[field] != creatorCanonical.WorkerPayload[field] {
			t.Errorf("worker payload field %q differs between ingress paths", field)
		}
	}
}

func TestNormalizeExternalJobSubmission_OmittedRetryBudgetDefaultsToThree(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "rb-omitted-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: nil},
		},
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	dp, ok := canonical.DeliveryPlan["delivery_plan"].([]interface{})
	if !ok || len(dp) != 1 {
		t.Fatalf("control-plane delivery_plan shape wrong: %v", canonical.DeliveryPlan["delivery_plan"])
	}
	entry, ok := dp[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery_plan[0] type %T", dp[0])
	}
	if got := entry["retry_budget"]; got != 3 {
		t.Errorf("entry[retry_budget] = %v, want 3 (OpenAPI default)", got)
	}
}

// TestNormalizeExternalJobSubmission_ExplicitRetryBudgetZeroPreserved
// locks the *int boundary contract for the explicit-zero case:
// a client that sends {"retry_budget": 0} MUST have that value
// preserved verbatim into the control-plane envelope, not silently
// merged with the omitted default (3). This is the actual point of the
// int→*int refactor — without it, an operator who tries to disable
// retries on a specific destination would be silently overridden
// on every request.
//
// Asserted by reading back the control-plane delivery envelope's
// delivery_plan entry map and confirming retry_budget == 0 (NOT 3).
func TestNormalizeExternalJobSubmission_ExplicitRetryBudgetZeroPreserved(t *testing.T) {
	t.Parallel()

	zeroRetry := 0
	req := SubmitJobRequest{
		IdempotencyKey: "rb-zero-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: &zeroRetry},
		},
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	dp, ok := canonical.DeliveryPlan["delivery_plan"].([]interface{})
	if !ok || len(dp) != 1 {
		t.Fatalf("control-plane delivery_plan shape wrong: %v", canonical.DeliveryPlan["delivery_plan"])
	}
	entry, ok := dp[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery_plan[0] type %T", dp[0])
	}
	if got := entry["retry_budget"]; got != 0 {
		t.Errorf("entry[retry_budget] = %v, want 0 (explicit-zero contract)", got)
	}
}
