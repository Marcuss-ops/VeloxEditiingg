package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
)

func TestEarlyUploadEligibleRequiresFMP4CompiledPlan(t *testing.T) {
	// An append-only plan can only exist while the streaming profile is
	// registered and enabled: the compiler resolves output.profile_id through
	// the same gate, so this is the state in which such a job is executable.
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	if earlyUploadEligible(nil) {
		t.Fatal("nil task must not be eligible")
	}
	if earlyUploadEligible(&PendingTaskExecution{ExecutorID: "scene.composite.v1"}) {
		t.Fatal("a task without a decodable compiled plan must not negotiate an early upload")
	}
	if earlyUploadEligible(&PendingTaskExecution{ExecutorID: "video.assemble.copy.v1", Spec: executorTaskSpecForEarlyUploadTest(t, false)}) {
		t.Fatal("progressive MP4 plan must not negotiate an early upload")
	}
	if !earlyUploadEligible(&PendingTaskExecution{ExecutorID: "video.assemble.copy.v1@1", Spec: executorTaskSpecForEarlyUploadTest(t, true)}) {
		t.Fatal("fMP4 compiled plan must negotiate an early upload")
	}
}

// TestEarlyUploadEligibleIsPlanDrivenNotExecutorDriven pins the admission
// contract: the append-only layout comes from the plan, and every renderer that
// can select that layout (the packet-copy / mixed_packet mux included) may
// publish while it renders. The executor name previously gated this, so a copy
// job that asked for the streaming profile under any other executor — or under
// a versioned ID — could never overlap its upload.
func TestEarlyUploadEligibleIsPlanDrivenNotExecutorDriven(t *testing.T) {
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	fragmented := executorTaskSpecForEarlyUploadTest(t, true)
	progressive := executorTaskSpecForEarlyUploadTest(t, false)

	for _, executorID := range []string{
		"video.assemble.copy.v1",
		"video.assemble.copy.v1@1",
		"scene.composite.v1",
		"scene.composite.v1@3",
	} {
		eligibility := &PendingTaskExecution{ExecutorID: executorID, Spec: fragmented}
		if !earlyUploadEligible(eligibility) {
			t.Errorf("executor %q with an append-only plan must be eligible", executorID)
		}
		rejected := &PendingTaskExecution{ExecutorID: executorID, Spec: progressive}
		if earlyUploadEligible(rejected) {
			t.Errorf("executor %q with a seekable progressive plan must not be eligible", executorID)
		}
	}
}

// TestEarlyUploadEligibleRefusesDisabledStreamingProfile pins the fail-closed
// direction: when the streaming profile is not enabled, an append-only plan is
// not executable either, so the worker must not negotiate a session it cannot
// feed (that would leave an abandoned server-side upload).
func TestEarlyUploadEligibleRefusesDisabledStreamingProfile(t *testing.T) {
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "0")
	plan := executorTaskSpecForEarlyUploadTest(t, true)
	if earlyUploadEligible(&PendingTaskExecution{ExecutorID: "video.assemble.copy.v1", Spec: plan}) {
		t.Fatal("a plan declaring a disabled streaming profile must not negotiate an early upload")
	}
}

func executorTaskSpecForEarlyUploadTest(t *testing.T, fragmented bool) executor.TaskSpec {
	t.Helper()
	assets := map[string][]byte{"v2-video": []byte("video"), "v2-audio": []byte("audio")}
	payload := compiledPlanAssetPayload(t, assets)
	plan, err := contract.DecodeCompiledRenderPlanV2Payload(payload)
	if err != nil {
		t.Fatalf("decode fixture plan: %v", err)
	}
	if fragmented {
		profile := contract.CanonicalVideoProfileFMP4StreamV1Default
		plan.VideoTracks[0].Segments[0].FrameCount = 24
		plan.Output = contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: profile.Width, Height: profile.Height,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, PixelFormat: profile.PixelFormat,
			ProfileID: profile.ProfileID, CodecProfile: profile.CodecProfile, CodecLevel: profile.CodecLevel,
			GOPSize: profile.GOPSize, BFrames: profile.BFrames, ClosedGOP: profile.ClosedGOP,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		}
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical fixture plan: %v", err)
	}
	digest := sha256.Sum256(canonical)
	payload[contract.PayloadKeyCompiledRenderPlanJSON] = string(canonical)
	payload[contract.PayloadKeyCompiledRenderPlanSHA] = hex.EncodeToString(digest[:])
	decoded, err := contract.DecodeCompiledRenderPlanV2Payload(payload)
	if err != nil {
		t.Fatalf("decode rewritten fixture plan: %v", err)
	}
	if fragmented && decoded.Output.ProfileID != contract.CanonicalVideoProfileFMP4StreamV1 {
		t.Fatalf("rewritten fixture profile = %q", decoded.Output.ProfileID)
	}
	return executor.TaskSpec{ExecutorID: "video.assemble.copy.v1", Payload: payload}
}
