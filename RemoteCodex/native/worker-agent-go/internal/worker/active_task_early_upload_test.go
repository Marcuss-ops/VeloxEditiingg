package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
)

func TestEarlyUploadEligibleRequiresFMP4CompiledPlan(t *testing.T) {
	if earlyUploadEligible(nil) {
		t.Fatal("nil task must not be eligible")
	}
	if earlyUploadEligible(&PendingTaskExecution{ExecutorID: "scene.composite.v1"}) {
		t.Fatal("legacy scene composite must not negotiate an early upload")
	}
	if earlyUploadEligible(&PendingTaskExecution{ExecutorID: "video.assemble.copy.v1", Spec: executorTaskSpecForEarlyUploadTest(t, false)}) {
		t.Fatal("progressive MP4 plan must not negotiate an early upload")
	}
	if !earlyUploadEligible(&PendingTaskExecution{ExecutorID: "video.assemble.copy.v1@1", Spec: executorTaskSpecForEarlyUploadTest(t, true)}) {
		t.Fatal("fMP4 compiled plan must negotiate an early upload")
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
