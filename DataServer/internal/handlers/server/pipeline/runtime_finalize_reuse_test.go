package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

type runtimeFinalizeTestPreparationRecovery struct {
	preparations []contract.VideoPrepareWindowTaskV1
	evidence     []taskattempts.SegmentTiming
	rewritten    map[string]voiceoverassets.PreparedWindowAssetBinding
}

func (r *runtimeFinalizeTestPreparationRecovery) PlanDriveStockWindowTasks(string, []map[string]interface{}) ([]contract.VideoPrepareWindowTaskV1, error) {
	return r.preparations, nil
}

func (r *runtimeFinalizeTestPreparationRecovery) ListSuccessfulSegmentTimings(context.Context, []string) ([]taskattempts.SegmentTiming, error) {
	return r.evidence, nil
}

func (r *runtimeFinalizeTestPreparationRecovery) RewriteDriveStockPlanFromWorkerResults(_ string, _ []map[string]interface{}, results map[string]voiceoverassets.PreparedWindowAssetBinding) (string, string, []map[string]interface{}, error) {
	r.rewritten = results
	return `{"plan_version":2,"assets":[]}`, strings.Repeat("f", 64), nil, nil
}

type runtimeFinalizeTestFragmentCache struct {
	hits    map[string]voiceoverassets.PreparedWindowAssetBinding
	lookups []string
}

func (c *runtimeFinalizeTestFragmentCache) PlanDriveStockWindowTasks(string, []map[string]interface{}) ([]contract.VideoPrepareWindowTaskV1, error) {
	return nil, nil
}

func (c *runtimeFinalizeTestFragmentCache) LookupPreparedWindowAsset(_ context.Context, identity string) (*voiceoverassets.PreparedWindowAssetBinding, error) {
	c.lookups = append(c.lookups, identity)
	binding, ok := c.hits[identity]
	if !ok {
		return nil, nil
	}
	copy := binding
	return &copy, nil
}

func TestCompleteWorkerPreparationsRecoversRegisteredCertificate(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source-a", SHA256: strings.Repeat("a", 64), SizeBytes: 2048, Kind: "video"},
		SourceStartFrame: 0, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	certificate := contract.VideoPrepareWindowCertificateV1{
		Version: contract.VideoPrepareWindowVersion, SourceSHA256: request.Source.SHA256,
		SourceStartFrame: request.SourceStartFrame, SourceFrameCount: request.SourceFrameCount,
		SourceFPSNum: request.SourceFPSNum, SourceFPSDen: request.SourceFPSDen,
		OutputFrameCount: request.OutputFrameCount, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision, EncoderVersion: "ffmpeg test",
		EncoderBuildSHA256: strings.Repeat("c", 64), OutputSHA256: strings.Repeat("d", 64), OutputSizeBytes: 4096,
		OutputEvidence: contract.VideoWindowOutputEvidenceV1{
			Codec: "h264", Profile: "high", Level: 40, Width: profile.Width, Height: profile.Height,
			PixelFormat: profile.PixelFormat, FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
			FrameCount: request.OutputFrameCount, FirstFrameKeyframe: true,
			ExtradataSHA256: contract.CanonicalVideoExtradataSHA256,
		},
		ClosedGOPAttested: true, PacketCopyUsed: true, SourceAssetID: request.Source.AssetID,
		SourceSizeBytes: request.Source.SizeBytes, TimelineRevision: request.TimelineRevision,
		TimelineSHA256: request.TimelineSHA256,
	}
	metadata, err := json.Marshal(struct {
		contract.VideoPrepareWindowCertificateV1
		RegisteredAssetID string `json:"registered_asset_id"`
	}{VideoPrepareWindowCertificateV1: certificate, RegisteredAssetID: "prepared-asset"})
	if err != nil {
		t.Fatal(err)
	}
	const taskID = "assembly-1-prep-0123456789abcdef01234567"
	recovery := &runtimeFinalizeTestPreparationRecovery{
		preparations: []contract.VideoPrepareWindowTaskV1{{Identity: identity, Request: request}},
		evidence: []taskattempts.SegmentTiming{{
			TaskID: taskID, SourceType: contract.VideoPrepareWindowExecutorID,
			Status: "succeeded", MetadataJSON: string(metadata),
		}},
	}
	processor := &RuntimeFinalizationProcessor{assets: recovery, evidence: recovery}
	patch, err := processor.completeWorkerPreparations(context.Background(),
		taskgraph.RuntimeFinalizationCandidate{TaskID: "assembly-1", JobID: "job-1"},
		`{"plan_version":2,"assets":[]}`, FinalizeJobRequest{RuntimePayload: map[string]interface{}{}},
		map[string]interface{}{}, nil, map[string]string{identity: taskID})
	if err != nil {
		t.Fatalf("completeWorkerPreparations: %v", err)
	}
	binding := recovery.rewritten[identity]
	if len(recovery.rewritten) != 1 || binding.AssetID != "prepared-asset" || binding.SHA256 != certificate.OutputSHA256 || binding.SizeBytes != certificate.OutputSizeBytes {
		t.Fatalf("rewritten bindings = %+v", recovery.rewritten)
	}
	if patch[contract.PayloadKeyCompiledRenderPlanSHA] != strings.Repeat("f", 64) || patch["runtime_assets_pending"] != false ||
		patch[runtimePrepareTasksKey] != nil || patch[runtimePrepareWindowReuseKey] != nil {
		t.Fatalf("finalized assembly patch = %#v", patch)
	}
}

func TestReusablePreparationsSplitsCacheHitsAndDiscoveryMisses(t *testing.T) {
	hit := strings.Repeat("a", 64)
	miss := strings.Repeat("b", 64)
	discovery := strings.Repeat("c", 64)
	binding := voiceoverassets.PreparedWindowAssetBinding{
		AssetID: "asset-hit", SHA256: strings.Repeat("d", 64), SizeBytes: 1024, ExtradataSHA256: strings.Repeat("e", 64),
	}
	cache := &runtimeFinalizeTestFragmentCache{hits: map[string]voiceoverassets.PreparedWindowAssetBinding{
		hit: binding, discovery: binding,
	}}
	processor := &RuntimeFinalizationProcessor{assets: cache}
	preparations := []contract.VideoPrepareWindowTaskV1{
		{Identity: hit},
		{Identity: miss},
		{Identity: discovery, Request: contract.VideoPrepareWindowV1{SourceIdentityDiscovery: true}},
	}
	reuse, misses, err := processor.reusablePreparations(context.Background(), preparations)
	if err != nil {
		t.Fatalf("reusablePreparations: %v", err)
	}
	if len(reuse) != 1 || reuse[hit].AssetID != "asset-hit" || reuse[hit].SizeBytes != 1024 {
		t.Fatalf("reuse bindings = %+v; want only the certified hit", reuse)
	}
	if len(misses) != 2 || misses[0].Identity != miss || misses[1].Identity != discovery {
		t.Fatalf("misses = %+v; want the cache miss and the discovery task", misses)
	}
	for _, looked := range cache.lookups {
		if looked == discovery {
			t.Fatal("a discovery task must not be looked up by a content identity")
		}
	}
}

func TestPublishWorkerPreparationsPersistsReuseBindings(t *testing.T) {
	expander := &runtimeFinalizeTestExpander{}
	notifier := &runtimeFinalizeTestNotifier{}
	processor := &RuntimeFinalizationProcessor{expander: expander, notifier: notifier}
	miss := strings.Repeat("a", 64)
	reused := strings.Repeat("b", 64)
	reuse := map[string]voiceoverassets.PreparedWindowAssetBinding{
		reused: {AssetID: "asset-b", SHA256: strings.Repeat("c", 64), SizeBytes: 42, ExtradataSHA256: strings.Repeat("d", 64)},
	}
	preparations := []contract.VideoPrepareWindowTaskV1{{
		Identity: miss, Request: contract.VideoPrepareWindowV1{Version: contract.VideoPrepareWindowVersion},
		SegmentIDs: []string{"segment-1"},
	}}
	if err := processor.publishWorkerPreparations(context.Background(),
		taskgraph.RuntimeFinalizationCandidate{TaskID: "assembly-1", JobID: "job-1"}, nil, preparations, reuse, "", "", nil); err != nil {
		t.Fatalf("publishWorkerPreparations: %v", err)
	}
	if len(expander.additions) != 1 || expander.additions[0].TaskID != "assembly-1-prep-"+miss[:24] {
		t.Fatalf("published additions = %+v; want only the cache miss", expander.additions)
	}
	persisted, ok := expander.patch[runtimePrepareWindowReuseKey].(map[string]voiceoverassets.PreparedWindowAssetBinding)
	if !ok || persisted[reused].AssetID != "asset-b" {
		t.Fatalf("persisted reuse bindings = %#v", expander.patch[runtimePrepareWindowReuseKey])
	}
	tasks, ok := expander.patch[runtimePrepareTasksKey].(map[string]string)
	if !ok || len(tasks) != 1 || tasks[miss] == "" {
		t.Fatalf("persisted preparation tasks = %#v; want only the cache miss", expander.patch[runtimePrepareTasksKey])
	}
}

func TestDecodePreparedWindowReuseAcceptsPersistedJSON(t *testing.T) {
	identity := strings.Repeat("a", 64)
	decoded, err := decodePreparedWindowReuse(map[string]interface{}{
		identity: map[string]interface{}{
			"asset_id": "asset-a", "sha256": strings.Repeat("b", 64), "size_bytes": float64(99),
		},
	})
	if err != nil {
		t.Fatalf("decodePreparedWindowReuse: %v", err)
	}
	binding := decoded[identity]
	if binding.AssetID != "asset-a" || binding.SizeBytes != 99 || binding.SHA256 != strings.Repeat("b", 64) {
		t.Fatalf("decoded binding = %+v", binding)
	}
	if empty, err := decodePreparedWindowReuse(nil); err != nil || len(empty) != 0 {
		t.Fatalf("nil reuse decoded to %+v err=%v", empty, err)
	}
}
