package pipeline

import (
	"context"
	"strings"
	"testing"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

type runtimeFinalizeTestFragmentCache struct {
	hits    map[string]voiceoverassets.PreparedWindowAssetBinding
	lookups []string
}

func (c *runtimeFinalizeTestFragmentCache) PrepareDriveStockWindows(context.Context, string, []map[string]interface{}) (string, string, error) {
	return "", "", nil
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

func TestReusablePreparationsSplitsCacheHitsAndDiscoveryMisses(t *testing.T) {
	hit := strings.Repeat("a", 64)
	miss := strings.Repeat("b", 64)
	discovery := strings.Repeat("c", 64)
	binding := voiceoverassets.PreparedWindowAssetBinding{
		AssetID: "asset-hit", SHA256: strings.Repeat("d", 64), SizeBytes: 1024,
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
		reused: {AssetID: "asset-b", SHA256: strings.Repeat("c", 64), SizeBytes: 42},
	}
	preparations := []contract.VideoPrepareWindowTaskV1{{
		Identity: miss, Request: contract.VideoPrepareWindowV1{Version: contract.VideoPrepareWindowVersion},
		SegmentIDs: []string{"segment-1"},
	}}
	if err := processor.publishWorkerPreparations(context.Background(),
		taskgraph.RuntimeFinalizationCandidate{TaskID: "assembly-1", JobID: "job-1"}, nil, preparations, reuse); err != nil {
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
