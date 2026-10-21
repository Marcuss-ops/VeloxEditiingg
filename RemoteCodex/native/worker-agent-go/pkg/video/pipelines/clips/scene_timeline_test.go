package clips

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestShuffleStockSceneAssetsShufflesKindClipStockButProtectsExplicitClip(t *testing.T) {
	assets := []sceneTimelineAsset{
		{AssetID: "stock-a", URL: "stock-a.mp4"},
		{AssetID: "stock-b", URL: "stock-b.mp4"},
		{AssetID: "stock-c", URL: "stock-c.mp4"},
		{AssetID: "stock-d", URL: "stock-d.mp4"},
	}
	introClip := &sceneTimelineAsset{AssetID: "intro", URL: "intro.mp4"}
	scenes := []sceneTimelineScene{{
		SceneID: "protected-intro",
		Kind:    "clip",
		Clip:    introClip,
	}}
	for index, asset := range assets {
		encoded, err := json.Marshal([]sceneTimelineAsset{asset})
		if err != nil {
			t.Fatalf("marshal stock asset: %v", err)
		}
		scenes = append(scenes, sceneTimelineScene{
			SceneID: fmt.Sprintf("stock-scene-%d", index),
			// Real creator payloads use kind="clip" for stock-only scenes.
			Kind:  "clip",
			Stock: encoded,
		})
	}

	jobID := ""
	var want []sceneTimelineAsset
	for seed := 0; seed < 100; seed++ {
		candidate := fmt.Sprintf("job-kind-clip-stock-%d", seed)
		ordered := shuffleStockPool(assets, candidate, -1)
		changed := false
		for index := range assets {
			if ordered[index].AssetID != assets[index].AssetID {
				changed = true
				break
			}
		}
		if changed {
			jobID, want = candidate, ordered
			break
		}
	}
	if jobID == "" {
		t.Fatal("could not find a deterministic seed that changes stock order")
	}

	got, err := shuffleStockSceneAssets(scenes, jobID)
	if err != nil {
		t.Fatalf("shuffleStockSceneAssets: %v", err)
	}
	if len(got) != len(scenes) || got[0].SceneID != "protected-intro" || got[0].Clip != introClip {
		t.Fatalf("scene order or protected intro clip changed: %+v", got[0])
	}
	for index, expected := range want {
		pool, err := sceneStockPool(got[index+1])
		if err != nil {
			t.Fatalf("scene %d stock pool: %v", index, err)
		}
		if len(pool) != 1 || pool[0].AssetID != expected.AssetID {
			t.Fatalf("stock scene %d asset = %+v, want %s", index, pool, expected.AssetID)
		}
	}
}
