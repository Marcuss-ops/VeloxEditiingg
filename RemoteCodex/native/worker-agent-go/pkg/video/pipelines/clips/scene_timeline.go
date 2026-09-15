package clips

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"

	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
)

// sceneTimelineAsset is the worker-facing subset of a canonical asset. The
// worker receives local paths after asset resolution, so URL remains the
// single source used by the native renderer and by the duration probe.
type sceneTimelineAsset struct {
	AssetID    string `json:"asset_id,omitempty"`
	URL        string `json:"url,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}

type sceneTimelineScene struct {
	SceneID         string              `json:"scene_id,omitempty"`
	DurationSeconds float64             `json:"duration_seconds"`
	Clip            *sceneTimelineAsset `json:"clip,omitempty"`
	Stock           json.RawMessage     `json:"stock,omitempty"`
	StockLinks      []string            `json:"stock_links,omitempty"`
	Voiceover       *sceneTimelineAsset `json:"voiceover,omitempty"`
}

func decodeSceneTimeline(encoded string) ([]sceneTimelineScene, error) {
	var scenes []sceneTimelineScene
	if err := json.Unmarshal([]byte(encoded), &scenes); err != nil {
		return nil, err
	}
	for index := range scenes {
		stock, err := decodeStockPool(scenes[index].Stock, scenes[index].StockLinks)
		if err != nil {
			return nil, fmt.Errorf("scene %d stock: %w", index, err)
		}
		encodedStock, err := json.Marshal(stock)
		if err != nil {
			return nil, fmt.Errorf("scene %d stock: %w", index, err)
		}
		scenes[index].Stock = encodedStock
		scenes[index].StockLinks = nil
	}
	return scenes, nil
}

func decodeStockPool(raw json.RawMessage, links []string) ([]sceneTimelineAsset, error) {
	if len(strings.TrimSpace(string(raw))) == 0 || string(raw) == "null" {
		pool := make([]sceneTimelineAsset, 0, len(links))
		for _, link := range links {
			if trimmed := strings.TrimSpace(link); trimmed != "" {
				pool = append(pool, sceneTimelineAsset{URL: trimmed})
			}
		}
		return pool, nil
	}
	var pool []sceneTimelineAsset
	if err := json.Unmarshal(raw, &pool); err == nil {
		return pool, nil
	}
	var single sceneTimelineAsset
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, err
	}
	return []sceneTimelineAsset{single}, nil
}

func sceneStockPool(scene sceneTimelineScene) ([]sceneTimelineAsset, error) {
	return decodeStockPool(scene.Stock, scene.StockLinks)
}

func sceneTimelineRequired(scenes []sceneTimelineScene) bool {
	for _, scene := range scenes {
		stock, _ := sceneStockPool(scene)
		if len(stock) > 0 || scene.Voiceover != nil {
			return true
		}
	}
	return false
}

func validateSceneTimeline(scenes []sceneTimelineScene) error {
	if len(scenes) == 0 {
		return fmt.Errorf("scenes_json must contain at least one scene")
	}
	for index, scene := range scenes {
		if scene.DurationSeconds <= 0 && scene.Voiceover == nil {
			return fmt.Errorf("scenes[%d].duration_seconds must be positive", index)
		}
		stock, err := sceneStockPool(scene)
		if err != nil {
			return fmt.Errorf("scenes[%d].stock: %w", index, err)
		}
		if scene.Clip == nil && len(stock) == 0 {
			return fmt.Errorf("scenes[%d]: clip or stock is required", index)
		}
		for assetIndex, asset := range stock {
			if strings.TrimSpace(asset.URL) == "" {
				return fmt.Errorf("scenes[%d].stock[%d].url is required", index, assetIndex)
			}
		}
		if scene.Voiceover != nil && strings.TrimSpace(scene.Voiceover.URL) == "" {
			return fmt.Errorf("scenes[%d].voiceover.url is required", index)
		}
		if scene.Clip != nil && strings.TrimSpace(scene.Clip.URL) == "" {
			return fmt.Errorf("scenes[%d].clip.url is required", index)
		}
	}
	return nil
}

func compileSceneTimeline(ctx context.Context, jobID string, scenes []sceneTimelineScene, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	if err := validateSceneTimeline(scenes); err != nil {
		return nil, fmt.Errorf("clips.v1: %w", err)
	}

	timeline := make([]plan.TimelineItem, 0, len(scenes)*2)
	audioTracks := make([]plan.AudioTrack, 0, len(scenes)*2)
	offset := 0.0
	for sceneIndex, scene := range scenes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stock, err := sceneStockPool(scene)
		if err != nil {
			return nil, fmt.Errorf("clips.v1: scene %d stock: %w", sceneIndex, err)
		}

		voiceoverDuration := 0.0
		if scene.Voiceover != nil {
			voiceoverDuration, err = resolveSceneAssetDuration(scene.Voiceover, 0, probe)
			if err != nil {
				return nil, fmt.Errorf("clips.v1: scene %d voiceover: %w", sceneIndex, err)
			}
		}
		targetDuration := scene.DurationSeconds
		if voiceoverDuration > 0 {
			targetDuration = voiceoverDuration
		}

		if len(stock) == 0 && scene.Voiceover != nil && scene.Clip != nil {
			stock = []sceneTimelineAsset{*scene.Clip}
		}
		if len(stock) > 0 {
			segments, loopErr := loopStockToDuration(stock, targetDuration, probe, jobID, sceneIndex)
			if loopErr != nil {
				return nil, fmt.Errorf("clips.v1: scene %d stock: %w", sceneIndex, loopErr)
			}
			for _, segment := range segments {
				timeline = append(timeline, plan.TimelineItem{
					Source:          plan.MediaSource{Type: "video", URL: segment.URL},
					SceneID:         scene.SceneID,
					DurationSeconds: segment.Duration,
					IncludeAudio:    false,
				})
			}
		}

		clipDuration := 0.0
		if scene.Clip != nil {
			clipDuration, err = resolveSceneAssetDuration(scene.Clip, scene.DurationSeconds, probe)
			if err != nil {
				return nil, fmt.Errorf("clips.v1: scene %d clip: %w", sceneIndex, err)
			}
			timeline = append(timeline, plan.TimelineItem{
				Source:          plan.MediaSource{Type: "video", URL: scene.Clip.URL},
				SceneID:         scene.SceneID,
				DurationSeconds: clipDuration,
				// Keep every video segment silent in the timeline. Clip audio is
				// represented as an explicit audio track below so the native batch
				// renderer can process all video segments in parallel, including a
				// leading intro clip.
				IncludeAudio: false,
			})
			if scene.Voiceover != nil {
				audioTracks = append(audioTracks, plan.AudioTrack{
					SourceURL:       scene.Voiceover.URL,
					Volume:          1,
					StartTimeOffset: offset,
					DurationSeconds: voiceoverDuration,
					Role:            "voiceover",
				})
				audioTracks = append(audioTracks, plan.AudioTrack{
					SourceURL:       scene.Clip.URL,
					Volume:          1,
					StartTimeOffset: offset + voiceoverDuration,
					DurationSeconds: clipDuration,
					Role:            "scene_clip_audio",
				})
			} else {
				audioTracks = append(audioTracks, plan.AudioTrack{
					SourceURL:       scene.Clip.URL,
					Volume:          1,
					StartTimeOffset: offset,
					DurationSeconds: clipDuration,
					Role:            "scene_clip_audio",
				})
			}
		} else if scene.Voiceover != nil {
			audioTracks = append(audioTracks, plan.AudioTrack{
				SourceURL:       scene.Voiceover.URL,
				Volume:          1,
				StartTimeOffset: offset,
				DurationSeconds: voiceoverDuration,
				Role:            "voiceover",
			})
		}
		// targetDuration belongs to the stock portion only. A clip-only scene
		// occupies clipDuration, not targetDuration plus clipDuration; counting
		// both shifts every following voiceover by the intro length.
		if len(stock) > 0 {
			offset += targetDuration + clipDuration
		} else if scene.Clip != nil {
			offset += clipDuration
		} else {
			offset += targetDuration
		}
	}

	return &plan.RenderPlan{
		Version:     1,
		JobID:       jobID,
		Canvas:      plan.DefaultCanvas(),
		CopyOnly:    false,
		Mixed:       sceneTimelineHasStock(scenes),
		Timeline:    timeline,
		AudioTracks: audioTracks,
		OutputPath:  outputPath,
	}, nil
}

type stockSegment struct {
	URL      string
	Duration float64
}

func sceneTimelineHasStock(scenes []sceneTimelineScene) bool {
	for _, scene := range scenes {
		stock, _ := sceneStockPool(scene)
		if len(stock) > 0 {
			return true
		}
	}
	return false
}

func loopStockToDuration(pool []sceneTimelineAsset, target float64, probe audio.Probe, jobID string, sceneIndex int) ([]stockSegment, error) {
	if len(pool) == 0 || target <= 0 {
		return nil, nil
	}
	segments := make([]stockSegment, 0, len(pool))
	remaining := target
	ordered := []sceneTimelineAsset(nil)
	for index := 0; remaining > 1e-9; index++ {
		if index > 10000 {
			return nil, fmt.Errorf("stock loop exceeded safety limit")
		}
		cycle := index / len(pool)
		if index%len(pool) == 0 {
			ordered = shuffleStockPool(pool, jobID, sceneIndex, cycle)
		}
		asset := ordered[index%len(ordered)]
		duration, err := resolveSceneAssetDuration(&asset, 0, probe)
		if err != nil || duration <= 0 {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("asset duration must be positive")
		}
		if duration > remaining {
			duration = remaining
		}
		segments = append(segments, stockSegment{URL: asset.URL, Duration: duration})
		remaining -= duration
	}
	return segments, nil
}

func shuffleStockPool(pool []sceneTimelineAsset, jobID string, sceneIndex int, cycle ...int) []sceneTimelineAsset {
	ordered := append([]sceneTimelineAsset(nil), pool...)
	if len(ordered) < 2 {
		return ordered
	}
	hash := fnv.New64a()
	cycleIndex := 0
	if len(cycle) > 0 {
		cycleIndex = cycle[0]
	}
	_, _ = hash.Write([]byte(fmt.Sprintf("%s:stock:%d:%d", jobID, sceneIndex, cycleIndex)))
	rng := rand.New(rand.NewSource(int64(hash.Sum64())))
	rng.Shuffle(len(ordered), func(i, j int) { ordered[i], ordered[j] = ordered[j], ordered[i] })
	return ordered
}

func resolveSceneAssetDuration(asset *sceneTimelineAsset, fallback float64, probe audio.Probe) (float64, error) {
	if asset == nil {
		return 0, fmt.Errorf("asset is required")
	}
	if asset.DurationMS > 0 {
		return float64(asset.DurationMS) / 1000, nil
	}
	if probe != nil && strings.TrimSpace(asset.URL) != "" {
		if duration := probe.DurationSeconds(asset.URL); duration > 0 {
			return duration, nil
		}
	}
	if fallback > 0 {
		return fallback, nil
	}
	return 0, fmt.Errorf("duration_ms is required or the asset must be probeable")
}
