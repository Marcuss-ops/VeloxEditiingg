package prefetch

import (
	"sort"
	"strings"

	"velox-shared/futureasset"
)

// assetPriorityScore is the single ordering authority for FutureAssetPlan
// assets. Distance remains the dominant job-level signal; within one future
// job we pull the critical path forward: primary audio/narration first,
// explicit opening/scene-zero assets next, then large long-pole objects.
// Scores intentionally remain below PriorityForeground so an attempt-time
// cache miss can always promote a shared transfer ahead of speculative work.
func assetPriorityScore(job futureasset.Job, asset futureasset.AssetManifest) int {
	score := priorityForDistance(job.Distance)
	role := strings.ToLower(strings.TrimSpace(asset.Role))

	switch {
	case containsAny(role, "voiceover", "narration", "primary_audio", "main_audio", "final_audio", "audio_master"):
		score += 260
	case containsAny(role, "audio", "music", "sound"):
		score += 180
	}
	if containsAny(role, "first_scene", "scene_0", "scene-0", "opening", "intro", "lead") {
		score += 150
	}
	if containsAny(role, "required", "mandatory", "primary") {
		score += 80
	}

	// Large files are the long poles. The bounded boost only breaks ties
	// inside a distance/criticality class; it never lets an N+3 decorative
	// clip outrank an N+1 primary audio asset.
	switch {
	case asset.SizeBytes >= 512<<20:
		score += 70
	case asset.SizeBytes >= 128<<20:
		score += 50
	case asset.SizeBytes >= 32<<20:
		score += 30
	case asset.SizeBytes >= 8<<20:
		score += 15
	}
	if score >= PriorityForeground {
		return PriorityForeground - 1
	}
	return score
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

// orderedAssets returns a deterministic copy ordered by the same priority
// score supplied to the downloader. Keeping queue order and transfer priority
// under one resolver prevents drift between "which asset is enqueued first"
// and "which queued transfer gets dispatched first".
func orderedAssets(job futureasset.Job) []futureasset.AssetManifest {
	assets := append([]futureasset.AssetManifest(nil), job.Assets...)
	sort.SliceStable(assets, func(i, j int) bool {
		pi := assetPriorityScore(job, assets[i])
		pj := assetPriorityScore(job, assets[j])
		if pi != pj {
			return pi > pj
		}
		if assets[i].SizeBytes != assets[j].SizeBytes {
			return assets[i].SizeBytes > assets[j].SizeBytes
		}
		return assets[i].AssetKey < assets[j].AssetKey
	})
	return assets
}
