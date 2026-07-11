// Package timeline provides duration allocation and timeline building services.
package timeline

import "math"

// AllocateDurations distributes totalDuration among count items.
// Explicit durations are preserved; remaining duration is distributed
// equally among items without explicit duration.
//
// Priority per item:
//  1. explicit duration (if > 0)
//  2. distributed share of remaining audio duration
//  3. fallback
func AllocateDurations(explicitDurations []float64, totalDuration float64, count int, fallback float64) []float64 {
	if count <= 0 {
		return nil
	}

	result := make([]float64, count)
	explicitTotal := 0.0
	unsetCount := 0

	for i := 0; i < count; i++ {
		if i < len(explicitDurations) && explicitDurations[i] > 0 {
			result[i] = explicitDurations[i]
			explicitTotal += explicitDurations[i]
		} else {
			unsetCount++
		}
	}

	// Distribute remaining duration among items without explicit duration
	remaining := totalDuration - explicitTotal
	if remaining < 0 {
		remaining = 0
	}
	perItem := 0.0
	if unsetCount > 0 && remaining > 0 {
		perItem = remaining / float64(unsetCount)
	}

	for i := 0; i < count; i++ {
		if result[i] <= 0 {
			if perItem > 0 {
				result[i] = perItem
			} else {
				result[i] = fallback
			}
		}
	}

	return result
}

// AlignToFrame rounds a duration to the nearest frame boundary.
func AlignToFrame(duration float64, fps int) float64 {
	if fps <= 0 {
		return duration
	}
	frames := math.Round(duration * float64(fps))
	return frames / float64(fps)
}
