package video

import (
	"fmt"
	"testing"
)

// buildBenchSegments builds a synthetic transcription corpus of the size and
// word density the entity matcher sees in production (many short segments,
// repeated entity names). It is shared by the benchmarks below so the
// allocation results are comparable.
func buildBenchSegments(segmentCount, entityCount int) ([]TranscriptionSegment, []string) {
	segments := make([]TranscriptionSegment, segmentCount)
	for i := range segments {
		segments[i] = TranscriptionSegment{
			Text:  fmt.Sprintf("Marco Rossi presenta il nuovo prodotto al mercato %d", i),
			Start: float64(i),
			End:   float64(i) + 4.5,
		}
	}
	entities := make([]string, entityCount)
	for i := range entities {
		entities[i] = fmt.Sprintf("Marco Rossi %d", i)
	}
	return segments, entities
}

// BenchmarkMatchEntityToSegmentsAllocs guards the hot path: matching every
// entity against every segment must not re-normalize segment text per entity
// nor re-normalize the needle per segment. The single normalized-segment
// pass plus preallocated result slice keeps per-op allocations minimal.
func BenchmarkMatchEntityToSegmentsAllocs(b *testing.B) {
	segments, entities := buildBenchSegments(200, 50)
	norm := normalizeSegments(segments)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, e := range entities {
			_ = matchEntityToSegments(e, norm, 80.0, "partial_fuzzy")
		}
	}
}

// BenchmarkMatchEntityByKeywordsAllocs guards the keyword fallback path with
// the same invariants: needle normalized/split once, segment text normalized
// once up front, result slice preallocated.
func BenchmarkMatchEntityByKeywordsAllocs(b *testing.B) {
	segments, entities := buildBenchSegments(200, 50)
	norm := normalizeSegments(segments)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, e := range entities {
			_ = matchEntityByKeywords(e, norm)
		}
	}
}
