package video

import (
	"fmt"
	"testing"
)

// keyword_match_bench_test.go measures the O(needleWords × hayWords) double
// loop in keywordMatchFields against a hypothetical word-set variant before
// deciding whether to add a `wordSet map[string]struct{}` to normalizedSegment.
// The set would be built once per segment in normalizeSegments and reused
// across every entity, so the benchmark models the full corpus: build the
// per-segment sets (only in the wordSet variant) and then match every entity
// against every segment.

// wordSetOf materializes the per-segment lookup set the proposed change would
// cache in normalizedSegment. Benchmark-only until the decision is made.
func wordSetOf(words []string) map[string]struct{} {
	set := make(map[string]struct{}, len(words))
	for _, w := range words {
		set[w] = struct{}{}
	}
	return set
}

// keywordMatchFieldsSet is the word-set equivalent of keywordMatchFields: it
// looks each needle word up in the pre-built set instead of scanning hayWords.
// Benchmark-only until the decision is made.
func keywordMatchFieldsSet(normNeedle string, needleWords []string, set map[string]struct{}) (int, float64) {
	for idx, w := range needleWords {
		if len(w) < 3 {
			continue
		}
		if _, ok := set[w]; ok {
			return idx, float64(len(w)) / float64(len(normNeedle)) * 100.0
		}
	}
	return -1, 0.0
}

// keywordBenchCase is one (entities × words/entity × words/segment × segments)
// cardinality point. Needle words and haystack words never overlap so both
// variants pay the full scan / full lookup cost (no early return).
type keywordBenchCase struct {
	name     string
	entities int
	needleW  int
	hayW     int
	segments int
}

var keywordBenchCases = []keywordBenchCase{
	{"e5_w3_h25_s100", 5, 3, 25, 100},
	{"e20_w3_h25_s1000", 20, 3, 25, 1000},
	{"e100_w1_h10_s1000", 100, 1, 10, 1000},
	{"e100_w6_h50_s1000", 100, 6, 50, 1000},
	{"e1_w6_h50_s1000", 1, 6, 50, 1000},
}

func makeKeywordWords(n, seed int) []string {
	words := make([]string, n)
	for i := 0; i < n; i++ {
		words[i] = fmt.Sprintf("word%d_%d", seed, i)
	}
	return words
}

func BenchmarkKeywordMatchFields(b *testing.B) {
	const normNeedle = "needle"
	for _, c := range keywordBenchCases {
		entities := make([][]string, c.entities)
		for i := range entities {
			entities[i] = makeKeywordWords(c.needleW, i)
		}
		segments := make([][]string, c.segments)
		for i := range segments {
			segments[i] = makeKeywordWords(c.hayW, 1000+i)
		}

		b.Run("doubleLoop/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for _, entity := range entities {
					for _, hay := range segments {
						keywordMatchFields(normNeedle, entity, hay)
					}
				}
			}
		})

		b.Run("wordSet/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				sets := make([]map[string]struct{}, len(segments))
				for i, hay := range segments {
					sets[i] = wordSetOf(hay)
				}
				for _, entity := range entities {
					for i := range segments {
						keywordMatchFieldsSet(normNeedle, entity, sets[i])
					}
				}
			}
		})
	}
}
