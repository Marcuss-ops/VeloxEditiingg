package video

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// naiveDeduplicateMatches is the previous O(n^2) reference implementation, kept
// only to pin the sorted-window rewrite to identical behavior.
func naiveDeduplicateMatches(matches []MatchResult) []MatchResult {
	if len(matches) <= 1 {
		return matches
	}
	sorted := make([]MatchResult, len(matches))
	copy(sorted, matches)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Score > sorted[j].Score
	})
	const dedupWindow = 5.0
	result := make([]MatchResult, 0, len(sorted))
	for _, m := range sorted {
		overlaps := false
		for _, kept := range result {
			if m.TimestampStart < kept.TimestampEnd+dedupWindow && m.TimestampEnd > kept.TimestampStart-dedupWindow {
				overlaps = true
				break
			}
		}
		if !overlaps {
			result = append(result, m)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].TimestampStart < result[j].TimestampStart
	})
	return result
}

func TestDeduplicateMatches(t *testing.T) {
	cases := []struct {
		name string
		in   []MatchResult
		want []MatchResult
	}{
		{
			name: "overlapping window keeps highest score",
			in: []MatchResult{
				{TimestampStart: 0, TimestampEnd: 3, Score: 80},
				{TimestampStart: 2, TimestampEnd: 5, Score: 90},
				{TimestampStart: 4, TimestampEnd: 7, Score: 70},
				{TimestampStart: 20, TimestampEnd: 22, Score: 60},
			},
			want: []MatchResult{
				{TimestampStart: 2, TimestampEnd: 5, Score: 90},
				{TimestampStart: 20, TimestampEnd: 22, Score: 60},
			},
		},
		{
			name: "exactly 5s gap does not overlap",
			in: []MatchResult{
				{TimestampStart: 0, TimestampEnd: 1, Score: 50},
				{TimestampStart: 6, TimestampEnd: 7, Score: 40},
			},
			want: []MatchResult{
				{TimestampStart: 0, TimestampEnd: 1, Score: 50},
				{TimestampStart: 6, TimestampEnd: 7, Score: 40},
			},
		},
		{
			name: "sub-5s gap overlaps",
			in: []MatchResult{
				{TimestampStart: 0, TimestampEnd: 1, Score: 50},
				{TimestampStart: 5, TimestampEnd: 6, Score: 40},
			},
			want: []MatchResult{
				{TimestampStart: 0, TimestampEnd: 1, Score: 50},
			},
		},
		{
			name: "single match passes through",
			in:   []MatchResult{{TimestampStart: 1, TimestampEnd: 2, Score: 10}},
			want: []MatchResult{{TimestampStart: 1, TimestampEnd: 2, Score: 10}},
		},
		{
			name: "empty passes through",
			in:   nil,
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deduplicateMatches(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("deduplicateMatches() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDeduplicateMatchesParityWithNaive(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 500; iter++ {
		n := 1 + rng.Intn(30)
		in := make([]MatchResult, n)
		for i := range in {
			start := float64(rng.Intn(50))
			in[i] = MatchResult{
				TimestampStart: start,
				TimestampEnd:   start + float64(rng.Intn(10)+1),
				Score:          float64(rng.Intn(100)),
				Method:         "m",
			}
		}
		got := deduplicateMatches(in)
		want := naiveDeduplicateMatches(in)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iter %d: deduplicateMatches() = %+v, want %+v", iter, got, want)
		}
	}
}
