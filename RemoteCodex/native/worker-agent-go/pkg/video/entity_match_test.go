package video

import (
	"reflect"
	"strings"
	"testing"
)

func TestPartialFuzzyRatioNormalizedBufMatchesWrapper(t *testing.T) {
	cases := []struct {
		needle, haystack string
	}{
		{"marco", "marco rossi presenta il prodotto"},
		{"marco rossi", "marco rossi presenta il prodotto"},
		{"marco rossi", "marco"}, // needle longer than haystack
		{"", "anything"},
		{"marco", ""},
		{"", ""},
		{"rossi", "  marco rossi  presenta  "}, // trimmed window edges
	}
	for _, tc := range cases {
		prev := make([]int, len(tc.needle)+1)
		curr := make([]int, len(tc.needle)+1)
		got := partialFuzzyRatioNormalizedBuf(tc.needle, tc.haystack, prev, curr)
		want := partialFuzzyRatioNormalized(tc.needle, tc.haystack)
		if got != want {
			t.Errorf("Buf(%q, %q) = %v, want wrapper %v", tc.needle, tc.haystack, got, want)
		}
	}
}

func TestNormalizeSegmentsPreSplitsWordsOnce(t *testing.T) {
	segments := []TranscriptionSegment{
		{Text: "Marco Rossi presenta il prodotto", Start: 0, End: 4.5},
		{Text: "  UN ALTRO !!! segmento  ", Start: 4.5, End: 9},
		{Text: "", Start: 9, End: 10},
	}
	norm := normalizeSegments(segments)
	for i, seg := range norm {
		want := strings.Fields(normalizeForMatch(segments[i].Text))
		if !reflect.DeepEqual(seg.words, want) {
			t.Errorf("segments[%d].words = %v, want %v", i, seg.words, want)
		}
	}
}

func TestMatchEntityByKeywordsUsesPreSplitWords(t *testing.T) {
	segments := []TranscriptionSegment{
		{Text: "Marco Rossi presenta il nuovo prodotto", Start: 1, End: 5},
	}
	norm := normalizeSegments(segments)

	matches := matchEntityByKeywords("Marco Rossi", norm)
	if len(matches) != 1 {
		t.Fatalf("matchEntityByKeywords returned %d matches, want 1: %+v", len(matches), matches)
	}
	got := matches[0]
	if got.Method != "keyword:marco" {
		t.Errorf("method = %q, want keyword:marco", got.Method)
	}
	if got.TimestampStart != 1 || got.TimestampEnd != 5 {
		t.Errorf("timestamps = [%v, %v], want [1, 5]", got.TimestampStart, got.TimestampEnd)
	}
	if got.Text != "Marco Rossi presenta il nuovo prodotto" {
		t.Errorf("raw text = %q, want original segment text", got.Text)
	}
}
