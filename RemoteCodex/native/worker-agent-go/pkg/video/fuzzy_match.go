package video

import (
	"strings"
	"unicode"
)

// levenshtein computes the Levenshtein edit distance between two strings.
func levenshtein(a, b string) int {
	la := len(a)
	lb := len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	// Use two rows instead of full matrix for memory efficiency
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min3(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

// min3 returns the minimum of three integers.
func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// fuzzyRatio computes a similarity ratio (0-100) between two strings.
func fuzzyRatio(a, b string) float64 {
	a = normalizeForMatch(a)
	b = normalizeForMatch(b)
	if len(a) == 0 && len(b) == 0 {
		return 100.0
	}
	if len(a) == 0 || len(b) == 0 {
		return 0.0
	}
	dist := levenshtein(a, b)
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	return (1.0 - float64(dist)/float64(maxLen)) * 100.0
}

// partialFuzzyRatio finds the best partial match of needle in haystack.
// Optimized: limits sliding window to exact-length needle windows only.
func partialFuzzyRatio(needle, haystack string) float64 {
	needle = normalizeForMatch(needle)
	haystack = normalizeForMatch(haystack)
	if len(needle) == 0 {
		return 100.0
	}
	if len(haystack) == 0 {
		return 0.0
	}
	if len(needle) > len(haystack) {
		return fuzzyRatio(needle, haystack)
	}

	bestScore := 0.0
	for i := 0; i <= len(haystack)-len(needle); i++ {
		substr := haystack[i : i+len(needle)]
		score := fuzzyRatio(needle, substr)
		if score > bestScore {
			bestScore = score
		}
	}
	return bestScore
}

// normalizeForMatch normalizes a string for fuzzy matching.
func normalizeForMatch(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// keywordMatch checks if any WORD (with boundary) from needle appears in haystack.
// Returns the best matching word and its coverage ratio (0-100).
// Word boundary: the word must be surrounded by non-letter/non-digit chars or string edges.
func keywordMatch(needle, haystack string) (bool, string, float64) {
	normNeedle := normalizeForMatch(needle)
	normHaystack := normalizeForMatch(haystack)
	words := strings.Fields(normNeedle)
	hayWords := strings.Fields(normHaystack)

	for _, w := range words {
		if len(w) < 3 {
			continue
		}
		// Check word boundary: look for exact word in tokenized haystack
		for _, hw := range hayWords {
			if w == hw {
				// Exact word match: score based on word length relative to needle
				coverage := float64(len(w)) / float64(len(normNeedle)) * 100.0
				return true, w, coverage
			}
		}
	}
	return false, "", 0.0
}
