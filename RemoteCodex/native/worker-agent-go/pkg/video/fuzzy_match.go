package video

import (
	"strings"
	"unicode"
)

// levenshtein computes the Levenshtein edit distance between two strings.
func levenshtein(a, b string) int {
	return levenshteinBuf(a, b, make([]int, len(b)+1), make([]int, len(b)+1))
}

// levenshteinBuf is the buffer-reusing core of levenshtein. prev and curr
// must each have length >= len(b)+1; the caller owns them so a hot loop
// (partialFuzzyRatio's sliding window) can allocate them once and reuse them
// across every window instead of paying two slice allocations per distance.
func levenshteinBuf(a, b string, prev, curr []int) int {
	la := len(a)
	lb := len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	// Use two rows instead of full matrix for memory efficiency. Reusing
	// the buffers across calls is safe: prev is re-seeded each call and curr
	// is fully overwritten left-to-right before each read.
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

// fuzzyRatioNormalized is the normalized-string variant of the fuzzy ratio.
// Hoisting normalization to the caller lets a hot loop skip a second
// normalization pass over strings normalized once upstream.
func fuzzyRatioNormalized(a, b string) float64 {
	return ratioFromDistance(a, b, levenshtein(a, b))
}

// ratioFromDistance converts a Levenshtein distance into the 0-100 similarity
// ratio fuzzyRatio reports. It is the single home of the empty-string and
// maxLen normalization rules so the buffer-reusing hot path and the
// convenience wrapper cannot drift.
func ratioFromDistance(a, b string, dist int) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 100.0
	}
	if len(a) == 0 || len(b) == 0 {
		return 0.0
	}
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	return (1.0 - float64(dist)/float64(maxLen)) * 100.0
}

// partialFuzzyRatioNormalized is the convenience wrapper of the partial ratio
// for two already-normalized strings. It allocates the two Levenshtein scratch
// rows and delegates to partialFuzzyRatioNormalizedBuf. Hot loops that match a
// fixed-length needle against many haystacks (matchEntityToSegments) call the
// Buf variant directly so the rows are allocated once per needle instead of
// once per segment.
func partialFuzzyRatioNormalized(needle, haystack string) float64 {
	return partialFuzzyRatioNormalizedBuf(needle, haystack,
		make([]int, len(needle)+1), make([]int, len(needle)+1))
}

// partialFuzzyRatioNormalizedBuf is the buffer-reusing core of the partial
// ratio. prev and curr must each have length >= len(needle)+1 and are owned by
// the caller, which reuses them across every haystack of the same needle.
// Hoisting normalization to the caller lets matchEntityToSegments normalize an
// entity ONCE and reuse it across every transcription segment. It limits the
// sliding window to exact-length needle windows and reuses the scratch rows
// across every window AND every segment.
func partialFuzzyRatioNormalizedBuf(needle, haystack string, prev, curr []int) float64 {
	if len(needle) == 0 {
		return 100.0
	}
	if len(haystack) == 0 {
		return 0.0
	}
	if len(needle) > len(haystack) {
		return fuzzyRatioNormalized(needle, haystack)
	}

	bestScore := 0.0
	for i := 0; i <= len(haystack)-len(needle); i++ {
		// The window is a slice of the already-normalized haystack
		// (lowercase, alphanumeric + whitespace only), so the only remaining
		// normalization is the leading/trailing whitespace trim.
		// strings.TrimSpace returns a sub-slice and does not copy bytes.
		substr := strings.TrimSpace(haystack[i : i+len(needle)])
		dist := levenshteinBuf(needle, substr, prev, curr)
		score := ratioFromDistance(needle, substr, dist)
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

// keywordMatchFields matches pre-normalized needle words against pre-split
// haystack words. matchEntityByKeywords precomputes the needle words ONCE
// (the needle is invariant across segments) and splits each haystack once per
// segment, so the per-segment loop no longer re-normalizes nor re-splits the
// needle.
func keywordMatchFields(normNeedle string, needleWords, hayWords []string) (bool, string, float64) {
	for _, w := range needleWords {
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
