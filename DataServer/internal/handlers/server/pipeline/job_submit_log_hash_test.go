package pipeline

import (
	"testing"
)

// TestLogHashShort locks the truncation invariant promised by the
// logHashShort helper (in logging.go). Without this, a future PR that
// flips to [:16] for "more entropy" or breaks on an upstream crypto
// library update goes unnoticed — the operator log line would silently
// change format.
//
// Three assertions:
//
//	(1) Length == 12 hex chars: matches the documented "48 bits of
//	    entropy — ample to distinguish concurrent jobs" choice.
//
//	(2) Same input yields the same hash on every call. This locks the
//	    SHA-256 determinism property that an operator relies on when
//	    searching Loki / journald for a specific job.
//
//	(3) Distinct inputs yield distinct hashes. With SHA-256 truncated
//	    to 48 bits, collision is theoretically possible but the test
//	    catches accidental no-op regressions (e.g., helper returning
//	    a constant or zero string) immediately.
func TestLogHashShort(t *testing.T) {
	t.Parallel()

	// (1) Length == 12 hex chars.
	got := logHashShort("video-001")
	if len(got) != 12 {
		t.Errorf("hash length = %d, want 12", len(got))
	}
	for _, c := range got {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("hash char %q is not lowercase hex", c)
			break
		}
	}

	// (2) Same input → same hash.
	if a, b := logHashShort("key-X"), logHashShort("key-X"); a != b {
		t.Errorf("hash not deterministic: %q vs %q", a, b)
	}

	// (3) Distinct inputs → distinct hashes.
	if a, b := logHashShort("video-001"), logHashShort("video-002"); a == b {
		t.Errorf("distinct inputs produced same hash: %q", a)
	}

	// (4) Empty-input contract. The helper's docstring promises
	// "Empty input is permitted and produces a stable hash value,
	// NOT a panic." A regression that returned an empty string for
	// empty input (or one that called a nil-receiver method and
	// panicked) would silently corrupt correlation grep. Lock both:
	// non-empty output AND determinism across two calls.
	if got := logHashShort(""); len(got) != 12 {
		t.Errorf("empty-input hash length = %d, want 12", len(got))
	}
	if a, b := logHashShort(""), logHashShort(""); a != b {
		t.Errorf("empty-input hash not deterministic: %q vs %q", a, b)
	}
}
