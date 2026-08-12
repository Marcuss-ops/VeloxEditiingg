package fleet

import (
	"strings"
	"testing"
)

func TestNormalizeDigestCanonicalizesWorkerBareHex(t *testing.T) {
	hex := strings.Repeat("a", 64)
	want := "sha256:" + hex
	for _, input := range []string{
		hex,
		"sha256:" + hex,
		"ghcr.io/marcuss-ops/velox-worker@sha256:" + hex,
		"  " + hex + "  ",
	} {
		if got := normalizeDigest(input); got != want {
			t.Errorf("normalizeDigest(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeDigestLeavesMalformedValuesUntouched(t *testing.T) {
	for _, input := range []string{"", "sha256:short", strings.Repeat("g", 64)} {
		if got := normalizeDigest(input); got != input {
			t.Errorf("normalizeDigest(%q) = %q, want unchanged", input, got)
		}
	}
}
