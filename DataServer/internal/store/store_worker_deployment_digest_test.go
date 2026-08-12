package store

import (
	"strings"
	"testing"
)

func TestDigestRefsEqualAcceptsWorkerBareHexAgainstPinnedRef(t *testing.T) {
	hex := strings.Repeat("b", 64)
	refs := []string{
		hex,
		"sha256:" + hex,
		"ghcr.io/marcuss-ops/velox-worker@sha256:" + hex,
	}
	for _, left := range refs {
		for _, right := range refs {
			if !DigestRefsEqual(left, right) {
				t.Errorf("DigestRefsEqual(%q, %q) = false, want true", left, right)
			}
		}
	}
}

func TestDigestRefsEqualRejectsEmptyOrDifferentDigest(t *testing.T) {
	hex := strings.Repeat("b", 64)
	if DigestRefsEqual("", "sha256:"+hex) {
		t.Fatal("empty digest must not compare equal")
	}
	if DigestRefsEqual(hex, "sha256:"+strings.Repeat("c", 64)) {
		t.Fatal("different digests must not compare equal")
	}
}
