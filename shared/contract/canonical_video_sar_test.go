package contract

import (
	"strings"
	"testing"
)

func TestCanonicalVideoGeometrySetsSquarePixelAspectRatio(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	filter := CanonicalVideoScaleGeometryFilter(profile)
	if !strings.HasSuffix(filter, "setsar=1") {
		t.Fatalf("canonical geometry filter must set square pixels, got %q", filter)
	}

	// SAR is a container/display geometry correction, not an encoder argument
	// or cache identity change. Keep the established bitstream identity intact.
	if CanonicalVideoExtradataSHA256 != "7201ce80a2b2b32a0c1a89501851099f267c1767d59ff799db744f72032aae5a" {
		t.Fatalf("canonical avcC identity changed unexpectedly: %s", CanonicalVideoExtradataSHA256)
	}
	revision, err := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	const wantRevision = "video-window-preparer.v1:3eeb93e952e54a8e525603b20804dae86dca0977619af1a3c3f2b2ef1b44d672"
	if revision != wantRevision {
		t.Fatalf("SAR-only filter change must preserve prepared-window revision: got %q want %q", revision, wantRevision)
	}
	if !strings.Contains(strings.Join(CanonicalVideoEncodeArgs(profile, CanonicalPreparationQualityPolicyDefault), " "), "-c:v libx264") {
		t.Fatal("canonical encoder argument projection unexpectedly changed")
	}
}
