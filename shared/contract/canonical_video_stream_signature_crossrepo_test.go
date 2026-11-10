package contract

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// readSiblingContractFile resolves a file in one of the three sibling
// repositories without assuming a fixed workspace layout. The test skips when
// the repository is checked out alone (for example a single-repo CI job); when
// the siblings are present, drift fails the build.
func readSiblingContractFile(t *testing.T, relative string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("cannot locate test source")
	}
	dir := filepath.Dir(source)
	for {
		candidate := filepath.Join(dir, filepath.FromSlash(relative))
		if data, err := os.ReadFile(candidate); err == nil {
			return string(data)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skipf("sibling checkout not found: %s", relative)
		}
		dir = parent
	}
}

// TestCanonicalVideoStreamSignatureMirrorsProducerAndTransport is the
// three-repository identity gate. RenderingGen declares the profile that must
// emit the stream, PipelineGen transports the identity, and Velox consumes it.
// A change to the canonical avcC, revision, profile id or the video-only
// packaging contract must be made in all three (or fail here) so no producer
// can certify bytes the assembler will reject.
func TestCanonicalVideoStreamSignatureMirrorsProducerAndTransport(t *testing.T) {
	pipeline := readSiblingContractFile(t, "refactored/internal/kernel/media/assembly_contract.go")
	rendering := readSiblingContractFile(t, "RenderingGen/renderinggen/internal/media/profile.go")

	hash := regexp.MustCompile(`CanonicalVideoExtradataSHA256\s*=\s*"([0-9a-f]{64})"`).FindStringSubmatch(pipeline)
	if hash == nil || hash[1] != CanonicalVideoExtradataSHA256 {
		t.Fatalf("PipelineGen canonical avcC = %v, Velox = %s", hash, CanonicalVideoExtradataSHA256)
	}
	revision := regexp.MustCompile(`CanonicalVideoProfileRevision\s*=\s*"([^"]+)"`).FindStringSubmatch(pipeline)
	if revision == nil || revision[1] != CanonicalVideoProfileRevision {
		t.Fatalf("PipelineGen encoder revision = %v, Velox = %s", revision, CanonicalVideoProfileRevision)
	}
	assemblyProfile := regexp.MustCompile(`OverlayVideoOnlyAssemblyProfileID\s*=\s*"([^"]+)"`).FindStringSubmatch(pipeline)
	if assemblyProfile == nil || assemblyProfile[1] != "VELOX_OVERLAY_VIDEO_ONLY_V3" {
		t.Fatalf("PipelineGen assembly-compatible overlay profile = %v", assemblyProfile)
	}
	if !strings.Contains(pipeline, `OverlayVideoOnlyProfileID = "VELOX_OVERLAY_VIDEO_ONLY_V3"`) {
		t.Fatal("PipelineGen's final-job default must request the V3 canonical overlay lane")
	}

	if !strings.Contains(rendering, `ProfileVeloxOverlayVideoOnlyV3 = "VELOX_OVERLAY_VIDEO_ONLY_V3"`) {
		t.Fatal("RenderingGen does not register the V3 canonical overlay profile")
	}
	if !strings.Contains(rendering, `p.CodecConfigSHA256 = "`+CanonicalVideoExtradataSHA256+`"`) {
		t.Fatal("RenderingGen V3 does not pin the canonical avcC")
	}
	if !strings.Contains(rendering, "p.VideoTimeBaseNum = 1") || !strings.Contains(rendering, "p.VideoTimeBaseDen = 90000") {
		t.Fatal("RenderingGen V3 does not pin the canonical 1/90000 video timebase")
	}
	if !regexp.MustCompile(`RequireNoAudio:\s+true`).MatchString(rendering) {
		t.Fatal("the overlay video-only profile must stay video-only (RequireNoAudio)")
	}
	if !regexp.MustCompile(`AudioStreams:\s+0`).MatchString(rendering) {
		t.Fatal("the overlay video-only profile must declare zero audio streams")
	}
}
