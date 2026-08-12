package performance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// validManifest builds a manifest that satisfies ValidateManifest for
// the canonical fixture.
func validManifest() FixtureManifest {
	spec := CanonicalFixtureSpecV1()
	m := FixtureManifest{
		FixtureID:  FixtureCopyOnlyCanonical5MV1,
		SpecSHA256: spec.SpecSHA256(),
		FinalAudio: ManifestAsset{Name: "final_audio.m4a", SHA256: strings.Repeat("a", 64), Frames: spec.TotalFrames()},
	}
	for i := 0; i < spec.ClipCount; i++ {
		m.Clips = append(m.Clips, ManifestAsset{
			Name:   fmt.Sprintf("clip_%03d.mp4", i+1),
			SHA256: strings.Repeat("b", 64),
			Frames: spec.PerClipFrames,
		})
	}
	return m
}

// TestManifestValidForCanonicalFixture pins that a manifest built from
// the canonical spec passes ValidateManifest, and that SpecMatches ties
// the manifest to the pinned fixture digest.
func TestManifestValidForCanonicalFixture(t *testing.T) {
	spec := CanonicalFixtureSpecV1()
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	m := validManifest()
	require.True(t, m.SpecMatches(f.AssetSHA256))
	require.True(t, m.SpecMatches(spec.SpecSHA256()))
	require.Empty(t, ValidateManifest(m, f, spec))
	require.Equal(t, 24, len(m.ClipNames()))
	require.Equal(t, "clip_001.mp4", m.ClipNames()[0])
	require.Equal(t, "clip_024.mp4", m.ClipNames()[23])
}

// TestManifestValidateRejectsDrift pins each consistency check: spec
// digest mismatch, wrong clip count, wrong per-clip frames, bad names,
// malformed SHA, wrong fixture.
func TestManifestValidateRejectsDrift(t *testing.T) {
	spec := CanonicalFixtureSpecV1()
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	badSpec := validManifest()
	badSpec.SpecSHA256 = "0" + badSpec.SpecSHA256[1:]
	require.NotEmpty(t, ValidateManifest(badSpec, f, spec), "spec digest mismatch must fail")

	badCount := validManifest()
	badCount.Clips = badCount.Clips[:23]
	require.NotEmpty(t, ValidateManifest(badCount, f, spec), "clip count mismatch must fail")

	badFrames := validManifest()
	badFrames.Clips[3].Frames = 374
	require.NotEmpty(t, ValidateManifest(badFrames, f, spec), "per-clip frame mismatch must fail")

	badName := validManifest()
	badName.Clips[0].Name = "clip_099.mp4"
	require.NotEmpty(t, ValidateManifest(badName, f, spec), "clip name mismatch must fail")

	badSHA := validManifest()
	badSHA.Clips[0].SHA256 = "tooshort"
	require.NotEmpty(t, ValidateManifest(badSHA, f, spec), "malformed sha256 must fail")

	badFixture := validManifest()
	badFixture.FixtureID = FixtureCopy5MHigh
	require.NotEmpty(t, ValidateManifest(badFixture, f, spec), "fixture mismatch must fail")
}

// TestManifestJSONRoundTrip pins the manifest wire format and the
// LoadFixtureManifest file path.
func TestManifestJSONRoundTrip(t *testing.T) {
	m := validManifest()
	data, err := json.Marshal(m)
	require.NoError(t, err)

	var back FixtureManifest
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, m, back)

	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, data, 0o644))
	loaded, err := LoadFixtureManifest(path)
	require.NoError(t, err)
	require.Equal(t, m, *loaded)

	_, err = LoadFixtureManifest(filepath.Join(t.TempDir(), "missing.json"))
	require.Error(t, err)
}
