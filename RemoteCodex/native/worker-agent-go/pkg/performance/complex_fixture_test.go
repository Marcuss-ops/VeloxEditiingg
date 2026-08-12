package performance

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComplexCanonicalSpecIsDeterministic(t *testing.T) {
	a := ComplexCanonicalFixtureSpecV1()
	b := ComplexCanonicalFixtureSpecV1()
	require.Equal(t, a, b)
	require.Equal(t, a.SpecSHA256(), b.SpecSHA256())
	require.Equal(t, 300, a.DurationSec)
	require.Equal(t, 24, a.ClipCount)
	require.Equal(t, 14, len(a.AudioTracks))
	require.Equal(t, 24, len(a.ClipSources))
	require.Equal(t, 1920, a.Output.Width)
	require.Equal(t, 1080, a.Output.Height)

	geometries := map[[2]int]bool{}
	for _, clip := range a.ClipSources {
		geometries[[2]int{clip.Width, clip.Height}] = true
	}
	require.Len(t, geometries, 3, "complex fixture must exercise mixed source geometries")
}

func TestBuildComplexRenderPlanV1(t *testing.T) {
	spec := ComplexCanonicalFixtureSpecV1()
	manifest := ComplexFixtureManifest{
		FixtureID:  FixtureComplexCanonical5MV1,
		SpecSHA256: spec.SpecSHA256(),
	}
	for i := 0; i < spec.ClipCount; i++ {
		manifest.Clips = append(manifest.Clips, ManifestAsset{
			Name: fmt.Sprintf("clip_%03d.mp4", i+1), SHA256: strings.Repeat("a", 64),
			SizeBytes: 100 + int64(i), Frames: spec.PerClipFrames,
		})
	}
	for i := 0; i < len(spec.AudioTracks); i++ {
		manifest.AudioTracks = append(manifest.AudioTracks, ManifestAsset{
			Name: fmt.Sprintf("audio_%03d.m4a", i+1), SHA256: strings.Repeat("b", 64), SizeBytes: 200,
		})
	}
	trackDir := t.TempDir()
	for _, asset := range append(append([]ManifestAsset{}, manifest.Clips...), manifest.AudioTracks...) {
		require.NoError(t, os.WriteFile(filepath.Join(trackDir, asset.Name), []byte("fixture"), 0o644))
	}
	p, err := BuildComplexRenderPlanV1(spec, manifest, trackDir, "complex-job", filepath.Join(trackDir, "out.mp4"))
	require.NoError(t, err)
	require.Equal(t, 1, p.Version)
	require.False(t, p.CopyOnly)
	require.Len(t, p.Timeline, 24)
	require.Len(t, p.AudioTracks, 14)
	require.Equal(t, "cover", p.Timeline[0].Transform.ScaleMode)
	require.Equal(t, filepath.Join(trackDir, "clip_001.mp4"), p.Timeline[0].Source.URL)
	require.Equal(t, filepath.Join(trackDir, "audio_001.m4a"), p.AudioTracks[0].SourceURL)
}
