package performance

// fixture_manifest.go owns the machine-readable manifest that records
// the BYTE-level identity of a generated canonical asset set: one
// SHA-256 per clip plus the final audio track. The manifest is produced
// by the generator (cmd/velox-fixture-gen) on the benchmark host and is
// consumed by the harness to register warm-cache assets and to verify
// the track actually built from the pinned spec.
//
// Determinism split (see canonical_fixture.go): the SPEC digest is
// cross-host deterministic and pinned in the registry; the per-asset
// SHAs depend on the exact encoder build, so they are recorded here
// instead of in the registry. SpecSHA256 is the link between the two:
// a manifest whose spec digest does not match the fixture's AssetSHA256
// was generated from a different track definition and must be rejected.

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// ManifestAsset is one generated file: name, content SHA-256 and the
// exact CFR frame count that lets a harness verify the file without a
// second ffprobe pass.
type ManifestAsset struct {
	Name        string  `json:"name"`
	SHA256      string  `json:"sha256"`
	SizeBytes   int64   `json:"size_bytes"`
	DurationSec float64 `json:"duration_sec"`
	Frames      int     `json:"frames"`
}

// FixtureManifest records one full generation of the track.
type FixtureManifest struct {
	FixtureID     BenchmarkFixtureID `json:"fixture_id"`
	SpecSHA256    string             `json:"spec_sha256"`
	FFmpegVersion string             `json:"ffmpeg_version,omitempty"` // informational
	GeneratedAt   time.Time          `json:"generated_at,omitempty"`   // informational
	Clips         []ManifestAsset    `json:"clips"`
	FinalAudio    ManifestAsset      `json:"final_audio"`
}

// SpecMatches reports whether the manifest was generated from the given
// spec digest — the fundamental consistency check between the track on
// disk and the pinned fixture.
func (m FixtureManifest) SpecMatches(specSHA256 string) bool {
	return m.SpecSHA256 == specSHA256
}

// ClipNames returns the ordered clip file names (clip_001.mp4 …).
func (m FixtureManifest) ClipNames() []string {
	names := make([]string, 0, len(m.Clips))
	for _, c := range m.Clips {
		names = append(names, c.Name)
	}
	return names
}

// Validate checks the manifest against the canonical fixture and spec:
// fixture identity, spec digest match, clip count and per-clip frame
// counts. Returns a list of problems (nil when consistent).
func ValidateManifest(m FixtureManifest, fixture BenchmarkFixture, spec CanonicalFixtureSpec) []string {
	var problems []string
	if m.FixtureID != fixture.ID {
		problems = append(problems, fmt.Sprintf("manifest fixture %s != %s", m.FixtureID, fixture.ID))
	}
	if !m.SpecMatches(spec.SpecSHA256()) {
		problems = append(problems, "manifest spec_sha256 does not match the canonical spec digest — regenerate the track")
	}
	if len(m.Clips) != spec.ClipCount {
		problems = append(problems, fmt.Sprintf("manifest has %d clips, spec requires %d", len(m.Clips), spec.ClipCount))
	}
	for i, c := range m.Clips {
		wantName := fmt.Sprintf("clip_%03d.mp4", i+1)
		if c.Name != wantName {
			problems = append(problems, fmt.Sprintf("clip[%d] name %q != %q", i, c.Name, wantName))
		}
		if c.Frames != spec.PerClipFrames {
			problems = append(problems, fmt.Sprintf("clip[%d] frames %d != spec %d", i, c.Frames, spec.PerClipFrames))
		}
		if len(c.SHA256) != 64 {
			problems = append(problems, fmt.Sprintf("clip[%d] sha256 %q is not a hex digest", i, c.SHA256))
		}
	}
	// FinalAudio.Frames is an informational VIDEO-frame-equivalent count
	// (clipCount × perClipFrames) — NOT an AAC frame count (AAC frames
	// are 1024 samples). Zero means "unknown" and is accepted.
	if m.FinalAudio.Frames != 0 && m.FinalAudio.Frames != spec.TotalFrames() {
		problems = append(problems, fmt.Sprintf("final audio frames %d != spec total %d", m.FinalAudio.Frames, spec.TotalFrames()))
	}
	if len(m.FinalAudio.SHA256) != 64 {
		problems = append(problems, fmt.Sprintf("final audio sha256 %q is not a hex digest", m.FinalAudio.SHA256))
	}
	return problems
}

// LoadFixtureManifest reads and parses a manifest JSON file.
func LoadFixtureManifest(path string) (*FixtureManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load fixture manifest: %w", err)
	}
	var m FixtureManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse fixture manifest %s: %w", path, err)
	}
	return &m, nil
}
