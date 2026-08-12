package performance

// complex_fixture.go defines the second canonical benchmark track. Unlike
// COPY_ONLY_CANONICAL_5M_V1, this fixture deliberately exercises real work:
// mixed source geometries, per-segment scaling and a deterministic
// multi-track audio mix. It is a measurement fixture, not an optimization
// claim; all timing budgets remain unset until the first baseline is recorded.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"velox-worker-agent/pkg/video/plan"
)

const ComplexFixtureManifestName = "complex-manifest.json"

// ComplexClipSpec describes one source clip in the complex track.
type ComplexClipSpec struct {
	Index     int `json:"index"`
	Frames    int `json:"frames"`
	Width     int `json:"width"`
	Height    int `json:"height"`
	ColorRGB  int `json:"color_rgb"`
	Frequency int `json:"frequency_hz"`
}

// ComplexAudioTrackSpec describes one deterministic source in the mix.
type ComplexAudioTrackSpec struct {
	Index     int     `json:"index"`
	Duration  int     `json:"duration_sec"`
	Frequency int     `json:"frequency_hz"`
	Volume    float64 `json:"volume"`
	Start     float64 `json:"start_sec"`
	Role      string  `json:"role"`
	Ducking   bool    `json:"ducking"`
}

// ComplexCanonicalFixtureSpec is the immutable definition of the complex
// benchmark. The output contract is fixed at 1080p/30fps while sources use
// three deterministic geometries so scaling is guaranteed to be exercised.
type ComplexCanonicalFixtureSpec struct {
	Version       int                     `json:"version"`
	DurationSec   int                     `json:"duration_sec"`
	ClipCount     int                     `json:"clip_count"`
	PerClipFrames int                     `json:"per_clip_frames"`
	Output        CanonicalVideoSpec      `json:"output"`
	Audio         CanonicalAudioSpec      `json:"audio"`
	ClipSources   []ComplexClipSpec       `json:"clip_sources"`
	AudioTracks   []ComplexAudioTrackSpec `json:"audio_tracks"`
	CacheMode     CacheMode               `json:"cache_mode"`
}

// ComplexCanonicalFixtureSpecV1 returns the fixed 5-minute complex workload:
// 24 video clips and 14 audio tracks. The clip/audio generation inputs are
// deterministic and the final output contract is always 1920x1080 at 30fps.
func ComplexCanonicalFixtureSpecV1() ComplexCanonicalFixtureSpec {
	const (
		clipCount     = 24
		perClipFrames = 375
	)
	geometries := [][2]int{{1280, 720}, {1920, 1080}, {1080, 1920}}
	clips := make([]ComplexClipSpec, 0, clipCount)
	for i := 0; i < clipCount; i++ {
		g := geometries[i%len(geometries)]
		clips = append(clips, ComplexClipSpec{
			Index: i + 1, Frames: perClipFrames, Width: g[0], Height: g[1],
			ColorRGB:  (i*17)%256<<16 | (i*29)%256<<8 | (i*43)%256,
			Frequency: 330 + i*17,
		})
	}
	roles := []string{"voiceover", "music", "sfx"}
	audio := make([]ComplexAudioTrackSpec, 0, 14)
	for i := 0; i < 14; i++ {
		role := roles[i%len(roles)]
		audio = append(audio, ComplexAudioTrackSpec{
			Index: i + 1, Duration: 300, Frequency: 440 + i*23,
			Volume: 0.35 + float64(i%4)*0.1, Start: float64(i%3) * 0.75,
			Role: role, Ducking: role == "music",
		})
	}
	return ComplexCanonicalFixtureSpec{
		Version: 1, DurationSec: 300, ClipCount: clipCount,
		PerClipFrames: perClipFrames,
		Output: CanonicalVideoSpec{Codec: "h264", Width: 1920, Height: 1080,
			FPS: 30, PixelFormat: "yuv420p", Timebase: "1/15360"},
		Audio:       CanonicalAudioSpec{Codec: "aac", SampleRate: 48000, Channels: 2},
		ClipSources: clips, AudioTracks: audio, CacheMode: CacheModeWarm,
	}
}

// SpecSHA256 is the immutable identity of the complex workload definition.
func (s ComplexCanonicalFixtureSpec) SpecSHA256() string {
	data, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s ComplexCanonicalFixtureSpec) ClipDurationSec() float64 {
	return float64(s.PerClipFrames) / float64(s.Output.FPS)
}

func (s ComplexCanonicalFixtureSpec) TotalFrames() int {
	return s.ClipCount * s.PerClipFrames
}

// ComplexFixtureManifest is intentionally separate from FixtureManifest:
// the copy-only track has one final audio asset, while this benchmark owns
// fourteen independent audio sources and must not overload that contract.
type ComplexFixtureManifest struct {
	FixtureID     BenchmarkFixtureID `json:"fixture_id"`
	SpecSHA256    string             `json:"spec_sha256"`
	FFmpegVersion string             `json:"ffmpeg_version,omitempty"`
	Clips         []ManifestAsset    `json:"clips"`
	AudioTracks   []ManifestAsset    `json:"audio_tracks"`
}

func LoadComplexFixtureManifest(path string) (*ComplexFixtureManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load complex fixture manifest: %w", err)
	}
	var m ComplexFixtureManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse complex fixture manifest %s: %w", path, err)
	}
	return &m, nil
}

func ValidateComplexManifest(m ComplexFixtureManifest, fixture BenchmarkFixture, spec ComplexCanonicalFixtureSpec) []string {
	var problems []string
	if m.FixtureID != fixture.ID {
		problems = append(problems, fmt.Sprintf("manifest fixture %s != %s", m.FixtureID, fixture.ID))
	}
	if m.SpecSHA256 != spec.SpecSHA256() {
		problems = append(problems, "manifest spec_sha256 does not match the complex canonical spec")
	}
	if len(m.Clips) != spec.ClipCount {
		problems = append(problems, fmt.Sprintf("manifest has %d clips, spec requires %d", len(m.Clips), spec.ClipCount))
	}
	for i, clip := range m.Clips {
		want := fmt.Sprintf("clip_%03d.mp4", i+1)
		if clip.Name != want {
			problems = append(problems, fmt.Sprintf("clip[%d] name %q != %q", i, clip.Name, want))
		}
		if clip.Frames != spec.PerClipFrames {
			problems = append(problems, fmt.Sprintf("clip[%d] frames %d != %d", i, clip.Frames, spec.PerClipFrames))
		}
		if len(clip.SHA256) != 64 {
			problems = append(problems, fmt.Sprintf("clip[%d] sha256 is not a digest", i))
		}
	}
	if len(m.AudioTracks) != len(spec.AudioTracks) {
		problems = append(problems, fmt.Sprintf("manifest has %d audio tracks, spec requires %d", len(m.AudioTracks), len(spec.AudioTracks)))
	}
	for i, audio := range m.AudioTracks {
		want := fmt.Sprintf("audio_%03d.m4a", i+1)
		if audio.Name != want {
			problems = append(problems, fmt.Sprintf("audio[%d] name %q != %q", i, audio.Name, want))
		}
		if len(audio.SHA256) != 64 {
			problems = append(problems, fmt.Sprintf("audio[%d] sha256 is not a digest", i))
		}
	}
	return problems
}

// BuildComplexRenderPlanV1 creates the V1 plan consumed by the existing
// complex engine path. Local paths are verified inputs for this benchmark;
// the production asset resolver remains the owner of Drive download/cache
// resolution before a worker submits an equivalent plan.
func BuildComplexRenderPlanV1(spec ComplexCanonicalFixtureSpec, manifest ComplexFixtureManifest, trackDir, jobID, outputPath string) (*plan.RenderPlan, error) {
	if len(manifest.Clips) != spec.ClipCount || len(manifest.AudioTracks) != len(spec.AudioTracks) {
		return nil, fmt.Errorf("complex plan: manifest cardinality does not match spec")
	}
	if jobID == "" || outputPath == "" {
		return nil, fmt.Errorf("complex plan: job_id and output_path are required")
	}
	p := &plan.RenderPlan{
		Version: 1, JobID: jobID, Canvas: plan.CanvasSpec{Width: spec.Output.Width, Height: spec.Output.Height, Fps: spec.Output.FPS},
		CopyOnly: false, OutputPath: outputPath,
		Timeline:    make([]plan.TimelineItem, 0, len(manifest.Clips)),
		AudioTracks: make([]plan.AudioTrack, 0, len(manifest.AudioTracks)),
	}
	for i, clip := range manifest.Clips {
		path := filepath.Join(trackDir, clip.Name)
		slowZoom := false
		p.Timeline = append(p.Timeline, plan.TimelineItem{
			Source:  plan.MediaSource{Type: "video", URL: path, CacheKey: path},
			SceneID: fmt.Sprintf("complex_%03d", i+1), DurationSeconds: spec.ClipDurationSec(),
			IncludeAudio: false, Transform: &plan.TransformSpec{
				ScaleMode: "cover", SlowZoom: &slowZoom,
			},
		})
	}
	for i, audio := range manifest.AudioTracks {
		cfg := spec.AudioTracks[i]
		p.AudioTracks = append(p.AudioTracks, plan.AudioTrack{
			SourceURL: filepath.Join(trackDir, audio.Name), Volume: cfg.Volume,
			StartTimeOffset: cfg.Start, DurationSeconds: float64(cfg.Duration),
			Role: cfg.Role, DuckingEnabled: cfg.Ducking,
		})
	}
	return p, nil
}
