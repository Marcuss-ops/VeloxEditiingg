package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// CompiledAudioPlan is the deterministic hand-off from the producer's audio
// compiler to the audio renderer. It carries the exact CanonicalTimeline
// identity so a generated final_audio asset can be bound to the editorial
// decision that produced it.
type CompiledAudioPlan struct {
	TimelineRevision int64                `json:"timeline_revision"`
	TimelineSHA256   string               `json:"timeline_sha256"`
	DurationUS       int64                `json:"duration_us"`
	Tracks           []CompiledAudioTrack `json:"tracks"`
}

// CompiledAudioTrack is a lossless integer-microsecond projection of one
// CanonicalTimeline audio track. No float seconds or inferred end timestamp
// are introduced at this boundary.
type CompiledAudioTrack struct {
	TrackID string `json:"track_id"`
	AssetID string `json:"asset_id"`
	Role    string `json:"role"`

	TimelineStartUS    int64   `json:"timeline_start_us"`
	TimelineDurationUS int64   `json:"timeline_duration_us"`
	SourceInUS         int64   `json:"source_in_us"`
	SourceDurationUS   int64   `json:"source_duration_us"`
	Gain               float64 `json:"gain"`
}

// AudioCompiler compiles the shared CanonicalTimeline into the exact audio
// renderer input. It is deliberately stateless: all editorial decisions and
// timestamps come from the supplied timeline.
type AudioCompiler struct{}

// NewAudioCompiler returns the stateless canonical audio compiler.
func NewAudioCompiler() AudioCompiler {
	return AudioCompiler{}
}

// Compile maps voiceovers, music, and SFX in their declared order into one
// deterministic track list. It does not probe media, round timestamps, or
// choose new placements.
func (AudioCompiler) Compile(timeline *CanonicalTimeline) (*CompiledAudioPlan, error) {
	if timeline == nil {
		return nil, fmt.Errorf("audio compiler: nil canonical timeline")
	}
	if err := timeline.Validate(); err != nil {
		return nil, err
	}
	timelineSHA, err := timeline.TimelineSHA256()
	if err != nil {
		return nil, err
	}

	tracks := make([]CompiledAudioTrack, 0,
		len(timeline.Voiceovers)+len(timeline.Music)+len(timeline.SFX))
	appendTracks := func(role string, source []TimelineAudioTrack) {
		for _, track := range source {
			tracks = append(tracks, CompiledAudioTrack{
				TrackID:            track.TrackID,
				AssetID:            track.AssetID,
				Role:               role,
				TimelineStartUS:    track.TimelineStartUS,
				TimelineDurationUS: track.TimelineDurationUS,
				SourceInUS:         track.SourceInUS,
				SourceDurationUS:   track.SourceDurationUS,
				Gain:               track.Gain,
			})
		}
	}
	appendTracks("voiceover", timeline.Voiceovers)
	appendTracks("music", timeline.Music)
	appendTracks("sfx", timeline.SFX)

	return &CompiledAudioPlan{
		TimelineRevision: timeline.Revision,
		TimelineSHA256:   timelineSHA,
		DurationUS:       timeline.DurationUS,
		Tracks:           tracks,
	}, nil
}

// CompileAudioTimeline is the function form for producers that do not need
// to retain an AudioCompiler value.
func CompileAudioTimeline(timeline *CanonicalTimeline) (*CompiledAudioPlan, error) {
	return NewAudioCompiler().Compile(timeline)
}

// ValidateCompiledAudioPlan proves that a renderer input was compiled from
// the supplied CanonicalTimeline. The producer may render the returned plan
// through the existing audio/video engine and then register its output, but
// it must not register a plan whose editorial identity differs from the
// timeline used for the job.
func ValidateCompiledAudioPlan(timeline *CanonicalTimeline, compiled *CompiledAudioPlan) error {
	if compiled == nil {
		return fmt.Errorf("audio compiler: compiled plan is required")
	}
	expected, err := CompileAudioTimeline(timeline)
	if err != nil {
		return err
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return fmt.Errorf("audio compiler: encode expected plan: %w", err)
	}
	actualJSON, err := json.Marshal(compiled)
	if err != nil {
		return fmt.Errorf("audio compiler: encode compiled plan: %w", err)
	}
	if !bytes.Equal(expectedJSON, actualJSON) {
		return fmt.Errorf("audio compiler: compiled plan does not match canonical timeline")
	}
	return nil
}
