package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
)

// FinalAudioRegistration describes the already-compiled audio file produced
// from one CanonicalTimeline. Bytes, SHA256, size, and READY/verified status
// are still established by ResolveAndRegister; this type carries only the
// producer-declared media contract and provenance.
type FinalAudioRegistration struct {
	Codec        string
	SampleRateHz int
	Channels     int
	DurationUS   int64
}

// Validate checks the V2 final-audio media contract before any asset I/O.
func (r FinalAudioRegistration) Validate(timeline *contract.CanonicalTimeline) error {
	if timeline == nil {
		return fmt.Errorf("final audio: canonical timeline is required")
	}
	if err := timeline.Validate(); err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(r.Codec)) != "aac" {
		return fmt.Errorf("final audio: codec must be aac")
	}
	if r.SampleRateHz != 48_000 {
		return fmt.Errorf("final audio: sample rate must be 48000 Hz")
	}
	if r.Channels != 2 {
		return fmt.Errorf("final audio: channels must be 2")
	}
	if r.DurationUS <= 0 {
		return fmt.Errorf("final audio: duration_us must be positive")
	}
	if !withinDuration(r.DurationUS, timeline.DurationUS, contract.CompiledPlanV2DurationToleranceUS) {
		return fmt.Errorf("final audio: duration_us %d differs from timeline duration_us %d by more than %d microseconds", r.DurationUS, timeline.DurationUS, contract.CompiledPlanV2DurationToleranceUS)
	}
	return nil
}

// MetadataJSON returns the canonical provenance metadata stored on the Velox
// asset row. It contains the timeline identity but never local paths.
func (r FinalAudioRegistration) MetadataJSON(timeline *contract.CanonicalTimeline) (string, error) {
	if err := r.Validate(timeline); err != nil {
		return "", err
	}
	timelineSHA, err := timeline.TimelineSHA256()
	if err != nil {
		return "", err
	}
	metadata := struct {
		Producer         string `json:"producer"`
		Mode             string `json:"mode"`
		Codec            string `json:"codec"`
		SampleRateHz     int    `json:"sample_rate_hz"`
		Channels         int    `json:"channels"`
		DurationUS       int64  `json:"duration_us"`
		TimelineRevision int64  `json:"timeline_revision"`
		TimelineSHA256   string `json:"timeline_sha256"`
	}{
		Producer:         "audio_compiler",
		Mode:             contract.AudioModeFinalAudioCopy,
		Codec:            "aac",
		SampleRateHz:     r.SampleRateHz,
		Channels:         r.Channels,
		DurationUS:       r.DurationUS,
		TimelineRevision: timeline.Revision,
		TimelineSHA256:   timelineSHA,
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("final audio: encode metadata: %w", err)
	}
	return string(encoded), nil
}

// RegisterCompiledFinalAudio verifies the producer-side AudioCompiler output
// against the CanonicalTimeline before registering the already-rendered file.
// Rendering remains delegated to the existing audio/video engine; this method
// is the boundary that prevents an output from a different editorial timeline
// from being registered as final_audio.
func (s *AssetService) RegisterCompiledFinalAudio(ctx context.Context, reference string, timeline *contract.CanonicalTimeline, compiled *contract.CompiledAudioPlan, registration FinalAudioRegistration) (*Asset, error) {
	if err := contract.ValidateCompiledAudioPlan(timeline, compiled); err != nil {
		return nil, err
	}
	return s.RegisterFinalAudio(ctx, reference, timeline, registration)
}

// RegisterFinalAudio registers an already-produced final_audio file through
// the canonical resolver/security/content-addressing pipeline. The returned
// asset is READY with an authoritative SHA256 and size, and its metadata
// binds it to the exact CanonicalTimeline revision/hash.
func (s *AssetService) RegisterFinalAudio(ctx context.Context, reference string, timeline *contract.CanonicalTimeline, registration FinalAudioRegistration) (*Asset, error) {
	metadataJSON, err := registration.MetadataJSON(timeline)
	if err != nil {
		return nil, err
	}
	asset, err := s.ResolveAndRegister(ctx, ResolveAssetCommand{
		Kind:         KindFinalAudio,
		Reference:    reference,
		SourceType:   "audio_compiler",
		MetadataJSON: metadataJSON,
		PreRegistrationValidation: func(probeCtx context.Context, path, _ string, _ int64) (*MediaMetadata, error) {
			if s == nil || s.mediaMetadata == nil {
				return nil, fmt.Errorf("final audio: media metadata resolver unavailable")
			}
			mediaMetadata, probeErr := s.mediaMetadata.Resolve(probeCtx, path)
			if probeErr != nil {
				return nil, fmt.Errorf("media probe failed: %w", probeErr)
			}
			if err := validateFinalAudioMediaMetadata(mediaMetadata, registration); err != nil {
				return nil, err
			}
			return mediaMetadata, nil
		},
	})
	if err != nil {
		return nil, err
	}
	if asset == nil {
		return nil, fmt.Errorf("final audio: resolver returned no registered asset")
	}
	if asset.Kind != KindFinalAudio {
		return nil, fmt.Errorf("final audio: content already registered as kind %q", asset.Kind)
	}
	if asset.Status != AssetStatusReady || strings.TrimSpace(asset.SHA256) == "" || asset.SizeBytes <= 0 || strings.TrimSpace(asset.VerifiedAt) == "" {
		return nil, fmt.Errorf("final audio: registered asset is not verified")
	}
	if err := validateFinalAudioProvenance(asset, timeline, registration); err != nil {
		return nil, err
	}

	// ResolveAndRegister verifies the bytes and performs a best-effort probe.
	// Re-read the canonical registry metadata here so this API cannot return a
	// READY final_audio asset whose actual codec/sample format is wrong or whose
	// probe failed. This also covers the SHA dedup path.
	if s == nil {
		return nil, fmt.Errorf("asset service unavailable")
	}
	metadata, err := s.EnsureMediaMetadata(ctx, asset.AssetID)
	if err != nil {
		return nil, fmt.Errorf("final audio: verified media metadata unavailable: %w", err)
	}
	if err := validateFinalAudioMediaMetadata(metadata, registration); err != nil {
		return nil, err
	}
	return asset, nil
}

func validateFinalAudioProvenance(asset *Asset, timeline *contract.CanonicalTimeline, registration FinalAudioRegistration) error {
	if asset == nil {
		return fmt.Errorf("final audio: asset is required")
	}
	var metadata struct {
		Producer         string `json:"producer"`
		Mode             string `json:"mode"`
		Codec            string `json:"codec"`
		SampleRateHz     int    `json:"sample_rate_hz"`
		Channels         int    `json:"channels"`
		DurationUS       int64  `json:"duration_us"`
		TimelineRevision int64  `json:"timeline_revision"`
		TimelineSHA256   string `json:"timeline_sha256"`
	}
	if strings.TrimSpace(asset.MetadataJSON) == "" {
		return fmt.Errorf("final audio: registered asset has no timeline provenance")
	}
	if err := json.Unmarshal([]byte(asset.MetadataJSON), &metadata); err != nil {
		return fmt.Errorf("final audio: invalid registered provenance: %w", err)
	}
	timelineSHA, err := timeline.TimelineSHA256()
	if err != nil {
		return err
	}
	if metadata.Producer != "audio_compiler" || metadata.Mode != contract.AudioModeFinalAudioCopy {
		return fmt.Errorf("final audio: registered asset provenance is not AudioCompiler FINAL_AUDIO_COPY")
	}
	if metadata.TimelineRevision != timeline.Revision || metadata.TimelineSHA256 != timelineSHA {
		return fmt.Errorf("final audio: registered asset timeline provenance mismatch")
	}
	if metadata.Codec != "aac" || metadata.SampleRateHz != 48_000 || metadata.Channels != 2 || metadata.DurationUS != registration.DurationUS {
		return fmt.Errorf("final audio: registered asset media contract metadata mismatch")
	}
	return nil
}

func validateFinalAudioMediaMetadata(metadata *MediaMetadata, registration FinalAudioRegistration) error {
	if metadata == nil {
		return fmt.Errorf("final audio: verified media metadata is missing")
	}
	if !strings.EqualFold(strings.TrimSpace(metadata.AudioCodec), "aac") {
		return fmt.Errorf("final audio: verified codec is %q, want aac", metadata.AudioCodec)
	}
	if metadata.AudioSampleRate != 48_000 {
		return fmt.Errorf("final audio: verified sample rate is %d Hz, want 48000 Hz", metadata.AudioSampleRate)
	}
	if metadata.AudioChannels != 2 {
		return fmt.Errorf("final audio: verified channels are %d, want 2", metadata.AudioChannels)
	}
	actualDurationUS := metadata.DurationMs * 1000
	if actualDurationUS <= 0 || !withinDuration(actualDurationUS, registration.DurationUS, contract.CompiledPlanV2DurationToleranceUS) {
		return fmt.Errorf("final audio: verified duration is %d microseconds, want %d", actualDurationUS, registration.DurationUS)
	}
	return nil
}

// withinDuration is intentionally local to this package so final-audio
// registration shares the V2 tolerance without duplicating duration policy.
func withinDuration(left, right, tolerance int64) bool {
	if left >= right {
		return left-right <= tolerance
	}
	return right-left <= tolerance
}
