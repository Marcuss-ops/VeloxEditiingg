package plan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// DecodeJSON is the single wire-to-runtime boundary for the legacy
// render_plan/render_plan_json contract. It rejects unknown fields and
// trailing JSON before returning an owned runtime plan.
func DecodeJSON(data []byte) (*RenderPlan, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("render plan: document is required")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var renderPlan RenderPlan
	if err := decoder.Decode(&renderPlan); err != nil {
		return nil, fmt.Errorf("render plan: strict decode: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("render plan: strict decode: trailing data")
		}
		return nil, fmt.Errorf("render plan: strict decode: %w", err)
	}
	if err := renderPlan.Validate(); err != nil {
		return nil, err
	}
	return &renderPlan, nil
}

// Validate checks intrinsic runtime-plan invariants. Task identity binding is
// checked separately by ValidateForJob because it belongs to the executor
// envelope rather than the reusable runtime-plan document.
func (p *RenderPlan) Validate() error {
	if p == nil {
		return errors.New("render plan: plan is required")
	}
	if p.Version != 1 {
		return fmt.Errorf("render plan: version must be 1 (got %d)", p.Version)
	}
	if strings.TrimSpace(p.JobID) == "" {
		return errors.New("render plan: job_id is required")
	}
	if p.Canvas.Width <= 0 || p.Canvas.Height <= 0 || p.Canvas.Fps <= 0 {
		return errors.New("render plan: canvas width, height and fps must be positive")
	}
	if len(p.Timeline) == 0 {
		return errors.New("render plan: timeline must not be empty")
	}
	for i, item := range p.Timeline {
		if item.DurationSeconds <= 0 || strings.TrimSpace(item.Source.Type) == "" {
			return fmt.Errorf("render plan: timeline[%d] has invalid source or duration", i)
		}
	}
	if err := validateAudioTracks(p); err != nil {
		return err
	}
	return validateSubtitleTracks(p, totalDuration(p))
}

// ValidateForJob additionally binds the already-decoded runtime plan to its
// enclosing task. The task identity is not part of the reusable C++ runtime
// contract, so this check remains at the executor boundary.
func (p *RenderPlan) ValidateForJob(jobID string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(p.JobID) == "" || (jobID != "" && p.JobID != jobID) {
		return fmt.Errorf("render plan: job_id must match task (%q)", jobID)
	}
	return nil
}

func validateAudioTracks(p *RenderPlan) error {
	for i, track := range p.AudioTracks {
		if strings.TrimSpace(track.SourceURL) == "" {
			return fmt.Errorf("render plan: audio_tracks[%d].source_url is required", i)
		}
		if track.Volume < 0 {
			return fmt.Errorf("render plan: audio_tracks[%d].volume must not be negative", i)
		}
		if track.Loop && track.DurationSeconds < 0 {
			return fmt.Errorf("render plan: audio_tracks[%d].duration_seconds must be non-negative", i)
		}
	}
	return nil
}

func validateSubtitleTracks(p *RenderPlan, endOfTimeline float64) error {
	for i, subtitle := range p.Subtitles {
		if len(subtitle.Events) == 0 {
			return fmt.Errorf("render plan: subtitle_tracks[%d] requires aligned events", i)
		}
		if err := validateSubtitleEvents(subtitle.Events, i, endOfTimeline); err != nil {
			return err
		}
	}
	return nil
}

func validateSubtitleEvents(events []SubtitleEvent, trackIndex int, endOfTimeline float64) error {
	var previousEnd float64
	for j, event := range events {
		if event.EndSeconds <= event.StartSeconds || event.StartSeconds < 0 || strings.TrimSpace(event.Text) == "" {
			return fmt.Errorf("render plan: subtitle_tracks[%d].events[%d] is invalid", trackIndex, j)
		}
		if event.EndSeconds-event.StartSeconds < 0.5 {
			return fmt.Errorf("render plan: subtitle_tracks[%d].events[%d] is shorter than 500ms", trackIndex, j)
		}
		if event.StartSeconds < previousEnd {
			return fmt.Errorf("render plan: subtitle_tracks[%d].events[%d] overlaps previous event", trackIndex, j)
		}
		if event.EndSeconds > endOfTimeline {
			return fmt.Errorf("render plan: subtitle_tracks[%d].events[%d] exceeds timeline", trackIndex, j)
		}
		if strings.Count(event.Text, "\\n") > 1 {
			return fmt.Errorf("render plan: subtitle_tracks[%d].events[%d] exceeds two lines", trackIndex, j)
		}
		previousEnd = event.EndSeconds
	}
	return nil
}

func totalDuration(p *RenderPlan) float64 {
	var total float64
	for _, item := range p.Timeline {
		total += item.DurationSeconds
	}
	return total
}
