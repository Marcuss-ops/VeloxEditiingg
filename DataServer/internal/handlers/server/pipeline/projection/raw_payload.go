package projection

import (
	"strings"

	"velox-shared/contract"
)

const defaultRetryBudgetValue = 3

// RawPayloadInput contains the request-independent values needed to build the
// top-level canonical payload envelope. Nested render timeline values remain
// owned by the pipeline adapter until their own boundary is extracted.
type RawPayloadInput struct {
	JobID              string
	JobType            string
	TemplateID         string
	TemplateVersion    int
	VideoMode          string
	VideoName          string
	ScriptText         string
	Spec               map[string]interface{}
	Output             map[string]interface{}
	RenderManifest     map[string]interface{}
	ManifestRef        map[string]interface{}
	ManifestSHA256     string
	PlacementPin       string
	LegacyVoiceovers   []string
	DeliveryPlan       []RawDeliveryPlanEntry
	RetryBudgetDefault int
}

// RawDeliveryPlanEntry is the projection-neutral delivery-plan input. The
// resulting map keeps the existing canonical wire keys and default behavior.
type RawDeliveryPlanEntry struct {
	DestinationID string
	Priority      int
	RetryBudget   *int
	Metadata      interface{}
}

// BuildRawPayloadEnvelope builds the top-level portion of the canonical raw
// payload. It deliberately does not know about pipeline request DTOs, so the
// pipeline package can call it without introducing an import cycle.
func BuildRawPayloadEnvelope(input RawPayloadInput) map[string]interface{} {
	payload := map[string]interface{}{
		// This is producer-side input assembly, not terminal job or delivery
		// state. Preserve the established wire spelling.
		"status": string(contract.InputAssemblyCompleted),
		"job_id": strings.TrimSpace(input.JobID),
	}
	if value := strings.TrimSpace(input.JobType); value != "" {
		payload["job_type"] = value
	}
	if value := strings.TrimSpace(input.TemplateID); value != "" {
		payload["template_id"] = value
	}
	if input.TemplateVersion > 0 {
		payload["template_version"] = input.TemplateVersion
	}
	if value := strings.TrimSpace(input.VideoMode); value != "" {
		payload["video_mode"] = value
	}
	if input.VideoName != "" {
		payload["video_name"] = strings.TrimSpace(input.VideoName)
	}
	if input.ScriptText != "" {
		payload["script_text"] = input.ScriptText
	}
	if len(input.Spec) > 0 {
		payload["spec"] = input.Spec
	}
	if input.Output != nil {
		payload["output"] = input.Output
	}
	if input.RenderManifest != nil {
		payload["render_manifest"] = input.RenderManifest
	}
	if input.ManifestRef != nil {
		payload["manifest_ref"] = input.ManifestRef
	}
	if input.ManifestSHA256 != "" {
		payload["manifest_sha256"] = input.ManifestSHA256
	}

	if len(input.LegacyVoiceovers) > 0 {
		audioTracks := make([]interface{}, 0, len(input.LegacyVoiceovers))
		for _, sourceURL := range input.LegacyVoiceovers {
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url": sourceURL,
				"role":       "voiceover",
			})
		}
		payload["audio_tracks"] = audioTracks
	}
	if value := strings.TrimSpace(input.PlacementPin); value != "" {
		payload["_placement_pin_worker_id"] = value
	}
	if len(input.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(input.DeliveryPlan))
		for _, delivery := range input.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": strings.TrimSpace(delivery.DestinationID),
			}
			if delivery.Priority > 0 {
				entry["priority"] = delivery.Priority
			}
			if delivery.RetryBudget == nil {
				defaultRetryBudget := input.RetryBudgetDefault
				if defaultRetryBudget <= 0 {
					defaultRetryBudget = defaultRetryBudgetValue
				}
				entry["retry_budget"] = defaultRetryBudget
			} else {
				entry["retry_budget"] = *delivery.RetryBudget
			}
			if delivery.Metadata != nil {
				entry["metadata"] = delivery.Metadata
			}
			plan = append(plan, entry)
		}
		payload["delivery_plan"] = plan
	}
	return payload
}
