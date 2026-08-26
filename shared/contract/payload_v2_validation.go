package contract

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendermanifest"
)

// NewJobPayloadV2Checked constructs a canonical writer payload and rejects
// lifecycle or unknown values in the producer-side status field.
func NewJobPayloadV2Checked(raw map[string]any) (*JobPayloadV2, error) {
	if raw != nil {
		if value, present := raw["status"]; present && value != nil {
			rawStatus, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("contract: payload status must be an input assembly status")
			}
			if strings.TrimSpace(rawStatus) == "COMPLETED" {
				return nil, fmt.Errorf("contract: payload status %q is ambiguous; use lowercase input-assembly value %q", rawStatus, InputAssemblyCompleted)
			}
			status, ok := ParseInputAssemblyStatus(rawStatus)
			if !ok {
				return nil, fmt.Errorf("contract: payload status %q is not an input assembly status", rawStatus)
			}
			copyPayload := make(map[string]any, len(raw)+1)
			for key, item := range raw {
				copyPayload[key] = item
			}
			copyPayload["status"] = string(status)
			raw = copyPayload
		}
	}
	if raw != nil {
		if value, present := raw["render_manifest"]; present {
			manifest, ok := value.(map[string]any)
			if !ok || len(manifest) == 0 {
				return nil, fmt.Errorf("contract: render_manifest must be a non-empty object")
			}
		}
	}
	payload := NewJobPayloadV2(raw)
	if deliveryPlanInputPresent(raw) && !isRenderOnlyEmptyDeliveryPlan(raw) {
		entries, err := deliveryplan.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("contract: delivery_plan: %w", err)
		}
		payload.DeliveryPlan = entries
	}
	if _, err := payload.TypedRenderManifest(); err != nil {
		return nil, err
	}
	return payload, nil
}

// TypedRenderManifest is the strict render-manifest boundary for canonical
// writers and the renderer compiler.
func (p *JobPayloadV2) TypedRenderManifest() (*rendermanifest.Manifest, error) {
	if p == nil || len(p.RenderManifest) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(p.RenderManifest)
	if err != nil {
		return nil, fmt.Errorf("contract: render_manifest encode: %w", err)
	}
	manifest, err := rendermanifest.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("contract: render_manifest: %w", err)
	}
	return manifest, nil
}
