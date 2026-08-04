package deliveryplan

// envelopeKeys is the complete set of delivery-routing aliases accepted at
// the payload boundary. Publication metadata is intentionally not included:
// it belongs to the control-plane publication contract, never the renderer
// payload.
var envelopeKeys = [...]string{
	// render_only is a control-plane opt-out: it explicitly permits a job
	// to have no delivery plan. It is kept alongside the envelope so the
	// atomic creator can enforce the same contract as enqueue validation.
	"render_only",
	"delivery_plan",
	"delivery_destination_ids",
	"delivery_destination_id",
	"delivery_metadata",
	"destinations",
	"delivery_destinations",
	"destination_ids",
	"destination_id",
}

// ExtractEnvelope returns a shallow copy containing only delivery-routing
// fields with non-nil values. It never mutates payload and returns nil when no
// delivery-routing field is present.
func ExtractEnvelope(payload map[string]interface{}) map[string]interface{} {
	if payload == nil {
		return nil
	}
	out := make(map[string]interface{})
	for _, key := range envelopeKeys {
		if value, ok := payload[key]; ok && value != nil {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// StripEnvelope returns a shallow clone with all delivery-routing aliases
// removed. It never mutates payload. A nil input produces a non-nil empty map,
// matching the renderer projection's historical nil-safe behavior.
func StripEnvelope(payload map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	for _, key := range envelopeKeys {
		delete(out, key)
	}
	return out
}
