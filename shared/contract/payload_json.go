package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// ParsePayloadObjectJSON decodes one payload object and rejects malformed or
// trailing JSON. It is intentionally shape-only: legacy persisted payloads
// remain readable, while writer-side semantics stay owned by
// NewJobPayloadV2Checked.
func ParsePayloadObjectJSON(data []byte) (map[string]interface{}, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]interface{}{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var payload map[string]interface{}
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("contract: decode payload object: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("contract: payload contains trailing JSON value")
		}
		return nil, fmt.Errorf("contract: payload contains trailing data: %w", err)
	}
	if payload == nil {
		return map[string]interface{}{}, nil
	}
	return payload, nil
}
