// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// ── Helpers ──────────────────────────────────────────────────────────────

func isTerminalSuccess(status string) bool {
	switch status {
	case "completed", "succeeded", "done":
		return true
	default:
		return false
	}
}

func isTerminalFailure(status string) bool {
	switch status {
	case "failed", "error":
		return true
	default:
		return false
	}
}

// marshalPayload serializes the result map. On JSON marshal failure,
// returns an empty payload with a zero hash — the caller should treat
// an empty SHA256 as a signal that the payload is not serializable.
func marshalPayload(result map[string]interface{}) (payloadJSON, payloadSHA256 string) {
	if result == nil {
		return "{}", sha256Hex([]byte("{}"))
	}
	raw, err := json.Marshal(result)
	if err != nil {
		// Non-serializable payload — return empty so the caller can
		// detect and mark BLOCKED rather than silently writing {}.
		return "", ""
	}
	payloadJSON = string(raw)
	payloadSHA256 = sha256Hex(raw)
	return
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
