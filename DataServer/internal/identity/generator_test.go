package identity

import (
	"encoding/hex"
	"testing"
)

func TestNewHex128(t *testing.T) {
	id, err := NewHex128()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(id) != 32 {
		t.Errorf("expected ID length of 32 characters, got %d (%q)", len(id), id)
	}

	decoded, err := hex.DecodeString(id)
	if err != nil {
		t.Errorf("failed to decode hex string: %v", err)
	}

	if len(decoded) != 16 {
		t.Errorf("expected 16 decoded bytes, got %d", len(decoded))
	}
}
