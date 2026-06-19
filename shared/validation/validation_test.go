package validation

import (
	"strings"
	"testing"
)

func TestIsAlphanumericID(t *testing.T) {
	good := []string{"a", "abc", "abc-123", "abc_123", "ABC", "0123", strings.Repeat("a", MaxIdentifierLength)}
	for _, s := range good {
		if !IsAlphanumericID(s) {
			t.Errorf("expected %q valid", s)
		}
	}
	bad := []string{"", " ", "abc.def", "abc/def", "abc!", strings.Repeat("a", MaxIdentifierLength+1), "ümlaut"}
	for _, s := range bad {
		if IsAlphanumericID(s) {
			t.Errorf("expected %q invalid", s)
		}
	}
}

func TestIsHexRun(t *testing.T) {
	good := []string{"0", "deadbeef", "DEADBEEF", "0123456789abcdefABCDEF"}
	for _, s := range good {
		if !IsHexRun(s) {
			t.Errorf("expected %q valid", s)
		}
	}
	bad := []string{"", "g", " ", "abc.def"}
	for _, s := range bad {
		if IsHexRun(s) {
			t.Errorf("expected %q invalid", s)
		}
	}
}
