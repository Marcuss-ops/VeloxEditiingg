package workers

import (
	"testing"
)

func TestExtractStringSlice_Nil(t *testing.T) {
	if got := ExtractStringSlice(nil); got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}

func TestExtractStringSlice_StringSlice(t *testing.T) {
	input := []interface{}{"a", "b", "c"}
	got := ExtractStringSlice(input)
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("want [a b c], got %v", got)
	}
}

func TestExtractStringSlice_Empty(t *testing.T) {
	got := ExtractStringSlice([]interface{}{})
	if got == nil || len(got) != 0 {
		t.Fatalf("want empty slice, got %v", got)
	}
}

func TestExtractStringSlice_MixedTypes(t *testing.T) {
	input := []interface{}{"hello", 42, true}
	got := ExtractStringSlice(input)
	if len(got) != 1 {
		t.Fatalf("want 1 string item (non-strings skipped), got %d: %v", len(got), got)
	}
	if got[0] != "hello" {
		t.Fatalf("want 'hello', got %q", got[0])
	}
}

func TestNormalizeCapabilities(t *testing.T) {
	input := map[string]interface{}{
		"ffmpeg":              true,
		"supported_job_types": []string{"health_check"},
	}
	got := normalizeCapabilities(input)
	if got["ffmpeg"] != true {
		t.Fatalf("want ffmpeg=true, got %v", got["ffmpeg"])
	}
}

func TestNormalizeCapabilities_Nil(t *testing.T) {
	got := normalizeCapabilities(nil)
	if got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}
