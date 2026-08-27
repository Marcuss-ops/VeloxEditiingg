package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestKnownCanonicalVideoProfileV1_AdmitsLegacyProfile(t *testing.T) {
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "")
	profile, err := KnownCanonicalVideoProfileV1(CanonicalVideoProfileIDV1)
	if err != nil {
		t.Fatalf("legacy profile admission: %v", err)
	}
	if profile.ProfileID != CanonicalVideoProfileIDV1 || profile.ContainerLayout != ContainerLayoutProgressive {
		t.Fatalf("legacy profile = %+v", profile)
	}
	if err := profile.Validate(); err != nil {
		t.Fatalf("legacy profile Validate: %v", err)
	}
}

// TestKnownCanonicalVideoProfileV1_FMP4DisabledByDefault pins the registered-
// but-DISABLED state: the profile is KNOWN to the registry (distinct error,
// not "not registered") but refused until the 100-job benchmark gate opens.
func TestKnownCanonicalVideoProfileV1_FMP4DisabledByDefault(t *testing.T) {
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "")
	_, err := KnownCanonicalVideoProfileV1(CanonicalVideoProfileFMP4StreamV1)
	if err == nil {
		t.Fatal("fMP4 profile must be refused while the gate is closed")
	}
	if !strings.Contains(err.Error(), "registered but DISABLED") || !strings.Contains(err.Error(), "VELOX_FMP4_STREAM_PROFILE") {
		t.Fatalf("disabled error = %v; want the registered-but-DISABLED gate error", err)
	}
	// Unknown profiles keep their distinct not-registered error.
	if _, err := KnownCanonicalVideoProfileV1("velox-h264-copy-v1"); err == nil || !strings.Contains(err.Error(), "is not registered") {
		t.Fatalf("unknown profile error = %v; want not-registered", err)
	}
}

func TestKnownCanonicalVideoProfileV1_FMP4AdmittedWhenEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "enabled", "TRUE", "Enabled"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("VELOX_FMP4_STREAM_PROFILE", value)
			profile, err := KnownCanonicalVideoProfileV1(CanonicalVideoProfileFMP4StreamV1)
			if err != nil {
				t.Fatalf("admission with gate=%q: %v", value, err)
			}
			if profile.ProfileID != CanonicalVideoProfileFMP4StreamV1 || profile.ContainerLayout != ContainerLayoutFragmented {
				t.Fatalf("fMP4 profile = %+v", profile)
			}
			if err := profile.Validate(); err != nil {
				t.Fatalf("fMP4 Validate: %v", err)
			}
			if profile.ProfileID == CanonicalVideoProfileIDV1 {
				t.Fatal("fMP4 profile must be a distinct profile identity")
			}
		})
	}
}

func TestCanonicalVideoProfileV1_ContainerLayoutValidation(t *testing.T) {
	for _, layout := range []string{"", ContainerLayoutProgressive, ContainerLayoutFragmented} {
		profile := CanonicalVideoProfileV1Default
		profile.ContainerLayout = layout
		if err := profile.Validate(); err != nil {
			t.Fatalf("layout %q rejected: %v", layout, err)
		}
	}
	bad := CanonicalVideoProfileV1Default
	bad.ContainerLayout = "banana"
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "container_layout") {
		t.Fatalf("bogus layout error = %v", err)
	}
}

func TestCanonicalVideoProfileJSONRoundTripCarriesContainerLayout(t *testing.T) {
	data, err := json.Marshal(CanonicalVideoProfileFMP4StreamV1Default)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"container_layout":"fragmented"`) {
		t.Fatalf("fMP4 JSON lacks container_layout: %s", data)
	}
	var decoded CanonicalVideoProfileV1
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ContainerLayout != ContainerLayoutFragmented {
		t.Fatalf("decoded layout = %q", decoded.ContainerLayout)
	}
	// Legacy documents without the field decode to the empty (legacy alias for
	// progressive) value — use a fresh variable so Unmarshal cannot retain the
	// previous document's field.
	var legacy CanonicalVideoProfileV1
	if err := json.Unmarshal([]byte(`{"profile_id":"VELOX_ASSEMBLY_READY_V1"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.ContainerLayout != "" {
		t.Fatalf("legacy doc decoded layout = %q, want empty (legacy alias)", legacy.ContainerLayout)
	}
}
