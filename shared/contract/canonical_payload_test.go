// Package contract_test — canonical_payload_test.go exercises the
// canonical-purity gate that Codice Step 7/8 of the Velox hardening plan
// promotes to a binding contract.
//
// Coverage:
//   - CanonicalTopLevelKeys / LegacyAliasKeys integrity (no duplicates,
//     no missing must-have anchors, mirror maps populated).
//   - ValidatePayload canonical accept (full + minimal + empty).
//   - ValidatePayload legacy alias rejection (5 denylisted aliases).
//   - ValidatePayload shape anomaly rejection (non-string job_id,
//     non-array scenes/voiceover_paths, wrong-type delivery_plan root).
//   - StrictValidatePayload drift-key rejection.
//   - UnknownKeys observer (canonical/legacy/noise filtering).
package contract

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

// ────────────────────────────────────────────────────────────────────────
// Keyset integrity
// ────────────────────────────────────────────────────────────────────────

func TestCanonicalTopLevelKeys_Integrity(t *testing.T) {
	if len(CanonicalTopLevelKeys) == 0 {
		t.Fatal("CanonicalTopLevelKeys is empty — the binding is broken")
	}

	// Required anchors that Step 7/8 documents in the package comment.
	mustHave := []string{
		"contract_version",
		"job_id", "job_run_id", "correlation_id",
		"video_name", "script_text",
		"scenes", "voiceover_paths", "items",
		"delivery_plan",
		"priority", "timeout_secs",
		"status",
	}
	seen := make(map[string]bool, len(CanonicalTopLevelKeys))
	for _, k := range CanonicalTopLevelKeys {
		if seen[k] {
			t.Errorf("duplicate canonical key: %q", k)
		}
		seen[k] = true
	}
	for _, k := range mustHave {
		if !seen[k] {
			t.Errorf("CanonicalTopLevelKeys missing required anchor %q", k)
		}
	}

	// Every entry must also be discoverable via IsCanonicalKey.
	for _, k := range CanonicalTopLevelKeys {
		if !IsCanonicalKey(k) {
			t.Errorf("IsCanonicalKey(%q) returned false for a key in the slice", k)
		}
	}

	// A non-canonical key must not be reported as canonical.
	if IsCanonicalKey("not_a_real_key") {
		t.Error("IsCanonicalKey returned true for an unknown key")
	}
}

func TestLegacyAliasKeys_Locked(t *testing.T) {
	// The denylist is the binding contract for Step 7/8 — pin it.
	expected := []string{"id", "run_id", "title", "voiceover_path", "audio_path"}
	if len(LegacyAliasKeys) != len(expected) {
		t.Fatalf("LegacyAliasKeys shape drift: got %d, want %d (entries was %v)",
			len(LegacyAliasKeys), len(expected), LegacyAliasKeys)
	}
	seen := make(map[string]bool, len(expected))
	for _, k := range LegacyAliasKeys {
		seen[k] = true
		if !IsLegacyAlias(k) {
			t.Errorf("IsLegacyAlias(%q) returned false for a key in the denylist", k)
		}
	}
	for _, k := range expected {
		if !seen[k] {
			t.Errorf("LegacyAliasKeys missing required denylist entry %q", k)
		}
	}
	// No legacy alias may also be canonical.
	for _, alias := range LegacyAliasKeys {
		if IsCanonicalKey(alias) {
			t.Errorf("legacy alias %q is ALSO canonical — keyset triangulation broke", alias)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────
// ValidatePayload — canonical accept
// ────────────────────────────────────────────────────────────────────────

func TestValidatePayload_Accept_Canonical(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]interface{}
	}{
		{
			name: "full-canonical",
			payload: map[string]interface{}{
				"contract_version":       2,
				"job_id":                 "job_123",
				"job_run_id":             "run_abc",
				"correlation_id":         "corr_xyz",
				"job_type":               "process_video",
				"version":                "v2",
				"created_at":             "2026-01-01T00:00:00Z",
				"updated_at":             "2026-01-01T00:00:00Z",
				"video_name":             "My Video",
				"script_text":            "Hello world",
				"voiceover_paths":        []interface{}{"vo.mp3"},
				"scenes":                 []interface{}{map[string]interface{}{"text": "S1", "duration_seconds": 5.0}},
				"items":                  []interface{}{map[string]interface{}{"role": "scene", "text": "S1"}},
				"audio_language_for_srt": "en",
				"video_mode":             "scene_image",
				"output_path":            "/tmp/out.mp4",
				"drive_output_folder":    "out_folder",
				"channel_id":             "ch1",
				"scene_image_paths":      []interface{}{"s1.jpg"},
				"priority":               1,
				"timeout_secs":           3600,
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "d1", "retry_budget": 3, "enabled": true},
				},
				"status": "PENDING",
			},
		},
		{
			name:    "minimal-anchors-only",
			payload: map[string]interface{}{"job_id": "x", "video_name": "y"},
		},
		{
			name:    "empty-map",
			payload: map[string]interface{}{},
		},
		{
			name: "voiceoverpaths-as-typed-string-slice",
			payload: map[string]interface{}{
				"job_id":            "x",
				"voiceover_paths":   []string{"a.mp3", "b.mp3"},
				"scene_image_paths": []string{"s1.jpg"},
			},
		},
		{
			name: "scenes-as-typed-map-slice",
			payload: map[string]interface{}{
				"job_id": "x",
				"scenes": []map[string]interface{}{{"text": "S1"}},
			},
		},
		{
			name: "delivery-plan-as-single-map",
			payload: map[string]interface{}{
				"job_id":        "x",
				"delivery_plan": map[string]interface{}{"destination_id": "d1", "retry_budget": 3, "enabled": true},
			},
		},
		{
			name: "delivery-plan-as-empty-array",
			payload: map[string]interface{}{
				"job_id":        "x",
				"delivery_plan": []interface{}{},
			},
		},
		{
			name: "all-canonical-string-fields-nil-allowed",
			payload: map[string]interface{}{
				"job_id":      nil,
				"video_name":  nil,
				"script_text": nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidatePayload(tt.payload); err != nil {
				t.Errorf("expected nil error for canonical payload, got %v", err)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────
// ValidatePayload — legacy alias rejection
// ────────────────────────────────────────────────────────────────────────

func TestValidatePayload_Reject_LegacyAlias(t *testing.T) {
	tests := []struct {
		name      string
		alias     string
		setupWith map[string]interface{}
	}{
		{"id", "id", map[string]interface{}{"id": "legacy_job"}},
		{"run_id", "run_id", map[string]interface{}{"run_id": "legacy_run"}},
		{"title", "title", map[string]interface{}{"title": "Legacy Title"}},
		{"voiceover_path", "voiceover_path", map[string]interface{}{
			"job_id":         "j",
			"voiceover_path": "audio.mp3",
		}},
		{"audio_path", "audio_path", map[string]interface{}{
			"job_id":     "j",
			"audio_path": "audio.mp3",
		}},
	}
	for _, tt := range tests {
		t.Run("alias-"+tt.name+"-rejected", func(t *testing.T) {
			err := ValidatePayload(tt.setupWith)
			if err == nil {
				t.Fatalf("expected ErrLegacyAlias for %q, got nil", tt.alias)
			}
			if !errors.Is(err, ErrLegacyAlias) {
				t.Errorf("expected errors.Is(err, ErrLegacyAlias), got %v", err)
			}
			if !strings.Contains(err.Error(), tt.alias) {
				t.Errorf("expected error to mention %q, got %q", tt.alias, err.Error())
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────
// ValidatePayload — shape anomaly rejection
// ────────────────────────────────────────────────────────────────────────

func TestValidatePayload_Reject_ShapeAnomaly(t *testing.T) {
	tests := []struct {
		name      string
		payload   map[string]interface{}
		wantMatch string
	}{
		{
			name:      "job_id-not-string",
			payload:   map[string]interface{}{"job_id": 123},
			wantMatch: `"job_id"`,
		},
		{
			name:      "job_id-as-bool",
			payload:   map[string]interface{}{"job_id": true},
			wantMatch: `"job_id"`,
		},
		{
			name:      "video_name-as-slice",
			payload:   map[string]interface{}{"video_name": []string{"x"}},
			wantMatch: `"video_name"`,
		},
		{
			name:      "script_text-as-map",
			payload:   map[string]interface{}{"script_text": map[string]interface{}{"k": "v"}},
			wantMatch: `"script_text"`,
		},
		{
			name:      "voiceover_paths-as-string",
			payload:   map[string]interface{}{"voiceover_paths": "not-an-array"},
			wantMatch: `"voiceover_paths"`,
		},
		{
			name:      "voiceover_paths-as-map",
			payload:   map[string]interface{}{"voiceover_paths": map[string]interface{}{"k": "v"}},
			wantMatch: `"voiceover_paths"`,
		},
		{
			name:      "scenes-as-map",
			payload:   map[string]interface{}{"scenes": map[string]interface{}{"text": "S1"}},
			wantMatch: `"scenes"`,
		},
		{
			name:      "scene_image_paths-as-bool",
			payload:   map[string]interface{}{"scene_image_paths": true},
			wantMatch: `"scene_image_paths"`,
		},
		{
			name:      "items-as-string",
			payload:   map[string]interface{}{"items": "not-an-array"},
			wantMatch: `"items"`,
		},
		{
			name:      "delivery_plan-as-string",
			payload:   map[string]interface{}{"delivery_plan": "should-be-array-or-map"},
			wantMatch: `"delivery_plan"`,
		},
		{
			name:      "delivery_plan-as-int",
			payload:   map[string]interface{}{"delivery_plan": 7},
			wantMatch: `"delivery_plan"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePayload(tt.payload)
			if err == nil {
				t.Fatalf("expected ErrShapeAnomaly for %q, got nil", tt.name)
			}
			if !errors.Is(err, ErrShapeAnomaly) {
				t.Errorf("expected errors.Is(err, ErrShapeAnomaly), got %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantMatch) {
				t.Errorf("expected error to mention %q, got %q", tt.wantMatch, err.Error())
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────
// Precedence — legacy alias beats shape anomaly (it is reported first)
// ────────────────────────────────────────────────────────────────────────

func TestValidatePayload_Precedence_LegacyAliasBeatsShape(t *testing.T) {
	// id (legacy alias) AND voiceover_paths non-array — legacy alias MUST
	// be reported first (rules are evaluated in order).
	payload := map[string]interface{}{
		"id":              "legacy",
		"voiceover_paths": "not-an-array",
	}
	err := ValidatePayload(payload)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrLegacyAlias) {
		t.Errorf("expected ErrLegacyAlias to take precedence, got %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// nil-payload rejection
// ────────────────────────────────────────────────────────────────────────

func TestValidatePayload_NilRejected(t *testing.T) {
	if err := ValidatePayload(nil); err == nil {
		t.Fatal("expected error for nil payload, got nil")
	} else if !errors.Is(err, ErrShapeAnomaly) {
		t.Errorf("expected ErrShapeAnomaly for nil, got %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// StrictValidatePayload — drift key rejection
// ────────────────────────────────────────────────────────────────────────

func TestStrictValidatePayload_RejectUnknownKey(t *testing.T) {
	payload := map[string]interface{}{
		"job_id":         "j",
		"video_name":     "v",
		"future_field_x": []interface{}{}, // not canonical, not legacy
	}
	err := StrictValidatePayload(payload)
	if err == nil {
		t.Fatal("expected ErrNonCanonicalKey, got nil")
	}
	if !errors.Is(err, ErrNonCanonicalKey) {
		t.Errorf("expected errors.Is(err, ErrNonCanonicalKey), got %v", err)
	}
	if !strings.Contains(err.Error(), "future_field_x") {
		t.Errorf("expected error to mention unknown key, got %q", err.Error())
	}
}

func TestStrictValidatePayload_AcceptsCanonical(t *testing.T) {
	payload := map[string]interface{}{
		"job_id":          "j",
		"video_name":      "v",
		"voiceover_paths": []interface{}{"vo.mp3"},
		"delivery_plan":   []interface{}{},
	}
	if err := StrictValidatePayload(payload); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestStrictValidatePayload_AcceptsUnderscoreNoise(t *testing.T) {
	// Header/envelope noise (e.g. _etag, x-traceid) is filtered.
	payload := map[string]interface{}{
		"job_id":    "j",
		"_etag":     "abc",
		"x-traceid": "xyz",
	}
	if err := StrictValidatePayload(payload); err != nil {
		t.Errorf("expected nil (underscore noise filtered), got %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// UnknownKeys observer
// ────────────────────────────────────────────────────────────────────────

func TestUnknownKeys(t *testing.T) {
	t.Run("nil-input-returns-nil", func(t *testing.T) {
		if got := UnknownKeys(nil); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("canonical-only-has-zero-unknown", func(t *testing.T) {
		payload := map[string]interface{}{
			"job_id":        "j",
			"video_name":    "v",
			"delivery_plan": []interface{}{},
		}
		if got := UnknownKeys(payload); len(got) != 0 {
			t.Errorf("expected no unknown keys, got %v", got)
		}
	})

	t.Run("alias-payload-is-NOT-reported-as-unknown", func(t *testing.T) {
		// Aliases surface under their own category (ValidatePayload); the
		// observer must not double-report them.
		payload := map[string]interface{}{
			"id":     "old",
			"job_id": "new",
		}
		if got := UnknownKeys(payload); len(got) != 0 {
			t.Errorf("expected no unknown keys when only legacy aliases mix in, got %v", got)
		}
	})

	t.Run("unknown-key-passes-through", func(t *testing.T) {
		payload := map[string]interface{}{
			"job_id":         "j",
			"future_field_x": []interface{}{},
			"future_field_z": "z",
		}
		got := UnknownKeys(payload)
		// Map iteration is unordered — sort before comparing for stable output.
		sort.Strings(got)
		want := []string{"future_field_x", "future_field_z"}
		if len(got) != len(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("index %d: got %q want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("underscore-noise-filtered", func(t *testing.T) {
		payload := map[string]interface{}{
			"job_id":    "j",
			"_etag":     "abc",
			"x-traceid": "xyz",
		}
		if got := UnknownKeys(payload); len(got) != 0 {
			t.Errorf("expected no unknown keys (underscore noise filtered), got %v", got)
		}
	})
}
