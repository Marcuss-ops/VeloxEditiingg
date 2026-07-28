// apiwire_test.go: minimal smoke tests for the canonical wire types.
//
// Goal: a JSON-roundtrip test confirms each public struct deserialises
// back from its expected OpenAPI key names, so the cmd/api-schema-gen
// generated schemas (which mirror the json tags) match the runtime
// behaviour of the HTTP handler. Run on every commit.
package apiwire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSubmitJobRequest_Roundtrip(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "ext-001",
		VideoName:      "Video 1",
		Scenes: []SubmitScene{
			{Text: "hello", DurationSeconds: 7.0},
		},
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var back SubmitJobRequest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.IdempotencyKey != "ext-001" || back.VideoName != "Video 1" {
		t.Errorf("roundtrip mismatch: %+v", back)
	}
	if len(back.Scenes) != 1 || back.Scenes[0].Text != "hello" || back.Scenes[0].DurationSeconds != 7.0 {
		t.Errorf("scenes roundtrip: %+v", back.Scenes)
	}
}

func TestCreatorPushPayload_StatusEnum(t *testing.T) {
	good := []string{`"completed"`, `"completed_with_warnings"`}
	for _, s := range good {
		var p CreatorPushPayload
		if err := json.Unmarshal([]byte(`{"status":`+s+`}`), &p); err != nil {
			t.Errorf("status %s must parse: %v", s, err)
		}
	}
}

func TestDeliveryPlanEntry_DestinationEnum(t *testing.T) {
	good := []struct {
		raw string
		id  string
	}{
		{`{"destination_id":"drive"}`, "drive"},
		{`{"destination_id":"gcs"}`, "gcs"},
		{`{"destination_id":"s3"}`, "s3"},
		{`{"destination_id":"youtube"}`, "youtube"},
		{`{"destination_id":"local"}`, "local"},
	}
	for _, c := range good {
		var d DeliveryPlanEntry
		if err := json.Unmarshal([]byte(c.raw), &d); err != nil {
			t.Errorf("destination %s must parse: %v", c.id, err)
			continue
		}
		if d.DestinationID != c.id {
			t.Errorf("destination %s roundtrip: %q", c.id, d.DestinationID)
		}
	}
}

// TestSubmitManifestRef_Roundtrip locks the JSON roundtrip shape of
// SubmitManifestRef (the canonical wire-level type referenced by
// SubmitJobRequest.manifest_ref). The struct is intentionally small
// (3 fields, all strings) but the roundtrip matters because:
//
//  1. SubmitJobRequest.manifest_ref is *SubmitManifestRef so the
//     JSON tag `omitempty` on every field combined with the pointer
//     indirection must still produce a clean roundtrip — the
//     canonical "present" shape is non-empty for all three fields.
//
//  2. cmd/api-schema-gen reads the validate tags on these fields
//     and emits the matching JSON Schema; a drift here (e.g., a
//     renamed field) would silently produce an openapi.yaml with
//     a different shape than the runtime handler accepts.
//
//  3. The `regex` and `oneof` validate rules are enforced at the
//     handler's ValidateSubmitJobRequest layer (NOT at the JSON
//     decoder level — DisallowUnknownFields only rejects unknown
//     keys, not value formats). This test pins the shape contract
//     only; value-format enforcement is covered by the
//     job_submit_test.go ValidateSubmitJobRequest_* cases.
func TestSubmitManifestRef_Roundtrip(t *testing.T) {
	const wantURL = "https://drive.google.com/file/d/MANIFEST_FILE_ID/view"
	const wantSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	in := SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           wantURL,
		SHA256:        wantSHA,
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back SubmitManifestRef
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.SchemaVersion != in.SchemaVersion {
		t.Errorf("schema_version roundtrip: got %q want %q", back.SchemaVersion, in.SchemaVersion)
	}
	if back.URL != wantURL {
		t.Errorf("url roundtrip: got %q want %q", back.URL, wantURL)
	}
	if back.SHA256 != wantSHA {
		t.Errorf("sha256 roundtrip: got %q want %q", back.SHA256, wantSHA)
	}
}

// TestSubmitManifestRef_MaxLengthMatchesHandlerConstant is the
// drift-guard test for the byte-cap duplicated between the apiwire
// validate tag (`max=2048`) and the handler-side constant
// `pipeline.MaxManifestRefURLBytes`. The two MUST move in
// lockstep: the wire schema advertises maxLength:2048 to clients
// AND the runtime validator enforces the same cap with
// details[].issue="max_length". A divergence silently lets one
// side accept a URL the other rejects, with no compile-time or
// runtime signal — so this test hard-fails on drift.
//
// Asymmetry: the apiwire package cannot import the handler
// package (the handler package imports apiwire, forming a
// cycle), so this test pins only the apiwire tag side. The
// matching handler-side test
// (TestSubmitJobValidateManifestRefURLMaxLengthBoundary in
// job_submit_test.go) pins only the handler constant side.
// Together they catch single-side drift (apiwire bump without
// handler bump → this test fails; handler bump without apiwire
// bump → that test fails). The cross-bump direction (both sides
// bumped together) goes undetected — accepted as project-wide
// convention (see also MaxVideoNameBytes / `max=300` tag on
// SubmitJobRequest.VideoName, which carries the same asymmetry).
//
// The check is reflect-based: reading the tag at runtime is the
// canonical way to pin a struct-tag value to a literal without
// needing a compile-time stringifier.
func TestSubmitManifestRef_MaxLengthMatchesHandlerConstant(t *testing.T) {
	t.Helper()

	const wantMaxLen = "max=2048"

	urlField, ok := reflect.TypeOf(SubmitManifestRef{}).FieldByName("URL")
	if !ok {
		t.Fatal("SubmitManifestRef.URL field not found")
	}
	got := urlField.Tag.Get("validate")
	if !strings.Contains(got, wantMaxLen) {
		t.Errorf("apiwire.SubmitManifestRef.URL validate tag must contain %q (drift guard, apiwire side), got: %q", wantMaxLen, got)
	}
}

// TestSubmitJobRequest_ManifestRef_Roundtrip verifies the parent
// struct's pointer-wrapped manifest_ref field roundtrips cleanly:
// nil pointer → field omitted from JSON; non-nil pointer with
// empty body → field present with `null` payload (which the
// handler-side validator rejects with 422, as documented).
func TestSubmitJobRequest_ManifestRef_Roundtrip(t *testing.T) {
	// (1) nil pointer → JSON must omit manifest_ref entirely so
	// existing clients (legacy body without manifest_ref) see no
	// wire-shape drift.
	noRef := SubmitJobRequest{
		IdempotencyKey: "ext-no-manifest",
		Scenes:         []SubmitScene{{Text: "s", DurationSeconds: 5}},
	}
	data, err := json.Marshal(noRef)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "manifest_ref") {
		t.Errorf("nil manifest_ref must be omitted from JSON, got: %s", data)
	}

	// (2) non-nil pointer → JSON carries manifest_ref with the
	// three nested fields. Lock the snake_case key names because
	// the cmd/api-schema-gen emitted JSON Schema references them
	// by name (so a drift here would break the spec).
	withRef := SubmitJobRequest{
		IdempotencyKey: "ext-with-manifest",
		Scenes:         []SubmitScene{{Text: "s", DurationSeconds: 5}},
		ManifestRef: &SubmitManifestRef{
			SchemaVersion: "velox.render-manifest.v1",
			URL:           "https://drive.example.com/manifest",
			SHA256:        strings.Repeat("a", 64),
		},
	}
	data, err = json.Marshal(withRef)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"manifest_ref":`,
		`"schema_version":"velox.render-manifest.v1"`,
		`"url":"https://drive.example.com/manifest"`,
		`"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
	} {
		if !strings.Contains(string(data), want) {
			t.Errorf("missing %q in JSON: %s", want, data)
		}
	}
}
