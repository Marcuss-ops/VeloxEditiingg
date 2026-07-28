package pipeline

import (
	"strings"
	"testing"
)

// makeValidSubmitJobRequest is a tiny constructor used by the
// manifest_ref validator tests below. The body satisfies every
// cross-field rule except the manifest_ref — each test mutates
// just the field under test.
func makeValidSubmitJobRequest() SubmitJobRequest {
	return SubmitJobRequest{
		IdempotencyKey: "mr-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
	}
}

// TestSubmitJobValidateManifestRefNilAccepts locks the
// "no manifest_ref at all" happy path: a nil pointer MUST pass
// through ValidateSubmitJobRequest without complaint, otherwise
// every existing client (legacy body shape) would 422 after the
// new field is shipped.
func TestSubmitJobValidateManifestRefNilAccepts(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = nil
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("nil manifest_ref MUST pass validator, got: bad=%v verr=%+v", bad, verr)
	}
}

// TestSubmitJobValidateManifestRefGoodShapeAccepts locks the
// canonical manifest_ref happy path: a non-nil pointer with
// well-formed fields MUST pass the validator. Pins the closed
// enum (`velox.render-manifest.v1`), the http(s)+velox-asset
// scheme allow-list, and the 64-lowercase-hex sha256 format in
// a single boundary case.
func TestSubmitJobValidateManifestRefGoodShapeAccepts(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "https://drive.google.com/file/d/MANIFEST/view",
		SHA256:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("well-formed manifest_ref MUST pass, got: bad=%v verr=%+v", bad, verr)
	}
}

// TestSubmitJobValidateManifestRefRejectsBadSchemaVersion pins
// the closed-enum rejection at the schema_version boundary. The
// test uses a value that LOOKS plausible ("v2", "v1.1") so a
// future refactor that widens the enum to a regex catches the
// regression at the wire level rather than silently accepting
// a future-version marker that no resolver can decode.
func TestSubmitJobValidateManifestRefRejectsBadSchemaVersion(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v2", // not in enum
		URL:           "https://drive.google.com/file/d/MANIFEST/view",
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("bad schema_version MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.schema_version" && d["issue"] == "unsupported_value" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.schema_version, issue:unsupported_value}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateManifestRefRejectsBadScheme pins the
// http(s) + velox-asset:// scheme allow-list. A file:// URL
// would silently bypass the SSRF blocklist (different layer)
// and a javascript: URL is a known exfiltration vector; both
// MUST be rejected at the wire-shape layer.
func TestSubmitJobValidateManifestRefRejectsBadScheme(t *testing.T) {
	t.Parallel()

	badURLs := []string{
		"file:///etc/passwd",
		"javascript:alert(1)",
		"data:text/plain,hello",
		"ftp://example.com/manifest",
		"ssh://example.com",
		"not-a-url",
	}
	for _, u := range badURLs {
		u := u
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			req := makeValidSubmitJobRequest()
			req.ManifestRef = &SubmitManifestRef{
				SchemaVersion: "velox.render-manifest.v1",
				URL:           u,
				SHA256:        strings.Repeat("a", 64),
			}
			verr, bad := ValidateSubmitJobRequest(req)
			if !bad || verr == nil || len(verr.Details) == 0 {
				t.Fatalf("URL %q MUST be rejected, got: bad=%v verr=%+v", u, bad, verr)
			}
			found := false
			for _, d := range verr.Details {
				if d["path"] == "manifest_ref.url" && d["issue"] == "unsupported_scheme" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected details entry {path:manifest_ref.url, issue:unsupported_scheme}, got: %+v", verr.Details)
			}
		})
	}
}

// TestSubmitJobValidateManifestRefAcceptsAllAllowedSchemes
// pins the positive boundary of the scheme allow-list —
// http, https, velox-asset. A future contributor that drops
// velox-asset:// from the allow-list (or adds e.g. file://)
// is caught at the wire-shape layer rather than at the SSRF
// layer (where the failure mode is a silent exfil path).
func TestSubmitJobValidateManifestRefAcceptsAllAllowedSchemes(t *testing.T) {
	t.Parallel()

	goodURLs := []string{
		"https://drive.google.com/file/d/X/view",
		"http://example.com/manifest.json",
		"velox-asset://manifests/abc123.json",
	}
	for _, u := range goodURLs {
		u := u
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			req := makeValidSubmitJobRequest()
			req.ManifestRef = &SubmitManifestRef{
				SchemaVersion: "velox.render-manifest.v1",
				URL:           u,
				SHA256:        strings.Repeat("a", 64),
			}
			if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
				t.Fatalf("URL %q MUST be accepted, got: bad=%v verr=%+v", u, bad, verr)
			}
		})
	}
}

// TestSubmitJobValidateManifestRefRejectsBadSHA256 pins the
// 64-lowercase-hex sha256 format. Multiple failure modes in
// one table: too short, too long, uppercase (sha256 of an
// always-lowercase canonical manifest is required because
// the resolver will compare byte-for-byte), non-hex chars,
// empty. A drift in the regex silently flips the runtime
// check to "any string accepted" — the test catches that.
func TestSubmitJobValidateManifestRefRejectsBadSHA256(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		hash string
	}{
		{"too_short", strings.Repeat("a", 63)},
		{"too_long", strings.Repeat("a", 65)},
		{"uppercase", strings.Repeat("A", 64)},
		{"mixed_case", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeF"},
		{"non_hex", strings.Repeat("z", 64)},
		{"empty", ""},
		{"with_0x_prefix", "0x" + strings.Repeat("a", 62)},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			req := makeValidSubmitJobRequest()
			req.ManifestRef = &SubmitManifestRef{
				SchemaVersion: "velox.render-manifest.v1",
				URL:           "https://drive.google.com/file/d/MANIFEST/view",
				SHA256:        c.hash,
			}
			verr, bad := ValidateSubmitJobRequest(req)
			if !bad || verr == nil || len(verr.Details) == 0 {
				t.Fatalf("sha256 %q MUST be rejected (case=%s), got: bad=%v verr=%+v",
					c.hash, c.name, bad, verr)
			}
			found := false
			for _, d := range verr.Details {
				if d["path"] == "manifest_ref.sha256" && d["issue"] == "malformed" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected details entry {path:manifest_ref.sha256, issue:malformed}, got: %+v", verr.Details)
			}
		})
	}
}

// TestSubmitJobValidateManifestRefRejectsEmptyURL pins the
// explicit-empty URL boundary. A client that supplies
// {"manifest_ref": {"url": "", ...}} MUST be rejected at the
// wire-shape layer (not silently forwarded to the resolver,
// where the empty URL would surface as a 500 from the HTTP
// fetch layer).
func TestSubmitJobValidateManifestRefRejectsEmptyURL(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "   ", // whitespace-only → trimmed to empty
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("empty URL MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.url" && d["issue"] == "empty" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.url, issue:empty}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateManifestRefAggregatesAllViolations locks
// the validator's "report everything" contract: a client that
// submits a manifest_ref with ALL three fields malformed MUST
// receive ONE 422 with details[0..2] populated, NOT a
// first-failure short-circuit. Same shape contract as the
// scenes/delivery_plan aggregations above.
func TestSubmitJobValidateManifestRefAggregatesAllViolations(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v9", // unsupported
		URL:           "file:///etc/passwd",       // unsupported scheme
		SHA256:        "abc",                      // malformed
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil {
		t.Fatalf("want rejection, got: bad=%v verr=%+v", bad, verr)
	}
	wantPaths := map[string]bool{
		"manifest_ref.schema_version": false,
		"manifest_ref.url":            false,
		"manifest_ref.sha256":         false,
	}
	for _, d := range verr.Details {
		if p, ok := d["path"].(string); ok {
			if _, expected := wantPaths[p]; expected {
				wantPaths[p] = true
			}
		}
	}
	for p, seen := range wantPaths {
		if !seen {
			t.Errorf("expected details path %q in aggregated violation report, got: %+v", p, verr.Details)
		}
	}
}

// TestSubmitJobValidateManifestRefEmptyObjectAggregatesThreeViolations
// pins the "non-nil pointer but every nested field empty" boundary:
// a client that sends `{"manifest_ref": {}}` (a JSON object with
// no nested keys) MUST be rejected with ALL three violations
// aggregated, not silently accepted by the validator. This is the
// exact failure mode the *SubmitManifestRef pointer indirection
// exists to distinguish from "field omitted entirely" (the nil
// pointer case, covered by TestSubmitJobValidateManifestRefNilAccepts).
func TestSubmitJobValidateManifestRefEmptyObjectAggregatesThreeViolations(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{} // all three fields empty
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil {
		t.Fatalf("empty manifest_ref MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	wantPaths := map[string]bool{
		"manifest_ref.schema_version": false,
		"manifest_ref.url":            false,
		"manifest_ref.sha256":         false,
	}
	for _, d := range verr.Details {
		if p, ok := d["path"].(string); ok {
			if _, expected := wantPaths[p]; expected {
				wantPaths[p] = true
			}
		}
	}
	for p, seen := range wantPaths {
		if !seen {
			t.Errorf("expected details path %q in aggregated violation report, got: %+v", p, verr.Details)
		}
	}
}

// TestSubmitJobValidateManifestRefRejectsEmptySchemaVersion pins
// the closed-enum rejection for the empty-string boundary. A client
// that supplies `{"schema_version": ""}` MUST be rejected — the
// enum contains only `velox.render-manifest.v1` and an empty
// string is not a member. Without this test, a future refactor that
// drops the empty check (e.g., switching to a regex without `+`
// quantifier) silently accepts malformed manifests.
func TestSubmitJobValidateManifestRefRejectsEmptySchemaVersion(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "",
		URL:           "https://drive.google.com/file/d/MANIFEST/view",
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("empty schema_version MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.schema_version" && d["issue"] == "unsupported_value" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.schema_version, issue:unsupported_value}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateManifestRefURLWhitespaceTrimmed pins the
// canonical trim policy: a URL padded with surrounding whitespace
// MUST pass the validator (after trim) — not be rejected by the
// regex. Without this test, a future refactor that drops the
// strings.TrimSpace call silently rejects URLs the spec advertises
// as valid (the regex anchors with `^(https?://|velox-asset://)`,
// so a leading space breaks the match).
func TestSubmitJobValidateManifestRefURLWhitespaceTrimmed(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "   https://drive.google.com/file/d/MANIFEST/view   ",
		SHA256:        strings.Repeat("a", 64),
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("URL with surrounding whitespace MUST be accepted (trim policy), got: bad=%v verr=%+v", bad, verr)
	}
}

// TestSubmitJobValidateManifestRefURLMaxLengthBoundary locks the
// byte-cap boundary at MaxManifestRefURLBytes (= 2048). A URL of
// exactly MaxManifestRefURLBytes bytes MUST pass; a URL of
// MaxManifestRefURLBytes+1 bytes MUST be rejected with
// details[].issue="max_length". Without this test, a future bump
// on one side of the drift-guard (apiwire tag vs handler constant)
// silently widens or narrows the cap without a test signal.
//
// The drift guard in apiwire_test.go
// (TestSubmitManifestRef_MaxLengthMatchesHandlerConstant) pins the
// numeric value; this test pins the runtime boundary on the handler
// side. Together they cover both sides of the drift vector.
func TestSubmitJobValidateManifestRefURLMaxLengthBoundary(t *testing.T) {
	t.Parallel()

	// (1) Exactly MaxManifestRefURLBytes bytes — MUST pass. The
	// scheme prefix + a tail of "a" characters to hit the cap.
	exactURL := "https://example.com/" + strings.Repeat("a", MaxManifestRefURLBytes-len("https://example.com/"))
	if len(exactURL) != MaxManifestRefURLBytes {
		t.Fatalf("test fixture broken: exactURL length = %d, want %d", len(exactURL), MaxManifestRefURLBytes)
	}
	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           exactURL,
		SHA256:        strings.Repeat("a", 64),
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("URL of exactly %d bytes MUST be accepted, got: bad=%v verr=%+v",
			MaxManifestRefURLBytes, bad, verr)
	}

	// (2) One byte over the cap — MUST be rejected.
	overURL := "https://example.com/" + strings.Repeat("a", MaxManifestRefURLBytes-len("https://example.com/")+1)
	if len(overURL) != MaxManifestRefURLBytes+1 {
		t.Fatalf("test fixture broken: overURL length = %d, want %d", len(overURL), MaxManifestRefURLBytes+1)
	}
	req = makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           overURL,
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("URL of %d bytes (over cap) MUST be rejected, got: bad=%v verr=%+v",
			len(overURL), bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.url" && d["issue"] == "max_length" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.url, issue:max_length}, got: %+v", verr.Details)
	}
}
