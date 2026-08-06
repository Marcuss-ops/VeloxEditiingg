package controltransport

import (
	"strings"
	"testing"
)

func stringsRepeat(s string, n int) string {
	return strings.Repeat(s, n)
}

func TestReleaseIdentity_RoundTripThroughCapabilities(t *testing.T) {
	ri := ReleaseIdentity{
		ImageDigest:      "sha256:" + stringsRepeat("a", 64),
		SourceCommit:     "fbbf7c1",
		SourceHash:       stringsRepeat("b", 64),
		BundleHash:       stringsRepeat("c", 64),
		EngineSHA256:     stringsRepeat("d", 64),
		SoftwareVersion:  "v1.2.20",
		ProtocolVersion:  "v3",
		CapabilitySchema: CapabilitySchemaVersion,
	}
	if err := ri.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	caps := map[string]interface{}{
		CapabilityReleaseIdentityKey: ri.AsCapabilitiesBlock(),
	}
	got, ok := ReleaseIdentityFromCapabilities(caps)
	if !ok {
		t.Fatal("ReleaseIdentityFromCapabilities: not found")
	}
	if got != ri {
		t.Fatalf("round trip mismatch:\n got=%+v\nwant=%+v", got, ri)
	}
}

func TestReleaseIdentity_FlatLegacyKeys(t *testing.T) {
	ri := ReleaseIdentity{
		ImageDigest:     "sha256:" + stringsRepeat("a", 64),
		SourceCommit:    "abc1234",
		SoftwareVersion: "v1.2.20",
	}
	keys := ri.FlatLegacyKeys()
	if keys[CapabilityKeyGitSHA] != "abc1234" {
		t.Fatalf("git_sha=%v want abc1234", keys[CapabilityKeyGitSHA])
	}
	if keys[CapabilityKeyDockerImageDigest] != "sha256:"+stringsRepeat("a", 64) {
		t.Fatalf("docker_image_digest=%v", keys[CapabilityKeyDockerImageDigest])
	}
	// engine_version must NOT be emitted: the master reads it from the
	// Hello/Heartbeat proto field, and SoftwareVersion (w.version) is not
	// the engine version. Emitting it would create a misleading second
	// source.
	if _, present := keys[CapabilityKeyEngineVersion]; present {
		t.Fatalf("engine_version must not be emitted as a flat legacy key")
	}
}

func TestReleaseIdentity_FromFlatLegacyKeys(t *testing.T) {
	caps := map[string]interface{}{
		CapabilityKeyGitSHA:            "abc1234",
		CapabilityKeyDockerImageDigest: "sha256:" + stringsRepeat("e", 64),
		CapabilityKeyEngineVersion:     "v1.2.20",
		"protocol_version":             "v3",
		"schema_version":               float64(CapabilitySchemaVersion),
	}
	got, ok := ReleaseIdentityFromCapabilities(caps)
	if !ok {
		t.Fatal("legacy fallback not detected")
	}
	if got.SourceCommit != "abc1234" {
		t.Fatalf("SourceCommit=%q want abc1234", got.SourceCommit)
	}
	if got.ImageDigest != "sha256:"+stringsRepeat("e", 64) {
		t.Fatalf("ImageDigest=%q", got.ImageDigest)
	}
	if got.SoftwareVersion != "v1.2.20" {
		t.Fatalf("SoftwareVersion=%q", got.SoftwareVersion)
	}
	if got.ProtocolVersion != "v3" {
		t.Fatalf("ProtocolVersion=%q", got.ProtocolVersion)
	}
	if got.CapabilitySchema != CapabilitySchemaVersion {
		t.Fatalf("CapabilitySchema=%d want %d", got.CapabilitySchema, CapabilitySchemaVersion)
	}
}

func TestReleaseIdentity_ValidateRejectsBadHashes(t *testing.T) {
	good := stringsRepeat("a", 64)
	for _, tc := range []struct {
		name  string
		field string
		value string
	}{
		{"short source", "source_hash", stringsRepeat("a", 63)},
		{"upper bundle", "bundle_hash", stringsRepeat("A", 64)},
		{"non-hex engine", "engine_sha256", stringsRepeat("g", 64)},
		{"image digest non-hex", "image_digest", "sha256:" + stringsRepeat("z", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ri := ReleaseIdentity{SourceHash: good, BundleHash: good, EngineSHA256: good, ImageDigest: "sha256:" + good}
			switch tc.field {
			case "source_hash":
				ri.SourceHash = tc.value
			case "bundle_hash":
				ri.BundleHash = tc.value
			case "engine_sha256":
				ri.EngineSHA256 = tc.value
			case "image_digest":
				ri.ImageDigest = tc.value
			}
			if err := ri.Validate(); err == nil {
				t.Fatalf("Validate accepted invalid %s=%q", tc.field, tc.value)
			}
		})
	}
}

func TestReleaseIdentity_Empty(t *testing.T) {
	var ri ReleaseIdentity
	if !ri.IsEmpty() {
		t.Fatal("zero ReleaseIdentity must be empty")
	}
	if _, ok := ReleaseIdentityFromCapabilities(map[string]interface{}{}); ok {
		t.Fatal("empty capabilities must not produce a certificate")
	}
}
