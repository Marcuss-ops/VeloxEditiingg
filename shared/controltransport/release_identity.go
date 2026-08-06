package controltransport

// release_identity.go — the single release certificate shared by CI, the
// worker agent and the master.
//
// ReleaseIdentity is generated once in CI (worker-image.yml baseline
// manifest), baked into the published image, advertised by the worker at
// register/hello time (and refreshed on heartbeat), registered by the
// master, exposed via the admin API and compared during rollout.
//
// The wire vehicle is the worker capabilities map: the Capabilities
// structpb field on Hello/Heartbeat carries an opaque JSON-shaped map, so a
// new certificate key rides without a protobuf regen. The canonical block is
// emitted under CapabilityReleaseIdentityKey; the master ALSO keeps reading
// the flat legacy keys (git_sha, docker_image_digest) that the worker runtime
// snapshot has always consumed, so the two views cannot drift.

import (
	"encoding/json"
	"strings"
)

// CapabilityReleaseIdentityKey is the canonical capabilities-map key under
// which the worker publishes its ReleaseIdentity certificate.
const CapabilityReleaseIdentityKey = "release_identity"

// Flat legacy keys the master runtime snapshot reads from the same
// capabilities map. The worker keeps emitting them so pre-existing snapshot
// columns stay populated without a second source of truth.
const (
	CapabilityKeyGitSHA            = "git_sha"
	CapabilityKeyDockerImageDigest = "docker_image_digest"
	CapabilityKeyEngineVersion     = "engine_version"
)

// ReleaseIdentity is the single canonical release certificate. Every field
// is sourced once (CI build of the published image) and carried through the
// worker register → master registry → admin API chain. Empty fields mean
// "not part of the certified evidence" — consumers fail closed instead of
// inferring values from tags, local files, or sibling fields.
type ReleaseIdentity struct {
	ImageDigest      string `json:"image_digest,omitempty"`
	SourceCommit     string `json:"source_commit,omitempty"`
	SourceHash       string `json:"source_hash,omitempty"`
	BundleHash       string `json:"bundle_hash,omitempty"`
	EngineSHA256     string `json:"engine_sha256,omitempty"`
	SoftwareVersion  string `json:"software_version,omitempty"`
	ProtocolVersion  string `json:"protocol_version,omitempty"`
	CapabilitySchema int    `json:"capability_schema"`
}

// IsEmpty reports whether no certificate field was populated. Callers use it
// to distinguish "worker did not advertise a release certificate yet" from a
// certificate whose (optional) hash fields are empty.
func (r ReleaseIdentity) IsEmpty() bool {
	return r.ImageDigest == "" &&
		r.SourceCommit == "" &&
		r.SourceHash == "" &&
		r.BundleHash == "" &&
		r.EngineSHA256 == "" &&
		r.SoftwareVersion == "" &&
		r.ProtocolVersion == "" &&
		r.CapabilitySchema == 0
}

// Validate returns a non-nil error when a non-empty hash field does not look
// like a 64-hex SHA-256 digest. Version/commit fields are free-form strings;
// only the cryptographic evidence is shape-checked.
func (r ReleaseIdentity) Validate() error {
	for field, value := range map[string]string{
		"image_digest":  strings.TrimPrefix(r.ImageDigest, "sha256:"),
		"source_hash":   r.SourceHash,
		"bundle_hash":   r.BundleHash,
		"engine_sha256": r.EngineSHA256,
	} {
		if value == "" {
			continue
		}
		if !isLowerHex64(value) {
			return &ReleaseIdentityError{Field: field, Value: value}
		}
	}
	return nil
}

func isLowerHex64(v string) bool {
	if len(v) != 64 {
		return false
	}
	for _, c := range v {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// ReleaseIdentityError reports a malformed field in a release certificate.
type ReleaseIdentityError struct {
	Field string
	Value string
}

func (e *ReleaseIdentityError) Error() string {
	return "release identity: " + e.Field + " is not a 64-lowercase-hex SHA-256 digest"
}

// AsCapabilitiesBlock returns the canonical map published under
// CapabilityReleaseIdentityKey in the worker capabilities map.
func (r ReleaseIdentity) AsCapabilitiesBlock() map[string]interface{} {
	return map[string]interface{}{
		"image_digest":      r.ImageDigest,
		"source_commit":     r.SourceCommit,
		"source_hash":       r.SourceHash,
		"bundle_hash":       r.BundleHash,
		"engine_sha256":     r.EngineSHA256,
		"software_version":  r.SoftwareVersion,
		"protocol_version":  r.ProtocolVersion,
		"capability_schema": r.CapabilitySchema,
	}
}

// FlatLegacyKeys returns the {git_sha, docker_image_digest} entries the
// master runtime snapshot has always consumed, derived from the certificate.
// The worker emits them alongside the canonical block so the snapshot columns
// and the typed certificate stay in lock-step.
//
// engine_version is intentionally NOT emitted here: the master reads it from
// the Hello/Heartbeat proto field (hello.GetEngineVersion), never from the
// capabilities map, and SoftwareVersion (w.version) is not the engine
// version — emitting it would create a misleading second source. The constant
// remains for the legacy *parse* fallback (ReleaseIdentityFromCapabilities).
func (r ReleaseIdentity) FlatLegacyKeys() map[string]interface{} {
	return map[string]interface{}{
		CapabilityKeyGitSHA:            r.SourceCommit,
		CapabilityKeyDockerImageDigest: r.ImageDigest,
	}
}

// ReleaseIdentityFromCapabilities parses the canonical certificate block out
// of a capabilities map. It prefers the canonical CapabilityReleaseIdentityKey
// block and falls back to the flat legacy keys so mixed-version workers are
// tolerated. The returned bool is false when no field could be populated.
func ReleaseIdentityFromCapabilities(caps map[string]interface{}) (ReleaseIdentity, bool) {
	var ri ReleaseIdentity
	found := false

	if block, ok := caps[CapabilityReleaseIdentityKey]; ok {
		if m, ok := block.(map[string]interface{}); ok {
			ri.ImageDigest = stringOf(m["image_digest"])
			ri.SourceCommit = stringOf(m["source_commit"])
			ri.SourceHash = stringOf(m["source_hash"])
			ri.BundleHash = stringOf(m["bundle_hash"])
			ri.EngineSHA256 = stringOf(m["engine_sha256"])
			ri.SoftwareVersion = stringOf(m["software_version"])
			ri.ProtocolVersion = stringOf(m["protocol_version"])
			ri.CapabilitySchema = intOf(m["capability_schema"])
			found = !ri.IsEmpty()
		}
	}

	if !found {
		legacy := ReleaseIdentity{
			SourceCommit:     stringOf(caps[CapabilityKeyGitSHA]),
			ImageDigest:      stringOf(caps[CapabilityKeyDockerImageDigest]),
			SoftwareVersion:  stringOf(caps[CapabilityKeyEngineVersion]),
			ProtocolVersion:  stringOf(caps["protocol_version"]),
			BundleHash:       stringOf(caps["bundle_hash"]),
			CapabilitySchema: intOf(caps["schema_version"]),
		}
		if !legacy.IsEmpty() {
			ri = legacy
			found = true
		}
	}
	return ri, found
}

func stringOf(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func intOf(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return int(i)
		}
	}
	return 0
}
