package pipeline

import (
	"strings"

	"github.com/gin-gonic/gin"
)

func validateSubmitManifestRef(req SubmitJobRequest) []gin.H {
	var details []gin.H
	// ManifestRef shape validation. Runs ONLY when the pointer is
	// non-nil — a nil pointer is the "client did not opt in" path
	// and MUST pass through this validator without complaint. When
	// the pointer is non-nil the body is treated as the canonical
	// shape contract: schema_version must be in the closed enum,
	// url must match the http(s) + velox-asset:// allow-list and
	// be 1..MaxManifestRefURLBytes after trim, sha256 must be
	// exactly 64 lowercase hex characters.
	//
	// The actual fetch + SHA-256 verification happens later in
	// ResolveRenderManifestRef; this layer is intentionally byte-level
	// only so the rejection paths are order-stable and a malformed
	// manifest_ref returns 422 invalid_payload BEFORE any downstream cost.
	if req.ManifestRef != nil {
		mr := req.ManifestRef
		mrPath := "manifest_ref"

		// schema_version must be in the closed enum. The allowed
		// list is the source of truth — changing it requires
		// bumping apiwire.SubmitManifestRef's `oneof` tag too, so
		// the wire schema and the runtime validator agree.
		if !containsString(manifestRefSchemaVersions, mr.SchemaVersion) {
			details = append(details, gin.H{
				"path":     mrPath + ".schema_version",
				"issue":    "unsupported_value",
				"observed": mr.SchemaVersion,
				"allowed":  manifestRefSchemaVersions,
			})
		}

		// url: 1..MaxManifestRefURLBytes after trim AND must match
		// the http(s) + velox-asset:// allow-list. The regex is
		// duplicated from the apiwire validate tag because the
		// schemagen cannot express the velox-asset:// scheme
		// natively; duplicating it here keeps the wire schema
		// and the runtime validator in lockstep.
		trimmedURL := strings.TrimSpace(mr.URL)
		if trimmedURL == "" {
			details = append(details, gin.H{
				"path":  mrPath + ".url",
				"issue": "empty",
			})
		} else if len(trimmedURL) > MaxManifestRefURLBytes {
			details = append(details, gin.H{
				"path":     mrPath + ".url",
				"issue":    "max_length",
				"max":      MaxManifestRefURLBytes,
				"observed": len(trimmedURL),
			})
		} else if !manifestRefURLRegexp.MatchString(trimmedURL) {
			details = append(details, gin.H{
				"path":     mrPath + ".url",
				"issue":    "unsupported_scheme",
				"observed": trimmedURL,
				"allowed":  []string{"https://", "http://", "velox-asset://"},
			})
		}

		// sha256: exactly 64 lowercase hex characters. The
		// hex-only check is intentionally strict (lowercase) so
		// a future drift to mixed case is caught at the wire
		// rather than silently producing a mismatch inside the
		// resolver.
		if !manifestRefSHA256Regexp.MatchString(mr.SHA256) {
			details = append(details, gin.H{
				"path":     mrPath + ".sha256",
				"issue":    "malformed",
				"observed": mr.SHA256,
				"expected": "64 lowercase hex characters ([0-9a-f]{64})",
			})
		}
	}

	return details
}
