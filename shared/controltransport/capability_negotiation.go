package controltransport

// HasCapability reports whether a capability is explicitly advertised with a
// true value in the protobuf Struct projection. Missing, false, or malformed
// values are all treated as unsupported.
func HasCapability(capabilities map[string]interface{}, capability string) bool {
	if capabilities == nil || capability == "" {
		return false
	}
	value, ok := capabilities[capability]
	supported, ok := value.(bool)
	return ok && supported
}

// HasProgressiveUploadCapability is the fail-closed admission predicate used
// by both worker and master progressive-upload paths.
func HasProgressiveUploadCapability(capabilities map[string]interface{}) bool {
	return HasCapability(capabilities, CapabilityArtifactProgressiveUploadV1)
}
