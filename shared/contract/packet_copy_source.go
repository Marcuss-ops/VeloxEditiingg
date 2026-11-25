package contract

// PacketCopySourceWindowCertified reports whether a source video asset carries
// enough immutable evidence for the native packet mux to consume a timeline
// window directly. The mux still checks that SourceInUS is an exact keyframe;
// this predicate only avoids materializing a separate MP4 fragment first.
func PacketCopySourceWindowCertified(
	plan *CompiledRenderPlanV2,
	segment VideoSegmentV2,
	asset AssetRefV2,
) bool {
	if plan == nil || asset.Kind != "video" || asset.AssetID != segment.AssetID ||
		asset.SHA256 == "" || asset.SHA256 != segment.SHA256 || asset.SizeBytes <= 0 ||
		asset.FrameCount <= 0 || asset.DurationUS <= 0 || asset.Width <= 0 || asset.Height <= 0 ||
		asset.ProfileID == "" || !asset.FirstFrameKeyframe || !asset.ClosedGOP ||
		asset.ExtradataSHA256 != CanonicalVideoExtradataSHA256 ||
		segment.SourceInUS < 0 || segment.SourceDurationUS <= 0 || segment.FrameCount <= 0 {
		return false
	}
	profile, err := KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil || asset.ProfileID != profile.StreamProfile() || asset.Width != profile.Width || asset.Height != profile.Height {
		return false
	}
	if segment.SourceDurationUS > asset.DurationUS || segment.SourceInUS > asset.DurationUS-segment.SourceDurationUS {
		return false
	}
	return true
}
