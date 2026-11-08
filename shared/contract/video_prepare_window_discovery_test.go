package contract

import (
	"strings"
	"testing"
)

func discoveryWindowRequest() VideoPrepareWindowV1 {
	return VideoPrepareWindowV1{
		Version: VideoPrepareWindowVersion,
		Source: AssetRefV2{
			AssetID: "drive-source", Kind: "video", SourceKind: "drive",
			DriveFileID: "drive-file-1", SizeBytes: 1_000_000, MIME: "video/mp4",
		},
		SourceIdentityDiscovery: true,
		SourceStartUS:           1_250_000, SourceDurationUS: 3_125_000,
		OutputFrameCount: 75, TimelineRevision: 4,
		TimelineSHA256: strings.Repeat("a", 64), TimelineStartFrame: 0,
		OutputProfileID:       CanonicalVideoProfileV1Default.ProfileID,
		OutputProfileRevision: "video-window-preparer.v1:" + strings.Repeat("b", 64),
	}
}

func TestVideoPrepareWindowDiscoveryIdentityIsNotAContentCacheKey(t *testing.T) {
	request := discoveryWindowRequest()
	if err := request.Validate(); err != nil {
		t.Fatalf("discovery request must validate: %v", err)
	}
	if _, err := VideoPrepareWindowCacheIdentity(request); err == nil {
		t.Fatal("a discovery request must not produce a content cache identity")
	}
	first, err := VideoPrepareWindowDiscoveryIdentity(request)
	if err != nil || len(first) != 64 {
		t.Fatalf("discovery identity = %q err=%v", first, err)
	}
	second, err := VideoPrepareWindowDiscoveryIdentity(request)
	if err != nil || first != second {
		t.Fatalf("discovery identity is not deterministic: %q vs %q (%v)", first, second, err)
	}
	// Timeline placement is not part of the preparation identity: the same
	// window at another placement reuses one discovery task.
	moved := request
	moved.TimelineStartFrame = 75
	movedIdentity, err := VideoPrepareWindowDiscoveryIdentity(moved)
	if err != nil || movedIdentity != first {
		t.Fatalf("timeline placement changed the discovery identity: %q vs %q (%v)", movedIdentity, first, err)
	}
	// A pre-declared SHA is contradictory and must fail closed.
	contradictory := request
	contradictory.Source.SHA256 = strings.Repeat("c", 64)
	if err := contradictory.Validate(); err == nil {
		t.Fatal("discovery request accepted a pre-declared SHA-256")
	}
}

func discoveryWindowCertificate(request VideoPrepareWindowV1, sourceSHA string) VideoPrepareWindowCertificateV1 {
	return VideoPrepareWindowCertificateV1{
		Version: VideoPrepareWindowVersion, SourceSHA256: sourceSHA,
		SourceAssetID: request.Source.AssetID, SourceSizeBytes: request.Source.SizeBytes,
		SourceStartUS: request.SourceStartUS, SourceDurationUS: request.SourceDurationUS,
		OutputFrameCount: request.OutputFrameCount, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision,
		EncoderVersion:        "ffmpeg 6.0", EncoderBuildSHA256: strings.Repeat("d", 64),
		OutputSHA256: strings.Repeat("e", 64), OutputSizeBytes: 4096,
		ClosedGOPAttested: true,
		OutputEvidence: VideoWindowOutputEvidenceV1{
			Codec: "h264", FrameCount: request.OutputFrameCount, FirstFrameKeyframe: true,
			ExtradataSHA256: strings.Repeat("f", 64), Width: 1, Height: 1,
			FPSNum: 24, FPSDen: 1, TimeBaseNum: 1, TimeBaseDen: 90000,
		},
		TimelineRevision: request.TimelineRevision, TimelineSHA256: request.TimelineSHA256,
		TimelineStartFrame: request.TimelineStartFrame,
	}
}

func TestVideoPrepareWindowDiscoveryCertificateBindsDiscoveredSource(t *testing.T) {
	request := discoveryWindowRequest()
	discovered := strings.Repeat("c", 64)
	certificate := discoveryWindowCertificate(request, discovered)
	if err := certificate.ValidateFor(request); err != nil {
		t.Fatalf("discovery certificate must validate: %v", err)
	}
	// The resolved identity is the content identity; the same certificate
	// validates against the resolved request the worker publishes.
	resolved := request
	resolved.Source.SHA256 = discovered
	resolved.SourceIdentityDiscovery = false
	if err := certificate.ValidateFor(resolved); err != nil {
		t.Fatalf("resolved certificate must validate: %v", err)
	}
	if _, err := VideoPrepareWindowCacheIdentity(resolved); err != nil {
		t.Fatalf("resolved content identity: %v", err)
	}
	// A certificate without a discovered SHA cannot close a discovery task.
	if err := discoveryWindowCertificate(request, "").ValidateFor(request); err == nil {
		t.Fatal("discovery certificate without a source SHA must be rejected")
	}
	// Non-discovery requests still require an exact SHA match.
	other := resolved
	other.Source.SHA256 = strings.Repeat("9", 64)
	if err := certificate.ValidateFor(other); err == nil {
		t.Fatal("certificate must not validate against a different source SHA")
	}
}
