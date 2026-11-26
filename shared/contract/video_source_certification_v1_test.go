package contract

import (
	"strings"
	"testing"
)

func TestVideoSourceCertificationIdentityExcludesJobAndWindow(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	revision, err := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := VideoSourceCertificationV1{Version: VideoSourceCertificationVersion, Source: AssetRefV2{AssetID: "one-job-id", Kind: "video", SHA256: strings.Repeat("a", 64), SizeBytes: 123}, OutputProfileID: profile.ProfileID, OutputProfileRevision: revision}
	first, err := VideoSourceCertificationIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Source.AssetID = "another-job-id"
	second, err := VideoSourceCertificationIdentity(request)
	if err != nil || first != second {
		t.Fatalf("content identity changed with job-local asset id: %q %q err=%v", first, second, err)
	}
	request.Source.SHA256 = strings.Repeat("b", 64)
	third, err := VideoSourceCertificationIdentity(request)
	if err != nil || third == first {
		t.Fatalf("changed bytes reused source identity: %q err=%v", third, err)
	}
}

func TestVideoSourceCertificationUnknownDigestIsLocatorScoped(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	revision, err := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := VideoSourceCertificationV1{Version: VideoSourceCertificationVersion, Source: AssetRefV2{AssetID: "source", SourceKind: "drive", DriveFileID: "drive-file", Kind: "video", SizeBytes: 123}, OutputProfileID: profile.ProfileID, OutputProfileRevision: revision}
	first, err := VideoSourceCertificationIdentity(request)
	if err != nil || !strings.HasPrefix(first, "discovery:") {
		t.Fatalf("provisional identity = %q err=%v", first, err)
	}
	request.Source.DriveFileID = "other-drive-file"
	second, err := VideoSourceCertificationIdentity(request)
	if err != nil || first == second {
		t.Fatalf("different provider locators shared provisional identity: %q %q err=%v", first, second, err)
	}
	request.Source.DriveFileID = ""
	if _, err := VideoSourceCertificationIdentity(request); err == nil {
		t.Fatal("unknown digest without authorized locator was accepted")
	}
}
