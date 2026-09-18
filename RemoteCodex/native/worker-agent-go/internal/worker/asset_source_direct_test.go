package worker

import "testing"

func TestAssetTransferRequestForSourceUsesDirectDriveWithoutMasterToken(t *testing.T) {
	url, token, client, err := (&masterAssetTransferer{}).assetTransferRequestForSource("drive-1", "https://drive.google.com/uc?export=download&id=drive-1")
	if err != nil {
		t.Fatal(err)
	}
	if url == "" || token == nil || token() != "" || client == nil {
		t.Fatalf("direct source wiring = url=%q token_present=%t client=%v", url, token != nil, client)
	}
}

func TestAssetTransferRequestForSourceRejectsNonDriveLocator(t *testing.T) {
	_, _, _, err := (&masterAssetTransferer{}).assetTransferRequestForSource("drive-1", "https://example.invalid/asset")
	if err == nil {
		t.Fatal("non-Drive source was accepted")
	}
}

func TestAllowedDirectDriveHostIncludesPublicDownloadRedirect(t *testing.T) {
	for _, host := range []string{
		"drive.google.com",
		"www.googleapis.com",
		"drive.usercontent.google.com",
	} {
		if !isAllowedDirectDriveHost(host) {
			t.Fatalf("host %q was rejected", host)
		}
	}
	for _, host := range []string{"evil.example", "usercontent.google.com", "drive.usercontent.google.com.evil.example"} {
		if isAllowedDirectDriveHost(host) {
			t.Fatalf("host %q was accepted", host)
		}
	}
}
