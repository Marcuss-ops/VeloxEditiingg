package assetref

import "testing"

func TestParseCanonicalWireClassifiesSchemes(t *testing.T) {
	local, err := ParseCanonicalWire("VELOX-ASSET://asset-1")
	if err != nil || local.Kind() != RefKindLocal || local.ID() != "asset-1" {
		t.Fatalf("local = %#v, %v", local, err)
	}
	deferred, err := ParseCanonicalWire("velox-drive://drive-file-1")
	if err != nil || deferred.Kind() != RefKindDeferredDrive || deferred.ID() != "drive-file-1" {
		t.Fatalf("deferred = %#v, %v", deferred, err)
	}
}

func TestParseCanonicalWireRejectsBareAndExternalReferences(t *testing.T) {
	for _, raw := range []string{"asset-1", "https://example.com/asset"} {
		if _, err := ParseCanonicalWire(raw); err == nil {
			t.Fatalf("ParseCanonicalWire(%q) accepted non-wire input", raw)
		}
	}
	if IsLocalWire("velox-drive://drive-file-1") {
		t.Fatal("deferred Drive wire classified as local")
	}
}
