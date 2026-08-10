package assetref

import (
	"encoding/json"
	"testing"
)

func TestAssetRefClassifiesLocalRemoteAndDeferredDrive(t *testing.T) {
	t.Parallel()

	local, err := Parse("velox-asset://local-asset")
	if err != nil {
		t.Fatal(err)
	}
	if local.Kind() != RefKindLocal || local.ID() != "local-asset" || local.Wire() != "velox-asset://local-asset" {
		t.Fatalf("local ref = %#v", local)
	}

	remote, err := Parse("https://cdn.example.test/clip.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if remote.Kind() != RefKindRemote || remote.ID() != "" || remote.Wire() != "https://cdn.example.test/clip.mp4" {
		t.Fatalf("remote ref = %#v", remote)
	}

	deferred, err := Parse("https://drive.google.com/file/d/ABC123/view")
	if err != nil {
		t.Fatal(err)
	}
	if deferred.Kind() != RefKindDeferredDrive || deferred.ID() != "ABC123" || deferred.Wire() != "velox-drive://ABC123" {
		t.Fatalf("deferred ref = %#v", deferred)
	}

	// The wire is self-sufficient: parsing the canonical wire yields the
	// same kind without any external annotation.
	selfSufficient, err := Parse("velox-drive://ABC123")
	if err != nil {
		t.Fatal(err)
	}
	if selfSufficient.Kind() != RefKindDeferredDrive || selfSufficient.ID() != "ABC123" {
		t.Fatalf("self-sufficient deferred ref = %#v", selfSufficient)
	}
}

func TestAssetRefJSONWireCarriesKindByScheme(t *testing.T) {
	t.Parallel()

	deferred, err := NewDeferredDrive("drive-file-123456")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(map[string]interface{}{"url": deferred})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"url":"velox-drive://drive-file-123456"}`; got != want {
		t.Fatalf("JSON = %s, want %s", got, want)
	}

	var decoded AssetRef
	if err := json.Unmarshal([]byte(`"velox-asset://local-asset"`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Kind() != RefKindLocal || decoded.Wire() != "velox-asset://local-asset" {
		t.Fatalf("decoded = %#v", decoded)
	}

	if err := json.Unmarshal([]byte(`"velox-drive://drive-file-123456"`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Kind() != RefKindDeferredDrive || decoded.ID() != "drive-file-123456" {
		t.Fatalf("decoded deferred = %#v", decoded)
	}
}

func TestParseWireHonorsDeferredDriveAnnotation(t *testing.T) {
	t.Parallel()
	ref, err := ParseWire("velox-asset://drive-file-123456", RefKindDeferredDrive)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.IsDeferredDrive() || ref.ID() != "drive-file-123456" {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestWireAssetIDSchemes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ref   string
		id    string
		valid bool
	}{
		{"velox-asset://clip-1", "clip-1", true},
		{"velox-asset://clips/scene1.mp4", "clips/scene1.mp4", true},
		{"velox-drive://drive-file-123456", "drive-file-123456", true},
		{"VELOX-DRIVE://UPPER-CASE", "UPPER-CASE", true},
		{"https://cdn.example.test/clip.mp4", "", false},
		{"http://cdn.example.test/clip.mp4", "", false},
		{"clip-1", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := WireAssetID(tc.ref)
		if ok != tc.valid || got != tc.id {
			t.Errorf("WireAssetID(%q) = (%q, %v), want (%q, %v)", tc.ref, got, ok, tc.id, tc.valid)
		}
	}
}
