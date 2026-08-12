package grpcserver

import (
	"reflect"
	"testing"
)

func TestFutureAssetManifestsIsDeterministicallyOrdered(t *testing.T) {
	payload := []byte(`{
		"audio": {"asset_key": "z-audio", "sha256": "sha-z", "size_bytes": 20},
		"video": [
			{"asset_key": "a-video", "sha256": "sha-a", "size_bytes": 10},
			{"asset_key": "z-audio", "sha256": "sha-z", "size_bytes": 20}
		]
	}`)

	want := futureAssetManifests(payload)
	if len(want) != 2 {
		t.Fatalf("manifest length=%d, want 2", len(want))
	}
	if want[0].AssetKey != "a-video" || want[1].AssetKey != "z-audio" {
		t.Fatalf("manifest order=%q,%q, want a-video,z-audio", want[0].AssetKey, want[1].AssetKey)
	}
	for i := 0; i < 20; i++ {
		if got := futureAssetManifests(payload); !reflect.DeepEqual(got, want) {
			t.Fatalf("manifest changed between runs: got=%v want=%v", got, want)
		}
	}
}
