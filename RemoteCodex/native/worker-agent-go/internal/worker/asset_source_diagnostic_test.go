package worker

import "testing"

func TestAssetSourceDiagnosticLabel(t *testing.T) {
	for _, tc := range []struct {
		name, source, want string
	}{
		{name: "bridge fallback", want: "master_asset_bridge"},
		{name: "signed direct source", source: "https://77.93.152.122/objects/secret?sig=private", want: "worker_direct_source host=77.93.152.122"},
		{name: "invalid source", source: "not a URL", want: "worker_direct_source host=invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := assetSourceDiagnosticLabel(tc.source); got != tc.want {
				t.Fatalf("assetSourceDiagnosticLabel() = %q, want %q", got, tc.want)
			}
		})
	}
}
