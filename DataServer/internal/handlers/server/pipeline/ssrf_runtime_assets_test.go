package pipeline

import (
	"testing"

	"velox-server/internal/config"
)

func TestValidateRuntimeAssetSourceURLs(t *testing.T) {
	cfg := &config.Config{}
	if errs := ValidateRuntimeAssetSourceURLs([]map[string]interface{}{{
		"source_uri": "https://93.184.216.34/objects/abc?sig=opaque",
		"url":        "https://93.184.216.34/objects/abc?sig=opaque",
	}}, cfg); len(errs) != 0 {
		t.Fatalf("valid signed HTTPS source rejected: %+v", errs)
	}
	errs := ValidateRuntimeAssetSourceURLs([]map[string]interface{}{{
		"source_uri": "http://127.0.0.1/private",
	}}, cfg)
	if len(errs) != 1 || errs[0].Reason != "ip_loopback" {
		t.Fatalf("loopback source errors=%+v, want one ip_loopback rejection", errs)
	}
}
