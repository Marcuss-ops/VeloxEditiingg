package api

import (
	"strings"
	"testing"

	"velox-server/internal/fleet"
)

func TestBindMutationRequestAcceptsDownloadConcurrency(t *testing.T) {
	_, err := bindConfigBody(t, fleet.OperationKindRestart, `{"asset_download_concurrency":30,"prefetch_max_concurrent":29}`)
	if err != nil {
		t.Fatalf("bind download tuning: %v", err)
	}
}

func TestBindMutationRequestAcceptsCacheIdleTTL(t *testing.T) {
	req, err := bindConfigBody(t, fleet.OperationKindRestart, `{"cache_idle_ttl_seconds":86400}`)
	if err != nil {
		t.Fatalf("bind cache TTL: %v", err)
	}
	if req.CacheIdleTTLSeconds == nil || *req.CacheIdleTTLSeconds != 86400 {
		t.Fatalf("cache idle TTL = %v, want 86400 seconds", req.CacheIdleTTLSeconds)
	}
}

func TestBindMutationRequestRejectsUnsafeDownloadConcurrency(t *testing.T) {
	cases := []string{
		`{"asset_download_concurrency":0}`,
		`{"asset_download_concurrency":129}`,
		`{"prefetch_max_concurrent":128}`,
		`{"asset_download_concurrency":30,"prefetch_max_concurrent":30}`,
		`{"cache_idle_ttl_seconds":59}`,
		`{"cache_idle_ttl_seconds":604801}`,
	}
	for _, body := range cases {
		t.Run(strings.NewReplacer("{", "", "}", "", ":", "-").Replace(body), func(t *testing.T) {
			if _, err := bindConfigBody(t, fleet.OperationKindRestart, body); err == nil {
				t.Fatalf("accepted unsafe download tuning %s", body)
			}
		})
	}
}
