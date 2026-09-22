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

func TestBindMutationRequestRejectsUnsafeDownloadConcurrency(t *testing.T) {
	cases := []string{
		`{"asset_download_concurrency":0}`,
		`{"asset_download_concurrency":129}`,
		`{"prefetch_max_concurrent":128}`,
		`{"asset_download_concurrency":30,"prefetch_max_concurrent":30}`,
	}
	for _, body := range cases {
		t.Run(strings.NewReplacer("{", "", "}", "", ":", "-").Replace(body), func(t *testing.T) {
			if _, err := bindConfigBody(t, fleet.OperationKindRestart, body); err == nil {
				t.Fatalf("accepted unsafe download tuning %s", body)
			}
		})
	}
}
