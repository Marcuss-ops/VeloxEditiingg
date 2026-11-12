package worker

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/downloader"
)

func TestClassifyPreparationTransientFailure(t *testing.T) {
	for _, tc := range []struct {
		name, executor string
		err            error
		status, code   string
	}{
		{"retryable HTTP", contract.VideoPrepareWindowExecutorID, fmt.Errorf("upload: %w", downloader.NewHTTPStatusError(http.StatusServiceUnavailable, "unavailable", 0)), "failed", contract.VideoPrepareWindowRetryableErrorCode},
		{"permanent HTTP", contract.VideoPrepareWindowExecutorID, downloader.NewHTTPStatusError(http.StatusUnprocessableEntity, "invalid", 0), "failed", ""},
		{"other executor", "video.assemble.copy.v1", downloader.NewHTTPStatusError(http.StatusServiceUnavailable, "unavailable", 0), "failed", ""},
		{"cancelled", contract.VideoPrepareWindowExecutorID, context.Canceled, "cancelled", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := classifyTaskResultOutcome(tc.executor, nil, tc.err)
			if status != tc.status || code != tc.code {
				t.Fatalf("status=%q code=%q", status, code)
			}
		})
	}
}
