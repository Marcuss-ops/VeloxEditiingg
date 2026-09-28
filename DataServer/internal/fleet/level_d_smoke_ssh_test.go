package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type smokeDownloadErrorSSH struct{ err error }

func (s smokeDownloadErrorSSH) Run(context.Context, string, string) (string, error) {
	return "", s.err
}

func TestSSHWorkerExecRedactsPickupURLFromDownloadError(t *testing.T) {
	const pickupURL = "https://master.test/api/assets/clip?token=short-lived-secret"
	exec := NewSSHWorkerExec(smokeDownloadErrorSSH{err: errors.New("curl failed for " + pickupURL)})
	err := exec.DownloadAsset(context.Background(), "run-1", "worker-1", pickupURL, "/tmp/smoke.in")
	if !errors.Is(err, ErrAssetDownloadFail) {
		t.Fatalf("DownloadAsset() error = %v, want ErrAssetDownloadFail", err)
	}
	if strings.Contains(err.Error(), "short-lived-secret") || strings.Contains(err.Error(), pickupURL) {
		t.Fatalf("DownloadAsset() leaked pickup credentials: %v", err)
	}
}
