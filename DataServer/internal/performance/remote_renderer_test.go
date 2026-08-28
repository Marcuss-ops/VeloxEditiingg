package performance

import (
	"context"
	"strings"
	"testing"
)

type remoteCommandStub struct{ calls []string }

func (s *remoteCommandStub) Run(_ context.Context, _ string, command string) (string, error) {
	s.calls = append(s.calls, command)
	if strings.Contains(command, " /bin/cat ") {
		return `{"artifact_sha":"abc","receipts":[{"wall_ms":42,"receipt":{"timing":{"wall_ms":42,"render_ms":40},"memory":{"peak_rss_bytes":123}}}]}`, nil
	}
	return "prepared", nil
}

func TestRemoteWorkerRendererUsesRealToolchainAndReusesFixture(t *testing.T) {
	ssh := &remoteCommandStub{}
	r := NewRemoteWorkerRenderer(ssh)
	req := BenchmarkRenderRequest{WorkerID: "worker-1", Fixture: BenchmarkFixture{ID: "COPY_ONLY_CANONICAL_5M_V1"}, CacheMode: CacheModeWarm}
	got, err := r.Render(context.Background(), req)
	if err != nil {
		t.Fatalf("Render() error: %v", err)
	}
	if got.Receipt == nil || got.Receipt.WallMS != 42 || got.Receipt.PeakRAMBytes != 123 {
		t.Fatalf("unexpected receipt: %+v", got.Receipt)
	}
	if got.ArtifactSHA256 != "abc" {
		t.Fatalf("artifact sha=%q, want abc", got.ArtifactSHA256)
	}
	if len(ssh.calls) != 4 {
		t.Fatalf("calls after first render=%d, want 4 (prepare, verify, benchmark, cat)", len(ssh.calls))
	}
	if _, err := r.Render(context.Background(), req); err != nil {
		t.Fatalf("second Render() error: %v", err)
	}
	if len(ssh.calls) != 6 {
		t.Fatalf("calls after second render=%d, want 6 (fixture prepared once)", len(ssh.calls))
	}
	for _, command := range ssh.calls {
		if strings.Contains(command, "-stub") {
			t.Fatalf("remote command selected stub backend: %q", command)
		}
	}
}
