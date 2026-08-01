package inputsecurity

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type staticResolver map[string][]net.IPAddr

func (r staticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if answer, ok := r[host]; ok {
		return answer, nil
	}
	return nil, errors.New("host not found")
}

type blockingResolver struct{}

func (blockingResolver) LookupIPAddr(ctx context.Context, _ string) ([]net.IPAddr, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type blockingBody struct{ ctx context.Context }

func (b blockingBody) Read(p []byte) (int, error) {
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-time.After(time.Second):
		return 0, io.EOF
	}
}

func (b blockingBody) Close() error { return nil }

type hostRewriteTransport struct{ target *url.URL }

func (t hostRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	return http.DefaultTransport.RoundTrip(clone)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func testPolicy(t *testing.T) Policy {
	t.Helper()
	dir := t.TempDir()
	policy := DefaultPolicy()
	policy.TempDir = dir
	policy.QuarantineDir = filepath.Join(dir, "quarantine")
	policy.Resolver = staticResolver{}
	policy.Metrics = NewMetrics()
	return policy
}

func TestFetchBlocksPrivateAndMetadataNetworks(t *testing.T) {
	policy := testPolicy(t)
	policy.Resolver = staticResolver{
		"private.test":  {{IP: net.ParseIP("10.0.0.7")}},
		"metadata.test": {{IP: net.ParseIP("169.254.169.254")}},
		"localhost":     {{IP: net.ParseIP("127.0.0.1")}},
	}
	fetcher := NewFetcher(policy)
	for _, tc := range []struct {
		name string
		host string
	}{
		{name: "private", host: "private.test"},
		{name: "metadata", host: "metadata.test"},
		{name: "loopback_literal", host: "127.0.0.1"},
		{name: "localhost", host: "localhost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fetcher.Fetch(context.Background(), "http://"+tc.host+"/asset", KindImage)
			if CodeOf(err) != ErrPrivateNetwork {
				t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), ErrPrivateNetwork, err)
			}
		})
	}
}

func TestFetchRejectsUnsafeURLForms(t *testing.T) {
	policy := testPolicy(t)
	policy.Resolver = staticResolver{"public.test": {{IP: net.ParseIP("8.8.8.8")}}}
	fetcher := NewFetcher(policy)
	for _, tc := range []struct {
		name string
		raw  string
		want ErrorCode
	}{
		{name: "file_scheme", raw: "file:///etc/passwd", want: ErrUnsupportedScheme},
		{name: "userinfo", raw: "http://user:password@public.test/input", want: ErrInvalidURL},
		{name: "missing_host", raw: "https:///input", want: ErrInvalidURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fetcher.Fetch(context.Background(), tc.raw, KindUnknown)
			if CodeOf(err) != tc.want {
				t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), tc.want, err)
			}
		})
	}
}

func TestFetchDNSLookupTimeoutIsCanonical(t *testing.T) {
	policy := testPolicy(t)
	policy.DNSLookupTimeout = 10 * time.Millisecond
	policy.Resolver = blockingResolver{}
	_, err := NewFetcher(policy).Fetch(context.Background(), "https://slow-dns.test/input", KindManifest)
	if CodeOf(err) != ErrDownloadTimeout {
		t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), ErrDownloadTimeout, err)
	}
}

func TestFetchTransferTimeoutIsCanonical(t *testing.T) {
	policy := testPolicy(t)
	policy.AllowPrivateNetworks = true
	policy.TransferTimeout = 10 * time.Millisecond
	policy.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       blockingBody{ctx: req.Context()},
			Request:    req,
		}, nil
	})
	_, err := NewFetcher(policy).Fetch(context.Background(), "http://example.test/slow", KindUnknown)
	if CodeOf(err) != ErrDownloadTimeout {
		t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), ErrDownloadTimeout, err)
	}
}

func TestFetchRejectsDNSRebindingAfterRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://rebind.test/final", http.StatusFound)
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL)
	policy := testPolicy(t)
	policy.Transport = hostRewriteTransport{target: target}
	policy.Resolver = staticResolver{
		"public.test": {{IP: net.ParseIP("8.8.8.8")}},
		"rebind.test": {{IP: net.ParseIP("127.0.0.1")}},
	}
	_, err := NewFetcher(policy).Fetch(context.Background(), "http://public.test/redirect", KindUnknown)
	if CodeOf(err) != ErrDNSRebinding {
		t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), ErrDNSRebinding, err)
	}
}

func TestFetchEnforcesStreamingLimitWhenContentLengthLies(t *testing.T) {
	policy := testPolicy(t)
	policy.MaxBytes = 5
	policy.AllowPrivateNetworks = true
	policy.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        make(http.Header),
			Body:          io.NopCloser(strings.NewReader("0123456789")),
			ContentLength: 1, // deliberately lies; the reader still exposes all bytes
			Request:       req,
		}, nil
	})
	_, err := NewFetcher(policy).Fetch(context.Background(), "http://example.test/oversized", KindUnknown)
	if CodeOf(err) != ErrDownloadTooLarge {
		t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), ErrDownloadTooLarge, err)
	}
	snapshot := policy.Metrics.Snapshot()
	if snapshot.Rejections["unknown:INPUT_DOWNLOAD_TOO_LARGE"] == 0 {
		t.Fatalf("missing rejection metric: %+v", snapshot.Rejections)
	}
	if snapshot.Quarantines["unknown:INPUT_DOWNLOAD_TOO_LARGE"] == 0 {
		t.Fatalf("missing quarantine metric: %+v", snapshot.Quarantines)
	}
}

func TestFetchRedirectLimitIsCanonical(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://public.test/again", http.StatusFound)
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL)
	policy := testPolicy(t)
	policy.MaxRedirects = 1
	policy.Transport = hostRewriteTransport{target: target}
	policy.Resolver = staticResolver{"public.test": {{IP: net.ParseIP("8.8.8.8")}}}
	_, err := NewFetcher(policy).Fetch(context.Background(), "http://public.test/start", KindUnknown)
	if CodeOf(err) != ErrRedirectLimit {
		t.Fatalf("error code = %s, want %s (err=%v)", CodeOf(err), ErrRedirectLimit, err)
	}
}

func TestValidateRejectsHTMLMIMEAndCorruptMedia(t *testing.T) {
	policy := testPolicy(t)
	fetcher := NewFetcher(policy)
	htmlPath := filepath.Join(policy.TempDir, "html.bin")
	if err := os.WriteFile(htmlPath, []byte("<html>not media</html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fetcher.ValidateFile(context.Background(), htmlPath, KindImage, "image/png"); CodeOf(err) != ErrHTMLPayload {
		t.Fatalf("HTML error code = %s, want %s (err=%v)", CodeOf(err), ErrHTMLPayload, err)
	}

	corruptPath := filepath.Join(policy.TempDir, "corrupt.mp3")
	if err := os.WriteFile(corruptPath, []byte("ID3 not a real audio stream"), 0o600); err != nil {
		t.Fatal(err)
	}
	code := CodeOf(func() error {
		_, err := fetcher.ValidateFile(context.Background(), corruptPath, KindVoiceover, "audio/mpeg")
		return err
	}())
	if code != ErrProbeFailed && code != ErrMediaCorrupt {
		t.Fatalf("corrupt media error code = %s, want probe/media corruption", code)
	}
}

func TestValidateRejectsClientPathAndQuarantinesWithoutURL(t *testing.T) {
	policy := testPolicy(t)
	fetcher := NewFetcher(policy)
	outside := filepath.Join(t.TempDir(), "outside.bin")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fetcher.ValidateFile(context.Background(), outside, KindManifest, "application/json"); CodeOf(err) != ErrPathViolation {
		t.Fatalf("outside path code = %s, want %s", CodeOf(err), ErrPathViolation)
	}

	suspicious := filepath.Join(policy.TempDir, "suspicious.bin")
	if err := os.WriteFile(suspicious, []byte("suspicious"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fetcher.Quarantine(suspicious, KindManifest, ErrMIMEUnsupported, "test quarantine"); err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	entries, err := os.ReadDir(policy.QuarantineDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("quarantine entries = %d, want payload plus metadata", len(entries))
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "http") || strings.Contains(entry.Name(), "token") {
			t.Fatalf("quarantine leaked sensitive input name: %s", entry.Name())
		}
	}
}
