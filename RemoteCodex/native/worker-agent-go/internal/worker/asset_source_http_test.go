package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPAssetSourceSupportsRange(t *testing.T) {
	src := newHTTPAssetSource("http://example.invalid/x", "", nil)
	if !src.SupportsRange() {
		t.Fatal("HTTP asset source should advertise range support")
	}
}

func TestHTTPAssetSourceOpenReturnsFullBodyAndMetadata(t *testing.T) {
	data := []byte("full asset payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "" {
			t.Errorf("full open Range=%q, want empty", got)
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	src := newHTTPAssetSource(srv.URL, "", srv.Client())
	body, meta, err := src.Open(context.Background(), 0)
	if err != nil {
		t.Fatalf("Open(0): %v", err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("body = %q, want %q", got, data)
	}
	if meta.SizeBytes != int64(len(data)) {
		t.Fatalf("SizeBytes = %d, want %d", meta.SizeBytes, len(data))
	}
	if meta.MIMEType != "audio/mpeg" {
		t.Fatalf("MIMEType = %q, want audio/mpeg", meta.MIMEType)
	}
}

func TestHTTPAssetSourceOpenReturnsRangeSuffix(t *testing.T) {
	data := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "asset.bin", time.Time{}, strings.NewReader(string(data)))
	}))
	defer srv.Close()

	src := newHTTPAssetSource(srv.URL, "", srv.Client())
	body, meta, err := src.Open(context.Background(), 3)
	if err != nil {
		t.Fatalf("Open(3): %v", err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != string(data[3:]) {
		t.Fatalf("suffix = %q, want %q", got, data[3:])
	}
	if meta.SizeBytes != int64(len(data)) {
		t.Fatalf("SizeBytes = %d, want total %d", meta.SizeBytes, len(data))
	}
}

func TestHTTPAssetSourceOpenRejectsIgnoredRange(t *testing.T) {
	data := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data) // ignore the Range header entirely
	}))
	defer srv.Close()

	src := newHTTPAssetSource(srv.URL, "", srv.Client())
	if _, _, err := src.Open(context.Background(), 3); !errors.Is(err, errRangeIgnored) {
		t.Fatalf("Open(3) err = %v, want errRangeIgnored", err)
	}
}

func TestHTTPAssetSourceOpenClassifiesStatuses(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		wantErr   error
		wantRetry bool
		wantPerm  bool
	}{
		{"not_found", http.StatusNotFound, errAssetNotFound, false, false},
		{"range_not_satisfiable", http.StatusRequestedRangeNotSatisfiable, errRangeNotSatisfiable, false, false},
		{"retryable_500", http.StatusInternalServerError, nil, true, false},
		{"retryable_429", http.StatusTooManyRequests, nil, true, false},
		{"permanent_403", http.StatusForbidden, nil, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			src := newHTTPAssetSource(srv.URL, "", srv.Client())
			_, _, err := src.Open(context.Background(), 0)
			if err == nil {
				t.Fatal("Open should fail for a non-2xx status")
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			var re *retryableStatusError
			var pe *permanentStatusError
			switch {
			case tc.wantRetry && !errors.As(err, &re):
				t.Fatalf("err = %v, want retryableStatusError", err)
			case tc.wantPerm && !errors.As(err, &pe):
				t.Fatalf("err = %v, want permanentStatusError", err)
			}
		})
	}
}
