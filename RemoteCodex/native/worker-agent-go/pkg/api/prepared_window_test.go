package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"velox-shared/contract"
)

func preparedWindowFixture(t *testing.T) (contract.VideoPrepareWindowV1, contract.VideoPrepareWindowCertificateV1, string) {
	t.Helper()
	profile := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source-asset", SHA256: strings.Repeat("a", 64), SizeBytes: 500, Kind: "video"},
		SourceStartFrame: 24, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	path := t.TempDir() + "/window.mp4"
	if err := os.WriteFile(path, []byte("canonical window bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	certificate := contract.VideoPrepareWindowCertificateV1{
		Version: contract.VideoPrepareWindowVersion, SourceSHA256: request.Source.SHA256,
		SourceStartFrame: request.SourceStartFrame, SourceFrameCount: request.SourceFrameCount,
		SourceFPSNum: request.SourceFPSNum, SourceFPSDen: request.SourceFPSDen,
		OutputFrameCount: request.OutputFrameCount, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision, EncoderVersion: "ffmpeg test",
		EncoderBuildSHA256: strings.Repeat("c", 64), OutputSHA256: strings.Repeat("d", 64),
		OutputSizeBytes: int64(len("canonical window bytes")),
		OutputEvidence: contract.VideoWindowOutputEvidenceV1{
			Codec: "h264", Profile: "high", Level: 40, Width: profile.Width, Height: profile.Height,
			PixelFormat: profile.PixelFormat, FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
			FrameCount: request.OutputFrameCount, FirstFrameKeyframe: true, ExtradataSHA256: strings.Repeat("e", 64),
		},
		ClosedGOPAttested: true, SourceAssetID: request.Source.AssetID, SourceSizeBytes: request.Source.SizeBytes,
		TimelineRevision: request.TimelineRevision, TimelineSHA256: request.TimelineSHA256,
	}
	return request, certificate, path
}

func TestPublishPreparedWindowChecksMasterCacheBeforeUploading(t *testing.T) {
	request, certificate, path := preparedWindowFixture(t)
	var puts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer worker-token" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"asset_id":"registered-sha","sha256":"`+certificate.OutputSHA256+`","size_bytes":`+fmt.Sprint(certificate.OutputSizeBytes)+`}`)
			return
		}
		puts++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := NewClient(server.URL)
	client.SetAuthToken("worker-token")
	assetID, reused, err := client.PublishPreparedWindow(context.Background(), request, certificate, path)
	if err != nil {
		t.Fatal(err)
	}
	if !reused || assetID != "registered-sha" || puts != 0 {
		t.Fatalf("result asset=%q reused=%t puts=%d", assetID, reused, puts)
	}
}

func TestPublishPreparedWindowUploadsAfterCacheMiss(t *testing.T) {
	request, certificate, path := preparedWindowFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Velox-Window-Request") == "" || r.Header.Get("X-Velox-Window-Certificate") == "" {
			t.Error("request or certificate header missing")
		}
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPut || r.ContentLength != certificate.OutputSizeBytes || r.Header.Get("Content-Type") != "video/mp4" {
			t.Errorf("upload method/size/content-type = %s/%d/%s", r.Method, r.ContentLength, r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "canonical window bytes" {
			t.Errorf("upload body=%q err=%v", body, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(preparedWindowRegistration{AssetID: certificate.OutputSHA256, SHA256: certificate.OutputSHA256, SizeBytes: certificate.OutputSizeBytes})
	}))
	defer server.Close()
	client := NewClient(server.URL)
	client.SetAuthToken("worker-token")
	assetID, reused, err := client.PublishPreparedWindow(context.Background(), request, certificate, path)
	if err != nil {
		t.Fatal(err)
	}
	if reused || assetID != certificate.OutputSHA256 {
		t.Fatalf("asset=%q reused=%t", assetID, reused)
	}
}
