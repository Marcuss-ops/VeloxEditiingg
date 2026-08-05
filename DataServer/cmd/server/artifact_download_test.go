package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

type artifactDownloadTestReader struct {
	artifact *store.Artifact
}

func (r artifactDownloadTestReader) GetByID(context.Context, string) (*store.Artifact, error) {
	return r.artifact, nil
}

func TestArtifactDownload_OnlyReadyArtifactsAreServed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tmp := t.TempDir()
	blobs, err := store.NewFilesystemBlobStore(filepath.Join(tmp, "staging"), filepath.Join(tmp, "final"))
	if err != nil {
		t.Fatal(err)
	}
	const storageKey = "artifacts/sha256/aa/rendered.mp4"
	blobPath := filepath.Join(blobs.FinalDir(), filepath.FromSlash(storageKey))
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		t.Fatal(err)
	}
	const body = "verified-video-bytes"
	if err := os.WriteFile(blobPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	r := gin.New()
	r.GET("/api/internal/artifacts/:artifact_id/download", artifactDownloadHandler(
		artifactDownloadTestReader{artifact: &store.Artifact{
			ID: "artifact-ready", Status: "READY", Type: "video/mp4", StorageKey: storageKey,
		}}, blobs,
	))
	request := func(method string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(method, "/api/internal/artifacts/artifact-ready/download", nil))
		return w
	}

	w := request(http.MethodGet)
	if w.Code != http.StatusOK {
		t.Fatalf("READY artifact status=%d body=%q, want 200", w.Code, w.Body.String())
	}
	if w.Body.String() != body {
		t.Fatalf("download body=%q, want %q", w.Body.String(), body)
	}
	if got := w.Header().Get("Content-Type"); got != "video/mp4" {
		t.Fatalf("Content-Type=%q, want video/mp4", got)
	}

	head := gin.New()
	head.GET("/api/internal/artifacts/:artifact_id/download", artifactDownloadHandler(
		artifactDownloadTestReader{artifact: &store.Artifact{
			ID: "artifact-ready", Status: "READY", Type: "video/mp4", StorageKey: storageKey,
		}}, blobs,
	))
	head.HEAD("/api/internal/artifacts/:artifact_id/download", artifactDownloadHandler(
		artifactDownloadTestReader{artifact: &store.Artifact{
			ID: "artifact-ready", Status: "READY", Type: "video/mp4", StorageKey: storageKey,
		}}, blobs,
	))
	headResponse := httptest.NewRecorder()
	head.ServeHTTP(headResponse, httptest.NewRequest(http.MethodHead, "/api/internal/artifacts/artifact-ready/download", nil))
	if headResponse.Code != http.StatusOK {
		t.Fatalf("HEAD READY artifact status=%d, want 200", headResponse.Code)
	}

	// The same durable blob must not be exposed while its database row is
	// still STAGING. This protects AWAITING_ARTIFACT from an early download.
	stagingReader := artifactDownloadTestReader{artifact: &store.Artifact{
		ID: "artifact-staging", Status: "STAGING", Type: "video/mp4", StorageKey: storageKey,
	}}
	staging := gin.New()
	staging.GET("/api/internal/artifacts/:artifact_id/download", artifactDownloadHandler(stagingReader, blobs))
	stagingResponse := httptest.NewRecorder()
	staging.ServeHTTP(stagingResponse, httptest.NewRequest(http.MethodGet, "/api/internal/artifacts/artifact-staging/download", nil))
	if stagingResponse.Code != http.StatusNotFound {
		t.Fatalf("STAGING artifact status=%d body=%q, want 404", stagingResponse.Code, stagingResponse.Body.String())
	}
}

func TestArtifactDownload_MissingOrNonReadyArtifactReturnsNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tmp := t.TempDir()
	blobs, err := store.NewFilesystemBlobStore(filepath.Join(tmp, "staging"), filepath.Join(tmp, "final"))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		artifact *store.Artifact
	}{
		{name: "missing", artifact: nil},
		{name: "failed", artifact: &store.Artifact{ID: "artifact-failed", Status: "FAILED", StorageKey: "missing.mp4"}},
		{name: "quarantined", artifact: &store.Artifact{ID: "artifact-quarantined", Status: "QUARANTINED", StorageKey: "missing.mp4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/api/internal/artifacts/:artifact_id/download", artifactDownloadHandler(
				artifactDownloadTestReader{artifact: tc.artifact}, blobs,
			))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/internal/artifacts/id/download", nil))
			if w.Code != http.StatusNotFound {
				t.Fatalf("status=%d body=%q, want 404", w.Code, w.Body.String())
			}
		})
	}
}
