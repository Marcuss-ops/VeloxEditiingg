package providers

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

func TestLocalExportProvider_Name(t *testing.T) {
	p := NewLocalExportProvider("/tmp")
	if p.Name() != "local_export" {
		t.Fatalf("want 'local_export', got %q", p.Name())
	}
}

func TestLocalExportProvider_NilReceiver(t *testing.T) {
	var p *LocalExportProvider
	_, err := p.Deliver(context.Background(), &store.Artifact{}, &deliveries.Destination{})
	if !errors.Is(err, deliveries.ErrProviderNotConfigured) {
		t.Fatalf("want ErrProviderNotConfigured, got %v", err)
	}
}

func TestLocalExportProvider_EmptyRoot(t *testing.T) {
	p := NewLocalExportProvider("")
	_, err := p.Deliver(context.Background(), &store.Artifact{}, &deliveries.Destination{})
	if !errors.Is(err, deliveries.ErrProviderNotConfigured) {
		t.Fatalf("want ErrProviderNotConfigured, got %v", err)
	}
}

func TestLocalExportProvider_NilArtifact(t *testing.T) {
	p := NewLocalExportProvider(t.TempDir())
	_, err := p.Deliver(context.Background(), nil, &deliveries.Destination{})
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("want ErrProviderPermanent, got %v", err)
	}
}

func TestLocalExportProvider_EmptyLocalPath(t *testing.T) {
	p := NewLocalExportProvider(t.TempDir())
	_, err := p.Deliver(context.Background(), &store.Artifact{LocalPath: ""}, &deliveries.Destination{})
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("want ErrProviderPermanent, got %v", err)
	}
}

func TestLocalExportProvider_FileNotFound(t *testing.T) {
	p := NewLocalExportProvider(t.TempDir())
	_, err := p.Deliver(context.Background(), &store.Artifact{LocalPath: "/nonexistent/file.mp4"}, &deliveries.Destination{})
	if err == nil {
		t.Fatal("want error for nonexistent file")
	}
}

func TestLocalExportProvider_Success(t *testing.T) {
	outDir := t.TempDir()
	p := NewLocalExportProvider(outDir)

	// Create a source file
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "video.mp4")
	if err := os.WriteFile(srcFile, []byte("fake video content"), 0o644); err != nil {
		t.Fatal(err)
	}

	artifact := &store.Artifact{
		ID:        "art-001",
		LocalPath: srcFile,
	}

	result, err := p.Deliver(context.Background(), artifact, &deliveries.Destination{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Success {
		t.Fatal("want Success=true")
	}
	if result.RemoteURL == "" {
		t.Fatal("want non-empty RemoteURL")
	}

	// Verify the file was copied
	content, err := os.ReadFile(result.RemoteURL)
	if err != nil {
		t.Fatalf("read copied file: %v", err)
	}
	if string(content) != "fake video content" {
		t.Fatalf("want copied content, got %q", string(content))
	}
}

func TestS3Provider_Name(t *testing.T) {
	p := NewS3Provider()
	if p.Name() != "s3" {
		t.Fatalf("want 's3', got %q", p.Name())
	}
}

func TestS3Provider_AlwaysNotConfigured(t *testing.T) {
	p := NewS3Provider()
	_, err := p.Deliver(context.Background(), &store.Artifact{}, &deliveries.Destination{})
	if !errors.Is(err, deliveries.ErrProviderNotConfigured) {
		t.Fatalf("want ErrProviderNotConfigured, got %v", err)
	}
}
