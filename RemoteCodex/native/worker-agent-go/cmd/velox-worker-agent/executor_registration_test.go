package main

import (
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner/executors"
)

func TestRegisterCanonicalRenderExecutors_DefaultsToNativeV2(t *testing.T) {
	reg := executor.NewRegistry()
	if err := registerCanonicalRenderExecutors(reg, t.TempDir(), false); err != nil {
		t.Fatalf("register canonical render executors: %v", err)
	}

	for _, id := range []string{
		executors.RenderBatchID,
	} {
		version := 1
		if id == executors.RenderBatchID {
			version = executors.RenderBatchVersion
		}
		if !reg.Has(id, version) {
			t.Errorf("registry missing %s@%d", id, version)
		}
	}
	if got := reg.Len(); got != 1 {
		t.Fatalf("registry length = %d, want 1", got)
	}

	descs := reg.Descriptors()
	for _, desc := range descs {
		if desc.ID != executors.RenderBatchID {
			continue
		}
		if len(desc.InputTypes) != 1 || desc.InputTypes[0] != "render.compiled.v2" {
			t.Fatalf("render_batch input types = %#v", desc.InputTypes)
		}
		if len(desc.OutputTypes) != 1 || desc.OutputTypes[0] != "video/mp4" {
			t.Fatalf("render_batch output types = %#v", desc.OutputTypes)
		}
		return
	}
	t.Fatal("render_batch descriptor not found")
}

func TestRegisterCanonicalRenderExecutors_LegacyRequiresExplicitOptIn(t *testing.T) {
	reg := executor.NewRegistry()
	if err := registerCanonicalRenderExecutors(reg, t.TempDir(), true); err != nil {
		t.Fatalf("register canonical render executors: %v", err)
	}

	for _, id := range []string{
		executors.SubtitleAlignID,
		executors.AudioMixID,
		executors.ComposeID,
		executors.EncodeID,
		executors.RenderBatchID,
	} {
		if !reg.Has(id, 1) {
			t.Errorf("registry missing %s@1", id)
		}
	}
	if got := reg.Len(); got != 5 {
		t.Fatalf("registry length = %d, want 5", got)
	}
}
