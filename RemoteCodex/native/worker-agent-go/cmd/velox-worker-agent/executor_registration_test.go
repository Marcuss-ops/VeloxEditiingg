package main

import (
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner/executors"
	"velox-worker-agent/pkg/video/pipeline"
)

func TestRegisterCanonicalRenderExecutorsRegistersSingleCompiledPlanExecutor(t *testing.T) {
	reg := executor.NewRegistry()
	if err := registerCanonicalRenderExecutors(reg, t.TempDir(), pipeline.NewRunner(nil, nil, nil)); err != nil {
		t.Fatalf("register canonical render executors: %v", err)
	}

	for _, id := range []string{
		executors.SubtitleAlignID,
		executors.AudioMixID,
		executors.ComposeID,
		executors.EncodeID,
		executors.VideoAssembleCopyID,
	} {
		if !reg.Has(id, 1) {
			t.Errorf("registry missing %s@1", id)
		}
	}
	if got := reg.Len(); got != 5 {
		t.Fatalf("registry length = %d, want 5", got)
	}
	if reg.Has(executors.RenderBatchID, executors.RenderBatchVersion) {
		t.Fatal("legacy render_batch executor must not be registered")
	}

	descs := reg.Descriptors()
	for _, desc := range descs {
		if desc.ID != executors.VideoAssembleCopyID {
			continue
		}
		if len(desc.InputTypes) != 1 || desc.InputTypes[0] != "render.compiled.v2" {
			t.Fatalf("video.assemble.copy input types = %#v", desc.InputTypes)
		}
		if len(desc.OutputTypes) != 1 || desc.OutputTypes[0] != "video/mp4" {
			t.Fatalf("video.assemble.copy output types = %#v", desc.OutputTypes)
		}
		return
	}
	t.Fatal("video.assemble.copy descriptor not found")
}
