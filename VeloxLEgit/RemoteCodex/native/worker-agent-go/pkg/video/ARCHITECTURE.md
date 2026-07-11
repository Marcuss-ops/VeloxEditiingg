# Pipeline Architecture

Each video endpoint produces a `RenderPlan` via its own compiler.
All compilers share the same C++ engine (`velox_video_engine --render`).

```text
Request API
    ↓
Handler specifico
    ↓
Pipeline Registry → Compiler specifico
    ↓
Servizi comuni (audio probe, timeline allocator)
    ↓
RenderPlan V1
    ↓
NativeRenderClient → velox_video_engine --render
    ↓
Video finale
```

## Directory Structure

```text
pkg/video/
├── pipeline/
│   ├── compiler.go         # Compiler interface
│   ├── registry.go         # Pipeline registry + auto-detection
│   └── runner.go           # Orchestrates compile → render
├── plan/
│   └── types.go            # RenderPlan, CanvasSpec, TimelineItem, etc.
├── pipelines/
│   ├── images/
│   │   └── compiler.go     # images.v1 — image slideshow + audio
│   ├── clips/
│   │   └── compiler.go     # clips.v1 — video clips + audio
│   ├── entities/
│   │   └── compiler.go     # entities.v1 — entity images + script
│   └── hybrid/
│       └── compiler.go     # hybrid.v1 — mixed sources
├── services/
│   ├── audio/
│   │   └── probe.go        # ffprobe wrapper
│   ├── timeline/
│   │   └── allocator.go    # Duration distribution
│   └── native/
│       └── render_client.go # C++ engine wrapper
├── workflow.go             # Main orchestrator (legacy + pipeline)
├── render_plan_bridge.go   # Legacy adapter (RenderJobParams → RenderPlan)
├── native_engine.go        # Legacy C++ engine (--full-video)
└── pipelines.go            # Registers all compilers
```

## Pipeline Interface

```go
type Compiler interface {
    ID() string
    Validate(input map[string]interface{}) error
    Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string) (*plan.RenderPlan, error)
}
```

## Available Pipelines

| Pipeline ID | Input | Description |
|---|---|---|
| `images.v1` | images[], audio_url, effect, orientation | Image slideshow with audio |
| `clips.v1` | clips[{url, duration}], audio_url, fit | Video clips with audio |
| `entities.v1` | script, audio_url, entity_style, output_format | Entity-based video |
| `hybrid.v1` | items[{type, url, duration}], audio_url | Mixed sources |

## Usage

```go
// Auto-detect pipeline from parameters
pipelineID := pipeline.DetectPipelineID(params)

// Or use explicitly
err := workflow.RunPipeline(ctx, "images.v1", jobID, params, outputPath)
```

## Adding a New Pipeline

1. Create `pkg/video/pipelines/mynew/compiler.go`
2. Implement `Validate()` and `Compile()`
3. Register in `pkg/video/pipelines.go`
4. Done — the C++ engine doesn't change
