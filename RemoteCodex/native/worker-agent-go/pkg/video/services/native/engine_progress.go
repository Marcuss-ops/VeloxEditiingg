package native

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	"velox-worker-agent/pkg/video/pipeline"
)

// engine_progress.go owns the streaming half of the subprocess pipeline:
// it drains the engine's stdout and stderr pipes line by line. stderr is
// parsed for JSON progress events ({"percent","scene","total_scenes","stage"})
// and forwarded to either pipeline.ProgressCallback(ctx) (context-aware)
// or the caller-supplied fallback onProgress. Plain stdout is accumulated
// for diagnostic logging on failure.
//
// The progressDone channel returned by streamEngineOutput closes when
// the stderr reader goroutine exits; it is the only sync point the
// caller uses to wait for output drain. The stdout goroutine is
// intentionally not synchronised on exit — same shape as the
// original, since the OS closes both pipes when the subprocess
// finishes and the readers stop naturally on EOF.

// streamEngineOutput starts two goroutines (stderr + stdout readers)
// and returns the channel that closes when the stderr reader exits.
// The buffers are written through pointers so the caller can read
// their final state after <-progressDone (stderr is guaranteed
// drained; stdout is best-effort — same race semantics as the
// original).
func streamEngineOutput(stdout, stderr io.ReadCloser, ctx context.Context, onProgress ProgressFunc, stderrBuf, stdoutBuf *strings.Builder) chan struct{} {
	progressDone := make(chan struct{})

	stderrReader := bufio.NewReader(stderr)
	go func() {
		defer close(progressDone)
		for {
			line, err := stderrReader.ReadString('\n')
			if len(line) > 0 {
				line = strings.TrimRight(line, "\n\r")
				stderrBuf.WriteString(line)
				stderrBuf.WriteString("\n")
				var prog struct {
					Percent int    `json:"percent"`
					Scene   int    `json:"scene"`
					Total   int    `json:"total_scenes"`
					Stage   string `json:"stage"`
				}
				if json.Unmarshal([]byte(line), &prog) == nil && prog.Percent > 0 {
					if fn := pipeline.ProgressCallback(ctx); fn != nil {
						fn(prog.Percent, prog.Scene, prog.Total, prog.Stage)
					} else if onProgress != nil {
						onProgress(prog.Percent, prog.Scene, prog.Total, prog.Stage)
					}
				}
			}
			if err != nil {
				break
			}
		}
	}()

	stdoutReader := bufio.NewReader(stdout)
	go func() {
		for {
			line, err := stdoutReader.ReadString('\n')
			if len(line) > 0 {
				stdoutBuf.WriteString(line)
			}
			if err != nil {
				break
			}
		}
	}()

	return progressDone
}
