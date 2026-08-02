// Package taskrunner / runner_logger.go
//
// Logger adapter between pkg/logger.Logger and the executor.Logger
// interface, plus the shared field-formatting helper.
package taskrunner

import (
	"fmt"

	"velox-worker-agent/pkg/logger"
)

// ── Logger adapter ────────────────────────────────────────────────────────

// workerExecLogger wraps pkg/logger.Logger so it satisfies the
// executor.Logger interface (Info/Warn/Error taking a string + fields).
// PR-3.2 invariant: every log line emitted from an executor surfaces
// the executor_id + job_id fields.
type workerExecLogger struct {
	inner  *logger.Logger
	fields map[string]interface{}
}

func (w *workerExecLogger) prefix() string {
	if w.fields == nil || len(w.fields) == 0 {
		return ""
	}
	// Stable, deterministic field order isn't required for human logs.
	keys := make([]string, 0, len(w.fields))
	for k := range w.fields {
		keys = append(keys, k)
	}
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%v", k, w.fields[k])
	}
	return "[" + out + "]"
}

func (w *workerExecLogger) with(msg string, fields map[string]interface{}) string {
	return w.prefix() + " " + msg + " " + formatFields(fields)
}

func (w *workerExecLogger) Info(msg string, fields map[string]interface{}) {
	if w.inner == nil {
		return
	}
	w.inner.Info("%s", w.with(msg, fields))
}

func (w *workerExecLogger) Warn(msg string, fields map[string]interface{}) {
	if w.inner == nil {
		return
	}
	w.inner.Warn("%s", w.with(msg, fields))
}

func (w *workerExecLogger) Error(msg string, err error, fields map[string]interface{}) {
	if w.inner == nil {
		return
	}
	extra := formatFields(fields)
	if err != nil {
		extra += " err=" + err.Error()
	}
	w.inner.Error("%s %s %s", w.prefix(), msg, extra)
}

func formatFields(fields map[string]interface{}) string {
	if len(fields) == 0 {
		return ""
	}
	out := ""
	first := true
	for k, v := range fields {
		if !first {
			out += " "
		}
		first = false
		out += fmt.Sprintf("%s=%v", k, v)
	}
	return out
}
