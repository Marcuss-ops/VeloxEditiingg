package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// captureLog swaps the global log writer/flags with a buffer for the lifetime
// of the call, returning whatever was written. Test-only helper.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	origOut := log.Writer()
	origFlags := log.Flags()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(origOut)
	defer log.SetFlags(origFlags)
	fn()
	return buf.String()
}

// restoreLoggingState returns the global log state to test-friendly defaults
// so a previous test cannot leak into the next one.
func restoreLoggingState(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		SetQuiet(false)
		SetJSONOutput(false)
	})
}

// TestQuietModeHidesNonErrors verifies that quiet mode suppresses WARN and
// INFO events but still surfaces ERROR events. The chosen code is one of the
// new event codes from the recent fmt.Printf → structured-logger migration.
func TestQuietModeHidesNonErrors(t *testing.T) {
	restoreLoggingState(t)
	SetQuiet(true)
	SetJSONOutput(false)

	l := NewLogger("test.drive_links")
	out := captureLog(t, func() {
		l.WarnWithMsg(CodeDriveLinkMigrateSkip,
			"Skipping drive link during migration",
			map[string]interface{}{"id": "drive-link-123", "err": "row exists"})
		l.Info("test.event.info", map[string]interface{}{"k": "v"})
		l.ErrorWithMsg(CodeDriveLinkMigrateSkip,
			"raw drive-link error",
			map[string]interface{}{"id": "drive-link-err"})
	})

	if strings.Contains(out, "WARN") {
		t.Errorf("quiet mode must hide WARN events; got output:\n%s", out)
	}
	if strings.Contains(out, "INFO") {
		t.Errorf("quiet mode must hide INFO events; got output:\n%s", out)
	}
	if !strings.Contains(out, "ERROR") {
		t.Errorf("quiet mode must still emit ERROR events; got output:\n%s", out)
	}
}

// TestJSONModeEmitsValidJSON asserts that JSON mode produces a parseable
// Event with the expected level, code, component, and fields for one of the
// new event codes from the recent migration.
func TestJSONModeEmitsValidJSON(t *testing.T) {
	restoreLoggingState(t)
	SetQuiet(false)
	SetJSONOutput(true)

	const component = "test.drive_links"

	l := NewLogger(component)
	out := captureLog(t, func() {
		l.WarnWithMsg(CodeDriveLinkMigrateSkip,
			"Skipping drive link during migration",
			map[string]interface{}{
				"id":  "drive-link-123",
				"err": "row already exists",
			})
	})

	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		t.Fatalf("expected JSON output, got empty buffer")
	}

	var ev Event
	if err := json.Unmarshal([]byte(trimmed), &ev); err != nil {
		t.Fatalf("expected valid JSON object, got parse error: %v\noutput: %s", err, out)
	}
	if ev.Code != CodeDriveLinkMigrateSkip {
		t.Errorf("want code %q, got %q", CodeDriveLinkMigrateSkip, ev.Code)
	}
	if ev.Component != component {
		t.Errorf("want component %q, got %q", component, ev.Component)
	}
	if ev.Level != LevelWarn {
		t.Errorf("want level %q, got %q", LevelWarn, ev.Level)
	}
	if ev.Message == "" {
		t.Errorf("expected non-empty message, got: %v", ev)
	}
	if ev.Fields == nil {
		t.Fatalf("expected fields map, got: %v", ev)
	}
	if id, _ := ev.Fields["id"].(string); id != "drive-link-123" {
		t.Errorf("want id=drive-link-123, got %v", ev.Fields["id"])
	}
	if errMsg, _ := ev.Fields["err"].(string); errMsg != "row already exists" {
		t.Errorf("want err=%q, got %v", "row already exists", ev.Fields["err"])
	}
	if ev.Timestamp.IsZero() {
		t.Errorf("expected non-zero timestamp, got: %v", ev.Timestamp)
	}
}

// TestContextInjectsTraceCorrelation verifies that the *Context log variants
// inject trace_id/span_id from the active span while preserving the caller's
// fields and without mutating the caller's map (GAP 4: trace ↔ log).
func TestContextInjectsTraceCorrelation(t *testing.T) {
	restoreLoggingState(t)
	SetQuiet(false)
	SetJSONOutput(true)

	tid := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	sid := trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	l := NewLogger("test.trace")
	fields := map[string]interface{}{"k": "v"}
	out := captureLog(t, func() {
		l.InfoContext(ctx, "test.event.info", fields)
	})

	var ev Event
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &ev); err != nil {
		t.Fatalf("expected valid JSON, got: %v\noutput: %s", err, out)
	}
	if got := ev.Fields["trace_id"]; got != tid.String() {
		t.Errorf("want trace_id=%q, got %v", tid.String(), got)
	}
	if got := ev.Fields["span_id"]; got != sid.String() {
		t.Errorf("want span_id=%q, got %v", sid.String(), got)
	}
	if got := ev.Fields["k"]; got != "v" {
		t.Errorf("want original field k=v preserved, got %v", got)
	}
	// The caller's map must not be mutated by injection.
	if _, injected := fields["trace_id"]; injected {
		t.Errorf("caller's fields map was mutated: %v", fields)
	}
}

// TestContextWithoutSpanDoesNotInject verifies that a context without an
// active span leaves the event untouched (no trace_id/span_id fields).
func TestContextWithoutSpanDoesNotInject(t *testing.T) {
	restoreLoggingState(t)
	SetQuiet(false)
	SetJSONOutput(true)

	l := NewLogger("test.trace")
	out := captureLog(t, func() {
		l.InfoContext(context.Background(), "test.event.info", map[string]interface{}{"k": "v"})
	})

	var ev Event
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &ev); err != nil {
		t.Fatalf("expected valid JSON, got: %v\noutput: %s", err, out)
	}
	if _, ok := ev.Fields["trace_id"]; ok {
		t.Errorf("expected no trace_id without an active span, got %v", ev.Fields)
	}
	if _, ok := ev.Fields["span_id"]; ok {
		t.Errorf("expected no span_id without an active span, got %v", ev.Fields)
	}
}
