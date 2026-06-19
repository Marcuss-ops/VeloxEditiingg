package logging

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"testing"
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
