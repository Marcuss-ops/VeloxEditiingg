package alerts_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-server/internal/alerts"
)

// ── LogNotifier ─────────────────────────────────────────────────────────

func TestLogNotifier_EmitsStructuredLine(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := log.New(buf, "", 0)
	n := &alerts.LogNotifier{Prefix: "[ALERTS-TEST]", Logger: logger}

	alert := alerts.Alert{
		Source:    "test.unit",
		Severity:  alerts.SeverityError,
		Subject:   "subject-x",
		Body:      "failure detail",
		Tags:      map[string]string{"error_code": "TEST_ERR", "job_id": "job-1"},
		Timestamp: time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC),
	}
	if err := n.Notify(context.Background(), alert); err != nil {
		t.Fatalf("LogNotifier returned error: %v", err)
	}

	out := buf.String()
	want := []string{
		"[ALERTS-TEST]",
		"severity=error",
		"source=test.unit",
		`subject="subject-x"`,
		`body="failure detail"`,
		"error_code=TEST_ERR", // deterministic ordering via insertion sort
		"job_id=job-1",
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("LogNotifier output missing %q\n%s", w, out)
		}
	}
}

// ── MultiNotifier ───────────────────────────────────────────────────────

type recordingNotifier struct {
	mu     sync.Mutex
	calls  []alerts.Alert
	err    error
	called int
}

func (r *recordingNotifier) Notify(_ context.Context, a alerts.Alert) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, a)
	r.called++
	return r.err
}

func TestMultiNotifier_FanOut_AllChildrenCalled(t *testing.T) {
	t.Parallel()
	a := &recordingNotifier{}
	b := &recordingNotifier{}
	m := &alerts.MultiNotifier{Children: []alerts.Notifier{a, b}}

	alert := alerts.Alert{Source: "unit.test", Severity: alerts.SeverityInfo}
	if err := m.Notify(context.Background(), alert); err != nil {
		t.Fatalf("unexpected error from MultiNotifier: %v", err)
	}
	if a.called != 1 || b.called != 1 {
		t.Errorf("expected both children called once; a=%d b=%d", a.called, b.called)
	}
}

func TestMultiNotifier_NilChildSkipped(t *testing.T) {
	t.Parallel()
	a := &recordingNotifier{}
	m := &alerts.MultiNotifier{Children: []alerts.Notifier{nil, a, nil}}

	if err := m.Notify(context.Background(), alerts.Alert{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.called != 1 {
		t.Errorf("expected non-nil child called once; got %d", a.called)
	}
}

func TestMultiNotifier_AggregatesFirstError(t *testing.T) {
	t.Parallel()
	firstErr := errors.New("first sink down")
	a := &recordingNotifier{err: firstErr}
	b := &recordingNotifier{}
	m := &alerts.MultiNotifier{Children: []alerts.Notifier{a, b}}

	err := m.Notify(context.Background(), alerts.Alert{})
	if !errors.Is(err, firstErr) {
		t.Errorf("MultiNotifier should propagate the first error; got %v", err)
	}
	// Both children should still have been called (no short-circuit).
	if a.called != 1 || b.called != 1 {
		t.Errorf("MultiNotifier should NOT short-circuit; a=%d b=%d", a.called, b.called)
	}
}

// ── NotifySink ──────────────────────────────────────────────────────────

func TestNotifySink_AdaptsEventToCanonicalAlert(t *testing.T) {
	t.Parallel()
	rec := &recordingNotifier{}
	sink := &alerts.NotifySink{Notifier: rec}

	event := alerts.AlertEvent{
		EventID:     "evt-1",
		Group:       alerts.GroupFleet,
		RuleID:      "disk_pressure",
		Severity:    "CRITICAL",
		Subject:     "worker-1",
		Summary:     "disk at 5%",
		Description: "worker disk below threshold",
		Labels:      map[string]string{"current_value": "5"},
		FiredAt:     time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	}
	if err := sink.Process(context.Background(), event); err != nil {
		t.Fatalf("NotifySink.Process returned error: %v", err)
	}

	if len(rec.calls) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(rec.calls))
	}
	alert := rec.calls[0]
	if alert.Source != "fleet.disk_pressure" {
		t.Errorf("Source = %q, want fleet.disk_pressure", alert.Source)
	}
	if alert.Severity != alerts.SeverityError {
		t.Errorf("Severity = %q, want error (normalized from CRITICAL)", alert.Severity)
	}
	if alert.Subject != "worker-1" || alert.Body != "worker disk below threshold" {
		t.Errorf("Subject/Body mismatch: %q / %q", alert.Subject, alert.Body)
	}
	if alert.Tags["current_value"] != "5" {
		t.Errorf("Labels not forwarded: %v", alert.Tags)
	}
	if !alert.Timestamp.Equal(event.FiredAt) {
		t.Errorf("Timestamp = %v, want %v", alert.Timestamp, event.FiredAt)
	}
}

func TestNotifySink_ForwardsNotifierError(t *testing.T) {
	t.Parallel()
	sinkErr := errors.New("webhook down")
	rec := &recordingNotifier{err: sinkErr}
	sink := &alerts.NotifySink{Notifier: rec}

	err := sink.Process(context.Background(), alerts.AlertEvent{Group: alerts.GroupCompute, RuleID: "error_rate", Subject: "x"})
	if !errors.Is(err, sinkErr) {
		t.Errorf("NotifySink should forward notifier errors; got %v", err)
	}
}

func TestNotifySink_NilNotifierFailsClosed(t *testing.T) {
	t.Parallel()
	sink := &alerts.NotifySink{}
	if err := sink.Process(context.Background(), alerts.AlertEvent{Group: alerts.GroupFleet, RuleID: "r"}); !errors.Is(err, alerts.ErrNotifierNotConfigured) {
		t.Errorf("NotifySink with nil Notifier error = %v, want ErrNotifierNotConfigured", err)
	}
}

// ── SeverityFromString ───────────────────────────────────────────────────

func TestSeverityFromString_NormalizesVocabularies(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want alerts.Severity
	}{
		{"info", alerts.SeverityInfo},
		{"INFO", alerts.SeverityInfo},
		{"warn", alerts.SeverityWarn},
		{"warning", alerts.SeverityWarn},
		{"WARNING", alerts.SeverityWarn},
		{"error", alerts.SeverityError},
		{"critical", alerts.SeverityError},
		{"CRITICAL", alerts.SeverityError},
		{"fatal", alerts.SeverityFatal},
		{"  warning  ", alerts.SeverityWarn},
	}
	for _, c := range cases {
		if got := alerts.SeverityFromString(c.in); got != c.want {
			t.Errorf("SeverityFromString(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSeverityFromString_UnknownPassesThroughLowercased(t *testing.T) {
	t.Parallel()
	if got := alerts.SeverityFromString("PAGE"); got != alerts.Severity("page") {
		t.Errorf("unknown severity should pass through lowercased; got %q", got)
	}
}
