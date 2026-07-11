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

// ── NopNotifier ─────────────────────────────────────────────────────────

func TestNopNotifier_NeverReturnsError(t *testing.T) {
	t.Parallel()
	var n alerts.NopNotifier
	if err := n.Notify(context.Background(), alerts.Alert{}); err != nil {
		t.Fatalf("NopNotifier must be a no-op; got err=%v", err)
	}
}

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

// ── Default ─────────────────────────────────────────────────────────────

func TestDefault_IsNopNotifier(t *testing.T) {
	t.Parallel()
	// Default is the safe-zero before SetDefault is called.
	if _, ok := alerts.Default.(alerts.NopNotifier); !ok {
		t.Errorf("alerts.Default should be NopNotifier before override; got %T", alerts.Default)
	}
}
