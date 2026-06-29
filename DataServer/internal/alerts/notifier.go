// Package alerts provides the canonical alert/notification surface
// that outbox event handlers forward to when a producer signals an
// operationally-interesting state change (e.g. JOB_FAILED).
//
// PR-OUTBOX-HANDLER: the JOB_FAILED outbox handler
// (internal/outbox/production.go) decodes the event payload and
// invokes Notifier.Notify with an Alert struct. The bootstrap
// composition root (cmd/server/bootstrap_alerts.go) constructs the
// concrete Notifier implementation at startup; tests inject
// alternatives via outbox.SetAlertNotifier.
//
// Design notes:
//
//   - The interface is intentionally tiny (one method) so a future
//     Slack / PagerDuty / Kafka sink can implement it without forcing
//     every existing call site to grow helpers.
//   - Notifier implementations MUST be safe for concurrent calls:
//     outbox dispatcher fires Notify from a single goroutine but a
//     future multi-dispatcher world would parallelise them.
//   - Returning a non-nil error signals "delivery failed" to the
//     outbox dispatcher. Transient errors (network blip) should be
//     wrapped as outbox.Transient; permanent (4xx from sink) as
//     outbox.Permanent. The handler in production.go decides based
//     on the call site.
package alerts

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

// Severity is the alert urgency level. Mirrors Prometheus-style
// severities so a future `/alerts/severity=error` query on the
// admin endpoint maps cleanly.
type Severity string

const (
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
	SeverityFatal Severity = "fatal"
)

// Alert is the canonical in-memory representation of a NOTIFICATION.
// Kept distinct from outbox.Event so the alerts package does NOT depend
// on outbox (one-way dependency: outbox → alerts).
type Alert struct {
	// Source identifies the producer (e.g. "outbox.JOB_FAILED").
	Source string
	// Severity is the urgency classification.
	Severity Severity
	// Subject is a short identifier (e.g. job_id, run_id). The
	// freeform Body carries the human-readable detail.
	Subject string
	// Body carries the operator-visible message.
	Body string
	// Tags are key=value provenance tags (e.g. error_code).
	Tags map[string]string
	// Timestamp is the wall-clock at which the alert was constructed.
	Timestamp time.Time
}

// Notifier is the interface every alert sink must satisfy.
type Notifier interface {
	Notify(ctx context.Context, alert Alert) error
}

// ── NopNotifier ───────────────────────────────────────────────────────────

// NopNotifier is the safe-zero default: satisfies Notifier without
// producing side-effects. Used as the package-level default in
// production.go BEFORE SetAlertNotifier is called at boot, and as the
// test default so unit tests don't have to wire a real sink.
type NopNotifier struct{}

// Notify implements Notifier.
func (NopNotifier) Notify(_ context.Context, _ Alert) error { return nil }

// ── LogNotifier ───────────────────────────────────────────────────────────

// LogNotifier writes alerts to a structured log line via log.Printf.
// Cheap, always-available, and what a staging cluster uses when no
// real sink is wired.
type LogNotifier struct {
	// Prefix is prepended to every log line (default "[ALERTS]").
	Prefix string
	// Logger is the optional logger to use; nil defaults to the stdlib
	// log.Default(). Production code typically passes nil.
	Logger *log.Logger
}

// NewLogNotifier builds a LogNotifier with the canonical prefix.
func NewLogNotifier(prefix string) *LogNotifier {
	if prefix == "" {
		prefix = "[ALERTS]"
	}
	return &LogNotifier{Prefix: prefix, Logger: nil}
}

// Notify implements Notifier by printing one structured line per alert.
func (n *LogNotifier) Notify(_ context.Context, alert Alert) error {
	logger := n.Logger
	if logger == nil {
		logger = log.Default()
	}
	tags := ""
	if len(alert.Tags) > 0 {
		// deterministic ordering for grep-ability (insertion sort).
		keys := make([]string, 0, len(alert.Tags))
		for k := range alert.Tags {
			keys = append(keys, k)
		}
		for i := 1; i < len(keys); i++ {
			for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
				keys[j-1], keys[j] = keys[j], keys[j-1]
			}
		}
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+"="+alert.Tags[k])
		}
		// stdlib strings.Join — keeps the dogfooding minimal.
		tags = " " + strings.Join(parts, ",")
	}
	logger.Printf("%s severity=%s source=%s subject=%q body=%q%s",
		n.Prefix, alert.Severity, alert.Source, alert.Subject, alert.Body, tags)
	return nil
}

// ── MultiNotifier ─────────────────────────────────────────────────────────

// MultiNotifier fans out a single Notify call to N child Notifiers.
// The first non-nil error from a child is collected; we do NOT short
// circuit so every child gets the alert even if a sibling fails. The
// returned error is the aggregated failure (or nil if all succeeded).
//
// Concurrency: fan-out is sequential (one goroutine — the dispatcher
// fired us — doesn't need internal goroutines). Sequential keeps stack
// traces meaningful and avoids silent ordering concerns.
type MultiNotifier struct {
	// Children: the list of fan-out targets. nil-tolerant (a nil child
	// is skipped — useful for tests that wire a partial graph).
	Children []Notifier
	// mu protects concurrent calls if the MultiNotifier itself is
	// shared across goroutines. In practice the dispatcher only fires
	// us from one goroutine, but a future multi-dispatcher world
	// would parallelise, so the lock is here.
	mu sync.Mutex
}

// Notify implements Notifier. Returns the first non-nil error OR an
// aggregated error if multiple children failed.
func (m *MultiNotifier) Notify(ctx context.Context, alert Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, child := range m.Children {
		if child == nil {
			continue
		}
		if err := child.Notify(ctx, alert); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ── Defaults ──────────────────────────────────────────────────────────────

// Default is the safe-zero no-op notifier. Used as the package-level
// default before SetDefault is called.
var Default Notifier = NopNotifier{}
