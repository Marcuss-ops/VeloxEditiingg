package main

// bootstrap_alerts.go — composition-root wiring for the alerts sink.
//
// PR-OUTBOX-HANDLER follow-up: the OUTBOX JOB_FAILED handler
// (internal/outbox/production.go) decodes the canonical payload
// {job_id, error_code, error} and forwards an Alert to a Notifier.
// This file builds the production Notifier and registers it with
// outbox.SetAlertNotifier BEFORE the OutboxDispatcher goroutine
// starts.
//
// Today the production sink is a MultiNotifier wrapping:
//
//   1. LogNotifier — guarantees operator-visibility even on a fresh
//      boot when no external sink is wired (every alert is at least
//      logged; nothing is silently dropped).
//
// Future sinks (Slack / PagerDuty / Kafka / admin HTTP endpoint) can
// append to buildAlerts's MultiNotifier without touching the
// outbox handler or the dispatcher — outbox.SetAlertNotifier is the
// single integration seam.

import (
	"velox-server/internal/alerts"
	"velox-server/internal/outbox"
)

// alertsDeps holds the production-wired alert surface. Right now only
// the Notifier is exposed; future fields (admin alert queue,
// /api/v1/admin/alerts subscriber channel) will land here so the
// composition root stays declarative.
type alertsDeps struct {
	Notifier alerts.Notifier
}

// buildAlerts constructs the canonical production Notifier. Called
// from runServer BEFORE any supervisor goroutine starts, so the
// outbox JOB_FAILED handler (which reads outbox.AlertNotifier()) sees
// the wired sink on its very first invocation.
//
// Sink composition (greppable from this single function):
//
//	┌─ alerts.MultiNotifier
//	│
//	├─ LogNotifier("[ALERTS]") — minimum-viable visibility, always on.
//
//	Future sinks: append to children here, e.g.:
//
//	  &slackNotifier{channel: cfg.Alerts.SlackChannel, token: cfg.Alerts.SlackToken},
//	  &pagerDutyNotifier{apiKey: cfg.Alerts.PagerDutyKey, ...},
//
// The MultiNotifier does NOT short-circuit on individual failures, so
// a Slack outage does NOT silence the log sink.
func buildAlerts() (*alertsDeps, error) {
	n := &alerts.MultiNotifier{
		Children: []alerts.Notifier{
			alerts.NewLogNotifier("[ALERTS]"),
		},
	}

	// Register the sink with the outbox package BEFORE the dispatcher
	// goroutine starts so no JOB_FAILED event is silently dropped
	// between composition and dispatcher wiring.
	outbox.SetAlertNotifier(n)

	return &alertsDeps{Notifier: n}, nil
}
