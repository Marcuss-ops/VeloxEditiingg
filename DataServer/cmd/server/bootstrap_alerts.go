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
// The production sink is a MultiNotifier wrapping the always-on
// LogNotifier and, when configured, one canonical Slack or Telegram
// webhook notifier. Future sinks (PagerDuty / Kafka / admin HTTP
// endpoint) can append here without touching the outbox handler or
// dispatcher — outbox.SetAlertNotifier is the single integration seam.

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
// from buildAppComponents BEFORE any supervisor goroutine starts, so
// both the outbox JOB_FAILED handler and compute alert engine see the
// same wired sink on their first invocation.
//
// Sink composition (greppable from this single function):
//
//	┌─ alerts.MultiNotifier
//	│
//	├─ LogNotifier("[ALERTS]") — minimum-viable visibility, always on.
//	├─ optional Slack/Telegram webhook notifier from typed config.
//	Future sinks can be appended to Children here without changing the
//	outbox handler or dispatcher.
//
// The MultiNotifier does NOT short-circuit on individual failures, so
// a Slack outage does NOT silence the log sink.
func buildAlerts(webhookURL, webhookType string) (*alertsDeps, error) {
	children := []alerts.Notifier{alerts.NewLogNotifier("[ALERTS]")}
	if webhook := alerts.NewWebhookNotifier(webhookURL, webhookType); webhook != nil {
		children = append(children, webhook)
	}
	n := &alerts.MultiNotifier{Children: children}

	// Register the sink with the outbox package BEFORE the dispatcher
	// goroutine starts so no JOB_FAILED event is silently dropped
	// between composition and dispatcher wiring.
	outbox.SetAlertNotifier(n)

	return &alertsDeps{Notifier: n}, nil
}
