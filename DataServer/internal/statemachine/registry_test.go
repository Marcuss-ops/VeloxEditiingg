package statemachine

import (
	"errors"
	"testing"
)

func TestRegistryRejectsIllegalTransitions(t *testing.T) {
	r := DefaultRegistry()
	cases := []struct {
		domain   Domain
		from, to string
		actor    Actor
	}{
		{DomainJob, "SUCCEEDED", "RUNNING", ActorScheduler},
		{DomainTask, "CANCELLED", "READY", ActorScheduler},
		{DomainArtifact, "READY", "VERIFYING", ActorArtifactFinalizer},
		{DomainDelivery, "SUCCEEDED", "RUNNING", ActorDeliveryRunner},
		{DomainWorkerSession, "REVOKED", "ACTIVE", ActorWorkerSession},
	}
	for _, tc := range cases {
		if err := r.Validate(tc.domain, tc.from, tc.to, tc.actor); err == nil {
			t.Errorf("Validate(%s, %s -> %s, %s) accepted illegal transition", tc.domain, tc.from, tc.to, tc.actor)
		} else {
			var transitionErr *TransitionError
			if !errors.As(err, &transitionErr) {
				t.Errorf("Validate error type=%T, want *TransitionError", err)
			}
		}
	}
}

func TestRegistryEnforcesCanonicalActor(t *testing.T) {
	r := DefaultRegistry()
	if err := r.Validate(DomainJob, "RUNNING", "SUCCEEDED", ActorScheduler); err == nil {
		t.Fatal("scheduler was allowed to write job SUCCEEDED")
	}
	if err := r.Validate(DomainJob, "RUNNING", "SUCCEEDED", ActorArtifactFinalizer); err != nil {
		t.Fatalf("artifact finalizer rejected canonical transition: %v", err)
	}
}

func TestRegistryRulesExposeEventsInvariantsAndIdempotency(t *testing.T) {
	r := DefaultRegistry()
	rule, ok := r.Rule(DomainJob, "RUNNING", "SUCCEEDED")
	if !ok {
		t.Fatal("missing RUNNING -> SUCCEEDED rule")
	}
	if rule.Actor != ActorArtifactFinalizer || rule.Idempotent || len(rule.Emits) != 1 || rule.Emits[0].Name != "JOB_SUCCEEDED" {
		t.Fatalf("unexpected success rule: %#v", rule)
	}
	if len(rule.Requires) != 2 {
		t.Fatalf("requires=%v, want job/task and artifact invariants", rule.Requires)
	}
	if got := len(r.Rules()); got < 50 {
		t.Fatalf("rule count=%d, registry appears incomplete", got)
	}
	if got := len(r.Invariants()); got < 6 {
		t.Fatalf("invariant count=%d, want at least 6", got)
	}
}

func TestRegistryAllowsOnlyExplicitIdempotency(t *testing.T) {
	r := DefaultRegistry()
	for _, tc := range []struct {
		domain Domain
		state  string
	}{
		{DomainJob, "SUCCEEDED"}, {DomainTask, "RUNNING"}, {DomainDelivery, "FAILED"},
	} {
		rule, ok := r.Rule(tc.domain, tc.state, tc.state)
		if !ok || !rule.Idempotent {
			t.Fatalf("missing idempotent rule for %s %s: %#v", tc.domain, tc.state, rule)
		}
	}
	if err := r.Validate(DomainJob, "SUCCEEDED", "RUNNING", ActorAny); err == nil {
		t.Fatal("terminal job transition unexpectedly accepted")
	}
}
