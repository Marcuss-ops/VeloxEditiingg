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

func TestTerminalStatesHaveOnlyCanonicalWriters(t *testing.T) {
	r := DefaultRegistry()
	allActors := []Actor{ActorAny, ActorSystem, ActorScheduler, ActorWorker, ActorReaper, ActorUploadReceiver, ActorOperator, ActorWorkerSession, ActorArtifactFinalizer, ActorDeliveryRunner}
	for _, tc := range []struct {
		from          string
		canonical     Actor
		label         string
	}{
		{"RUNNING", ActorArtifactFinalizer, "job finalization"},
		{"AWAITING_ARTIFACT", ActorArtifactFinalizer, "job finalization after artifact wait"},
		{"DELIVERING", ActorDeliveryRunner, "job delivery completion"},
	} {
		rule, ok := r.Rule(DomainJob, tc.from, "SUCCEEDED")
		if !ok || rule.Actor != tc.canonical {
			t.Fatalf("%s writer rule = %#v, want %q", tc.label, rule, tc.canonical)
		}
		for _, actor := range allActors {
			wantAllowed := actor == tc.canonical
			err := r.Validate(DomainJob, tc.from, "SUCCEEDED", actor)
			if (err == nil) != wantAllowed {
				t.Errorf("%s actor %q allowed=%v, want %v (err=%v)", tc.label, actor, err == nil, wantAllowed, err)
			}
		}
	}
	for _, actor := range allActors {
		wantAllowed := actor == ActorDeliveryRunner
		err := r.Validate(DomainDelivery, "RUNNING", "SUCCEEDED", actor)
		if (err == nil) != wantAllowed {
			t.Errorf("delivery SUCCEEDED actor %q allowed=%v, want %v (err=%v)", actor, err == nil, wantAllowed, err)
		}
	}
	if err := r.Validate(DomainJob, "RUNNING", "PUBLISHED", ActorArtifactFinalizer); err == nil {
		t.Fatal("job lifecycle accepted publication state PUBLISHED")
	}
	if err := r.Validate(DomainDelivery, "RUNNING", "PUBLISHED", ActorDeliveryRunner); err == nil {
		t.Fatal("delivery lifecycle accepted publication state PUBLISHED")
	}
	if err := r.Validate(DomainJob, "RUNNING", "completed", ActorSystem); err == nil {
		t.Fatal("job lifecycle accepted input-assembly state completed")
	}
}

func TestAllTerminalTransitionsHaveExplicitLifecycleWriters(t *testing.T) {
	r := DefaultRegistry()
	allActors := []Actor{ActorAny, ActorSystem, ActorScheduler, ActorWorker, ActorReaper, ActorUploadReceiver, ActorOperator, ActorWorkerSession, ActorArtifactFinalizer, ActorDeliveryRunner}
	cases := []struct {
		domain       Domain
		from, to     string
		canonical    Actor
	}{
		{DomainJob, "PENDING", "FAILED", ActorSystem},
		{DomainJob, "PENDING", "CANCELLED", ActorOperator},
		{DomainJob, "LEASED", "FAILED", ActorSystem},
		{DomainJob, "LEASED", "CANCELLED", ActorOperator},
		{DomainJob, "RUNNING", "FAILED", ActorSystem},
		{DomainJob, "RUNNING", "CANCELLED", ActorOperator},
		{DomainJob, "AWAITING_ARTIFACT", "FAILED", ActorArtifactFinalizer},
		{DomainJob, "AWAITING_ARTIFACT", "CANCELLED", ActorOperator},
		{DomainJob, "DELIVERING", "FAILED", ActorDeliveryRunner},
		{DomainJob, "DELIVERING", "CANCELLED", ActorOperator},
		{DomainJob, "RETRY_WAIT", "FAILED", ActorScheduler},
		{DomainJob, "RETRY_WAIT", "CANCELLED", ActorOperator},
		{DomainDelivery, "RUNNING", "FAILED", ActorDeliveryRunner},
		{DomainDelivery, "RUNNING", "BLOCKED_AUTH", ActorDeliveryRunner},
		{DomainDelivery, "RUNNING", "CANCELLED", ActorOperator},
		{DomainDelivery, "RETRY_WAIT", "CANCELLED", ActorOperator},
		{DomainDelivery, "PENDING", "CANCELLED", ActorOperator},
	}
	for _, tc := range cases {
		rule, ok := r.Rule(tc.domain, tc.from, tc.to)
		if !ok || rule.Actor != tc.canonical {
			t.Fatalf("%s %s -> %s rule = %#v, want actor %q", tc.domain, tc.from, tc.to, rule, tc.canonical)
		}
		for _, actor := range allActors {
			wantAllowed := actor == tc.canonical
			err := r.Validate(tc.domain, tc.from, tc.to, actor)
			if (err == nil) != wantAllowed {
				t.Errorf("%s %s -> %s actor %q allowed=%v, want %v (err=%v)", tc.domain, tc.from, tc.to, actor, err == nil, wantAllowed, err)
			}
		}
	}
}

func TestEveryNonIdempotentTerminalTransitionHasLifecycleActor(t *testing.T) {
	r := DefaultRegistry()
	terminalStates := map[Domain]map[string]bool{
		DomainJob:            {"SUCCEEDED": true, "FAILED": true, "CANCELLED": true},
		DomainTask:           {"SUCCEEDED": true, "FAILED": true, "CANCELLED": true, "TIMED_OUT": true},
		DomainArtifact:       {"DELETED": true, "QUARANTINED": true, "FAILED": true},
		DomainArtifactUpload: {"COMPLETED": true, "FAILED": true, "EXPIRED": true},
		DomainDelivery:       {"SUCCEEDED": true, "FAILED": true, "BLOCKED_AUTH": true, "CANCELLED": true},
		DomainWorkerSession:  {"REVOKED": true},
	}
	for _, rule := range r.Rules() {
		if !terminalStates[rule.Domain][rule.To] || rule.From == rule.To {
			continue
		}
		if rule.Actor == ActorAny || rule.Actor == "" {
			t.Errorf("terminal transition %s %s -> %s has no lifecycle writer actor", rule.Domain, rule.From, rule.To)
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
