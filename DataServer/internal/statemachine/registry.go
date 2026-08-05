// Package statemachine is the executable catalogue of Velox lifecycle rules.
//
// It is deliberately persistence-agnostic: store repositories remain the
// single writers of state, while every writer-facing service can validate its
// intended transition against this registry before issuing its existing CAS.
package statemachine

import (
	"fmt"
	"sort"
)

// Domain identifies one persisted lifecycle owned by the platform.
type Domain string

const (
	DomainJob            Domain = "job"
	DomainTask           Domain = "task"
	DomainArtifact       Domain = "artifact"
	DomainArtifactUpload Domain = "artifact_upload"
	DomainDelivery       Domain = "delivery"
	DomainWorkerSession  Domain = "worker_session"
)

// Actor identifies the canonical writer/authority for a transition.
type Actor string

const (
	ActorAny               Actor = "any"
	ActorSystem            Actor = "system"
	ActorScheduler         Actor = "scheduler"
	ActorWorker            Actor = "worker"
	ActorReaper            Actor = "reaper"
	ActorArtifactFinalizer Actor = "artifact_finalizer"
	ActorUploadReceiver    Actor = "upload_receiver"
	ActorDeliveryRunner    Actor = "delivery_runner"
	ActorOperator          Actor = "operator"
	ActorWorkerSession     Actor = "worker_session"
)

// Event is a stable domain event emitted after a successful transition.
type Event struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// Invariant names are shared by transition rules and the read-only auditor.
const (
	InvariantTaskAttemptPair      = "task_attempt_pair"
	InvariantJobTaskConvergence   = "job_task_convergence"
	InvariantArtifactReadyBlob    = "artifact_ready_blob"
	InvariantDeliveryRemoteID     = "delivery_remote_id"
	InvariantWorkerSingleSession  = "worker_single_active_session"
	InvariantUploadTerminalFields = "upload_terminal_fields"
)

// Invariant describes a named condition required by a transition or checked
// by audit-invariants. SQL implementations live in the auditor, not here.
type Invariant struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// TransitionRule is an executable lifecycle rule. Actor is the canonical
// authority; callers that need a different authority must add an explicit
// rule instead of silently widening ownership.
type TransitionRule struct {
	Domain     Domain   `json:"domain"`
	From       string   `json:"from"`
	To         string   `json:"to"`
	Actor      Actor    `json:"actor"`
	Requires   []string `json:"requires,omitempty"`
	Emits      []Event  `json:"emits,omitempty"`
	Idempotent bool     `json:"idempotent"`
}

// TransitionError is returned for an unknown or illegal transition.
type TransitionError struct {
	Domain Domain
	From   string
	To     string
	Actor  Actor
	Reason string
}

func (e *TransitionError) Error() string {
	if e.Actor != "" {
		return fmt.Sprintf("%s: illegal transition %s -> %s for actor %s: %s", e.Domain, e.From, e.To, e.Actor, e.Reason)
	}
	return fmt.Sprintf("%s: illegal transition %s -> %s: %s", e.Domain, e.From, e.To, e.Reason)
}

// Registry contains immutable transition and invariant definitions.
type Registry struct {
	rules      map[Domain]map[string]TransitionRule
	invariants map[string]Invariant
}

// DefaultRegistry returns the process-wide canonical lifecycle catalogue.
// The returned value owns copied maps/slices and is safe for concurrent reads.
func DefaultRegistry() *Registry {
	r := &Registry{
		rules:      make(map[Domain]map[string]TransitionRule),
		invariants: make(map[string]Invariant),
	}
	for _, invariant := range []Invariant{
		{Name: InvariantTaskAttemptPair, Description: "a non-terminal task has a matching active attempt; terminal task and attempt states converge"},
		{Name: InvariantJobTaskConvergence, Description: "a SUCCEEDED job has only SUCCEEDED tasks; a terminal failed/cancelled job has no active task"},
		{Name: InvariantArtifactReadyBlob, Description: "a READY artifact has a durable storage key or local path"},
		{Name: InvariantDeliveryRemoteID, Description: "a SUCCEEDED delivery has a remote object identifier"},
		{Name: InvariantWorkerSingleSession, Description: "a worker has at most one active, non-revoked session per session type"},
		{Name: InvariantUploadTerminalFields, Description: "a COMPLETED upload has completion metadata"},
	} {
		r.invariants[invariant.Name] = invariant
	}

	add := func(domain Domain, from, to string, actor Actor, requires []string, event string, idempotent bool) {
		rule := TransitionRule{Domain: domain, From: from, To: to, Actor: actor, Requires: append([]string(nil), requires...), Idempotent: idempotent}
		if event != "" {
			rule.Emits = []Event{{Name: event, Description: fmt.Sprintf("%s transition %s -> %s", domain, from, to)}}
		}
		if r.rules[domain] == nil {
			r.rules[domain] = make(map[string]TransitionRule)
		}
		r.rules[domain][transitionKey(from, to)] = rule
	}
	idempotent := func(domain Domain, states []string) {
		for _, state := range states {
			add(domain, state, state, ActorAny, nil, "", true)
		}
	}

	// Jobs: the verified artifact finalizer is the sole canonical SUCCEEDED writer.
	add(DomainJob, "", "PENDING", ActorSystem, nil, "JOB_CREATED", false)
	add(DomainJob, "", "LEASED", ActorScheduler, nil, "JOB_LEASED", false)
	add(DomainJob, "PENDING", "LEASED", ActorScheduler, nil, "JOB_LEASED", false)
	add(DomainJob, "PENDING", "RUNNING", ActorScheduler, nil, "JOB_STARTED", false)
	add(DomainJob, "PENDING", "RETRY_WAIT", ActorScheduler, nil, "JOB_RETRY_SCHEDULED", false)
	add(DomainJob, "PENDING", "FAILED", ActorSystem, nil, "JOB_FAILED", false)
	add(DomainJob, "PENDING", "CANCELLED", ActorOperator, nil, "JOB_CANCELLED", false)
	add(DomainJob, "LEASED", "RUNNING", ActorScheduler, []string{InvariantTaskAttemptPair}, "JOB_STARTED", false)
	add(DomainJob, "LEASED", "FAILED", ActorSystem, nil, "JOB_FAILED", false)
	add(DomainJob, "LEASED", "CANCELLED", ActorOperator, nil, "JOB_CANCELLED", false)
	add(DomainJob, "RUNNING", "AWAITING_ARTIFACT", ActorSystem, []string{InvariantTaskAttemptPair}, "JOB_AWAITING_ARTIFACT", false)
	add(DomainJob, "RUNNING", "SUCCEEDED", ActorArtifactFinalizer, []string{InvariantJobTaskConvergence, InvariantArtifactReadyBlob}, "JOB_SUCCEEDED", false)
	add(DomainJob, "RUNNING", "FAILED", ActorSystem, nil, "JOB_FAILED", false)
	add(DomainJob, "RUNNING", "RETRY_WAIT", ActorScheduler, nil, "JOB_RETRY_SCHEDULED", false)
	add(DomainJob, "RUNNING", "CANCELLED", ActorOperator, nil, "JOB_CANCELLED", false)
	add(DomainJob, "AWAITING_ARTIFACT", "SUCCEEDED", ActorArtifactFinalizer, []string{InvariantJobTaskConvergence, InvariantArtifactReadyBlob}, "JOB_SUCCEEDED", false)
	add(DomainJob, "AWAITING_ARTIFACT", "DELIVERING", ActorArtifactFinalizer, []string{InvariantArtifactReadyBlob}, "JOB_DELIVERING", false)
	add(DomainJob, "RUNNING", "DELIVERING", ActorArtifactFinalizer, []string{InvariantArtifactReadyBlob}, "JOB_DELIVERING", false)
	add(DomainJob, "DELIVERING", "SUCCEEDED", ActorDeliveryRunner, []string{InvariantJobTaskConvergence, InvariantArtifactReadyBlob, InvariantDeliveryRemoteID}, "JOB_SUCCEEDED", false)
	add(DomainJob, "DELIVERING", "FAILED", ActorDeliveryRunner, nil, "JOB_FAILED", false)
	add(DomainJob, "DELIVERING", "CANCELLED", ActorOperator, nil, "JOB_CANCELLED", false)
	add(DomainJob, "AWAITING_ARTIFACT", "FAILED", ActorArtifactFinalizer, nil, "JOB_FAILED", false)
	add(DomainJob, "AWAITING_ARTIFACT", "CANCELLED", ActorOperator, nil, "JOB_CANCELLED", false)
	add(DomainJob, "RETRY_WAIT", "PENDING", ActorScheduler, nil, "JOB_REQUEUED", false)
	add(DomainJob, "RETRY_WAIT", "FAILED", ActorScheduler, nil, "JOB_FAILED", false)
	add(DomainJob, "RETRY_WAIT", "CANCELLED", ActorOperator, nil, "JOB_CANCELLED", false)
	idempotent(DomainJob, []string{"PENDING", "LEASED", "RUNNING", "AWAITING_ARTIFACT", "DELIVERING", "RETRY_WAIT", "SUCCEEDED", "FAILED", "CANCELLED"})

	// Tasks: lease expiry is master-owned; terminal reports are worker-owned.
	add(DomainTask, "", "PENDING", ActorSystem, nil, "TASK_CREATED", false)
	add(DomainTask, "PENDING", "READY", ActorScheduler, nil, "TASK_READY", false)
	add(DomainTask, "PENDING", "LEASED", ActorScheduler, nil, "TASK_LEASED", false)
	add(DomainTask, "PENDING", "RUNNING", ActorWorker, []string{InvariantTaskAttemptPair}, "TASK_STARTED", false)
	add(DomainTask, "PENDING", "FAILED", ActorSystem, nil, "TASK_FAILED", false)
	add(DomainTask, "PENDING", "CANCELLED", ActorOperator, nil, "TASK_CANCELLED", false)
	add(DomainTask, "READY", "LEASED", ActorScheduler, nil, "TASK_LEASED", false)
	add(DomainTask, "READY", "RUNNING", ActorWorker, []string{InvariantTaskAttemptPair}, "TASK_STARTED", false)
	add(DomainTask, "READY", "FAILED", ActorSystem, nil, "TASK_FAILED", false)
	add(DomainTask, "READY", "CANCELLED", ActorOperator, nil, "TASK_CANCELLED", false)
	add(DomainTask, "LEASED", "RUNNING", ActorWorker, []string{InvariantTaskAttemptPair}, "TASK_STARTED", false)
	add(DomainTask, "LEASED", "FAILED", ActorReaper, nil, "TASK_FAILED", false)
	add(DomainTask, "LEASED", "CANCELLED", ActorOperator, nil, "TASK_CANCELLED", false)
	add(DomainTask, "RUNNING", "SUCCEEDED", ActorWorker, []string{InvariantTaskAttemptPair}, "TASK_SUCCEEDED", false)
	add(DomainTask, "RUNNING", "FAILED", ActorWorker, []string{InvariantTaskAttemptPair}, "TASK_FAILED", false)
	add(DomainTask, "RUNNING", "CANCELLED", ActorOperator, nil, "TASK_CANCELLED", false)
	add(DomainTask, "RUNNING", "TIMED_OUT", ActorReaper, []string{InvariantTaskAttemptPair}, "TASK_TIMED_OUT", false)
	idempotent(DomainTask, []string{"PENDING", "READY", "LEASED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"})

	// Artifact and upload state machines.
	add(DomainArtifact, "STAGING", "VERIFYING", ActorArtifactFinalizer, nil, "ARTIFACT_VERIFYING", false)
	add(DomainArtifact, "STAGING", "FAILED", ActorArtifactFinalizer, nil, "ARTIFACT_FAILED", false)
	add(DomainArtifact, "VERIFYING", "READY", ActorArtifactFinalizer, []string{InvariantArtifactReadyBlob}, "ARTIFACT_READY", false)
	add(DomainArtifact, "VERIFYING", "QUARANTINED", ActorArtifactFinalizer, nil, "ARTIFACT_QUARANTINED", false)
	add(DomainArtifact, "READY", "QUARANTINED", ActorSystem, nil, "ARTIFACT_QUARANTINED", false)
	add(DomainArtifact, "FAILED", "DELETED", ActorSystem, nil, "ARTIFACT_DELETED", false)
	add(DomainArtifact, "QUARANTINED", "DELETED", ActorSystem, nil, "ARTIFACT_DELETED", false)
	add(DomainArtifact, "DELETED", "DELETED", ActorSystem, nil, "", true)
	idempotent(DomainArtifact, []string{"STAGING", "VERIFYING", "READY", "QUARANTINED", "FAILED", "DELETED"})

	add(DomainArtifactUpload, "CREATED", "UPLOADING", ActorUploadReceiver, nil, "UPLOAD_STARTED", false)
	add(DomainArtifactUpload, "UPLOADING", "RECEIVED", ActorUploadReceiver, nil, "UPLOAD_RECEIVED", false)
	add(DomainArtifactUpload, "RECEIVED", "FINALIZING", ActorArtifactFinalizer, nil, "UPLOAD_FINALIZING", false)
	add(DomainArtifactUpload, "FINALIZING", "COMPLETED", ActorArtifactFinalizer, []string{InvariantUploadTerminalFields}, "UPLOAD_COMPLETED", false)
	add(DomainArtifactUpload, "CREATED", "FAILED", ActorSystem, nil, "UPLOAD_FAILED", false)
	add(DomainArtifactUpload, "UPLOADING", "FAILED", ActorSystem, nil, "UPLOAD_FAILED", false)
	add(DomainArtifactUpload, "RECEIVED", "FAILED", ActorSystem, nil, "UPLOAD_FAILED", false)
	add(DomainArtifactUpload, "FINALIZING", "FAILED", ActorSystem, nil, "UPLOAD_FAILED", false)
	add(DomainArtifactUpload, "CREATED", "EXPIRED", ActorReaper, nil, "UPLOAD_EXPIRED", false)
	add(DomainArtifactUpload, "UPLOADING", "EXPIRED", ActorReaper, nil, "UPLOAD_EXPIRED", false)
	add(DomainArtifactUpload, "RECEIVED", "EXPIRED", ActorReaper, nil, "UPLOAD_EXPIRED", false)
	add(DomainArtifactUpload, "FINALIZING", "EXPIRED", ActorReaper, nil, "UPLOAD_EXPIRED", false)
	idempotent(DomainArtifactUpload, []string{"CREATED", "UPLOADING", "RECEIVED", "FINALIZING", "COMPLETED", "FAILED", "EXPIRED"})

	// Deliveries.
	add(DomainDelivery, "PENDING", "RUNNING", ActorDeliveryRunner, nil, "DELIVERY_STARTED", false)
	add(DomainDelivery, "RUNNING", "RETRY_WAIT", ActorDeliveryRunner, nil, "DELIVERY_RETRY_WAIT", false)
	add(DomainDelivery, "RUNNING", "SUCCEEDED", ActorDeliveryRunner, []string{InvariantDeliveryRemoteID}, "DELIVERY_COMPLETED", false)
	add(DomainDelivery, "RUNNING", "FAILED", ActorDeliveryRunner, nil, "DELIVERY_FAILED", false)
	add(DomainDelivery, "RUNNING", "BLOCKED_AUTH", ActorDeliveryRunner, nil, "DELIVERY_BLOCKED_AUTH", false)
	add(DomainDelivery, "RUNNING", "CANCELLED", ActorOperator, nil, "DELIVERY_CANCELLED", false)
	add(DomainDelivery, "RETRY_WAIT", "RUNNING", ActorDeliveryRunner, nil, "DELIVERY_STARTED", false)
	add(DomainDelivery, "RETRY_WAIT", "CANCELLED", ActorOperator, nil, "DELIVERY_CANCELLED", false)
	add(DomainDelivery, "PENDING", "CANCELLED", ActorOperator, nil, "DELIVERY_CANCELLED", false)
	idempotent(DomainDelivery, []string{"PENDING", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED", "BLOCKED_AUTH", "CANCELLED"})

	// Worker sessions are master-admitted and never resurrected in place.
	add(DomainWorkerSession, "", "ACTIVE", ActorWorkerSession, nil, "WORKER_SESSION_CONNECTED", false)
	add(DomainWorkerSession, "ACTIVE", "DISCONNECTED", ActorWorkerSession, nil, "WORKER_SESSION_DISCONNECTED", false)
	add(DomainWorkerSession, "ACTIVE", "REVOKED", ActorOperator, nil, "WORKER_SESSION_REVOKED", false)
	add(DomainWorkerSession, "DISCONNECTED", "REVOKED", ActorOperator, nil, "WORKER_SESSION_REVOKED", false)
	add(DomainWorkerSession, "DISCONNECTED", "ACTIVE", ActorWorkerSession, nil, "WORKER_SESSION_RECONNECTED", false)
	idempotent(DomainWorkerSession, []string{"ACTIVE", "DISCONNECTED", "REVOKED"})

	return r
}

func transitionKey(from, to string) string { return from + "\x00" + to }

// Rule returns a defensive copy of one exact transition rule.
func (r *Registry) Rule(domain Domain, from, to string) (TransitionRule, bool) {
	if r == nil || r.rules[domain] == nil {
		return TransitionRule{}, false
	}
	rule, ok := r.rules[domain][transitionKey(from, to)]
	if !ok {
		return TransitionRule{}, false
	}
	rule.Requires = append([]string(nil), rule.Requires...)
	rule.Emits = append([]Event(nil), rule.Emits...)
	return rule, true
}

// Validate checks legality, actor ownership, and rejects empty targets.
func (r *Registry) Validate(domain Domain, from, to string, actor Actor) error {
	if to == "" {
		return &TransitionError{Domain: domain, From: from, To: to, Actor: actor, Reason: "target state is empty"}
	}
	if rule, ok := r.Rule(domain, from, to); ok {
		if actor != "" && rule.Actor != actor && rule.Actor != ActorAny {
			return &TransitionError{Domain: domain, From: from, To: to, Actor: actor, Reason: fmt.Sprintf("canonical actor is %s", rule.Actor)}
		}
		return nil
	}
	return &TransitionError{Domain: domain, From: from, To: to, Actor: actor, Reason: "no registered rule"}
}

// CanTransition is the actor-neutral validation used by compatibility adapters.
func (r *Registry) CanTransition(domain Domain, from, to string) bool {
	return r.Validate(domain, from, to, "") == nil
}

// States returns every state mentioned by the domain's registered rules in
// stable order. Auditors use this instead of maintaining a second allowlist.
func (r *Registry) States(domain Domain) []string {
	if r == nil {
		return nil
	}
	seen := make(map[string]struct{})
	for _, rule := range r.rules[domain] {
		if rule.From != "" {
			seen[rule.From] = struct{}{}
		}
		if rule.To != "" {
			seen[rule.To] = struct{}{}
		}
	}
	states := make([]string, 0, len(seen))
	for state := range seen {
		states = append(states, state)
	}
	sort.Strings(states)
	return states
}

// Rules returns all rules in stable order for audit tooling and docs.
func (r *Registry) Rules() []TransitionRule {
	if r == nil {
		return nil
	}
	var out []TransitionRule
	for _, byKey := range r.rules {
		for _, rule := range byKey {
			out = append(out, rule)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Domain != out[j].Domain {
			return out[i].Domain < out[j].Domain
		}
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

// Invariants returns all invariant definitions in stable order.
func (r *Registry) Invariants() []Invariant {
	if r == nil {
		return nil
	}
	out := make([]Invariant, 0, len(r.invariants))
	for _, invariant := range r.invariants {
		out = append(out, invariant)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
