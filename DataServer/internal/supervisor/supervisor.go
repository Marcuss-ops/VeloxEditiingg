// Package supervisor / supervisor.go
//
// Background runner supervisor: owns a set of Runner entries and drives
// their lifecycle with class-specific restart semantics. The supervisor
// is the canonical entry point for long-lived background loops (delivery,
// outbox, forwarding, metrics, …) so a single component owns the
// goroutine topology, the supervised-state map, the /ready diagnostics,
// and the failure-escalation contract.
//
// Three RunnerClass values drive the restart semantics:
//
//   - ClassOneShot     — runs once, exits, never restarted. Setup tasks.
//   - ClassRestartable — runs forever; bounded retries + exponential
//     backoff. After exhaustion the runner is removed
//     and the supervisor emits a WARN.
//   - ClassCritical    — runs forever; infinite retries (or bounded when
//     Policy.MaxRetries > 0). On exhaustion cancels
//     the supervisor-internal ctx and returns a fatal
//     error so Kubernetes restarts the pod.
//
// File split by responsibility:
//   - supervisor.go           → package doc (this file)
//   - supervisor_types.go     → classes, policy, state, Runner/Supervisor types
//   - supervisor_run.go       → Run / runLoop / safeCall / sleepCtx
//   - supervisor_diagnostics.go → Len / Names / Classes / Missing / States
package supervisor
