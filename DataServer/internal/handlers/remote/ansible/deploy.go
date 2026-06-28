package ansible

// PR 1: the in-process ansible-playbook executor was removed in PR 8 and
// the RunPlaybook fake-executor stub in manager_runs.go followed in PR 1.
// All deploy-time staging helpers (batch planning, root-run creation,
// wait-for-run goroutine, plan struct) were private to this file and
// were called only from the RunPlaybook-driven goroutine that the legacy
// deploy path used. Now that every ansible action route resolves with
// ErrExecutorRemoved synchronously, the entire staging surface is dead
// code. The HTTP layer still invokes the deploy entry-points
// (runActionForTargets / runDeployWorkers) which now live in
// http_handlers.go + handlers.go + manager_runs.go respectively; this
// file is preserved as the package-level deployment manifest so future
// PRs (real executor under internal/ansible/executor) can hang their
// plan builders and helpers here without re-introducing the legacy
// RunPlaybook bridge.

