// worker_comms.go is intentionally a shell. The original 357-line
// worker_comms.go has been redistributed across these focused files,
// each with a single concern:
//
//	heartbeat_intervals.go    - interval policy + backoff constants
//	heartbeat_loop.go        - ticker loop + wake signal + status change
//	heartbeat_payload.go     - Heartbeat proto construction + serialization
//	lease_renewal.go         - lease renewal loop (TaskLeaseRenewal proto)
//	active_lease_registry.go - Add/Remove/Snapshot for active task leases
//	worker_capacity.go       - hardware-derived max_parallel_jobs fallback
//
// This split keeps the existing call sites intact (all symbols remain
// package-private methods on *Worker) so the refactor is purely
// structural: no behaviour, schema, API, or protocol change.
package worker
