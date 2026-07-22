// workers_handler_types.go is intentionally a shell. The original
// workers_handler_types.go (mixing two concerns — DTO type definitions
// and host-redaction helpers) has been redistributed across these
// focused files, each with a single concern:
//
//	workers_dto.go        - DTO shapes (WorkerResponse, ActiveTaskRuntime,
//	                        ExecutorEntry, WorkersListResponse, HostSummary,
//	                        ExecutorSummary, TaskSummary), the WorkerInfo
//	                        alias, ConnectionStaleThreshold, the
//	                        Reason* / FilterStatus* canonical enum
//	                        constants, and the Filters struct + IsZero()
//	                        method.
//	workers_sanitise.go   - security-critical hostname redaction surface
//	                        (sanitiseHostname, redactIPv4/IPv6/secretHex,
//	                        path/hex/IP regexes). RW-PROD-005 §3 A6.
//
//	workers_metrics.go    - typed WorkerMetrics / ActiveTaskMetrics and the
//	                        single-pass ParseWorkerMetrics converter that
//	                        replaces repeated map[string]interface{}
//	                        lookups.
//
//	workers_numeric.go    - tiny JSON-unmarshalled numeric coercion helpers
//	                        (toInt64, toFloat64, toString, toInt64Zero)
//	                        shared by metrics and executor parsing.
//
//	workers_executors.go  - executor extraction from capabilities and the
//	                        filter-time executor-advertisement check.
//
//	workers_filters.go    - GET param parsing and in-memory filter applier.
//
//	workers_mapper.go     - top-level worker→DTO conversion orchestration
//	                        (sanitizeWorker, heartbeatAgeSeconds,
//	                        canonicalReason).
//
// Single-instance contract enforced by this split:
//   - sanitiseHostname exists only on the package api, only in
//     workers_sanitise.go. The RW-PROD-005 §3 A6 fuzz test pins every
//     redaction branch.
//   - The IP / hex / path regexes used by sanitiseHostname are package
//     vars declared only in workers_sanitise.go.
//   - The 3-element Reason taxonomy (ReasonDrain / ReasonDetachedSession /
//     ReasonHeartbeatStale) is declared only in workers_dto.go; the
//     canonicalReason mapper consumes those constants verbatim.
//   - The Filters struct + IsZero method lives in workers_dto.go; the
//     ParseFilters / ApplyFilters mapper functions live in
//     workers_filters.go (same package, free cross-file visibility).
//
// Pure structural refactor: zero behaviour, schema, API, or test
// surface change. Every error-wrap message, regex pattern, and
// redaction token is preserved verbatim. Public surface
// (WorkerResponse, ActiveTaskRuntime, ExecutorEntry,
// WorkersListResponse, HostSummary, ExecutorSummary, TaskSummary,
// Filters + IsZero, WorkerInfo alias, ConnectionStaleThreshold,
// Reason* / FilterStatus* constants) is unchanged.
package api
