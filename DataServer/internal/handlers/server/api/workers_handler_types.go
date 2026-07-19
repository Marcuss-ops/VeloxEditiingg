// workers_handler_types.go is intentionally a shell. The original
// workers_handler_types.go (mixing two concerns — DTO type definitions
// and host-redaction helpers) has been redistributed across these
// focused files, each with a single concern:
//
//	workers_dto.go     - DTO shapes (WorkerResponse, ActiveTaskRuntime,
//	                     ExecutorEntry, WorkersListResponse, HostSummary,
//	                     ExecutorSummary, TaskSummary), the WorkerInfo
//	                     alias, ConnectionStaleThreshold, the
//	                     Reason* / FilterStatus* canonical enum
//	                     constants, and the Filters struct + IsZero()
//	                     method.
//	workers_mapper.go  - the security-critical redaction surface
//	                     (sanitiseHostname, redactIPv4/IPv6/secretHex,
//	                     canonicalReason) plus the worker→DTO
//	                     conversion helpers (sanitizeWorker,
//	                     extractExecutors, heartbeatAgeSeconds) and
//	                     the numeric-type tolerant JSON parsers
//	                     (floatFromGeneric, intFromMap, asString,
//	                     intFromAny, floatFromMap).
//
// Single-instance contract enforced by this split:
//   - sanitiseHostname exists only on the package api, only in
//     workers_mapper.go. The RW-PROD-005 §3 A6 fuzz test pins every
//     redaction branch.
//   - The IP / hex / path regexes used by sanitiseHostname are package
//     vars declared only in workers_mapper.go.
//   - The 3-element Reason taxonomy (ReasonDrain / ReasonDetachedSession /
//     ReasonHeartbeatStale) is declared only in workers_dto.go; the
//     canonicalReason mapper consumes those constants verbatim.
//   - The Filters struct + IsZero method lives in workers_dto.go; the
//     ParseFilters / ApplyFilters mapper functions live in
//     workers_mapper.go (same package, free cross-file visibility).
//
// Pure structural refactor: zero behaviour, schema, API, or test
// surface change. Every error-wrap message, regex pattern, and
// redaction token is preserved verbatim. Public surface
// (WorkerResponse, ActiveTaskRuntime, ExecutorEntry,
// WorkersListResponse, HostSummary, ExecutorSummary, TaskSummary,
// Filters + IsZero, WorkerInfo alias, ConnectionStaleThreshold,
// Reason* / FilterStatus* constants) is unchanged.
package api
