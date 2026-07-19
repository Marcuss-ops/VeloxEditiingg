// workers_handler_filters.go is intentionally a shell. The original
// workers_handler_filters.go (filter parsing + in-memory filter
// application + per-worker executor advertisement check) has been
// redistributed across these focused files, each with a single
// concern:
//
//	workers_dto.go     - the Filters struct + IsZero method and the
//	                     FilterStatus* canonical enum constants.
//	workers_mapper.go  - ParseFilters (query-param → typed Filters;
//	                     emits 400 on invalid ?status=),
//	                     ApplyFilters (in-memory defense layer above
//	                     the SQL WHERE filter), and
//	                     workerAdvertisesExecutor (Capabilities
//	                     matcher with the "@<version>" tail
//	                     stripped).
//
// Single-instance contract enforced by this split:
//   - The Filters struct lives only in workers_dto.go.
//   - The FilterStatus* constants live only in workers_dto.go.
//   - ParseFilters, ApplyFilters, workerAdvertisesExecutor, and the
//     errFilterInvalid sentinel + filterParseError type live only in
//     workers_mapper.go.
//   - The 400 Bad Request emitted by ParseFilters on an invalid
//     ?status= value uses the exact same JSON shape as the original
//     (gin.H{"error": "...", "got": v}).
//
// Pure structural refactor: zero behaviour, schema, API, or test
// surface change. The in-memory applier remains the defense layer
// above the SQL WHERE filter (RW-PROD-005 A7); the parser remains
// case-insensitive on ?status= and exact-match on ?class= and
// ?rollout_group=.
package api
