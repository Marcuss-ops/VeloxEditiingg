// Package store / store_worker_control.go — worker control-plane persistence.
//
// The worker control-plane persistence was split per table (per-domain):
//
//	store_worker_commands.go    — worker_commands table (persistent command
//	                              outbox: InsertCommand, GetPendingCommands,
//	                              AckCommandByID, MarkCommandDelivered,
//	                              ExpireCommands, CleanupOldCommands,
//	                              HasPendingCommand, nextSequence, scanCommands,
//	                              PersistedCommand).
//	store_worker_sessions.go    — worker_sessions table (persistent tokens:
//	                              CheckActiveSessionCollision, InsertSession,
//	                              DeleteWorkerRuntimeSnapshotBySession,
//	                              ValidateSession, ValidateSessionByID,
//	                              UpdateSessionLastSeen, RevokeWorkerSessions,
//	                              RevokeSession, CleanupExpiredSessions,
//	                              IsSessionActive, ListWorkerSessions,
//	                              GetActiveSessionsByWorkerIDs,
//	                              PersistedSession, WorkerSessionRow,
//	                              ErrWorkerIDCollision,
//	                              WorkerSessionFreshnessWindow).
//	store_worker_credentials.go — worker_credentials table (persistent
//	                              identity: SetWorkerCredential,
//	                              ValidateWorkerCredential,
//	                              HasWorkerCredential, HashCredential).
package store
