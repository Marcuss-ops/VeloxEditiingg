package logging

// Event codes for structured logging (Agente 1 - Backend Logging)
// Per 11_LOGGING_OPERATIVO_SENZA_RUMORE.md: uniformare codici errore per parser automatici.

// Master communication codes
const (
	CodeMasterURLUnreachable = "MASTER_URL_UNREACHABLE"
	CodeLocalhostForRemote   = "LOCALHOST_FOR_REMOTE_WORKERS"
	CodeMasterURLFallback    = "MASTER_URL_FALLBACK"
	CodeMasterURLResolved    = "MASTER_URL_RESOLVED"
)

// Ansible/Playbook codes
const (
	CodePlaybookNotFound  = "PLAYBOOK_NOT_FOUND"
	CodeAnsibleNotFound   = "ANSIBLE_NOT_FOUND"
	CodeUnsupportedAction = "UNSUPPORTED_ACTION"
	CodeInvalidInventory  = "INVALID_INVENTORY"
	CodeRunStarted        = "ANSIBLE_RUN_STARTED"
	CodeRunCompleted      = "ANSIBLE_RUN_COMPLETED"
	CodeRunFailed         = "ANSIBLE_RUN_FAILED"
	CodePreflightOK       = "PREFLIGHT_OK"
	CodePreflightFail     = "PREFLIGHT_FAIL"
)

// SSH/Credentials codes
const (
	CodeSSHKeyMissing         = "SSH_KEY_MISSING"
	CodeSSHKeyPermissions     = "SSH_KEY_PERMISSIONS"
	CodeSSHCredentialsMissing = "SSH_CREDENTIALS_MISSING"
)

// Worker lifecycle codes (existing)
const (
	CodeWorkerOffline         = "WORKER_OFFLINE"
	CodeWorkerDegraded        = "WORKER_DEGRADED"
	CodeWorkerUnhealthy       = "WORKER_UNHEALTHY"
	CodeWorkerHealthy         = "WORKER_HEALTHY"
	CodeWorkerStatusChange    = "WORKER_STATUS_CHANGE"
	CodeWorkerShutdownRequest = "WORKER_SHUTDOWN_REQUEST"
	CodeWorkerShutdownTimeout = "WORKER_SHUTDOWN_TIMEOUT"
	CodeWorkerForceShutdown   = "WORKER_FORCE_SHUTDOWN"
	CodeWorkerJobRecovery     = "WORKER_JOB_RECOVERY"
	CodeWorkerAlert           = "WORKER_ALERT"
)

// Worker registry persistence codes (existing)
const (
	CodeRegistryLoadWorkersFail       = "REGISTRY_LOAD_WORKERS_FAIL"
	CodeRegistryLoadRevokedFail       = "REGISTRY_LOAD_REVOKED_FAIL"
	CodeRegistryLoadedSummary         = "REGISTRY_LOADED_SUMMARY"
	CodeSQLiteUpsertHeartbeatFail     = "REGISTRY_UPSERT_HEARTBEAT_FAIL"
	CodeSQLiteUpsertRegisterFail      = "REGISTRY_UPSERT_REGISTER_FAIL"
	CodeSQLiteUpsertWorkerUpdateFail  = "REGISTRY_UPSERT_WORKER_UPDATE_FAIL"
	CodeRegistryDeleteWorkerFail      = "REGISTRY_DELETE_WORKER_FAIL"
	CodeRegistryDeleteStaleWorkerFail = "REGISTRY_DELETE_STALE_WORKER_FAIL"
	CodeRegistryPersistRevokeFail     = "REGISTRY_PERSIST_REVOKE_FAIL"
	CodeRegistryPersistUnrevokeFail   = "REGISTRY_PERSIST_UNREVOKE_FAIL"
	CodeRegistryStaleWorkerCleanup    = "REGISTRY_STALE_WORKER_CLEANUP"

	// CONNECTED/STALE/DISCONNECTED read-model hydration (PR: session_active plumbing)
	CodeRegistryLoadSessionsQueryFail = "REGISTRY_LOAD_SESSIONS_QUERY_FAIL"
	CodeRegistryLoadSessionQueryFail  = "REGISTRY_LOAD_SESSION_QUERY_FAIL"
)

// Queue/Job codes
const (
	CodeJobRequeued = "JOB_REQUEUED"
	CodeJobFailed   = "JOB_FAILED"
	CodeNoTargets   = "NO_TARGETS"
)

// Migration codes
const (
	CodeDriveLinkMigrateSkip    = "DRIVE_LINK_MIGRATE_SKIP"
	CodeMasterFolderMigrateSkip = "MASTER_FOLDER_MIGRATE_SKIP"
)

// Worker update lifecycle (replaces log.Printf in worker_update_*.go)
const (
	CodeWorkerUpdateDownloaded        = "WORKER_UPDATE_DOWNLOADED"
	CodeWorkerUpdateApplied           = "WORKER_UPDATE_APPLIED"
	CodeWorkerOnlineAligned           = "WORKER_ONLINE_ALIGNED"
	CodeWorkerOnlineMisaligned        = "WORKER_ONLINE_MISALIGNED"
	CodeWorkerUpdateFinalized         = "WORKER_UPDATE_FINALIZED"
	CodeWorkerUpdateFailed            = "WORKER_UPDATE_FAILED"
	CodeWorkerUpdateApplyFailRollback = "WORKER_UPDATE_APPLY_FAIL_ROLLBACK"
	CodeWorkerUpdateAck               = "WORKER_UPDATE_ACK"
	CodeWorkerUpdateStatusQuery       = "WORKER_UPDATE_STATUS_QUERY"
	CodeWorkerBundleSymlinkMade       = "WORKER_BUNDLE_SYMLINK_MADE"
)

// Worker update commands (replaces log.Printf in worker_update_update.go + control.go)
const (
	CodeUpdateFullLinuxQueued    = "UPDATE_FULL_LINUX_QUEUED"
	CodeUpdateLatestBundleQueued = "UPDATE_LATEST_BUNDLE_QUEUED"
	CodeUpdateRestartAllQueued   = "UPDATE_RESTART_ALL_QUEUED"
	CodeRolloutStarted           = "ROLLOUT_STARTED"
	CodeRolloutCounts            = "ROLLOUT_COUNTS"
	CodeControlRestartRequested  = "CONTROL_RESTART_REQUESTED"
	CodeControlRevoked           = "CONTROL_REVOKED"
	CodeControlUnrevoked         = "CONTROL_UNREVOKED"
	CodeControlDrainSet          = "CONTROL_DRAIN_SET"
)

// Worker bundle rebuild (replaces log.Printf in bundle_rebuild.go)
const (
	CodeBundleRebuildDebug     = "BUNDLE_REBUILD_DEBUG"
	CodeBundleRebuildFailed    = "BUNDLE_REBUILD_FAILED"
	CodeBundleRebuildCompleted = "BUNDLE_REBUILD_COMPLETED"
)

// Worker validation (replaces log.Printf in validation/handler.go)
const (
	CodeValidationTableCreateFail = "VALIDATION_TABLE_CREATE_FAIL"
	CodeValidationSaveFail        = "VALIDATION_SAVE_FAIL"
	CodeValidationReport          = "VALIDATION_REPORT"
)

// Worker install (replaces log.Printf in install_handlers.go)
const (
	CodeInstallRuntimeModeUnexpected = "INSTALL_RUNTIME_MODE_UNEXPECTED"
	CodeInstallRuntimeModeBlocked    = "INSTALL_RUNTIME_MODE_BLOCKED"
	CodeInstallIPNotAllowed          = "INSTALL_IP_NOT_ALLOWED"
	CodeInstallScriptServed          = "INSTALL_SCRIPT_SERVED"
	CodeInstallSetupScriptGenerated  = "INSTALL_SETUP_SCRIPT_GENERATED"
	CodeInstallScriptNotFound        = "INSTALL_SCRIPT_NOT_FOUND"
)

// Uploads pipeline (replaces log.Printf in uploads/video.go)
const (
	CodeUploadJobUpdateFail       = "UPLOAD_JOB_UPDATE_FAIL"
	CodeUploadArtifactMarshalFail = "UPLOAD_ARTIFACT_MARSHAL_FAIL"
	CodeUploadVideoCompleted      = "UPLOAD_VIDEO_COMPLETED"
)

// Worker SSH/secret side-codes (handler-side supplements)
const (
	CodeSecretStored          = "SECRET_STORED"
	CodeSecretStaleRemoveFail = "SECRET_STALE_REMOVE_FAIL"
	CodeSecretStaleRemoved    = "SECRET_STALE_REMOVED"
)

// Worker lifecycle handler-side supplements (heartbeat handlers)
const (
	CodeWorkerHeartbeatBindFail = "WORKER_HEARTBEAT_BIND_FAIL"
	CodeWorkerHeartbeatFail     = "WORKER_HEARTBEAT_FAIL"
	CodeWorkerRegistered        = "WORKER_REGISTERED"
	CodeWorkerReconnectedUpdate = "WORKER_RECONNECTED_UPDATE"
	CodeWorkerStatusUpdate      = "WORKER_STATUS_UPDATE"
)

// Store (sqlite) lifecycle
const (
	CodeSQLitePingCloseAfterFail = "SQLITE_PING_CLOSE_AFTER_FAIL"
	CodeSQLitePragmaFail         = "SQLITE_PRAGMA_FAIL"
	CodeSQLiteMigrationCloseFail = "SQLITE_MIGRATION_CLOSE_AFTER_FAIL"
	CodeSQLiteClosePostMigration = "SQLITE_CLOSE_POST_MIGRATION"
	CodeSQLiteMigrationApplied   = "SQLITE_MIGRATION_APPLIED"
)

// Forwarding runner (replaces log.Printf in forwarding/runner_*.go)
const (
	CodeForwardingClaimed             = "FORWARDING_CLAIMED"
	CodeForwardingLeaseLost           = "FORWARDING_LEASE_LOST"
	CodeForwardingPollFailed          = "FORWARDING_POLL_FAILED"
	CodeForwardingNilResponse         = "FORWARDING_NIL_RESPONSE"
	CodeForwardingRenewLeaseFailed    = "FORWARDING_RENEW_LEASE_FAILED"
	CodeForwardingPayloadMarshalFail  = "FORWARDING_PAYLOAD_MARSHAL_FAILED"
	CodeForwardingMarkReadyFail       = "FORWARDING_MARK_READY_FAILED"
	CodeForwardingReadyToForward      = "FORWARDING_READY_TO_FORWARD"
	CodeForwardingFailed              = "FORWARDING_FAILED"
	CodeForwardingMaxAttempts         = "FORWARDING_MAX_ATTEMPTS"
	CodeForwardingRetryCASFail        = "FORWARDING_RETRY_CAS_FAILED"
	CodeForwardingResolverUnavailable = "FORWARDING_RESOLVER_UNAVAILABLE"
	CodeForwardingResolveFailed       = "FORWARDING_RESOLVE_FAILED"
	CodeForwardingForwarded           = "FORWARDING_FORWARDED"
	CodeForwardingMetricsRefreshFail  = "FORWARDING_METRICS_REFRESH_FAILED"
)

// Delivery runner (replaces log.Printf in deliveries/runner*.go)
const (
	CodeDeliveryReconcileSweepFail   = "DELIVERY_RECONCILE_SWEEP_FAILED"
	CodeDeliveryLeaseAbandoned       = "DELIVERY_LEASE_ABANDONED"
	CodeDeliveryProcessFailed        = "DELIVERY_PROCESS_FAILED"
	CodeDeliveryMarkFailed           = "DELIVERY_MARK_FAILED"
	CodeDeliveryMarkReconcileFail    = "DELIVERY_MARK_RECONCILE_FAILED"
	CodeDeliveryCredentialRefFail    = "DELIVERY_CREDENTIAL_REF_FAILED"
	CodeDeliveryCredentialAuthFail   = "DELIVERY_CREDENTIAL_AUTH_FAILED"
	CodeDeliveryCredentialAuditFail  = "DELIVERY_CREDENTIAL_AUDIT_FAILED"
	CodeDeliveryLeaseRenewalFail     = "DELIVERY_LEASE_RENEWAL_FAILED"
	CodeDeliveryResultValidationFail = "DELIVERY_RESULT_VALIDATION_FAILED"
	CodeDeliveryMarkBlockedAuth      = "DELIVERY_MARK_BLOCKED_AUTH"
	CodeDeliveryMarkRetry            = "DELIVERY_MARK_RETRY"
)

// Completion reconcile supervisor (replaces log.Printf in reconcile_supervisor.go)
const (
	CodeCompletionReconcileStarted      = "COMPLETION_RECONCILE_STARTED"
	CodeCompletionReconcileScanFail     = "COMPLETION_RECONCILE_SCAN_FAILED"
	CodeCompletionReconcileTick         = "COMPLETION_RECONCILE_TICK"
	CodeCompletionReconcileDispatchFail = "COMPLETION_RECONCILE_DISPATCH_FAILED"
)

// gRPC server (replaces log.Printf in internal/grpcserver)
const (
	CodeGRPCWorkerConnected        = "GRPC_WORKER_CONNECTED"
	CodeGRPCWorkerDisconnected     = "GRPC_WORKER_DISCONNECTED"
	CodeGRPCWorkerReconnecting     = "GRPC_WORKER_RECONNECTING"
	CodeGRPCStreamAuthenticated    = "GRPC_STREAM_AUTHENTICATED"
	CodeGRPCStreamRejected         = "GRPC_STREAM_REJECTED"
	CodeGRPCStreamHelloCollision   = "GRPC_STREAM_HELLO_COLLISION"
	CodeGRPCStreamWriterFailure    = "GRPC_STREAM_WRITER_FAILURE"
	CodeGRPCStreamReplay           = "GRPC_STREAM_REPLAY"
	CodeGRPCStreamUnknownMessage   = "GRPC_STREAM_UNKNOWN_MESSAGE"
	CodeGRPCSessionCleanupFailed   = "GRPC_SESSION_CLEANUP_FAILED"
	CodeGRPCTaskAccepted           = "GRPC_TASK_ACCEPTED"
	CodeGRPCTaskAcceptRefused      = "GRPC_TASK_ACCEPT_REFUSED"
	CodeGRPCTaskAcceptFailed       = "GRPC_TASK_ACCEPT_FAILED"
	CodeGRPCTaskRejected           = "GRPC_TASK_REJECTED"
	CodeGRPCTaskRejectRefused      = "GRPC_TASK_REJECT_REFUSED"
	CodeGRPCTaskRejectFailed       = "GRPC_TASK_REJECT_FAILED"
	CodeGRPCTaskResult             = "GRPC_TASK_RESULT"
	CodeGRPCTaskResultRejected     = "GRPC_TASK_RESULT_REJECTED"
	CodeGRPCTaskResultFailed       = "GRPC_TASK_RESULT_FAILED"
	CodeGRPCLeaseRenewal           = "GRPC_LEASE_RENEWAL"
	CodeGRPCLeaseRenewalRefused    = "GRPC_LEASE_RENEWAL_REFUSED"
	CodeGRPCLeaseRenewalFailed     = "GRPC_LEASE_RENEWAL_FAILED"
	CodeGRPCCompletion             = "GRPC_COMPLETION"
	CodeGRPCCompletionRejected     = "GRPC_COMPLETION_REJECTED"
	CodeGRPCCompletionFailed       = "GRPC_COMPLETION_FAILED"
	CodeGRPCArtifactUpload         = "GRPC_ARTIFACT_UPLOAD"
	CodeGRPCArtifactUploadRejected = "GRPC_ARTIFACT_UPLOAD_REJECTED"
	CodeGRPCArtifactUploadFailed   = "GRPC_ARTIFACT_UPLOAD_FAILED"
	CodeGRPCPlacement              = "GRPC_PLACEMENT"
	CodeGRPCPlacementFailed        = "GRPC_PLACEMENT_FAILED"
	CodeGRPCRenderPlan             = "GRPC_RENDERPLAN"
	CodeGRPCPrefetch               = "GRPC_PREFETCH"
	CodeGRPCPrefetchFailed         = "GRPC_PREFETCH_FAILED"
	CodeGRPCHeartbeatFailed        = "GRPC_HEARTBEAT_FAILED"
	CodeGRPCSessionInvalid         = "GRPC_SESSION_INVALID"
	CodeGRPCCommandDispatch        = "GRPC_COMMAND_DISPATCH"
	CodeGRPCCommandFailed          = "GRPC_COMMAND_FAILED"
	CodeGRPCTelemetryRejected      = "GRPC_TELEMETRY_REJECTED"
	CodeGRPCAssetProgressRejected  = "GRPC_ASSET_PROGRESS_REJECTED"
	CodeGRPCAssetProgressFailed    = "GRPC_ASSET_PROGRESS_FAILED"
	CodeGRPCSecurity               = "GRPC_SECURITY"
	CodeGRPCSecurityFailed         = "GRPC_SECURITY_FAILED"
	CodeGRPCAuthz                  = "GRPC_AUTHZ"
	CodeGRPCServerLifecycle        = "GRPC_SERVER_LIFECYCLE"
	CodeGRPCServerFailed           = "GRPC_SERVER_FAILED"
	CodeGRPCArtifactProtocolLog    = "GRPC_ARTIFACT_PROTOCOL_LOG"
	CodeGRPCRegistryBridge         = "GRPC_REGISTRY_BRIDGE"
	CodeGRPCMetricsDerivation      = "GRPC_METRICS_DERIVATION"
)

// cmd/server bootstrap + lifecycle (replaces log.Printf in cmd/server)
const (
	CodeServerBootstrap       = "SERVER_BOOTSTRAP"
	CodeServerBootstrapWarn   = "SERVER_BOOTSTRAP_WARN"
	CodeServerBootstrapError  = "SERVER_BOOTSTRAP_ERROR"
	CodeServerLifecycle       = "SERVER_LIFECYCLE"
	CodeServerLifecycleError  = "SERVER_LIFECYCLE_ERROR"
	CodeServerSupervisor      = "SERVER_SUPERVISOR"
	CodeServerSupervisorWarn  = "SERVER_SUPERVISOR_WARN"
	CodeServerSupervisorError = "SERVER_SUPERVISOR_ERROR"
	CodeServerCapability      = "SERVER_CAPABILITY"
	CodeServerCapabilityWarn  = "SERVER_CAPABILITY_WARN"
	CodeServerRoutes          = "SERVER_ROUTES"
	CodeServerRoutesError     = "SERVER_ROUTES_ERROR"
	CodeServerMetrics         = "SERVER_METRICS"
	CodeServerMetricsWarn     = "SERVER_METRICS_WARN"
	CodeServerAudit           = "SERVER_AUDIT"
	CodeServerAuditWarn       = "SERVER_AUDIT_WARN"
	CodeServerAuditError      = "SERVER_AUDIT_ERROR"
	CodeServerHTTP            = "SERVER_HTTP"
	CodeServerSmoke           = "SERVER_SMOKE"
	CodeServerTaskgraph       = "SERVER_TASKGRAPH"
)

// Level constants
const (
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelDebug = "DEBUG"
)

// codeDescriptions maps codes to human-readable descriptions.
// Keep the registry private so callers cannot mutate operator-facing text.
var codeDescriptions = map[string]string{
	// Master URL.
	CodeMasterURLUnreachable: "Master URL is not reachable from worker",
	CodeLocalhostForRemote:   "Cannot use localhost URL for remote workers",
	CodeMasterURLFallback:    "Master URL using fallback resolution",
	CodeMasterURLResolved:    "Master URL resolved successfully",

	// Ansible.
	CodePlaybookNotFound:  "Ansible playbook file not found",
	CodeAnsibleNotFound:   "ansible-playbook binary not found",
	CodeUnsupportedAction: "Action not supported",
	CodeInvalidInventory:  "Generated inventory is empty or invalid",
	CodeRunStarted:        "Ansible run started",
	CodeRunCompleted:      "Ansible run completed successfully",
	CodeRunFailed:         "Ansible run failed",
	CodePreflightOK:       "Preflight checks passed",
	CodePreflightFail:     "Preflight checks failed",

	// SSH.
	CodeSSHKeyMissing:         "SSH key file not found",
	CodeSSHKeyPermissions:     "SSH key file not readable",
	CodeSSHCredentialsMissing: "No SSH key or password configured",

	// Worker lifecycle (existing).
	CodeWorkerOffline:         "Worker is offline",
	CodeWorkerDegraded:        "Worker health degraded",
	CodeWorkerUnhealthy:       "Worker is unhealthy",
	CodeWorkerHealthy:         "Worker is healthy",
	CodeWorkerStatusChange:    "Worker health status changed",
	CodeWorkerShutdownRequest: "Graceful shutdown requested",
	CodeWorkerShutdownTimeout: "Graceful shutdown timed out",
	CodeWorkerForceShutdown:   "Worker force shutdown",
	CodeWorkerJobRecovery:     "Jobs recovered from offline worker",
	CodeWorkerAlert:           "Worker alert generated",

	// Worker lifecycle supplement.
	CodeWorkerHeartbeatBindFail: "Failed to bind heartbeat JSON",
	CodeWorkerHeartbeatFail:     "Worker heartbeat persistence failed",
	CodeWorkerRegistered:        "Worker registered",
	CodeWorkerReconnectedUpdate: "Worker reconnected after update",
	CodeWorkerStatusUpdate:      "Worker reported status update",

	// Worker registry (existing).
	CodeRegistryLoadWorkersFail:       "Failed to load workers from SQLite",
	CodeRegistryLoadRevokedFail:       "Failed to load revoked workers from SQLite",
	CodeRegistryLoadedSummary:         "Workers loaded from SQLite",
	CodeSQLiteUpsertHeartbeatFail:     "SQLite upsert worker heartbeat failed",
	CodeSQLiteUpsertRegisterFail:      "SQLite upsert worker register failed",
	CodeSQLiteUpsertWorkerUpdateFail:  "SQLite upsert worker update failed",
	CodeRegistryDeleteWorkerFail:      "Failed to delete worker",
	CodeRegistryDeleteStaleWorkerFail: "Failed to delete stale worker",
	CodeRegistryPersistRevokeFail:     "Failed to persist worker revoke",
	CodeRegistryPersistUnrevokeFail:   "Failed to persist worker unrevoke", CodeRegistryStaleWorkerCleanup: "Cleaned up stale worker",

	// CONNECTED/STALE/DISCONNECTED read-model hydration.
	CodeRegistryLoadSessionsQueryFail: "Bulk session query failed; demoting fleet to conservative (DISCONNECTED) state",
	CodeRegistryLoadSessionQueryFail:  "Per-worker session query failed; treating worker as DISCONNECTED",

	// Queue/job.
	CodeJobRequeued: "Job requeued",
	CodeJobFailed:   "Job failed",
	CodeNoTargets:   "No target computers selected",

	// Migrations.
	CodeDriveLinkMigrateSkip:    "Skipping drive link during migration",
	CodeMasterFolderMigrateSkip: "Skipping master folder during migration",

	// Worker update lifecycle.
	CodeWorkerUpdateDownloaded:        "Worker update downloaded",
	CodeWorkerUpdateApplied:           "Worker update applied",
	CodeWorkerOnlineAligned:           "Worker online + aligned with target artifact",
	CodeWorkerOnlineMisaligned:        "Worker online but artifact not aligned",
	CodeWorkerUpdateFinalized:         "Worker update files/dirs finalized",
	CodeWorkerUpdateFailed:            "Worker update failed",
	CodeWorkerUpdateApplyFailRollback: "Worker update apply failed, rolled back",
	CodeWorkerUpdateAck:               "Worker update ack recorded",
	CodeWorkerUpdateStatusQuery:       "Worker update status query",
	CodeWorkerBundleSymlinkMade:       "Created latest->hash symlink",

	// Worker update commands.
	CodeUpdateFullLinuxQueued:    "Full Linux update queued",
	CodeUpdateLatestBundleQueued: "Latest bundle update queued",
	CodeUpdateRestartAllQueued:   "Restart-all queued",
	CodeRolloutStarted:           "Rollout update started",
	CodeRolloutCounts:            "Rollout counts computed",
	CodeControlRestartRequested:  "Worker restart requested",
	CodeControlRevoked:           "Worker revoked",
	CodeControlUnrevoked:         "Worker unrevoked",
	CodeControlDrainSet:          "Worker drain set",

	// Bundle rebuild.
	CodeBundleRebuildDebug:     "Bundle rebuild debug context",
	CodeBundleRebuildFailed:    "Bundle rebuild failed",
	CodeBundleRebuildCompleted: "Bundle rebuild completed",

	// Validation.
	CodeValidationTableCreateFail: "Failed to create validation table",
	CodeValidationSaveFail:        "Failed to save validation report",
	CodeValidationReport:          "Validation report received",

	// Install.
	CodeInstallRuntimeModeUnexpected: "Unexpected runtime mode requested",
	CodeInstallRuntimeModeBlocked:    "Runtime mode blocked",
	CodeInstallIPNotAllowed:          "Client IP not allowed for install handler",
	CodeInstallScriptServed:          "Install script served",
	CodeInstallSetupScriptGenerated:  "Install setup script generated",
	CodeInstallScriptNotFound:        "Install script not found",

	// Secret.
	CodeSecretStored:          "Stored SSH password secret file",
	CodeSecretStaleRemoveFail: "Failed to remove stale secret file",
	CodeSecretStaleRemoved:    "Removed stale secret file",

	// Uploads pipeline.
	CodeUploadJobUpdateFail:       "Failed to update job after upload",
	CodeUploadArtifactMarshalFail: "Failed to marshal artifact",
	CodeUploadVideoCompleted:      "Video upload completed",

	// Store sqlite.
	CodeSQLitePingCloseAfterFail: "SQLite close after ping failure",
	CodeSQLitePragmaFail:         "SQLite PRAGMA failed",
	CodeSQLiteMigrationCloseFail: "SQLite close after migration failure",
	CodeSQLiteClosePostMigration: "SQLite close after post-migration",
	CodeSQLiteMigrationApplied:   "SQLite migration applied",

	// Forwarding runner.
	CodeForwardingClaimed:             "Forwardings claimed from the queue",
	CodeForwardingLeaseLost:           "Forwarding lease lost",
	CodeForwardingPollFailed:          "Remote creator poll failed",
	CodeForwardingNilResponse:         "Remote creator returned nil without error",
	CodeForwardingRenewLeaseFailed:    "Forwarding lease renewal failed",
	CodeForwardingPayloadMarshalFail:  "Forwarding payload is not JSON-serializable",
	CodeForwardingMarkReadyFail:       "Failed to mark forwarding ready-to-forward",
	CodeForwardingReadyToForward:      "Forwarding ready to forward",
	CodeForwardingFailed:              "Forwarding failed",
	CodeForwardingMaxAttempts:         "Forwarding max attempts exhausted",
	CodeForwardingRetryCASFail:        "Forwarding enqueue retry CAS failed",
	CodeForwardingResolverUnavailable: "Forwarding resolver unavailable",
	CodeForwardingResolveFailed:       "Forwarding resolver failed",
	CodeForwardingForwarded:           "Forwarding forwarded to job",
	CodeForwardingMetricsRefreshFail:  "Forwarding metrics refresh failed",

	// Delivery runner.
	CodeDeliveryReconcileSweepFail:   "Delivery reconciliation sweep failed",
	CodeDeliveryLeaseAbandoned:       "Delivery lease abandoned (runner shutting down)",
	CodeDeliveryProcessFailed:        "Delivery processing failed",
	CodeDeliveryMarkFailed:           "Failed to mark delivery failed",
	CodeDeliveryMarkReconcileFail:    "Failed to mark delivery reconciliation failure",
	CodeDeliveryCredentialRefFail:    "Failed to mark delivery credential-reference failure",
	CodeDeliveryCredentialAuthFail:   "Failed to mark delivery credential auth failure",
	CodeDeliveryCredentialAuditFail:  "Delivery credential usage audit failed",
	CodeDeliveryLeaseRenewalFail:     "Delivery lease renewal failed",
	CodeDeliveryResultValidationFail: "Delivery provider result validation failed",
	CodeDeliveryMarkBlockedAuth:      "Failed to mark delivery blocked-auth",
	CodeDeliveryMarkRetry:            "Failed to mark delivery retry",

	// Completion reconcile supervisor.
	CodeCompletionReconcileStarted:      "Reconcile supervisor started",
	CodeCompletionReconcileScanFail:     "Reconcile scan failed",
	CodeCompletionReconcileTick:         "Reconcile tick candidates",
	CodeCompletionReconcileDispatchFail: "Reconcile dispatch failed",

	// gRPC server.
	CodeGRPCWorkerConnected:        "Worker connected",
	CodeGRPCWorkerDisconnected:     "Worker disconnected",
	CodeGRPCWorkerReconnecting:     "Worker reconnecting",
	CodeGRPCStreamAuthenticated:    "Worker authenticated via mTLS",
	CodeGRPCStreamRejected:         "Stream admission rejected",
	CodeGRPCStreamHelloCollision:   "Worker hello collision",
	CodeGRPCStreamWriterFailure:    "Session writer failure",
	CodeGRPCStreamReplay:           "Duplicate or replayed message",
	CodeGRPCStreamUnknownMessage:   "Unknown message type",
	CodeGRPCSessionCleanupFailed:   "Session cleanup failed",
	CodeGRPCTaskAccepted:           "Task accepted",
	CodeGRPCTaskAcceptRefused:      "Task accept refused",
	CodeGRPCTaskAcceptFailed:       "Task accept failed",
	CodeGRPCTaskRejected:           "Task rejected by worker",
	CodeGRPCTaskRejectRefused:      "Task reject refused",
	CodeGRPCTaskRejectFailed:       "Task reject release failed",
	CodeGRPCTaskResult:             "Task result reported",
	CodeGRPCTaskResultRejected:     "Task result rejected",
	CodeGRPCTaskResultFailed:       "Task result ingest failed",
	CodeGRPCLeaseRenewal:           "Task lease renewal",
	CodeGRPCLeaseRenewalRefused:    "Task lease renewal refused",
	CodeGRPCLeaseRenewalFailed:     "Task lease renewal failed",
	CodeGRPCCompletion:             "Completion protocol event",
	CodeGRPCCompletionRejected:     "Completion protocol rejected",
	CodeGRPCCompletionFailed:       "Completion protocol failed",
	CodeGRPCArtifactUpload:         "Artifact upload reported",
	CodeGRPCArtifactUploadRejected: "Artifact upload rejected",
	CodeGRPCArtifactUploadFailed:   "Artifact upload failed",
	CodeGRPCPlacement:              "Placement event",
	CodeGRPCPlacementFailed:        "Placement failed",
	CodeGRPCRenderPlan:             "Render plan compiled",
	CodeGRPCPrefetch:               "Prefetch event",
	CodeGRPCPrefetchFailed:         "Prefetch failed",
	CodeGRPCHeartbeatFailed:        "Worker heartbeat failed",
	CodeGRPCSessionInvalid:         "Worker session invalid",
	CodeGRPCCommandDispatch:        "Command dispatched",
	CodeGRPCCommandFailed:          "Command delivery failed",
	CodeGRPCTelemetryRejected:      "Telemetry snapshot rejected",
	CodeGRPCAssetProgressRejected:  "Asset download progress rejected",
	CodeGRPCAssetProgressFailed:    "Asset download progress ingest failed",
	CodeGRPCSecurity:               "Worker credential event",
	CodeGRPCSecurityFailed:         "Worker credential lookup failed",
	CodeGRPCAuthz:                  "Worker allowlist decision",
	CodeGRPCServerLifecycle:        "gRPC server lifecycle",
	CodeGRPCServerFailed:           "gRPC server error",
	CodeGRPCArtifactProtocolLog:    "Artifact protocol log",
	CodeGRPCRegistryBridge:         "Worker registry bridge event",
	CodeGRPCMetricsDerivation:      "Worker metrics derivation fallback",

	// cmd/server bootstrap + lifecycle.
	CodeServerBootstrap:       "Server bootstrap event",
	CodeServerBootstrapWarn:   "Server bootstrap warning",
	CodeServerBootstrapError:  "Server bootstrap error",
	CodeServerLifecycle:       "Server lifecycle event",
	CodeServerLifecycleError:  "Server lifecycle error",
	CodeServerSupervisor:      "Supervisor/runner started",
	CodeServerSupervisorWarn:  "Supervisor warning",
	CodeServerSupervisorError: "Supervisor error",
	CodeServerCapability:      "Capability wiring event",
	CodeServerCapabilityWarn:  "Capability wiring warning",
	CodeServerRoutes:          "Route wiring event",
	CodeServerRoutesError:     "Route wiring error",
	CodeServerMetrics:         "Metrics snapshot event",
	CodeServerMetricsWarn:     "Metrics snapshot warning",
	CodeServerAudit:           "Data layer audit event",
	CodeServerAuditWarn:       "Data layer audit warning",
	CodeServerAuditError:      "Data layer audit error",
	CodeServerHTTP:            "HTTP request log",
	CodeServerSmoke:           "Smoke test event",
	CodeServerTaskgraph:       "Taskgraph tick event",
}

// GetDescription returns the human-readable description for a code
func GetDescription(code string) string {
	if desc, ok := codeDescriptions[code]; ok {
		return desc
	}
	return code
}
