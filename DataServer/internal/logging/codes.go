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

// Dark-editor / migration codes
const (
	CodeDarkEditorUpscaleFallback = "DARKEDITOR_UPSCALER_FALLBACK"
	CodeDriveLinkMigrateSkip      = "DRIVE_LINK_MIGRATE_SKIP"
	CodeMasterFolderMigrateSkip   = "MASTER_FOLDER_MIGRATE_SKIP"
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

// Component identifiers for structured logging
const (
	ComponentAnsible   = "ansible"
	ComponentWorker    = "worker"
	ComponentQueue     = "queue"
	ComponentMaster    = "master"
	ComponentPreflight = "preflight"
)

// Components added for handler/store domains
const (
	ComponentWorkerReg        = "workers.registry"
	ComponentWorkerUpdate     = "workers.update_handler"
	ComponentWorkerBundle     = "workers.bundle"
	ComponentWorkerValidation = "workers.validation"
	ComponentWorkerLifecycle  = "workers.lifecycle"
	ComponentInstall    = "install"
	ComponentUploads    = "uploads"
	ComponentStore      = "store.sqlite"
	ComponentDriveLink  = "store.drive_links"
	ComponentDarkEditor = "dark_editor"
)

// Level constants
const (
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelDebug = "DEBUG"
)

// CodeDescriptions maps codes to human-readable descriptions
var CodeDescriptions = map[string]string{
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

	// Dark editor + migrations.
	CodeDarkEditorUpscaleFallback: "Real-ESRGAN unavailable, falling back to imaging.Lanczos",
	CodeDriveLinkMigrateSkip:      "Skipping drive link during migration",
	CodeMasterFolderMigrateSkip:   "Skipping master folder during migration",

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
}

// GetDescription returns the human-readable description for a code
func GetDescription(code string) string {
	if desc, ok := CodeDescriptions[code]; ok {
		return desc
	}
	return code
}
