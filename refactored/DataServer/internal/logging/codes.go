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

// Worker lifecycle codes
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

// Worker registry codes (mirrors log.Printf messages in internal/workers/registry_*.go)
const (
	CodeRegistryLoadWorkersFail      = "REGISTRY_LOAD_WORKERS_FAIL"
	CodeRegistryLoadRevokedFail      = "REGISTRY_LOAD_REVOKED_FAIL"
	CodeRegistryLoadedSummary        = "REGISTRY_LOADED_SUMMARY"
	CodeSQLiteUpsertHeartbeatFail    = "REGISTRY_UPSERT_HEARTBEAT_FAIL"
	CodeSQLiteUpsertRegisterFail     = "REGISTRY_UPSERT_REGISTER_FAIL"
	CodeSQLiteUpsertWorkerUpdateFail = "REGISTRY_UPSERT_WORKER_UPDATE_FAIL"
	CodeRegistryDeleteWorkerFail     = "REGISTRY_DELETE_WORKER_FAIL"
	CodeRegistryDeleteStaleWorkerFail = "REGISTRY_DELETE_STALE_WORKER_FAIL"
	CodeRegistryPersistRevokeFail    = "REGISTRY_PERSIST_REVOKE_FAIL"
	CodeRegistryPersistUnrevokeFail  = "REGISTRY_PERSIST_UNREVOKE_FAIL"
	CodeRegistryStaleWorkerCleanup   = "REGISTRY_STALE_WORKER_CLEANUP"
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

// Component identifiers for structured logging
const (
	ComponentAnsible   = "ansible"
	ComponentWorker    = "worker"
	ComponentQueue     = "queue"
	ComponentMaster    = "master"
	ComponentPreflight = "preflight"
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
	CodeMasterURLUnreachable:  "Master URL is not reachable from worker",
	CodeLocalhostForRemote:    "Cannot use localhost URL for remote workers",
	CodeMasterURLFallback:     "Master URL using fallback resolution",
	CodeMasterURLResolved:     "Master URL resolved successfully",

	CodePlaybookNotFound:      "Ansible playbook file not found",
	CodeAnsibleNotFound:       "ansible-playbook binary not found",
	CodeUnsupportedAction:     "Action not supported",
	CodeInvalidInventory:      "Generated inventory is empty or invalid",
	CodeRunStarted:            "Ansible run started",
	CodeRunCompleted:          "Ansible run completed successfully",
	CodeRunFailed:             "Ansible run failed",
	CodePreflightOK:           "Preflight checks passed",
	CodePreflightFail:         "Preflight checks failed",
	CodeSSHKeyMissing:         "SSH key file not found",
	CodeSSHKeyPermissions:     "SSH key file not readable",
	CodeSSHCredentialsMissing: "No SSH key or password configured",
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
	CodeJobRequeued:           "Job requeued",
	CodeJobFailed:             "Job failed",
	CodeNoTargets:             "No target computers selected",

	CodeDarkEditorUpscaleFallback: "Real-ESRGAN unavailable, falling back to imaging.Lanczos",
	CodeDriveLinkMigrateSkip:      "Skipping drive link during migration",
	CodeMasterFolderMigrateSkip:   "Skipping master folder during migration",

	CodeRegistryLoadWorkersFail:       "Failed to load workers from SQLite",
	CodeRegistryLoadRevokedFail:       "Failed to load revoked workers from SQLite",
	CodeRegistryLoadedSummary:         "Workers loaded from SQLite",
	CodeSQLiteUpsertHeartbeatFail:     "SQLite upsert worker heartbeat failed",
	CodeSQLiteUpsertRegisterFail:      "SQLite upsert worker register failed",
	CodeSQLiteUpsertWorkerUpdateFail:  "SQLite upsert worker update failed",
	CodeRegistryDeleteWorkerFail:      "Failed to delete worker",
	CodeRegistryDeleteStaleWorkerFail: "Failed to delete stale worker",
	CodeRegistryPersistRevokeFail:     "Failed to persist worker revoke",
	CodeRegistryPersistUnrevokeFail:   "Failed to persist worker unrevoke",
	CodeRegistryStaleWorkerCleanup:    "Cleaned up stale worker",
}

// GetDescription returns the human-readable description for a code
func GetDescription(code string) string {
	if desc, ok := CodeDescriptions[code]; ok {
		return desc
	}
	return code
}
