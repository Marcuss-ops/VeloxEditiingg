package workers

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// ValidationReport represents the systemd unit validation report from Ansible
type ValidationReport struct {
	WorkerID           string `json:"worker_id"`
	ValidationCode     string `json:"validation_code"`
	ExecStart          string `json:"exec_start"`
	CanonicalUnit      string `json:"canonical_unit"`
	LegacyUnitsRemoved int    `json:"legacy_units_removed"`
	Timestamp          string `json:"timestamp"`
}

// ValidationStatus is an alias to the store type for backward compatibility
type ValidationStatus = store.WorkerValidationStatus

// ValidationStore holds validation statuses in memory and persists to SQLite
type ValidationStore struct {
	db *store.SQLiteStore
}

// NewValidationStore creates a new validation store
func NewValidationStore(db *store.SQLiteStore) *ValidationStore {
	return &ValidationStore{db: db}
}

// globalValidationStore is the global instance
var globalValidationStore *ValidationStore

// InitValidationStore initializes the global validation store
func InitValidationStore(db *store.SQLiteStore) {
	globalValidationStore = NewValidationStore(db)

	// Create table if not exists
	if db != nil {
		err := db.CreateValidationTableIfNotExists()
		if err != nil {
			log.Printf("[WARN] Failed to create validation table: %v", err)
		}
	}
}

// SaveValidation saves a validation report to the store
func (vs *ValidationStore) SaveValidation(report *ValidationReport) error {
	if vs.db == nil {
		return nil // No persistence if no DB
	}

	// Parse timestamp
	var validatedAt time.Time
	if report.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, report.Timestamp); err == nil {
			validatedAt = t
		}
	}
	if validatedAt.IsZero() {
		validatedAt = time.Now()
	}

	// Determine failure reason for non-PASS codes
	failureReason := ""
	switch report.ValidationCode {
	case "PASS":
		// No failure reason for successful validation
	case "LEGACY_PYTHON":
		failureReason = "Unit contains legacy Python worker (job_worker.py or worker_bootstrap.py)"
	case "MISSING_UNIT":
		failureReason = "Canonical unit does not exist"
	case "EMPTY_EXECSTART":
		failureReason = "Unit exists but ExecStart is empty or invalid"
	case "UNKNOWN_FORMAT":
		failureReason = "ExecStart format not recognized as Docker or Python"
	default:
		failureReason = "Unknown validation code: " + report.ValidationCode
	}

	return vs.db.SaveWorkerValidation(report.WorkerID, report.ValidationCode, report.CanonicalUnit, report.ExecStart, validatedAt, failureReason)
}

// GetValidation retrieves validation status for a worker
func (vs *ValidationStore) GetValidation(workerID string) (*ValidationStatus, error) {
	if vs.db == nil {
		return nil, nil
	}

	return vs.db.GetWorkerValidation(workerID)
}

// HandleValidationReport handles POST /api/workers/validation
func HandleValidationReport() gin.HandlerFunc {
	return func(c *gin.Context) {
		var report ValidationReport
		if err := c.ShouldBindJSON(&report); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "Invalid validation report: " + err.Error(),
			})
			return
		}

		// Validate required fields
		if report.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "missing worker_id",
			})
			return
		}

		if report.ValidationCode == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "missing validation_code",
			})
			return
		}

		// Save to store
		if globalValidationStore != nil {
			if err := globalValidationStore.SaveValidation(&report); err != nil {
				log.Printf("[WARN] Failed to save validation report for %s: %v", report.WorkerID, err)
			}
		}

		// Log the validation
		log.Printf("[VALID] Validation Report: worker=%s code=%s unit=%s",
			report.WorkerID, report.ValidationCode, report.CanonicalUnit)

		// Determine if worker is allowed to run jobs
		isValid := report.ValidationCode == "PASS"

		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"worker_id": report.WorkerID,
			"valid":     isValid,
			"code":      report.ValidationCode,
			"message":   getValidationMessage(report.ValidationCode),
		})
	}
}

// GetWorkerValidationHandler handles GET /api/workers/:id/validation
func GetWorkerValidationHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Param("id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "missing worker id",
			})
			return
		}

		if globalValidationStore == nil {
			c.JSON(http.StatusOK, gin.H{
				"worker_id": workerID,
				"valid":     true, // Assume valid if no store
				"code":      "UNKNOWN",
				"message":   "Validation store not initialized",
			})
			return
		}

		status, err := globalValidationStore.GetValidation(workerID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": err.Error(),
			})
			return
		}

		if status == nil {
			c.JSON(http.StatusOK, gin.H{
				"worker_id": workerID,
				"valid":     false,
				"code":      "NOT_VALIDATED",
				"message":   "Worker has not been validated yet",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"worker_id":      status.WorkerID,
			"valid":          status.ValidationCode == "PASS",
			"code":           status.ValidationCode,
			"canonical_unit": status.CanonicalUnit,
			"exec_start":     status.ExecStart,
			"validated_at":   status.ValidatedAt,
			"failure_reason": status.FailureReason,
		})
	}
}

// GetAllValidationsHandler handles GET /api/workers/validations
func GetAllValidationsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if globalValidationStore == nil || globalValidationStore.db == nil {
			c.JSON(http.StatusOK, gin.H{
				"validations": []interface{}{},
			})
			return
		}

		validations, err := globalValidationStore.db.GetAllWorkerValidations()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":          true,
			"validations": validations,
		})
	}
}

// CheckWorkerValidation checks if a worker is validated and allowed to run jobs
// Returns true if the worker is valid or if validation is not available
func CheckWorkerValidation(workerID string) (bool, string) {
	if globalValidationStore == nil {
		return true, "" // Allow if no store
	}

	status, err := globalValidationStore.GetValidation(workerID)
	if err != nil || status == nil {
		return true, "" // Allow if no status (not yet validated)
	}

	return status.ValidationCode == "PASS", status.FailureReason
}

func getValidationMessage(code string) string {
	switch code {
	case "PASS":
		return "Canonical Docker/Go worker unit validated"
	case "LEGACY_PYTHON":
		return "Legacy Python unit detected - reinstall required"
	case "MISSING_UNIT":
		return "Canonical unit missing - install required"
	case "EMPTY_EXECSTART":
		return "Unit exists but ExecStart is empty - reinstall required"
	case "UNKNOWN_FORMAT":
		return "ExecStart format not recognized - manual verification required"
	default:
		return "Unknown validation status"
	}
}
