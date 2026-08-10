package validation

import (
	"errors"
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

// ValidationRepository is the persistence boundary required by the HTTP
// handlers. Keeping this interface local makes handlers independently
// testable without opening SQLite.
type ValidationRepository interface {
	SaveValidation(report *ValidationReport) error
	GetValidation(workerID string) (*ValidationStatus, error)
	GetAllValidations() ([]map[string]any, error)
}

// ValidationStore persists validation statuses to SQLite.
type ValidationStore struct {
	db *store.SQLiteStore
}

var errValidationStoreNotConfigured = errors.New("validation store not configured")

// NewValidationStore creates a validation store backed by db.
func NewValidationStore(db *store.SQLiteStore) *ValidationStore {
	return &ValidationStore{db: db}
}

// GetAllValidations retrieves all validation statuses from the backing store.
func (vs *ValidationStore) GetAllValidations() ([]map[string]any, error) {
	if vs == nil || vs.db == nil {
		return nil, errValidationStoreNotConfigured
	}
	return vs.db.GetAllWorkerValidations()
}

// Handler serves worker validation endpoints using an injected repository.
type Handler struct {
	repository ValidationRepository
}

// NewHandler creates validation handlers backed by repository. Production
// composition must pass a non-nil repository; a nil repository is treated as
// a wiring failure by every request path and never as a successful fallback.
func NewHandler(repository ValidationRepository) *Handler {
	return &Handler{repository: repository}
}

func (h *Handler) repositoryReady() bool {
	return h != nil && h.repository != nil
}

// SaveValidation saves a validation report to the store.
func (vs *ValidationStore) SaveValidation(report *ValidationReport) error {
	if vs == nil || vs.db == nil {
		return errValidationStoreNotConfigured
	}

	var validatedAt time.Time
	if report.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, report.Timestamp); err == nil {
			validatedAt = t
		}
	}
	if validatedAt.IsZero() {
		validatedAt = time.Now()
	}

	failureReason := ""
	switch report.ValidationCode {
	case "PASS":
	case "MISSING_UNIT":
		failureReason = "Canonical unit does not exist"
	case "EMPTY_EXECSTART":
		failureReason = "Unit exists but ExecStart is empty or invalid"
	case "UNKNOWN_FORMAT":
		failureReason = "ExecStart format not recognized as Docker"
	default:
		failureReason = "Unknown validation code: " + report.ValidationCode
	}

	return vs.db.SaveWorkerValidation(report.WorkerID, report.ValidationCode, report.CanonicalUnit, report.ExecStart, validatedAt, failureReason)
}

// GetValidation retrieves validation status for a worker.
func (vs *ValidationStore) GetValidation(workerID string) (*ValidationStatus, error) {
	if vs == nil || vs.db == nil {
		return nil, errValidationStoreNotConfigured
	}

	return vs.db.GetWorkerValidation(workerID)
}

// HandleValidationReport handles the canonical POST /api/v1/agent/validation
// route. Legacy /api/workers paths are test-only and are not mounted.
func (h *Handler) HandleValidationReport() gin.HandlerFunc {
	return func(c *gin.Context) {
		var report ValidationReport
		if err := c.ShouldBindJSON(&report); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "Invalid validation report: " + err.Error(),
			})
			return
		}

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

		if !h.repositoryReady() {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "validation repository not configured",
			})
			return
		}
		if authenticatedWorkerID, exists := c.Get("authenticated_worker_id"); exists {
			if workerID, ok := authenticatedWorkerID.(string); !ok || workerID != report.WorkerID {
				c.JSON(http.StatusForbidden, gin.H{
					"ok":    false,
					"error": "worker identity does not match validation report",
				})
				return
			}
		}
		if authenticatedAdmin, exists := c.Get("authenticated_admin"); exists {
			if isAdmin, ok := authenticatedAdmin.(bool); !ok || !isAdmin {
				c.JSON(http.StatusForbidden, gin.H{
					"ok":    false,
					"error": "invalid authenticated admin context",
				})
				return
			}
		}
		if err := h.repository.SaveValidation(&report); err != nil {
			log.Printf("[ERROR] Failed to save validation report for %s: %v", report.WorkerID, err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": "failed to persist validation report",
			})
			return
		}

		log.Printf("[VALID] Validation Report: worker=%s code=%s unit=%s",
			report.WorkerID, report.ValidationCode, report.CanonicalUnit)

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

// GetWorkerValidationHandler handles the canonical
// GET /api/v1/admin/workers/:worker_id/validation route.
func (h *Handler) GetWorkerValidationHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Param("worker_id")
		if workerID == "" {
			workerID = c.Param("id")
		}
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "missing worker id",
			})
			return
		}

		if !h.repositoryReady() {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "validation repository not configured",
			})
			return
		}

		status, err := h.repository.GetValidation(workerID)
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

// GetAllValidationsHandler handles the canonical
// GET /api/v1/fleet/validations route.
func (h *Handler) GetAllValidationsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !h.repositoryReady() {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "validation repository not configured",
			})
			return
		}

		validations, err := h.repository.GetAllValidations()
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

func getValidationMessage(code string) string {
	switch code {
	case "PASS":
		return "Canonical Docker/Go worker unit validated"
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
