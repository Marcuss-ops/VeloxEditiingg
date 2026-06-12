package workers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// WorkersList same response shape as Python GET /workers
func WorkersList(reg *workersreg.Registry, workersRepo store.WorkersRepository, updateHandler ...*WorkerUpdateHandler) gin.HandlerFunc {
	return func(c *gin.Context) {
		master := workerStatusMetadata(firstUpdateHandler(updateHandler))
		if workersRepo != nil {
			if dbWorkers, err := workersRepo.ListWorkers(); err == nil && len(dbWorkers) > 0 {
				c.JSON(http.StatusOK, gin.H{"workers": dbWorkers, "master": master})
				return
			}
		}
		list := reg.List(c.Request.Context())
		c.JSON(http.StatusOK, gin.H{"workers": list, "master": master})
	}
}

func firstUpdateHandler(updateHandler []*WorkerUpdateHandler) *WorkerUpdateHandler {
	if len(updateHandler) == 0 || updateHandler[0] == nil {
		return nil
	}
	return updateHandler[0]
}

func WorkerStatusMetadata(h *WorkerUpdateHandler) gin.H {
	return workerStatusMetadata(h)
}

func workerStatusMetadata(h *WorkerUpdateHandler) gin.H {
	version := ""
	codeVersion := ""
	if h != nil {
		version = h.cfg.VersionNumber
		codeVersion = h.codeVersion
	}
	if codeVersion == "" {
		codeVersion = version
	}
	bundleHash := ""
	if h != nil {
		if bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64"); err == nil {
			bundleHash = computeFileSHA256(bundlePath)
		}
	}
	return gin.H{
		"bundle_version":   version,
		"bundle_hash":      bundleHash,
		"code_version":     codeVersion,
		"protocol_version": workersreg.DefaultWorkerProtocolVersion,
		"engine_version":   codeVersion,
	}
}

func workerStatusItem(w workersreg.WorkerInfo, master gin.H) gin.H {
	warnings := []string{}
	if masterVersion := stringFromAny(master["bundle_version"]); masterVersion != "" && w.BundleVersion != "" && w.BundleVersion != masterVersion {
		warnings = append(warnings, "bundle_version mismatch")
	}
	if masterHash := stringFromAny(master["bundle_hash"]); masterHash != "" && w.BundleHash != "" && w.BundleHash != masterHash {
		warnings = append(warnings, "bundle_hash mismatch")
	}
	if masterCode := stringFromAny(master["code_version"]); masterCode != "" && w.CodeVersion != "" && w.CodeVersion != masterCode {
		warnings = append(warnings, "code_version mismatch")
	}
	if masterProtocol := stringFromAny(master["protocol_version"]); masterProtocol != "" && w.ProtocolVersion != "" && w.ProtocolVersion != masterProtocol {
		warnings = append(warnings, "protocol_version mismatch")
	}
	return gin.H{
		"worker_id":            w.WorkerID,
		"worker_name":          w.WorkerName,
		"display_name":         w.WorkerName,
		"name":                 w.WorkerName,
		"status":               w.Status,
		"last_heartbeat":       w.LastHB,
		"time_since_heartbeat": 0,
		"active":               true,
		"current_job":          w.CurrentJob,
		"code_version":         w.CodeVersion,
		"bundle_version":       w.BundleVersion,
		"bundle_hash":          w.BundleHash,
		"protocol_version":     w.ProtocolVersion,
		"engine_version":       w.EngineVersion,
		"capabilities":         w.Capabilities,
		"drain":                w.Drain,
		"schedulable":          w.Schedulable,
		"worker_group":         w.WorkerGroup,
		"first_seen":           w.FirstSeen,
		"ip_address":           w.IPAddress,
		"readiness":            w.Readiness,
		"metadata_warnings":    warnings,
		"metadata_ok":          len(warnings) == 0,
	}
}

func stringFromAny(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// WorkersStatus returns same shape as Python GET /workers_status for installer/dashboard
func WorkersStatus(reg *workersreg.Registry, q *queue.Queue, updateHandler ...*WorkerUpdateHandler) gin.HandlerFunc {
	const heartbeatTimeoutSec = 900 // 15 min like Python
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		master := workerStatusMetadata(firstUpdateHandler(updateHandler))
		list := reg.List(ctx)
		now := time.Now().UTC()
		var workersList []gin.H
		activeCount := 0
		for _, w := range list {
			var since float64
			if w.LastHB != "" {
				if t, err := time.Parse(time.RFC3339, w.LastHB); err == nil {
					since = now.Sub(t.UTC()).Seconds()
				}
			}
			active := since < heartbeatTimeoutSec
			if active {
				activeCount++
			}
			item := workerStatusItem(w, master)
			item["time_since_heartbeat"] = since
			item["active"] = active
			workersList = append(workersList, item)
		}
		pending, _ := q.ReadyCount(ctx)
		processing, _ := q.LeasedCount(ctx)

		// Include revoked workers count for dashboard awareness
		revokedList := reg.ListRevoked()

		c.JSON(http.StatusOK, gin.H{
			"workers":         workersList,
			"master":          master,
			"active_workers":  activeCount,
			"total_workers":   len(workersList),
			"revoked_workers": len(revokedList),
			"revoked_ids":     revokedList,
			"pending_jobs":    pending,
			"processing_jobs": processing,
			"completed_jobs":  0,
			"error_jobs":      0,
			"total_jobs":      pending + processing,
		})
	}
}
