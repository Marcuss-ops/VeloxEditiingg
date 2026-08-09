package workers

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"velox-server/internal/outbox"

	"github.com/gin-gonic/gin"
)

// ForceRegenerateZipHandler handles POST /install_worker/force_regenerate_zip
//
// Synchronous path (?wait=1) runs velox-bundler inline and returns 200 OK
// with the new bundle hash. Asynchronous path (?wait=0, the default)
// enqueues a WORKER_BUNDLE_REBUILD_REQUESTED event in the canonical
// outbox (durable) and ACKs 202 ONLY after the INSERT commits — never
// before. The fire-and-forget goroutine that previously lived here was
// the source of "operator saw a 202 then bundle was never rebuilt"
// regressions when the master pod died between ACK and exec.
// velox-bundler itself is naturally idempotent (overwrites zips) so the
// dispatcher's transient retries are safe.
func (h *WorkerUpdateHandler) ForceRegenerateZipHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		wait := c.DefaultQuery("wait", "0") == "1"
		log.Printf("[DEBUG] DEBUG: h.bundleDir = %q", h.bundleDir)
		repoRoot := findRepoRootFrom(h.bundleDir)
		log.Printf("[DEBUG] DEBUG: repoRoot = %q", repoRoot)
		if repoRoot == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "repo root not found for rebuild tool", "bundleDir": h.bundleDir})
			return
		}
		bundleBinaryPath := getBundlerPath(repoRoot)
		if _, err := os.Stat(bundleBinaryPath); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "velox-bundler binary not found", "path": bundleBinaryPath})
			return
		}

		run := func() (string, error) {
			outputDir := h.bundleDir
			cmd := exec.Command(bundleBinaryPath, "--source", repoRoot, "--output", outputDir)
			cmd.Dir = filepath.Join(repoRoot, "DataServer")

			out, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("[ERROR] rebuild bundle failed: %v | %s", err, strings.TrimSpace(string(out)))
				return "", err
			}
			log.Printf("[OK] rebuild bundle V2 completed: %s", strings.TrimSpace(string(out)))
			if err := ensureWorkerBundleRuntime(repoRoot, outputDir); err != nil {
				log.Printf("[ERROR] worker bundle runtime normalization failed: %v", err)
				return "", err
			}
			bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64")
			if err != nil {
				return "", err
			}
			return computeFileSHA256(bundlePath), nil
		}

		if wait {
			newHash, err := run()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "rebuild failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"message":         "bundle rebuild completed",
				"new_bundle_hash": newHash,
				"binary":          bundleBinaryPath,
			})
			return
		}

		// ── Async (wait=0) ─────────────────────────────────────────
		// ASYNC-PATH-DURABILITY: Insert BEFORE the 202 ACK. The
		// outbox.Store.Insert function auto-commits a row with
		// status='PENDING'; only after the commit returns do we
		// emit 202 to the client. This is the canonical
		// transactional-outbox pattern: the ACK is gated on the
		// persistence of intent, not on the side-effect's
		// completion. If the master pod dies after the Insert
		// commits but before the dispatcher picks the event up,
		// the row is re-claimable at next boot.
		//
		// Defensive: nil outbox means bootstrap did not wire the
		// dependency — fail loudly so the misconfiguration is
		// observable instead of silently dropping the rebuild
		// request in a 202 ACK.
		if h.outbox == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "outbox not configured on WorkerUpdateHandler; async bundle rebuild cannot be durably enqueued",
			})
			return
		}
		payload, payloadErr := encodeBundleRebuildPayload(repoRoot, h.bundleDir, bundleBinaryPath)
		if payloadErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "encode rebuild payload",
				"detail": payloadErr.Error(),
			})
			return
		}
		eventID, insertErr := h.outbox.Insert(c.Request.Context(), nil, outbox.InsertParams{
			AggregateType: "worker_bundle",
			AggregateID:   fmt.Sprintf("rebuild:%s", repoRoot),
			EventType:     BundleRebuildRequestedEventType,
			Payload:       payload,
		})
		if insertErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "outbox enqueue failed",
				"detail": insertErr.Error(),
			})
			return
		}
		// 202 ACK ONLY after the row committed.
		c.JSON(http.StatusAccepted, gin.H{
			"ok":       true,
			"event_id": eventID,
			"message":  "bundle rebuild queued for dispatch",
			"binary":   bundleBinaryPath,
		})
	}
}

func ensureWorkerBundleRuntime(repoRoot, bundleDir string) error {
	script := filepath.Join(repoRoot, "scripts", "ops", "ensure-worker-bundle-runtime.sh")
	if _, err := os.Stat(script); err != nil {
		if os.IsNotExist(err) {
			// Test fixtures and older development checkouts may not carry the
			// production post-processor. Preserve the bundler contract there.
			return nil
		}
		return err
	}
	cmd := exec.Command(script, bundleDir)
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("normalize worker bundle runtime: %w: %s", err, strings.TrimSpace(string(out)))
	}
	log.Printf("[OK] worker bundle runtime normalized: %s", strings.TrimSpace(string(out)))
	return nil
}
