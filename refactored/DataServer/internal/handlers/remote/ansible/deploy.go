package ansible

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"velox-shared/payload"

	"github.com/google/uuid"
)

type deployPlan struct {
	RootRunID     string
	CanaryWorkers []string
	Batches       [][]string
	BatchSize     int
	CanaryPercent float64
	TargetWorkers []string
}

func splitIntoBatches(items []string, size int) [][]string {
	if size <= 0 {
		size = 1
	}
	if len(items) == 0 {
		return nil
	}
	batches := make([][]string, 0, int(math.Ceil(float64(len(items))/float64(size))))
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		batch := make([]string, end-i)
		copy(batch, items[i:end])
		batches = append(batches, batch)
	}
	return batches
}

func (h *AnsibleHandlers) buildDeployPlan(targets []string, batchSize int, canaryPercent float64) deployPlan {
	if batchSize <= 0 {
		batchSize = 5
	}
	if canaryPercent <= 0 {
		canaryPercent = 10
	}

	totalWorkers := len(targets)
	canaryCount := int(math.Ceil(float64(totalWorkers) * canaryPercent / 100.0))
	if canaryCount < 1 {
		canaryCount = 1
	}
	if canaryCount > totalWorkers {
		canaryCount = totalWorkers
	}

	canaryWorkers := append([]string{}, targets[:canaryCount]...)
	remainingWorkers := append([]string{}, targets[canaryCount:]...)

	return deployPlan{
		RootRunID:     uuid.NewString(),
		CanaryWorkers: canaryWorkers,
		Batches:       splitIntoBatches(remainingWorkers, batchSize),
		BatchSize:     batchSize,
		CanaryPercent: canaryPercent,
		TargetWorkers: append([]string{}, targets...),
	}
}

func (h *AnsibleHandlers) createDeployRun(plan deployPlan) error {
	if h.manager == nil {
		return fmt.Errorf("ansible run manager unavailable")
	}

	return h.manager.CreateRun(AnsibleRunRecord{
		ID:        plan.RootRunID,
		Action:    "deploy_workers",
		Playbook:  "update_workers.yml",
		Hosts:     plan.TargetWorkers,
		Commands:  []string{"deploy_workers"},
		Status:    "running",
		StartedAt: time.Now().Unix(),
		Preamble: fmt.Sprintf(
			"deploy_mode=canary-batch\ncanary_percent=%.2f\nbatch_size=%d\ntargets=%s\n",
			plan.CanaryPercent,
			plan.BatchSize,
			strings.Join(plan.TargetWorkers, ","),
		),
	})
}

func (h *AnsibleHandlers) waitForRun(ctx context.Context, runID string, timeout time.Duration) (AnsibleRunRecord, error) {
	if h.manager == nil {
		return AnsibleRunRecord{}, fmt.Errorf("ansible run manager unavailable")
	}
	deadline := time.Now().Add(timeout)
	for {
		run, ok := h.manager.GetRun(runID)
		if ok && run.Status != "" && run.Status != "running" && run.Status != "queued" {
			return run, nil
		}
		if time.Now().After(deadline) {
			return AnsibleRunRecord{}, fmt.Errorf("timed out waiting for run %s", runID)
		}
		select {
		case <-ctx.Done():
			return AnsibleRunRecord{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (h *AnsibleHandlers) runDeployWorkers(targets []string, batchSize int, canaryPercent float64) (string, error) {
	plan := h.buildDeployPlan(targets, batchSize, canaryPercent)
	if err := h.createDeployRun(plan); err != nil {
		return "", err
	}

	go func() {
		bgCtx := context.Background()
		masterURL := payload.FirstNonEmpty(
			h.masterURL,
			os.Getenv("VELOX_MASTER_URL"),
			os.Getenv("VELOX_MASTER_SERVER_URL"),
			DetectLocalMasterURL(),
		)
		summary := []string{
			fmt.Sprintf("canary=%s", strings.Join(plan.CanaryWorkers, ",")),
		}

		updateRoot := func(mut func(*AnsibleRunRecord)) {
			if err := h.manager.UpdateRun(plan.RootRunID, mut); err != nil {
				// Best effort: the root run is still the source of truth.
				_ = err
			}
		}

		updateRoot(func(run *AnsibleRunRecord) {
			run.Preamble += "stage=canary\n"
		})

		runBatch := func(label string, hosts []string) error {
			if len(hosts) == 0 {
				return nil
			}
			runID, err := h.manager.RunPlaybook(bgCtx, strings.Join(hosts, ","), "update_workers.yml", map[string]interface{}{
				"master_url": masterURL,
			})
			if err != nil {
				return err
			}
			summary = append(summary, fmt.Sprintf("%s=%s", label, strings.Join(hosts, ",")))
			updateRoot(func(run *AnsibleRunRecord) {
				run.Preamble += fmt.Sprintf("queued_%s_run=%s\n", label, runID)
			})
			batchRun, err := h.waitForRun(bgCtx, runID, 45*time.Minute)
			if err != nil {
				return err
			}
			if batchRun.Status == "failed" {
				return fmt.Errorf("%s batch failed", label)
			}
			summary = append(summary, fmt.Sprintf("%s_status=%s", label, batchRun.Status))
			updateRoot(func(run *AnsibleRunRecord) {
				run.Preamble += fmt.Sprintf("completed_%s_run=%s\n", label, runID)
			})
			return nil
		}

		err := runBatch("canary", plan.CanaryWorkers)
		if err == nil {
			for idx, batch := range plan.Batches {
				if err = runBatch(fmt.Sprintf("batch_%d", idx+1), batch); err != nil {
					break
				}
			}
		}

		if err != nil {
			updateRoot(func(run *AnsibleRunRecord) {
				run.Status = "failed"
				run.EndedAt = time.Now().Unix()
				run.Output = strings.Join(summary, "\n") + "\nerror=" + err.Error() + "\n"
			})
			return
		}

		updateRoot(func(run *AnsibleRunRecord) {
			run.Status = "completed"
			run.EndedAt = time.Now().Unix()
			run.Output = strings.Join(summary, "\n") + "\n"
		})
	}()

	return plan.RootRunID, nil
}
