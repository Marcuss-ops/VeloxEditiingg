package workers

import (
	"context"
	"testing"
	"time"

	workersreg "velox-server/internal/workers"
)

func TestNewWorkerLifecycleManager(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)
	if lm == nil {
		t.Fatal("expected non-nil lifecycle manager")
	}
}

func TestWorkerLifecycleManagerStart(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lm.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
}

func TestEvaluateWorkerHealth_Healthy(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	info := reg.GetWorker(ctx, "w1")
	now := time.Now()
	health := lm.evaluateWorkerHealth(ctx, *info, now)

	if health.Status != "healthy" {
		t.Errorf("expected healthy, got %s", health.Status)
	}
	if health.HealthScore != 1.0 {
		t.Errorf("expected score 1.0, got %f", health.HealthScore)
	}
}

func TestEvaluateWorkerHealth_Degraded(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	info := reg.GetWorker(ctx, "w1")
	info.LastHB = time.Now().UTC().Add(-cfg.HeartbeatTimeout * 3 / 4).Format(time.RFC3339)
	now := time.Now()
	health := lm.evaluateWorkerHealth(ctx, *info, now)

	if health.Status != "degraded" {
		t.Errorf("expected degraded, got %s", health.Status)
	}
}

func TestEvaluateWorkerHealth_Unhealthy(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	info := reg.GetWorker(ctx, "w1")
	info.LastHB = time.Now().UTC().Add(-cfg.HeartbeatTimeout - 1*time.Second).Format(time.RFC3339)
	now := time.Now()
	health := lm.evaluateWorkerHealth(ctx, *info, now)

	if health.Status != "unhealthy" {
		t.Errorf("expected unhealthy, got %s", health.Status)
	}
}

func TestEvaluateWorkerHealth_Offline(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	info := reg.GetWorker(ctx, "w1")
	info.LastHB = time.Now().UTC().Add(-cfg.HeartbeatTimeout*2 - 1*time.Second).Format(time.RFC3339)
	now := time.Now()
	health := lm.evaluateWorkerHealth(ctx, *info, now)

	if health.Status != "offline" {
		t.Errorf("expected offline, got %s", health.Status)
	}
	if health.HealthScore != 0 {
		t.Errorf("expected score 0, got %f", health.HealthScore)
	}
}

func TestEvaluateWorkerHealth_ErrorRate(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	info := reg.GetWorker(ctx, "w1")
	info.Metrics = map[string]interface{}{
		"jobs_completed": float64(10),
		"jobs_failed":    float64(5),
	}
	now := time.Now()
	health := lm.evaluateWorkerHealth(ctx, *info, now)

	if health.JobsCompleted != 10 {
		t.Errorf("expected JobsCompleted=10, got %d", health.JobsCompleted)
	}
	if health.JobsFailed != 5 {
		t.Errorf("expected JobsFailed=5, got %d", health.JobsFailed)
	}
	errorRate := float64(health.JobsFailed) / float64(health.JobsCompleted+health.JobsFailed)
	if errorRate <= 0.3 {
		t.Errorf("expected error rate > 0.3, got %f", errorRate)
	}
	if health.Status != "degraded" {
		t.Errorf("expected degraded status with high error rate, got %s", health.Status)
	}
}

func TestGetAllHealth(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)
	_ = reg.RegisterWorker(ctx, "w2", "worker-2", "10.0.0.2", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	lm.checkAllWorkersHealth(ctx)

	allHealth := lm.GetAllHealth()
	if len(allHealth) != 2 {
		t.Errorf("expected 2 health entries, got %d", len(allHealth))
	}
	if _, ok := allHealth["w1"]; !ok {
		t.Error("expected w1 in health map")
	}
	if _, ok := allHealth["w2"]; !ok {
		t.Error("expected w2 in health map")
	}
}

func TestStats(t *testing.T) {
	reg := workersreg.New(nil, false, nil)
	ctx := context.Background()
	_ = reg.RegisterWorker(ctx, "w1", "worker-1", "10.0.0.1", nil)

	cfg := DefaultLifecycleConfig()
	lm := NewWorkerLifecycleManager(cfg, reg, nil, nil)

	lm.checkAllWorkersHealth(ctx)

	stats := lm.Stats()
	if _, ok := stats["pending_shutdowns"]; !ok {
		t.Error("expected pending_shutdowns in stats")
	}
	if _, ok := stats["tracked_workers"]; !ok {
		t.Error("expected tracked_workers in stats")
	}
	if _, ok := stats["health_summary"]; !ok {
		t.Error("expected health_summary in stats")
	}
	if stats["tracked_workers"] != 1 {
		t.Errorf("expected 1 tracked worker, got %v", stats["tracked_workers"])
	}
}
