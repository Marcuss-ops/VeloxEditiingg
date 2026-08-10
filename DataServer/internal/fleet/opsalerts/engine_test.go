package opsalerts

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

type engineTestStore struct{}

func (engineTestStore) InsertAlertEvent(context.Context, store.AlertEvent) error { return nil }
func (engineTestStore) ResolveAlertEvent(context.Context, string, string, string, time.Time) error {
	return nil
}
func (engineTestStore) TouchActiveAlertEvent(context.Context, string, string, string, time.Time, string, string) error {
	return nil
}
func (engineTestStore) GetActiveAlertEventForWorkerRule(context.Context, string, string, string) (*store.AlertEvent, error) {
	return nil, store.ErrAlertEventNotFound
}

type typedNilEngineStore struct{}

func (*typedNilEngineStore) InsertAlertEvent(context.Context, store.AlertEvent) error { return nil }
func (*typedNilEngineStore) ResolveAlertEvent(context.Context, string, string, string, time.Time) error {
	return nil
}
func (*typedNilEngineStore) TouchActiveAlertEvent(context.Context, string, string, string, time.Time, string, string) error {
	return nil
}
func (*typedNilEngineStore) GetActiveAlertEventForWorkerRule(context.Context, string, string, string) (*store.AlertEvent, error) {
	return nil, store.ErrAlertEventNotFound
}

type engineTestSource struct{}

func (engineTestSource) WorkerIDs(CallCtx) ([]string, error) { return nil, nil }
func (engineTestSource) Snapshot(CallCtx, string) (*WorkerSnapshot, error) {
	return nil, nil
}

type evaluationSource struct {
	workerIDs    []string
	listErr      error
	snapshots    map[string]*WorkerSnapshot
	snapshotErrs map[string]error
}

func (s evaluationSource) WorkerIDs(CallCtx) ([]string, error) {
	return s.workerIDs, s.listErr
}
func (s evaluationSource) Snapshot(_ CallCtx, workerID string) (*WorkerSnapshot, error) {
	if err := s.snapshotErrs[workerID]; err != nil {
		return nil, err
	}
	return s.snapshots[workerID], nil
}

type evaluationMetrics struct {
	counts map[string]uint64
}

func (m *evaluationMetrics) RecordWorkerEvaluationErrors(category string, count uint64) {
	if m.counts == nil {
		m.counts = make(map[string]uint64)
	}
	m.counts[category] += count
}

type errorEngineStore struct {
	engineTestStore
	insertErr error
}

type countingEngineStore struct {
	engineTestStore
	mu      sync.Mutex
	inserts int
}

func (s *countingEngineStore) InsertAlertEvent(context.Context, store.AlertEvent) error {
	s.mu.Lock()
	s.inserts++
	s.mu.Unlock()
	return nil
}

func (s *countingEngineStore) insertCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inserts
}

type failingNotificationSink struct {
	calls int
}

func (s *failingNotificationSink) Process(context.Context, runtimealerts.AlertEvent) error {
	s.calls++
	return errors.New("notifier unavailable")
}

type infraErrorEngineStore struct {
	engineTestStore
}

func (infraErrorEngineStore) InsertAlertEvent(context.Context, store.AlertEvent) error {
	return sql.ErrConnDone
}

func (s errorEngineStore) InsertAlertEvent(context.Context, store.AlertEvent) error {
	return s.insertErr
}

type typedNilEngineSource struct{}

func (*typedNilEngineSource) WorkerIDs(CallCtx) ([]string, error) { return nil, nil }
func (*typedNilEngineSource) Snapshot(CallCtx, string) (*WorkerSnapshot, error) {
	return nil, nil
}

func TestNewEngineRejectsNilDataSource(t *testing.T) {
	engine, err := NewEngine(engineTestStore{}, nil)
	if engine != nil {
		t.Fatal("nil datasource must not produce an engine")
	}
	if !errors.Is(err, ErrDataSourceNotConfigured) {
		t.Fatalf("error = %v, want ErrDataSourceNotConfigured", err)
	}
}

func TestNewEngineRejectsTypedNilDataSource(t *testing.T) {
	var source *typedNilEngineSource
	engine, err := NewEngine(engineTestStore{}, source)
	if engine != nil {
		t.Fatal("typed-nil datasource must not produce an engine")
	}
	if !errors.Is(err, ErrDataSourceNotConfigured) {
		t.Fatalf("error = %v, want ErrDataSourceNotConfigured", err)
	}
}

func TestNewEngineRejectsTypedNilAlertStore(t *testing.T) {
	var alertStore *typedNilEngineStore
	engine, err := NewEngine(alertStore, engineTestSource{})
	if engine != nil {
		t.Fatal("typed-nil alert store must not produce an engine")
	}
	if !errors.Is(err, ErrAlertStoreNotConfigured) {
		t.Fatalf("error = %v, want ErrAlertStoreNotConfigured", err)
	}
}

func TestNewEngineRejectsNilAlertStore(t *testing.T) {
	engine, err := NewEngine(nil, engineTestSource{})
	if engine != nil {
		t.Fatal("nil alert store must not produce an engine")
	}
	if !errors.Is(err, ErrAlertStoreNotConfigured) {
		t.Fatalf("error = %v, want ErrAlertStoreNotConfigured", err)
	}
}

func TestEngineEvaluatePropagatesInfrastructureError(t *testing.T) {
	cause := errors.New("worker inventory unavailable")
	engine, err := NewEngine(engineTestStore{}, evaluationSource{listErr: cause})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	alerts, err := engine.Evaluate(context.Background())
	if alerts != nil {
		t.Fatalf("alerts = %v, want nil on infrastructure failure", alerts)
	}
	if !errors.Is(err, supervisor.ErrInfrastructure) || !errors.Is(err, cause) {
		t.Fatalf("error = %v, want infrastructure classification and original cause", err)
	}
}

func TestEngineRunPropagatesInfrastructureErrorToSupervisorContract(t *testing.T) {
	engine, err := NewEngine(engineTestStore{}, evaluationSource{listErr: errors.New("database offline")})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	err = engine.Run(context.Background())
	if !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("Run error = %v, want supervisor infrastructure error", err)
	}
}

func TestEngineEvaluatePropagatesInfrastructureSnapshotErrors(t *testing.T) {
	engine, err := NewEngine(engineTestStore{}, evaluationSource{
		workerIDs:    []string{"worker-1"},
		snapshotErrs: map[string]error{"worker-1": sql.ErrConnDone},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	_, err = engine.Evaluate(context.Background())
	if !errors.Is(err, supervisor.ErrInfrastructure) || !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("error = %v, want infrastructure classification and sql.ErrConnDone", err)
	}
}

func TestEngineRunPropagatesInfrastructurePersistenceErrors(t *testing.T) {
	goodValue := 95.0
	engine, err := NewEngine(infraErrorEngineStore{}, evaluationSource{
		workerIDs: []string{"worker-1"},
		snapshots: map[string]*WorkerSnapshot{
			"worker-1": {WorkerID: "worker-1", HeartbeatAgeSeconds: &goodValue},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := engine.Run(context.Background()); !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("Run error = %v, want supervisor infrastructure error", err)
	}
}

func TestEngineEvaluateAggregatesIsolatedWorkerErrorsAndContinues(t *testing.T) {
	goodValue := 95.0
	metrics := &evaluationMetrics{}
	engine, err := NewEngine(engineTestStore{}, evaluationSource{
		workerIDs: []string{"bad-worker", "good-worker"},
		snapshots: map[string]*WorkerSnapshot{
			"good-worker": {WorkerID: "good-worker", HeartbeatAgeSeconds: &goodValue},
		},
		snapshotErrs: map[string]error{"bad-worker": errors.New("worker snapshot unavailable")},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	engine.SetErrorMetrics(metrics)
	alerts, err := engine.Evaluate(context.Background())
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	foundGood := false
	for _, alert := range alerts {
		if alert.Subject == "good-worker" && alert.RuleID == string(RuleHeartbeatStale) {
			foundGood = true
		}
	}
	if !foundGood {
		t.Fatalf("good worker alert missing after isolated failure: %v", alerts)
	}
	if got := metrics.counts["snapshot"]; got != 1 {
		t.Fatalf("snapshot metric count = %d, want 1", got)
	}
}

func TestEngineEvaluatePropagatesInfrastructureStoreErrors(t *testing.T) {
	goodValue := 95.0
	engine, err := NewEngine(infraErrorEngineStore{}, evaluationSource{
		workerIDs: []string{"worker-1"},
		snapshots: map[string]*WorkerSnapshot{
			"worker-1": {WorkerID: "worker-1", HeartbeatAgeSeconds: &goodValue},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	_, err = engine.Evaluate(context.Background())
	if !errors.Is(err, supervisor.ErrInfrastructure) || !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("error = %v, want infrastructure classification and sql.ErrConnDone", err)
	}
}

func TestEngineNotifierFailureDoesNotReleasePersistedAlertClaim(t *testing.T) {
	diskValue := 90.0
	store := &countingEngineStore{}
	notifier := &failingNotificationSink{}
	metrics := &evaluationMetrics{}
	engine, err := NewEngine(store, evaluationSource{
		workerIDs: []string{"worker-1"},
		snapshots: map[string]*WorkerSnapshot{
			"worker-1": {WorkerID: "worker-1", DiskUsedPercent: &diskValue},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	engine.AddSink(notifier)
	engine.SetErrorMetrics(metrics)
	if _, err := engine.Evaluate(context.Background()); err != nil {
		t.Fatalf("first Evaluate: %v", err)
	}
	if _, err := engine.Evaluate(context.Background()); err != nil {
		t.Fatalf("second Evaluate: %v", err)
	}
	if got := store.insertCount(); got != 1 {
		t.Fatalf("persistence inserts = %d, want 1 after notifier failure", got)
	}
	if notifier.calls != 2 {
		t.Fatalf("notifier calls = %d, want 2 attempts across the firing and suppressed retry", notifier.calls)
	}
	if got := metrics.counts["notifier"]; got != 2 {
		t.Fatalf("notifier metric count = %d, want 2 attempts", got)
	}
}

func TestEngineEvaluateAggregatesIsolatedStoreErrors(t *testing.T) {
	goodValue := 95.0
	metrics := &evaluationMetrics{}
	engine, err := NewEngine(errorEngineStore{insertErr: errors.New("sqlite busy")}, evaluationSource{
		workerIDs: []string{"worker-1"},
		snapshots: map[string]*WorkerSnapshot{
			"worker-1": {WorkerID: "worker-1", HeartbeatAgeSeconds: &goodValue},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	engine.SetErrorMetrics(metrics)
	if _, err := engine.Evaluate(context.Background()); err != nil {
		t.Fatalf("isolated store error must not fail whole evaluation: %v", err)
	}
	if got := metrics.counts["store_insert"]; got != 1 {
		t.Fatalf("store_insert metric count = %d, want 1", got)
	}
}

func TestNewEngineWithClockReturnsReadyEngineWithDependencies(t *testing.T) {
	engine, err := NewEngineWithClock(engineTestStore{}, engineTestSource{}, time.Millisecond, 1)
	if err != nil {
		t.Fatalf("NewEngineWithClock: %v", err)
	}
	if engine == nil {
		t.Fatal("ready dependencies must produce an engine")
	}
	if engine.tick != time.Millisecond {
		t.Fatalf("tick = %s, want 1ms", engine.tick)
	}
	if engine.maxBatch != 1 {
		t.Fatalf("maxBatch = %d, want 1", engine.maxBatch)
	}
}
