package worker

import (
	"context"
	"testing"
	"time"
)

func TestCPUTokenPoolWeightedAdmission(t *testing.T) {
	p := NewCPUTokenPool(3)
	if err := p.Acquire(context.Background(), 2); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- p.Acquire(context.Background(), 2) }()
	select {
	case err := <-blocked:
		t.Fatalf("weighted acquire bypassed capacity: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	p.Release(2)
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("second acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("weighted acquire did not unblock after release")
	}
	p.Release(2)
}

func TestCPUTokenPoolCancellationRollsBackPartialAcquire(t *testing.T) {
	p := NewCPUTokenPool(2)
	if err := p.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Acquire(ctx, 2); err == nil {
		t.Fatal("canceled acquire succeeded")
	}
	p.Release(1)
	if err := p.Acquire(context.Background(), 2); err != nil {
		t.Fatalf("partial acquisition leaked a token: %v", err)
	}
	p.Release(2)
}
