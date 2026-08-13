package logger

import (
	"io"
	"sync"
	"testing"
)

// TestConcurrentSetLevelAndLog guards the fix that moved the level gate and
// prefix read under the logger mutex. Before the fix, a concurrent SetLevel
// (or SetPrefix/SetOutput) raced the lock-free level/prefix reads in the hot
// log path; the race detector flags it here deterministically under -race.
func TestConcurrentSetLevelAndLog(t *testing.T) {
	l := New(InfoLevel, io.Discard)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			l.Info("worker=%d", i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			l.SetLevel(DebugLevel)
			l.SetLevel(ErrorLevel)
			l.SetPrefix("[TEST]")
			l.SetOutput(io.Discard)
		}
	}()

	wg.Wait()
}

// TestConcurrentWithPrefixSnapshot guards WithPrefix's lock-protected
// level/output snapshot read.
func TestConcurrentWithPrefixSnapshot(t *testing.T) {
	l := New(InfoLevel, io.Discard)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			_ = l.WithPrefix("[x]")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			l.SetLevel(DebugLevel)
			l.SetOutput(io.Discard)
		}
	}()
	wg.Wait()
}
