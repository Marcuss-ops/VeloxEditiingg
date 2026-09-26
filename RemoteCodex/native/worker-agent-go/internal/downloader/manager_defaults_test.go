package downloader

import "testing"

func TestManagerDefaultsToEightAssetTransfers(t *testing.T) {
	m := NewManager(Config{}, nil)
	defer m.Close()
	if m.cfg.Concurrency != 8 {
		t.Fatalf("default download concurrency=%d want 8", m.cfg.Concurrency)
	}
}
