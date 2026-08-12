package main

// bootstrap_worker_nodes.go — Phase 9 inventory unification bridge.
//
// The persistent `ansible_hosts` worker-node view (store.ListWorkerNodes)
// is the single source of truth for fleet connectivity. This file builds
// the canonical fleet.WorkerRegistry from that view at composition time;
// every SSH-backed consumer (health probes Level A/B, Level D smoke) then
// derives its client from the registry. There is intentionally NO
// hardcoded SSH target map anywhere in the composition root anymore — an
// empty inventory fails per-target at Run time with a clear error instead
// of silently carrying stale IPs.

import (
	"fmt"
	"log"

	"velox-server/internal/fleet"
	"velox-shared/identity"
)

// buildWorkerRegistryFromStore populates a fleet.WorkerRegistry from the
// canonical worker-node view of the persistent inventory. Rows without a
// host or SSH user are skipped (they cannot back a real SSH operation);
// duplicate worker_ids are logged and dropped. A nil store yields an empty
// registry (nil-tolerant, matching the surrounding wiring).
func buildWorkerRegistryFromStore(p *persistenceDeps) (*fleet.WorkerRegistry, error) {
	reg := fleet.NewWorkerRegistry()
	if p == nil || p.SQLite == nil {
		return nil, fmt.Errorf("worker node registry: persistent store is not configured")
	}
	nodes, err := p.SQLite.ListWorkerNodes()
	if err != nil {
		return nil, fmt.Errorf("worker node registry: list persistent inventory: %w", err)
	}
	added := 0
	for _, n := range nodes {
		entry := fleet.WorkerRegistryEntry{
			WorkerID: identity.ParseWorkerID(n.WorkerID),
			Host:     n.SSHHost,
			SSHUser:  n.SSHUser,
		}
		if worker, workerErr := p.SQLite.GetWorker(n.WorkerID); workerErr == nil && worker != nil {
			entry.WorkerName, _ = worker["worker_name"].(string)
		}
		if entry.Host == "" || entry.SSHUser == "" {
			log.Printf("[BOOTSTRAP] WorkerNodeRegistry: skipping worker %s (missing host or ssh_user)", n.WorkerID)
			continue
		}
		if err := reg.AddWorker(entry); err != nil {
			log.Printf("[BOOTSTRAP] WorkerNodeRegistry: skipping worker %s: %v", n.WorkerID, err)
			continue
		}
		added++
	}
	log.Printf("[BOOTSTRAP] WorkerNodeRegistry: loaded %d worker nodes from persistent inventory (enabled + mapped)", added)
	return reg, nil
}

// workerNameResolverFromStore returns a fleet.WorkerNameResolver that maps
// an immutable worker_id to its operator-facing worker_name (the friendly
// velox-worker-01 style name the dashboard shows). The name is read from
// the persistent workers registry raw_json; worker_id is the mTLS-bound
// security principal and is NEVER rewritten here. A nil store yields a
// nil resolver so callers degrade gracefully to the ID-only view.
func workerNameResolverFromStore(p *persistenceDeps) fleet.WorkerNameResolver {
	if p == nil || p.SQLite == nil {
		return nil
	}
	return func(workerID string) string {
		w, err := p.SQLite.GetWorker(workerID)
		if err != nil || w == nil {
			return ""
		}
		name, _ := w["worker_name"].(string)
		return name
	}
}
