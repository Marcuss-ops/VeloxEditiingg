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
	"log"

	"velox-server/internal/fleet"
	"velox-shared/identity"
)

// buildWorkerRegistryFromStore populates a fleet.WorkerRegistry from the
// canonical worker-node view of the persistent inventory. Rows without a
// host or SSH user are skipped (they cannot back a real SSH operation);
// duplicate worker_ids are logged and dropped. A nil store yields an empty
// registry (nil-tolerant, matching the surrounding wiring).
func buildWorkerRegistryFromStore(p *persistenceDeps) *fleet.WorkerRegistry {
	reg := fleet.NewWorkerRegistry()
	if p == nil || p.SQLite == nil {
		log.Printf("[BOOTSTRAP] WorkerNodeRegistry: no store — SSH surface left empty (health/smoke will fail per-target with a clear error)")
		return reg
	}
	nodes, err := p.SQLite.ListWorkerNodes()
	if err != nil {
		log.Printf("[BOOTSTRAP] WorkerNodeRegistry: ListWorkerNodes failed: %v — SSH surface left empty", err)
		return reg
	}
	added := 0
	for _, n := range nodes {
		entry := fleet.WorkerRegistryEntry{
			WorkerID: identity.ParseWorkerID(n.WorkerID),
			Host:     n.SSHHost,
			SSHUser:  n.SSHUser,
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
	return reg
}
