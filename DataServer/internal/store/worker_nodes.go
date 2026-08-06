package store

// ============================================================
// WorkerNodeRegistry — canonical persistent fleet inventory (Phase 9)
// ============================================================
//
// The `ansible_hosts` table (migration 004) is the single persistent
// source of truth for fleet node connectivity. This file exposes the
// CANONICAL WorkerNode view of that table — the model the master,
// fleet operator surface, SSH backends and inventory generation all
// consume — so no other representation (static inventory files,
// hardcoded SSH targets, alias maps) can drift away from it.
//
// WorkerNode fields are a strict subset of AnsibleHostFields, projected
// with canonical names:
//
//	WorkerID    ← worker_id            (canonical identity; empty rows excluded)
//	SSHHost     ← host                 (IP or hostname)
//	SSHUser     ← ansible_user
//	SecretRef   ← secret_ref           (credential reference, never a value)
//	Environment ← host_group           (fleet cohort: production, staging, ...)
//	Enabled     ← enabled
//
// The view deliberately returns ONLY rows that can back a real SSH
// operation: enabled=1 AND worker_id != ''. A disabled or unmapped host
// is not a schedulable node and must not leak into the SSH registry.

// WorkerNode is the canonical persistent fleet inventory entry.
type WorkerNode struct {
	WorkerID    string
	SSHHost     string
	SSHUser     string
	SecretRef   string
	Environment string
	Enabled     bool
}

// ListWorkerNodes returns the canonical fleet inventory: every enabled
// ansible_hosts row that carries a non-empty worker_id, ordered by
// worker_id for a stable registry. This is the ONLY sanctioned source
// for populating the fleet WorkerRegistry and for generating the
// per-operation Ansible inventory.
func (s *SQLiteStore) ListWorkerNodes() ([]WorkerNode, error) {
	rows, err := s.db.Query(
		`SELECT host, ansible_user, secret_ref, host_group, worker_id, enabled
		 FROM ansible_hosts
		 WHERE enabled=1 AND worker_id != ''
		 ORDER BY worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []WorkerNode
	for rows.Next() {
		var n WorkerNode
		var enabled int
		if err := rows.Scan(&n.SSHHost, &n.SSHUser, &n.SecretRef, &n.Environment, &n.WorkerID, &enabled); err != nil {
			continue
		}
		n.Enabled = enabled == 1
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

// ListWorkerNodesWithDisabled returns ALL nodes carrying a worker_id
// (enabled or not), ordered by worker_id. Used by audit/invariant
// tooling that must see disabled nodes; runtime consumers must use
// ListWorkerNodes so disabled nodes never enter the SSH registry.
func (s *SQLiteStore) ListWorkerNodesWithDisabled() ([]WorkerNode, error) {
	rows, err := s.db.Query(
		`SELECT host, ansible_user, secret_ref, host_group, worker_id, enabled
		 FROM ansible_hosts
		 WHERE worker_id != ''
		 ORDER BY worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []WorkerNode
	for rows.Next() {
		var n WorkerNode
		var enabled int
		if err := rows.Scan(&n.SSHHost, &n.SSHUser, &n.SecretRef, &n.Environment, &n.WorkerID, &enabled); err != nil {
			continue
		}
		n.Enabled = enabled == 1
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}
