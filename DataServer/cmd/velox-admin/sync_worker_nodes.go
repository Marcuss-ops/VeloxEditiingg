// sync_worker_nodes.go — velox-admin sync-worker-nodes
//
// Phase 9 inventory unification: one-time migration of a legacy static
// Ansible inventory (deploy/ansible/inventory.ini) into the persistent
// WorkerNodeRegistry (`ansible_hosts` worker-node view). After this
// command, the static file is NOT a source of truth anymore — the master
// reads fleet connectivity exclusively from the DB, and the file can be
// kept only as an ops reference or deleted.
//
// Parsed line format (matching deploy/ansible/inventory.ini):
//
//	<alias> ansible_host=<host> ansible_user=<user>
//	         ansible_ssh_private_key_file=<key> worker_id=<worker_id>
//
// Only lines inside a [velox_workers]-style group with a worker_id are
// imported; alias is preserved as LinkedWorkerID for traceability.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"velox-server/internal/store"
)

// inventoryNode is one parsed [velox_workers] line from a legacy INI.
type inventoryNode struct {
	Alias    string
	Host     string
	User     string
	KeyPath  string
	WorkerID string
	Group    string
}

// parseLegacyInventory reads a static Ansible INI and returns the
// worker-bearing rows from every [<group>] section. Non-worker sections
// (e.g. [master]) are ignored. Rows without worker_id or host are
// reported but skipped (a row that cannot identify a node cannot be
// imported into the canonical registry).
func parseLegacyInventory(r io.Reader) ([]inventoryNode, []string, error) {
	var nodes []inventoryNode
	var skipped []string
	group := ""
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			group = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		alias := fields[0]
		kv := make(map[string]string)
		for _, f := range fields[1:] {
			if k, v, ok := strings.Cut(f, "="); ok {
				kv[k] = strings.Trim(v, "\"'")
			}
		}
		host := kv["ansible_host"]
		user := kv["ansible_user"]
		key := kv["ansible_ssh_private_key_file"]
		workerID := kv["worker_id"]
		if workerID == "" || host == "" {
			skipped = append(skipped, fmt.Sprintf("%s (group=%s): missing worker_id or ansible_host", alias, group))
			continue
		}
		nodes = append(nodes, inventoryNode{
			Alias:    alias,
			Host:     host,
			User:     user,
			KeyPath:  key,
			WorkerID: workerID,
			Group:    group,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return nodes, skipped, nil
}

func runSyncWorkerNodes(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("sync-worker-nodes", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	inventoryPath := fs.String("inventory", "", "legacy Ansible inventory file (deploy/ansible/inventory.ini)")
	dryRun := fs.Bool("dry-run", false, "parse and report without writing")
	secretRef := fs.String("secret-ref", "", "secret_ref value stamped on imported rows (leave empty to keep existing)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" || strings.TrimSpace(*inventoryPath) == "" {
		return fmt.Errorf("--db and --inventory are required")
	}

	raw, err := os.Open(*inventoryPath)
	if err != nil {
		return fmt.Errorf("open inventory: %w", err)
	}
	defer raw.Close()
	nodes, skipped, err := parseLegacyInventory(raw)
	if err != nil {
		return fmt.Errorf("parse inventory: %w", err)
	}
	for _, s := range skipped {
		fmt.Fprintf(stderr, "skip: %s\n", s)
	}

	db, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	upserted := 0
	for _, n := range nodes {
		existing, _ := db.GetAnsibleHost(n.Host)
		secret := *secretRef
		if secret == "" && existing != nil {
			secret = existing.SecretRef
		}
		env := n.Group
		if env == "" {
			env = "production"
		}
		fields := store.AnsibleHostFields{
			Host:           n.Host,
			AnsibleUser:    n.User,
			SSHKeyPath:     n.KeyPath,
			SecretRef:      secret,
			Enabled:        true,
			Group:          n.Group,
			LinkedWorkerID: n.Alias,
			WorkerID:       n.WorkerID,
		}
		if existing != nil {
			// Preserve operator-side fields that the INI cannot express.
			fields.Availability = existing.Availability
			fields.Subgroup = existing.Subgroup
			fields.Tags = existing.Tags
			fields.Notes = existing.Notes
			// Operator-intent wins on Enabled: an explicitly-disabled node
			// stays disabled after a re-seed (the INI only proves the node
			// exists, not that it is schedulable). A node that was enabled
			// stays enabled.
			if !existing.Enabled {
				fields.Enabled = false
			}
		}
		if !*dryRun {
			if err := db.UpsertAnsibleHost(fields); err != nil {
				return fmt.Errorf("upsert host %s: %w", n.Host, err)
			}
		}
		upserted++
		fmt.Fprintf(stdout, "%s host=%s worker_id=%s user=%s group=%s env=%s\n",
			map[bool]string{true: "would-upsert", false: "upserted"}[*dryRun],
			n.Host, n.WorkerID, n.User, n.Group, env)
	}
	fmt.Fprintf(stdout, "summary: %d worker nodes (%s), %d skipped\n",
		upserted, map[bool]string{true: "dry-run", false: "synced"}[*dryRun], len(skipped))
	return nil
}
