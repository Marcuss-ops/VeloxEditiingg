// Package api — Fleet-operator SSH connectivity diagnostic endpoint.
//
// Surface:
//
//	GET /api/v1/admin/workers/ssh-check
//	  → {checked_at, key_file, known_hosts_file, workers:[...], summary}
//
// Auth: mounted under the existing adminWorkers adminAuth-gated
// group (adminAuth middleware from VELOX_ADMIN_TOKEN). Same gating
// contract as the other /api/v1/admin/workers endpoints.
//
// Purpose: a single operator surface that answers, for every worker in
// the canonical WorkerNodeRegistry, the three questions the master's
// SSH executors actually depend on:
//
//	ssh     — reachable + key authenticates
//	hostkey — present in the centralized /etc/velox/ssh/known_hosts
//	sudo    — passwordless `sudo -n true`
//
// The check is a PURE diagnostic: it shells out to the same hardened
// `ssh` invocation the production executors use (identical key,
// identical known_hosts, BatchMode, StrictHostKeyChecking=yes) but
// NEVER mutates the worker. Source of truth for host/port/user is the
// fleet.WorkerRegistry (persistent ansible_hosts view) — there is no
// second inventory file here.
//
// Failure modes (mirrors the other admin handlers):
//
//	reg == nil   → 503 Service Unavailable
//	otherwise    → 200 with one row per registered worker
//	               (FAIL/SKIP rows carry a short `detail`)
package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
)

// SSHCheckDeps carries the endpoint's optional overrides. keyPath and
// knownHosts default to the canonical /etc/velox/ssh paths (see
// fleet.DefaultSSHKeyPath / fleet.DefaultKnownHostsPath) when empty.
type SSHCheckDeps struct {
	Reg        *fleet.WorkerRegistry
	KeyPath    string
	KnownHosts string
}

// AdminWorkersSSHCheckHandler exposes GET /api/v1/admin/workers/ssh-check.
type AdminWorkersSSHCheckHandler struct {
	reg        *fleet.WorkerRegistry
	keyPath    string
	knownHosts string
}

// NewAdminWorkersSSHCheckHandler wires the SSH diagnostic to the
// canonical WorkerNodeRegistry.
func NewAdminWorkersSSHCheckHandler(reg *fleet.WorkerRegistry, deps SSHCheckDeps) *AdminWorkersSSHCheckHandler {
	kp := deps.KeyPath
	if kp == "" {
		kp = fleet.DefaultSSHKeyPath
	}
	kh := deps.KnownHosts
	if kh == "" {
		kh = fleet.DefaultKnownHostsPath
	}
	return &AdminWorkersSSHCheckHandler{reg: reg, keyPath: kp, knownHosts: kh}
}

// SSHCheckResponse is the JSON envelope returned by the endpoint.
type SSHCheckResponse struct {
	CheckedAt  string                  `json:"checked_at"`
	KeyFile    string                  `json:"key_file"`
	KnownHosts string                  `json:"known_hosts_file"`
	Workers    []fleet.WorkerSSHStatus `json:"workers"`
	Summary    SSHCheckSummary         `json:"summary"`
}

// SSHCheckSummary aggregates the PASS/READY verdicts across the fleet.
type SSHCheckSummary struct {
	Total    int `json:"total"`
	SSHPass  int `json:"ssh_pass"`
	KeyPass  int `json:"key_pass"`
	SudoPass int `json:"sudo_pass"`
	Ready    int `json:"ready"`
}

// RunSSHCheck returns GET /api/v1/admin/workers/ssh-check.
func (h *AdminWorkersSSHCheckHandler) RunSSHCheck() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		workers := fleet.SSHConnectivityCheck(c.Request.Context(), h.reg, h.keyPath, h.knownHosts)
		sum := SSHCheckSummary{Total: len(workers)}
		for _, w := range workers {
			if w.SSH == "PASS" {
				sum.SSHPass++
			}
			if w.HostKey == "PASS" {
				sum.KeyPass++
			}
			if w.Sudo == "PASS" {
				sum.SudoPass++
			}
			if w.Ready() {
				sum.Ready++
			}
		}
		c.JSON(http.StatusOK, SSHCheckResponse{
			Workers:    workers,
			KeyFile:    h.keyPath,
			KnownHosts: h.knownHosts,
			CheckedAt:  time.Now().UTC().Format(time.RFC3339),
			Summary:    sum,
		})
	}
}
