package main

// bootstrap_prefetch.go wires the fleet layer: SSH clients, worker node
// registry, fleet executor registry, fleet operator handlers (admin
// mutations / health / smoke / metrics), and supervisor registration.
// Split out of bootstrap_composition.go so the orchestrator stays focused
// on dependency order.

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/fleet"
	"velox-server/internal/logging"
	"velox-server/internal/supervisor"
)

// wireFleetComposition builds the fleet dependency chain: worker node
// registry, SSH clients, fleet executor registry, and fleet operator
// handlers.  Returns the FleetDep and smoke capability status.
func wireFleetComposition(
	cfg *config.Config,
	p *persistenceDeps,
	w *workerDeps,
	m *moduleDeps,
	sup *supervisor.Supervisor,
) (*FleetDep, fleet.SmokeCapabilityStatus, error) {
	workerNodeRegistry, err := buildWorkerRegistryFromStore(p)
	if err != nil {
		return nil, fleet.SmokeCapabilityStatus{}, fmt.Errorf("bootstrap: worker node registry: %w", err)
	}

	sharedSSH := fleet.NewSSHClientFromRegistry(workerNodeRegistry)
	updateSSH := fleet.NewSecureSSHClient(
		workerNodeRegistry,
		fleet.DefaultSSHKeyPath,
		fleet.DefaultKnownHostsPath,
	)

	fleetDep, err := buildFleet(p, w.Registry, updateSSH)
	if err != nil {
		return nil, fleet.SmokeCapabilityStatus{}, fmt.Errorf("bootstrap: fleet: %w", err)
	}

	var smokeCapability fleet.SmokeCapabilityStatus
	if err := wireFleetOperatorHandlers(cfg, fleetDep, m, p, workerNodeRegistry, sharedSSH, &smokeCapability); err != nil {
		return nil, fleet.SmokeCapabilityStatus{}, fmt.Errorf("bootstrap: fleet operator wiring: %w", err)
	}

	if fleetDep != nil && fleetDep.Registry != nil {
		if err := fleetDep.Registry.ValidateRequiredExecutors(); err != nil {
			return nil, fleet.SmokeCapabilityStatus{}, fmt.Errorf("bootstrap: fleet executor registry: %w", err)
		}
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] Fleet executor registry validated: %s", strings.Join(fleetDep.Registry.Kinds(), ", "))
	}

	if fleetDep != nil {
		if err := registerFleetRunner(sup, fleetDep); err != nil {
			return nil, fleet.SmokeCapabilityStatus{}, fmt.Errorf("bootstrap: fleet supervisor: %w", err)
		}
		fleetDep.tickWiredAtBoot = true
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] FleetController wired and supervised (operation ledger tick enabled)")
	}

	return fleetDep, smokeCapability, nil
}
