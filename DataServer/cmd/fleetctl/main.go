// Package main — fleetctl is the unified operator CLI for Step 15/15.
//
// fleetctl is a SINGLE Go binary installed on the Master (see
// Q7 of the design review: /opt/velox/bin/fleetctl + a
// /usr/local/bin/fleetctl symlink). It wraps the Master REST API
// primitives Steps 1/15, 6/15, 10/15, 12/15 + 13/15 ship and is
// the operator's first surface for fleet-wide mutations
// (drain/smoke/update/resume/rollback) and reads
// (status/inspect).
//
// Sub-command set (matches the user spec verbatim — 7 listed):
//
//	status                       GET  /api/v1/admin/workers
//	inspect <worker_id>          GET  /api/v1/admin/workers/{id}
//	drain    <worker_id>         POST /api/v1/admin/workers/{id}/drain
//	update   <worker_id> [--digest sha256:...]   POST /api/v1/admin/workers/{id}/update
//	smoke    <worker_id>         POST /api/v1/admin/workers/{id}/smoke
//	resume   <worker_id>         POST /api/v1/admin/workers/{id}/resume
//	rollback <worker_id>         POST /api/v1/admin/workers/{id}/rollback
//
// Sub-commands 7-only is INTENTIONAL — restart + logs are not in
// the user's literal spec. Host-side restart/log inspection remain
// outside this binary and are documented in the operator runbooks.
//
// CLI parser: stdlib `flag` per non-dep convention (Q2). Each
// sub-command is its own FlagSet; the first positional arg
// routes to the handler. Top-level -h / -v / --version are
// hidden sub-flag helpers.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

// subCommand is a stringer-friendly enum for routing. Strings
// are stable so scripts can rely on them in shell pipelines.
type subCommand string

const (
	subStatus   subCommand = "status"
	subInspect  subCommand = "inspect"
	subDrain    subCommand = "drain"
	subUpdate   subCommand = "update"
	subSmoke    subCommand = "smoke"
	subResume   subCommand = "resume"
	subRollback subCommand = "rollback"
	subSSHCheck subCommand = "ssh-check"
	subJob      subCommand = "job"
	subDoctor   subCommand = "doctor"
)

func (s subCommand) valid() bool {
	switch s {
	case subStatus, subInspect, subDrain, subUpdate, subSmoke, subResume, subRollback, subSSHCheck, subJob, subDoctor:
		return true
	default:
		return false
	}
}

// usage is printed on -h / missing arg / unknown sub-command.
// Kept short — per-sub-command examples live in
// deploy/fleetctl/README.md, not in -h output, so the operator's
// local --help is fast (essential for a CLI invoked from
// production-recovery contexts where stdout is piped).
const usage = `fleetctl — Velox fleet-operator unified CLI (Step 15/15).

Usage:
  fleetctl -h
  fleetctl [--master=URL] [--token-file=PATH] <sub-command> [args]

Sub-commands:
  status                  list all workers + WorkerCard snapshot
  inspect <worker_id>     one worker detailed WorkerCard
  drain    <worker_id>    DRAINING transition (waits for active_jobs=0)
  update   <worker_id> [--digest sha256:...]   image update cascade
  smoke    <worker_id>    on-demand Level-D smoke (await terminal state)
  resume   <worker_id>    RESUME (RESUMED after drain/quarantine)
  rollback <worker_id>    rollback to previous_digest (Step 9/15 cascade)
  ssh-check               per-worker SSH connectivity (ssh/hostkey/sudo -n)
  job inspect <job_id>    complete job diagnostics (metrics/cache/artifact/delivery)
  job metrics <job_id>    execution and cache metrics for one job
  job watch <job_id>      follow the persisted job event timeline
  doctor --production     fleet/readiness/digest production checks

Auth (in precedence order):
  1. --token-file=PATH    chmod-600 file holding the bare token
  2. $VELOX_ADMIN_TOKEN   env var
  3. /opt/velox/secrets/admin-token    canonical Master path

See deploy/fleetctl/README.md for per-sub-command examples
+ exit-code matrix.
`

// runMain is the testable entry point. exit code per exit_codes.go.
func runMain(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return ExitMisuse
	}
	// Hidden pre-routing flags.
	fsGlobal := flag.NewFlagSet("fleetctl-globals", flag.ContinueOnError)
	var showVersion bool
	fsGlobal.BoolVar(&showVersion, "version", false, "print fleetctl version + commit hash (debug)")
	fsGlobal.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	if err := fsGlobal.Parse(args); err != nil {
		return ExitMisuse
	}
	if showVersion {
		fmt.Println("fleetctl v1.0.0 (Step 15/15 fleet-operator rollout)")
		return ExitOK
	}
	rest := fsGlobal.Args()
	if len(rest) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return ExitMisuse
	}
	sub := subCommand(rest[0])
	if !sub.valid() {
		fmt.Fprintf(os.Stderr, "fleetctl: unknown sub-command %q\n\n%s\n", rest[0], usage)
		return ExitMisuse
	}

	// Resolve auth + master URL once (shared across all sub-commands).
	cfg, err := loadClientConfig(rest[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl: %v\n", err)
		return ExitMisuse
	}
	client, err := newFleetClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl: %v\n", err)
		return ExitMisuse
	}

	// Route to per-sub-command handler. Each handler returns its
	// own exit code via the ExitXxx constants; handlers do not
	// os.Exit(exit) directly so tests can assert the integer.
	switch sub {
	case subStatus:
		production := false
		for _, arg := range rest[1:] {
			if arg == "--production" {
				production = true
			}
		}
		return runStatusMode(client, production)
	case subInspect:
		return runInspect(client, rest[1:])
	case subDrain:
		return runDrain(client, rest[1:])
	case subUpdate:
		return runUpdate(client, rest[1:])
	case subSmoke:
		return runSmoke(client, rest[1:])
	case subResume:
		return runResume(client, rest[1:])
	case subRollback:
		return runRollback(client, rest[1:])
	case subSSHCheck:
		return runSSHCheck(client)
	case subJob:
		return runJob(client, rest[1:])
	case subDoctor:
		return runDoctor(client, rest[1:])
	}
	return ExitUnexpected
}

func main() {
	os.Exit(runMain(os.Args[1:]))
}

// Sentinel guard so future routings fail loud at compile time
// if a sub-command enum value is added without a handler.
var _ = errors.New
