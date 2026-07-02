// Binary recover_output: velox-worker recover-output.
//
// Phase 6.4 of the Artifact Commit Protocol. The
// `velox-worker recover-output` CLI is the administrative escape
// hatch for an MP4 that was rendered to disk but the worker
// crashed BEFORE declaring it. The CLI runs on the master host
// (or any host with read access to the master's SQLite DB + the
// local MP4) and drives the canonical pipeline:
//
//  1. Hashes + sizes the MP4 from --file;
//  2. Opens the master's SQLite DB at --db;
//  3. Builds the in-process completion.Coordinator against that DB;
//  4. Calls DeclareOutputs (the master INSERT-OR-IGNOREs the
//     attempt_commits + per-declaration rows idempotently);
//  5. Registers the recovered artifact in artifact_uploads
//     (recovery-path-only: pre-pipeline setup so CompleteUpload
//     can advance the row);
//  6. Calls CompleteUpload (the master STAGING→READY + bumps
//     ready_output_count via deterministic derived count);
//  7. Calls CommitAttempt (the master ratifies the attempt as
//     SUCCEEDED, flips tasks + jobs, and idempotently INSERTs
//     job_deliveries rows so the DeliveryRunner picks up the
//     Drive upload on its next tick).
//
// The CLI is intentionally Coordinator-only for steps 4, 6, 7.
// Step 5 (artifact_uploads registration) is a recovery-path-only
// setup that the normal pipeline creates via a separate
// BeginUpload call (out of scope for the recovery flow). The CLI
// does NOT:
//   - push to Drive (the DeliveryRunner does that via
//     job_deliveries.claim — see internal/deliveries/runner.go);
//   - write to artifacts (the coordinator's CompleteUpload does
//     that in the same tx);
//   - touch job_deliveries directly (the coordinator does that
//     in step 5 of CommitAttempt's tx, idempotently).
//
// A re-run with the same (task_id, attempt_id) is a no-op on the
// database side (INSERT-OR-IGNORE on attempt_commits; CAS-gated
// EXPIRED-or-COMMITTED on CompleteUpload; COMMITTED-noop on
// CommitAttempt). Operators can re-run the CLI as many times as
// needed; the recovery is monotonic.
//
// CLI flags:
//
//	--task-id     required, the canonical task_id
//	--attempt-id  required, the canonical attempt_id
//	--worker-id   required, the worker_id to stamp on the fence
//	--lease-id    required, the lease_id to stamp on the fence
//	--job-id      required, the canonical job_id
//	--file        required, path to the local MP4
//	--db          required, path to the master's SQLite file
//	--logical-name  optional, default "out.mp4"
//	--output-kind   optional, default "final_video"

// ─────────────────────────────────────────────────────────────────────
// TRIAGE FINDING (2026-07-02, audit-driven disposition)
//
// Prior categorization was (c) "active architectural violation" because
// the file lives in cmd/worker/ — a directory named like a worker-process
// bootstrap. Three db.ExecContext hits landed the file in the cross-
// package audit's offender list.
//
// The file's own docstring fully contradicts the (c) classification:
//
//	"the administrative escape hatch for an MP4 that was rendered to
//	 disk but the worker crashed BEFORE declaring it... runs on the
//	 master host (or any host with read access to the master's
//	 SQLite DB + the local MP4)"
//
// This is a MASTER-SIDE admin CLI, NOT a worker-process bootstrap.
// The `cmd/worker/` path-name is historical artifact predating the
// planned gRPC recovery admin endpoint (Phase 6.4+ follow-up).
//
// Disposition: case (b) — recovery path that legitimately needs
// server-side tx. The 2 INSERTs in the original
// registerRecoveredArtifact helper (now removed) move to
// internal/store/artifact_recovery.go (`store.RegisterRecoveryUploadSession`).
// The CLI retains its local `*sql.DB` open because opening a
// connection is an admin tool's bootstrap concern; only the SQL
// itself moves to the typed store/ helper.
//
// newUUIDLowerHexRecover is removed: the typed helper requires the
// caller to derive IDs deterministically from a stable commit_id so
// INSERT OR IGNORE absorbs re-runs of the CLI on the same attempt.
// A fresh UUID per call would break that contract.
// ─────────────────────────────────────────────────────────────────────
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"velox-server/internal/completion"
	"velox-server/internal/store"
)

// cliOptions captures the parsed CLI flags. All fields are
// validated in parseOptions; an empty / missing required field
// surfaces as a flag.Usage + non-zero exit before any DB I/O.
type cliOptions struct {
	TaskID      string
	AttemptID   string
	WorkerID    string
	LeaseID     string
	JobID       string
	File        string
	DBPath      string
	HMACKey     string
	LogicalName string
	OutputKind  string
}

func parseOptions(args []string) (*cliOptions, error) {
	fs := flag.NewFlagSet("velox-worker recover-output", flag.ContinueOnError)
	opts := &cliOptions{}
	fs.StringVar(&opts.TaskID, "task-id", "", "canonical task_id (required)")
	fs.StringVar(&opts.AttemptID, "attempt-id", "", "canonical attempt_id (required)")
	fs.StringVar(&opts.WorkerID, "worker-id", "", "worker_id stamped on the fence (required)")
	fs.StringVar(&opts.LeaseID, "lease-id", "", "lease_id stamped on the fence (required)")
	fs.StringVar(&opts.JobID, "job-id", "", "canonical job_id (required)")
	fs.StringVar(&opts.File, "file", "", "path to the local MP4 (required)")
	fs.StringVar(&opts.DBPath, "db", "", "path to the master's SQLite file (required)")
	// --hmac-key takes precedence; if absent we fall back to the
	// master-side VELOX_COMMIT_HMAC_KEY env var, then to an
	// operator-provided stdin prompt as a last resort. The key is
	// expected as hex (64 chars = 32 raw bytes); non-hex input is
	// rejected with a usage error.
	fs.StringVar(&opts.HMACKey, "hmac-key", "", "master VELOX_COMMIT_HMAC_KEY (hex, required)")
	fs.StringVar(&opts.LogicalName, "logical-name", "", "logical name of the output (default: out.mp4)")
	fs.StringVar(&opts.OutputKind, "output-kind", "", "output kind (default: final_video)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	missing := []string{}
	for k, v := range map[string]string{
		"--task-id":    opts.TaskID,
		"--attempt-id": opts.AttemptID,
		"--worker-id":  opts.WorkerID,
		"--lease-id":   opts.LeaseID,
		"--job-id":     opts.JobID,
		"--file":       opts.File,
		"--db":         opts.DBPath,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	// Fall-back: VELOX_COMMIT_HMAC_KEY env if --hmac-key missing.
	if opts.HMACKey == "" {
		opts.HMACKey = strings.TrimSpace(os.Getenv("VELOX_COMMIT_HMAC_KEY"))
	}
	if opts.HMACKey == "" {
		return nil, fmt.Errorf("--hmac-key (or VELOX_COMMIT_HMAC_KEY env) required (must decode to >= 32 raw bytes)")
	}
	rawKey, kerr := hex.DecodeString(opts.HMACKey)
	if kerr != nil || len(rawKey) < 32 {
		return nil, fmt.Errorf("--hmac-key must be hex of >= 32 raw bytes (got %d raw bytes after decode)", len(rawKey))
	}
	opts.HMACKey = string(rawKey) // raw bytes; hex-decoded
	return opts, nil
}

// recoverOutput drives the in-process Coordinator against the
// supplied SQLite file. Returns process exit code 0 on success.
//
// The fence is reconstructed from the supplied (worker_id,
// lease_id, revision=1). If the canonical attempt_commits row
// already exists, the master reuses its commit_id and the
// pipeline advances idempotently.
func recoverOutput(ctx context.Context, opts *cliOptions) (int, error) {
	// 1. Hash + size the file BEFORE any DB I/O. The master
	// will compare against the worker's hash; if the file
	// shifted between hash and CompleteUpload, the CAS rejects
	// the second hash and we surface the mismatch.
	size, sha, err := hashFile(opts.File)
	if err != nil {
		return 1, fmt.Errorf("recover_output: hash %s: %w", opts.File, err)
	}
	log.Printf("[RECOVER] file=%s size=%d sha256=%s", opts.File, size, sha)

	// 2 + 3. Open the master's DB through the canonical typed
	// store factory. store.NewSQLiteStore applies the same
	// WAL+busy_timeout+FK pragmas the master uses (see
	// internal/store/sqlite.go). The CLI does NOT register the
	// sqlite3 driver or run additional pragmas — those are
	// encapsulated in the typed store/open primitive.
	sqliteStore, err := store.NewSQLiteStore(opts.DBPath)
	if err != nil {
		return 1, fmt.Errorf("recover_output: open db: %w", err)
	}
	defer sqliteStore.Close()

	// Build the in-process Coordinator. This is the SAME type the
	// master uses; the recovery path uses exactly the same code
	// that the happy path uses. The HMAC key is the master-side
	// VELOX_COMMIT_HMAC_KEY (passed via --hmac-key or the env
	// var); without it DeclareOutputs cannot derive a
	// commit_token, and the master will refuse to start (Verdetto
	// P0 #6).
	coord, err := completion.NewCoordinator(completion.CoordinatorConfig{
		DB:      sqliteStore.DB(),
		HMACKey: []byte(opts.HMACKey),
	})
	if err != nil {
		return 1, fmt.Errorf("recover_output: build coordinator: %w", err)
	}

	fence := completion.FenceTuple{
		TaskID:    opts.TaskID,
		AttemptID: opts.AttemptID,
		WorkerID:  opts.WorkerID,
		LeaseID:   opts.LeaseID,
		Revision:  1,
	}
	logicalName := opts.LogicalName
	if logicalName == "" {
		logicalName = "out.mp4"
	}
	outputKind := opts.OutputKind
	if outputKind == "" {
		outputKind = "final_video"
	}

	// 4. DeclareOutputs. Idempotent on (task_id, attempt_id).
	plan, err := coord.DeclareOutputs(ctx, completion.DeclareOutputsCommand{
		Fence: fence,
		JobID: opts.JobID,
		OutputManifests: []completion.OutputManifest{{
			OutputKind:     outputKind,
			LogicalName:    logicalName,
			MimeType:       "video/mp4",
			SizeBytes:      size,
			SHA256:         sha,
			WorkerSpoolKey: "spool-recover-" + opts.AttemptID,
		}},
	})
	if err != nil {
		return 1, fmt.Errorf("recover_output: DeclareOutputs: %w", err)
	}
	if plan == nil || plan.CommitID == "" {
		return 1, fmt.Errorf("recover_output: DeclareOutputs returned empty commit_id")
	}
	log.Printf("[RECOVER] DeclareOutputs commit_id=%s token_len=%d",
		plan.CommitID, len(plan.CommitToken))

	// 5. Recovery-path: register the artifact + artifact_uploads
	// rows so CompleteUpload's CAS has something to advance.
	// The normal pipeline creates these via a separate
	// BeginUpload gRPC call; the recovery flow does it inline
	// because the file is already at rest locally and we do
	// NOT need the master-stream chunked upload.
	// IDs derived deterministically from commit_id so the
	// INSERT OR IGNORE inside RegisterRecoveryUploadSession absorbs
	// re-runs of the CLI on the same (task_id, attempt_id). See
	// internal/store/artifact_recovery.go docstring for the
	// idempotency contract.
	uploadID := "recover-" + plan.CommitID
	artifactID := "art_recover_" + plan.CommitID
	if err := store.RegisterRecoveryUploadSession(ctx, sqliteStore.DB(), store.RecoveryUploadSession{
		UploadID:   uploadID,
		ArtifactID: artifactID,
		JobID:      opts.JobID,
		WorkerID:   opts.WorkerID,
		LeaseID:    opts.LeaseID,
		FilePath:   opts.File,
		SizeBytes:  size,
		SHA256:     sha,
	}); err != nil {
		return 1, fmt.Errorf("recover_output: register artifact: %w", err)
	}
	log.Printf("[RECOVER] artifact_uploads registered upload_id=%s artifact_id=%s", uploadID, artifactID)

	// 6. CompleteUpload. We pass the freshly-computed size +
	// SHA so the master's CAS advances artifact_uploads →
	// COMPLETED and artifacts STAGING → READY in the same tx.
	// ServerSHA256 must equal sha: this is the admin recovery path
	// and the file is already at rest on the master host. The CLI
	// just hashed the bytes via hashFile() so sha IS the master-
	// derived SHA. Omitting it would route the artifact through
	// Branch A/B (verifying) and stall the recovery (Verdetto P0 #5).
	if err := coord.CompleteUpload(ctx, completion.CompleteUploadCommand{
		Fence:             fence,
		UploadID:          uploadID,
		UploadedSizeBytes: size,
		WorkerSHA256:      sha,
		ServerSHA256:      sha,
	}); err != nil {
		return 1, fmt.Errorf("recover_output: CompleteUpload: %w", err)
	}
	log.Printf("[RECOVER] CompleteUpload OK commit_id=%s upload_id=%s", plan.CommitID, uploadID)

	// 6. CommitAttempt. The master ratifies the attempt as
	// SUCCEEDED, flips tasks + jobs, and idempotently INSERTs
	// job_deliveries rows. The DeliveryRunner picks up the
	// rows on the next tick and uploads to Drive natively.
	commitResp, err := coord.CommitAttempt(ctx, plan.CommitID)
	if err != nil {
		return 1, fmt.Errorf("recover_output: CommitAttempt: %w", err)
	}
	log.Printf("[RECOVER] CommitAttempt OK commit_id=%s task_status=%s job_status=%s",
		commitResp.CommitID, commitResp.TaskStatus, commitResp.JobStatus)

	// 7. Print the canonical evidence line so the operator
	// can paste it into the recovery ticket. The Drive upload
	// is async (the runner claims the new job_deliveries row
	// on its next tick), so we report the master's ratifying
	// state and the next-step hint.
	fmt.Printf("commit_id=%s task_id=%s attempt_id=%s size=%d sha256=%s task_status=%s job_status=%s\n",
		commitResp.CommitID, opts.TaskID, opts.AttemptID, size, sha,
		commitResp.TaskStatus, commitResp.JobStatus)
	fmt.Fprintf(os.Stderr, "next: DeliveryRunner will claim the new job_deliveries row on its next tick and push to Drive.\n")
	return 0, nil
}

// hashFile streams the file through SHA-256. The file is read in
// 1 MiB chunks so a multi-GB MP4 does NOT load entirely in
// memory. Returns size + lowercase hex SHA-256.
func hashFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20) // 1 MiB
	n, err := io.CopyBuffer(h, f, buf)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func main() {
	opts, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "recover_output: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	code, err := recoverOutput(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	os.Exit(code)
}
