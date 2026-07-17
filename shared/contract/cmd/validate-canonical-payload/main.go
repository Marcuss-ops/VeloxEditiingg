// Package main — validate-canonical-payload is the Step 8/8 closure
// semantic validator. It walks operator-submitted job fixtures
// (ops/jobs/*.json) plus any extra paths passed on the command line,
// parses each into a map[string]interface{}, and drives
// contract.ValidatePayload over the result.
//
// Why ValidatePayload (and not StrictValidatePayload)?
//
//   - ValidatePayload rejects the 5 forbidden legacy aliases
//     (id/run_id/title/voiceover_path/audio_path) and the typed-shape
//     anomalies on the canonical key set. This is the same gate the
//     master enqueue preflight invokes at runtime, so the validator
//     answers the operator-facing question "will this fixture survive
//     the master's canonical-purity preflight?".
//   - StrictValidatePayload additionally rejects unknown drift keys
//     (e.g. skip_creator, fit, reference_voiceovers). These drift keys
//     are operator-helper fields that the master's enqueue builders
//     tolerate (and the corresponding NewJobPayloadV2 reader pattern
//     folds them into canonical form). Forcing them to be strict-
//     canonical would break the operator workflow that submits fixtures
//     like ops/jobs/jackie_chan_doc_voiceover.generate-from-clips.json.
//   - When the caller passes --strict, the validator switches to
//     StrictValidatePayload to surface drift keys for fixtures that
//     ARE expected to be already-strict (e.g. CI fixtures, future
//     canonical-only operator surfaces).
//
// The validator is the DATA-side counterpart to the SOURCE-side gate
// scripts/ci/check-payload-canonical-form.sh (which grep-fails the
// writer source). Together they form the two halves of the Step 7+8/8
// canonical-purity closure:
//
//	Source side  → check-payload-canonical-form.sh  (greps writer .go files)
//	Data side    → validate-canonical-payload     (runs over .json fixtures)
//
// Run:  go run ./shared/contract/cmd/validate-canonical-payload [--strict] <repo-root> [extra.json ...]
// Exit: 0 all fixtures pass, 1 any fixture fails, 2 usage error.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"velox-shared/contract"
)

func main() {
	strict := flag.Bool("strict", false, "use StrictValidatePayload (rejects drift keys in addition to legacy aliases)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [--strict] <repo-root> [extra-fixture.json ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(2)
	}
	repoRoot := args[0]
	extra := args[1:]

	// Default fixture set: every JSON file under ops/jobs/.
	fixtures, err := discoverFixtures(repoRoot, extra)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: discover fixtures: %v\n", err)
		os.Exit(1)
	}
	if len(fixtures) == 0 {
		fmt.Fprintf(os.Stderr, "FAIL: no fixtures discovered under %s/ops/jobs (and no extra paths given)\n", repoRoot)
		os.Exit(1)
	}

	sort.Strings(fixtures)

	mode := "ValidatePayload (tolerates drift keys; rejects 5 legacy aliases)"
	if *strict {
		mode = "StrictValidatePayload (rejects drift keys + 5 legacy aliases)"
	}
	fmt.Printf("[validate-canonical-payload] mode=%s\n", mode)
	fmt.Printf("[validate-canonical-payload] scanning %d fixture(s)\n", len(fixtures))

	failures := 0
	for _, f := range fixtures {
		ok, errMsg := validateOne(f, *strict)
		if ok {
			fmt.Printf("  PASS  %s\n", f)
		} else {
			fmt.Printf("  FAIL  %s\n    %s\n", f, errMsg)
			failures++
		}
	}

	if failures > 0 {
		fmt.Fprintf(os.Stderr, "\n[validate-canonical-payload] FAIL: %d/%d fixture(s) failed canonical-purity check\n", failures, len(fixtures))
		fmt.Fprintf(os.Stderr, "  See shared/contract/canonical_payload.go::LegacyAliasKeys for the binding denylist.\n")
		fmt.Fprintf(os.Stderr, "  Run with --strict to additionally catch drift keys.\n")
		os.Exit(1)
	}
	fmt.Printf("[validate-canonical-payload] OK — all %d fixture(s) pass canonical-purity check\n", len(fixtures))
}

// discoverFixtures returns the union of (a) every *.json file directly
// under <repoRoot>/ops/jobs, and (b) every extra path passed on the
// command line. The extra paths may be files or directories (recursive
// scan over *.json) and may be absolute or relative to repoRoot.
func discoverFixtures(repoRoot string, extra []string) ([]string, error) {
	seen := make(map[string]bool)
	var out []string

	// Default surface: ops/jobs/*.json (operator-submitted job fixtures).
	opsJobs := filepath.Join(repoRoot, "ops", "jobs")
	entries, err := os.ReadDir(opsJobs)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", opsJobs, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		full := filepath.Join(opsJobs, e.Name())
		if !seen[full] {
			seen[full] = true
			out = append(out, full)
		}
	}

	// Extra paths: resolve relative paths against repoRoot.
	for _, raw := range extra {
		path := raw
		if !filepath.IsAbs(path) {
			path = filepath.Join(repoRoot, raw)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat extra path %s: %w", path, err)
		}
		if info.IsDir() {
			err := filepath.Walk(path, func(p string, fi os.FileInfo, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if fi.IsDir() {
					return nil
				}
				if filepath.Ext(p) != ".json" {
					return nil
				}
				if !seen[p] {
					seen[p] = true
					out = append(out, p)
				}
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("walk extra dir %s: %w", path, err)
			}
		} else {
			if filepath.Ext(path) != ".json" {
				return nil, fmt.Errorf("extra path %s is not a .json file", path)
			}
			if !seen[path] {
				seen[path] = true
				out = append(out, path)
			}
		}
	}

	return out, nil
}

// validateOne parses the JSON file and drives the chosen validator
// over the resulting map. Returns (true, "") on success, (false, msg)
// on any validation error or parse failure.
func validateOne(path string, strict bool) (bool, string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Sprintf("read failed: %v", err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return false, fmt.Sprintf("parse failed: %v", err)
	}
	if payload == nil {
		return false, "top-level value is null (expected a JSON object)"
	}
	var verr error
	if strict {
		verr = contract.StrictValidatePayload(payload)
	} else {
		verr = contract.ValidatePayload(payload)
	}
	if verr != nil {
		return false, verr.Error()
	}
	return true, ""
}
