package main

// velox-benchmark-compare produces the §22 BASELINE/CANDIDATE/DELTA
// report between two BenchmarkRun JSON files (output of
// velox-benchmark). Only same-fixture, same-cache-mode runs are
// comparable.
//
// Usage:
//
//	velox-benchmark-compare -base base.json -candidate candidate.json
//	                         [-json] [-fail-on-regression]
//
//	-base/-candidate   BenchmarkRun JSON files
//	-json              print the machine-readable comparison instead of
//	                   the human table
//	-fail-on-regression  exit 1 when any KPI regressed (dedicated
//	                   benchmark worker gate; do NOT use on shared CI
//	                   runners — plan §17)
//
// Exit codes:
//	0 comparable and (with -fail-on-regression) no regression
//	1 incomparable inputs, parse error, or regression under the flag
//	2 usage error

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"velox-worker-agent/pkg/performance"
)

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[velox-benchmark-compare][FAIL] "+format+"\n", args...)
	os.Exit(1)
}

func failUsage(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[velox-benchmark-compare][ERROR] "+format+"\n", args...)
	os.Exit(2)
}

func main() {
	basePath := flag.String("base", "", "baseline BenchmarkRun JSON")
	candidatePath := flag.String("candidate", "", "candidate BenchmarkRun JSON")
	asJSON := flag.Bool("json", false, "print machine-readable JSON")
	failOnRegression := flag.Bool("fail-on-regression", false, "exit 1 when any KPI regressed")
	flag.Parse()
	if *basePath == "" || *candidatePath == "" {
		failUsage("-base and -candidate are required")
	}
	if flag.NArg() > 0 {
		failUsage("unexpected arguments: %v", flag.Args())
	}

	base, err := loadRun(*basePath)
	if err != nil {
		fail("baseline: %v", err)
	}
	candidate, err := loadRun(*candidatePath)
	if err != nil {
		fail("candidate: %v", err)
	}

	cmp, err := performance.CompareBenchmarkRuns(base, candidate)
	if err != nil {
		fail("%v", err)
	}

	if *asJSON {
		data, err := json.MarshalIndent(cmp, "", "  ")
		if err != nil {
			fail("marshal comparison: %v", err)
		}
		fmt.Println(string(data))
	} else {
		fmt.Print(cmp.FormatTable())
	}

	if *failOnRegression && cmp.AnyRegression {
		os.Exit(1)
	}
}

func loadRun(path string) (*performance.BenchmarkRun, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var r performance.BenchmarkRun
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &r, nil
}
