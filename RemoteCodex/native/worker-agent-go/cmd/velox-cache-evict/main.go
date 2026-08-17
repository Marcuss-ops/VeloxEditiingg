// Command velox-cache-evict plans or performs selective worker asset-cache
// eviction. It is intentionally separate from the worker bootstrap command:
// cache administration must never start a worker or contact the Master.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"velox-worker-agent/internal/cacheevict"
	"velox-worker-agent/internal/workercache"
)

type assetIDsFlag []string

func (f *assetIDsFlag) String() string { return strings.Join(*f, ",") }

func (f *assetIDsFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func main() {
	var assetIDs assetIDsFlag
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	cacheRoot := flags.String("cache-root", "", "explicit worker asset-cache root to inspect")
	indexPath := flags.String("index", "", "path to the worker cache SQLite index (required)")
	execute := flags.Bool("execute", false, "actually remove the selected cache entries; default is dry-run")
	flags.Var(&assetIDs, "asset-id", "asset ID to select; repeat for each asset (required)")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s --cache-root ROOT --index PATH --asset-id ID [--asset-id ID ...] [--execute]\n\n", os.Args[0])
		fmt.Fprintln(flags.Output(), "Resolves asset IDs to their content-addressed blob path through the SQLite index. Plans removals by default; --execute removes the indexed files, refusing assets with active leases or reservations.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "error: positional arguments are not supported")
		os.Exit(2)
	}
	if strings.TrimSpace(*cacheRoot) == "" {
		fmt.Fprintln(os.Stderr, "error: --cache-root is required")
		os.Exit(2)
	}
	if strings.TrimSpace(*indexPath) == "" {
		fmt.Fprintln(os.Stderr, "error: --index is required")
		os.Exit(2)
	}
	info, err := os.Lstat(*indexPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: --index must reference an existing SQLite index: %v\n", err)
		os.Exit(2)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		fmt.Fprintln(os.Stderr, "error: --index must not be a symlink")
		os.Exit(2)
	}
	if info.IsDir() {
		fmt.Fprintln(os.Stderr, "error: --index must be a file")
		os.Exit(2)
	}

	index, err := workercache.Open(*indexPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: open cache index: %v\n", err)
		os.Exit(1)
	}
	defer index.Close()

	items, err := cacheevict.Run(context.Background(), cacheevict.Options{
		Root:     *cacheRoot,
		AssetIDs: assetIDs,
		Index:    index,
		Execute:  *execute,
	})
	if err != nil {
		if errors.Is(err, cacheevict.ErrNoAssetIDs) || errors.Is(err, cacheevict.ErrInvalidID) || errors.Is(err, cacheevict.ErrUnsafeRoot) {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(struct {
		Mode  string            `json:"mode"`
		Items []cacheevict.Item `json:"items"`
	}{
		Mode:  map[bool]string{true: "execute", false: "dry_run"}[*execute],
		Items: items,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "error: write report: %v\n", err)
		os.Exit(1)
	}
}
