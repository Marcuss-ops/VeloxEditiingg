// Command velox-migrate-json imports legacy JSON data into SQLite.
//
// Usage:
//
//	velox-migrate-json inventory --data-dir .velox/data
//	velox-migrate-json dry-run --data-dir .velox/data --db .velox/data/velox.db
//	velox-migrate-json apply  --data-dir .velox/data --db .velox/data/velox.db
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"velox-server/internal/store"
)

func main() {
	var (
		dataDir string
		dbPath  string
	)

	cmd := ""
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.StringVar(&dataDir, "data-dir", "", "Path to data directory")
	fs.StringVar(&dbPath, "db", "", "Path to SQLite database (default: <data-dir>/velox.db)")

	switch cmd {
	case "inventory":
		fs.Parse(os.Args[2:])
		runInventory(dataDir)
	case "dry-run":
		fs.Parse(os.Args[2:])
		runDryRun(dataDir, dbPath)
	case "apply":
		fs.Parse(os.Args[2:])
		runApply(dataDir, dbPath)
	case "":
		fmt.Fprintf(os.Stderr, "Usage: velox-migrate-json <inventory|dry-run|apply> [flags]\n")
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		fmt.Fprintf(os.Stderr, "  inventory  List all legacy JSON files and their status\n")
		fmt.Fprintf(os.Stderr, "  dry-run    Validate imports without writing\n")
		fmt.Fprintf(os.Stderr, "  apply      Import JSON data into SQLite\n")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

// jsonFile describes a legacy JSON file.
type jsonFile struct {
	Name   string
	Path   string
	Domain string
	Exists bool
	Size   int64
	SHA256 string
}

// knownJSONFiles returns the list of known legacy JSON files for inventory/dry-run.
func knownJSONFiles(dataDir string) []jsonFile {
	entries := []struct {
		name, path, domain string
	}{
		{name: "Workers", path: "workers.json", domain: "workers"},
		{name: "YouTube Channels", path: "youtube/channels/channels.json", domain: "youtube_channels"},
		{name: "YouTube Groups", path: "youtube/groups.json", domain: "youtube_groups"},
		{name: "YouTube Manager", path: "youtube/GroupYoutubeManager/ChannelsSaved.json", domain: "youtube_manager"},
		{name: "Ansible Computers", path: "ansible/ansible_computers.json", domain: "ansible_hosts"},
		{name: "Ansible Runs", path: "ansible_runs.json", domain: "ansible_runs"},
		{name: "Analytics Cache", path: "analytics/analytics_cache.json", domain: "analytics_cache"},
		{name: "YouTube API Cache", path: "analytics/youtube_api_cache.json", domain: "youtube_cache"},
	}

	var files []jsonFile
	for _, e := range entries {
		f := jsonFile{Name: e.name, Path: e.path, Domain: e.domain}
		fullPath := filepath.Join(dataDir, e.path)
		info, err := os.Stat(fullPath)
		if err == nil {
			f.Exists = true
			f.Size = info.Size()
			if data, err := os.ReadFile(fullPath); err == nil {
				hash := sha256.Sum256(data)
				f.SHA256 = fmt.Sprintf("%x", hash)
			}
		}
		files = append(files, f)
	}
	return files
}

// inventory lists all legacy JSON files.
func runInventory(dataDir string) {
	if dataDir == "" {
		dataDir = "."
	}

	files := knownJSONFiles(dataDir)

	fmt.Println("Legacy JSON Inventory")
	fmt.Println("=====================")
	fmt.Printf("%-40s %-12s %-10s %-64s\n", "File", "Size", "Exists", "SHA256")
	fmt.Println(strings.Repeat("-", 130))

	for _, f := range files {
		existsStr := "NO"
		sizeStr := "-"
		shaStr := "-"
		if f.Exists {
			existsStr = "YES"
			sizeStr = fmt.Sprintf("%d", f.Size)
			shaStr = f.SHA256[:16] + "..."
		}
		fmt.Printf("%-40s %-12s %-10s %-64s\n", f.Path, sizeStr, existsStr, shaStr)
	}
}

// dry-run validates what would be imported without writing.
func runDryRun(dataDir, dbPath string) {
	if dataDir == "" {
		dataDir = "."
	}
	if dbPath == "" {
		dbPath = filepath.Join(dataDir, "velox.db")
	}

	files := knownJSONFiles(dataDir)
	result := make(map[string]any)

	for _, f := range files {
		if !f.Exists {
			continue
		}
		fullPath := filepath.Join(dataDir, f.Path)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			log.Printf("[WARN] Cannot read %s: %v", f.Path, err)
			continue
		}
		// Count top-level keys as records
		var m map[string]any
		recordCount := 0
		if err := json.Unmarshal(data, &m); err == nil {
			recordCount = len(m)
		} else {
			var a []any
			if err := json.Unmarshal(data, &a); err == nil {
				recordCount = len(a)
			}
		}

		result[f.Domain] = map[string]any{
			"source":   f.Path,
			"exists":   true,
			"sha256":   f.SHA256[:16],
			"records":  recordCount,
			"imported": 0,
		}
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(output))
}

// apply imports JSON data into SQLite, delegating to the common ImportLegacyJSON pipeline.
func runApply(dataDir, dbPath string) {
	if dataDir == "" {
		dataDir = "."
	}
	if dbPath == "" {
		dbPath = filepath.Join(dataDir, "velox.db")
	}

	s, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer s.Close()

	results, err := s.ImportLegacyJSON(dataDir)
	if err != nil {
		log.Printf("[WARN] Import completed with errors: %v", err)
	}

	output := make(map[string]any)
	for _, r := range results {
		key := r.Source.Domain
		if key == "" {
			key = r.Source.Name
		}
		output[key] = map[string]any{
			"source":   r.Source.Path,
			"status":   r.Status,
			"sha256":   r.SHA256,
			"records":  r.Records,
			"imported": r.Imported,
			"backup":   r.Backup,
			"error":    r.Error,
		}
	}

	resultJSON, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(resultJSON))
}
