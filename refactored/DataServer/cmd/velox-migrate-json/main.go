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
	"time"

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

// JSONSource describes a legacy JSON file that can be imported.
type JSONSource struct {
	Name    string
	Path    string
	Domain  string
	Keypath string // natural key path for idempotency
	Exists  bool
	Size    int64
	SHA256  string
}

// inventory lists all legacy JSON files.
func runInventory(dataDir string) {
	if dataDir == "" {
		dataDir = "."
	}

	sources := discoverJSONSources(dataDir)

	fmt.Println("Legacy JSON Inventory")
	fmt.Println("=====================")
	fmt.Printf("%-40s %-12s %-10s %-64s\n", "File", "Size", "Exists", "SHA256")
	fmt.Println(strings.Repeat("-", 130))

	for _, src := range sources {
		existsStr := "NO"
		sizeStr := "-"
		shaStr := "-"
		if src.Exists {
			existsStr = "YES"
			sizeStr = fmt.Sprintf("%d", src.Size)
			shaStr = src.SHA256[:16] + "..."
		}
		fmt.Printf("%-40s %-12s %-10s %-64s\n", src.Path, sizeStr, existsStr, shaStr)
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

	sources := discoverJSONSources(dataDir)

	result := map[string]any{}

	for _, src := range sources {
		if !src.Exists {
			continue
		}

		domain := src.Domain
		data, err := os.ReadFile(src.Path)
		if err != nil {
			log.Printf("[WARN] Cannot read %s: %v", src.Path, err)
			continue
		}

		count, err := countRecords(domain, data)
		if err != nil {
			log.Printf("[WARN] Cannot parse %s: %v", src.Path, err)
			continue
		}

		result[domain] = map[string]any{
			"source":    src.Path,
			"exists":    true,
			"sha256":    src.SHA256[:16],
			"records":   count,
			"imported":  0,
			"rejected":  0,
			"conflicts": 0,
		}
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(output))
}

// apply imports JSON data into SQLite.
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

	sources := discoverJSONSources(dataDir)
	result := map[string]any{}

	for _, src := range sources {
		if !src.Exists {
			continue
		}

		domain := src.Domain
		data, err := os.ReadFile(src.Path)
		if err != nil {
			log.Printf("[WARN] Cannot read %s: %v", src.Path, err)
			continue
		}

		count, imported, err := importDomain(s, domain, data, src.Path)
		if err != nil {
			log.Printf("[ERROR] Import %s failed: %v", domain, err)
			result[domain] = map[string]any{"error": err.Error()}
			continue
		}

		result[domain] = map[string]any{
			"source":   src.Path,
			"records":  count,
			"imported": imported,
		}
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(output))
}

func discoverJSONSources(dataDir string) []JSONSource {
	sources := []JSONSource{
		{Name: "Workers", Path: "workers.json", Domain: "workers"},
		{Name: "YouTube Channels", Path: "youtube/channels/channels.json", Domain: "youtube_channels"},
		{Name: "YouTube Groups", Path: "youtube/groups.json", Domain: "youtube_groups"},
		{Name: "YouTube Manager", Path: "youtube/GroupYoutubeManager/ChannelsSaved.json", Domain: "youtube_manager"},
		{Name: "Ansible Computers", Path: "ansible/ansible_computers.json", Domain: "ansible_hosts"},
		{Name: "Ansible Runs", Path: "ansible/ansible_runs.json", Domain: "ansible_runs"},
		{Name: "Analytics Cache", Path: "analytics/analytics_cache.json", Domain: "analytics_cache"},
		{Name: "YouTube API Cache", Path: "analytics/youtube_api_cache.json", Domain: "youtube_cache"},
	}

	for i := range sources {
		fullPath := filepath.Join(dataDir, sources[i].Path)
		sources[i].Path = fullPath

		info, err := os.Stat(fullPath)
		if err != nil {
			sources[i].Exists = false
			continue
		}

		sources[i].Exists = true
		sources[i].Size = info.Size()

		data, err := os.ReadFile(fullPath)
		if err == nil {
			hash := sha256.Sum256(data)
			sources[i].SHA256 = fmt.Sprintf("%x", hash)
		}
	}

	return sources
}

func countRecords(domain string, data []byte) (int, error) {
	switch domain {
	case "workers":
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return 0, err
		}
		return len(m), nil
	case "youtube_channels", "youtube_groups", "youtube_manager", "youtube_cache":
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			var arr []any
			if err2 := json.Unmarshal(data, &arr); err2 == nil {
				return len(arr), nil
			}
			return 0, err
		}
		return len(m), nil
	case "ansible_hosts", "ansible_runs":
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return 0, err
		}
		return len(m), nil
	case "analytics_cache":
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return 0, err
		}
		return len(m), nil
	default:
		return 0, fmt.Errorf("unknown domain: %s", domain)
	}
}

func importDomain(s *store.SQLiteStore, domain string, data []byte, path string) (int, int, error) {
	_ = time.Now() // for potential logging

	switch domain {
	case "workers":
		return importWorkers(s, data)
	case "youtube_channels":
		return importYouTubeChannels(s, data)
	case "youtube_groups":
		return importYouTubeGroups(s, data)
	case "youtube_manager":
		return importYouTubeManager(s, data)
	case "ansible_hosts":
		return importAnsibleHosts(s, data)
	case "ansible_runs":
		return importAnsibleRuns(s, data)
	case "analytics_cache":
		return importAnalyticsCache(s, data)
	default:
		return 0, 0, fmt.Errorf("unknown domain: %s", domain)
	}
}

func importWorkers(s *store.SQLiteStore, data []byte) (int, int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, 0, err
	}
	total := len(m)
	imported := 0
	for _, raw := range m {
		b, _ := json.Marshal(raw)
		if err := s.UpsertWorker(b); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func importYouTubeChannels(s *store.SQLiteStore, data []byte) (int, int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, 0, err
	}
	total := len(m)
	imported := 0
	for id, raw := range m {
		ch, _ := raw.(map[string]any)
		if ch == nil {
			continue
		}
		title, _ := ch["title"].(string)
		displayName, _ := ch["display_name"].(string)
		if displayName == "" {
			displayName = title
		}
		if err := s.UpsertYouTubeChannel(id, title, displayName, "", "", "", "", 0, 0, "", "", "{}"); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func importYouTubeGroups(s *store.SQLiteStore, data []byte) (int, int, error) {
	var groups []map[string]any
	if err := json.Unmarshal(data, &groups); err != nil {
		return 0, 0, err
	}
	total := len(groups)
	imported := 0
	for _, g := range groups {
		name, _ := g["name"].(string)
		desc, _ := g["description"].(string)
		privacy, _ := g["privacy"].(string)
		if _, err := s.UpsertYouTubeGroupV2(name, "upload", desc, privacy); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func importYouTubeManager(s *store.SQLiteStore, data []byte) (int, int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, 0, err
	}
	total := len(m)
	imported := 0
	for id, raw := range m {
		ch, _ := raw.(map[string]any)
		if ch == nil {
			continue
		}
		title, _ := ch["title"].(string)
		groupName, _ := ch["group"].(string)
		url, _ := ch["url"].(string)
		if err := s.UpsertYouTubeManagerChannel(id, groupName, url, title, "", "", "", "", nil, "", "", 0, 0, ""); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func importAnsibleHosts(s *store.SQLiteStore, data []byte) (int, int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, 0, err
	}
	total := len(m)
	imported := 0
	for _, raw := range m {
		c, _ := raw.(map[string]any)
		if c == nil {
			continue
		}
		host, _ := c["host"].(string)
		user, _ := c["ansible_user"].(string)
		group, _ := c["group"].(string)
		enabled := true
		if e, ok := c["enabled"].(bool); ok {
			enabled = e
		}
		fields := store.AnsibleHostFields{
			Host:        host,
			AnsibleUser: firstNonEmptyString(user, "pierone"),
			Group:       group,
			Enabled:     enabled,
		}
		if err := s.UpsertAnsibleHost(fields); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func importAnsibleRuns(s *store.SQLiteStore, data []byte) (int, int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, 0, err
	}
	total := len(m)
	imported := 0
	for _, raw := range m {
		r, _ := raw.(map[string]any)
		if r == nil {
			continue
		}
		runID, _ := r["run_id"].(string)
		if runID == "" {
			runID, _ = r["id"].(string)
		}
		action, _ := r["action"].(string)
		status, _ := r["status"].(string)
		playbook, _ := r["playbook"].(string)
		output, _ := r["output"].(string)
		if err := s.UpsertAnsibleRun(runID, action, playbook, status, 0, 0, 0, "[]", output, "", "", ""); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func importAnalyticsCache(s *store.SQLiteStore, data []byte) (int, int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, 0, err
	}
	total := len(m)
	imported := 0
	for key, raw := range m {
		b, _ := json.Marshal(raw)
		if err := s.UpsertAnalyticsCache(key, float64(time.Now().Unix()), b); err == nil {
			imported++
		}
	}
	return total, imported, nil
}

func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
