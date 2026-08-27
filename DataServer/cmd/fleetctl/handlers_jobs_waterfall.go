package main

// handlers_jobs_waterfall.go — rendering of the attempt waterfall block for
// `fleetctl job inspect --waterfall`. The Master snapshot (GET
// /api/v1/admin/jobs/{id}) carries the waterfall either execution-level
// (execution.waterfall, preferred) or per-attempt
// (execution.attempts[].attempt_waterfall, older masters / mixed-retry jobs).
// Both spellings decode into the same view and render identically; nothing
// is invented client-side — missing fields render as missing data, exactly
// what the Master computed (or did not compute).

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type waterfallBucketView struct {
	Name       string `json:"name"`
	StartMS    int64  `json:"start_ms"`
	EndMS      int64  `json:"end_ms"`
	DurationMS int64  `json:"duration_ms"`
}

// waterfallView mirrors observability.AttemptWaterfall's JSON shape.
type waterfallView struct {
	AttemptID         string                `json:"attempt_id"`
	WallMS            int64                 `json:"wall_ms"`
	Buckets           []waterfallBucketView `json:"buckets"`
	AccountedMS       int64                 `json:"accounted_ms"`
	UnaccountedMS     int64                 `json:"unaccounted_ms"`
	CoveragePct       float64               `json:"coverage_pct"`
	MissingMilestones []string              `json:"missing_milestones,omitempty"`
	InvertedBuckets   []string              `json:"inverted_buckets,omitempty"`
	Publish           *publishWaterfallView `json:"publish,omitempty"`
}

type publishWaterfallView struct {
	SlotWaitMS       int64 `json:"slot_wait_ms"`
	DeclareMS        int64 `json:"declare_ms"`
	UploadMS         int64 `json:"upload_ms"`
	RemoteFinalizeMS int64 `json:"remote_finalize_ms"`
	CommitWaitMS     int64 `json:"commit_wait_ms"`
	SpoolCommitMS    int64 `json:"spool_commit_ms"`
}

type waterfallAttemptHeaderView struct {
	AttemptID         string `json:"attempt_id"`
	Status            string `json:"status"`
	WorkerID          string `json:"worker_id"`
	MasterReceivedAt  string `json:"master_received_at,omitempty"`
	MasterCommittedAt string `json:"master_committed_at,omitempty"`

	AttemptWaterfall *waterfallView `json:"attempt_waterfall"`
}

// renderJobInspectWaterfall prints the waterfall block from the raw inspect
// snapshot. Exit code stays OK when the job simply has no waterfall yet:
// a missing timeline is an expected state (pre-milestone reports), not an
// error — inventing buckets would be worse than printing none.
func renderJobInspectWaterfall(response map[string]any) {
	execution, _ := response["execution"].(map[string]any)
	if execution == nil {
		fmt.Println("no execution snapshot in job inspect output; nothing to render")
		return
	}
	if wf := decodeWaterfallAny(execution["waterfall"]); wf != nil {
		printWaterfall("latest attempt", "", "", "", *wf)
		return
	}
	attemptsRaw, _ := execution["attempts"].([]any)
	rendered := 0
	for _, raw := range attemptsRaw {
		attempt, _ := raw.(map[string]any)
		if attempt == nil {
			continue
		}
		wf := decodeWaterfallAny(attempt["attempt_waterfall"])
		if wf == nil {
			continue
		}
		label := stringField(attempt, "attempt_id")
		if label == "" {
			label = fmt.Sprintf("attempt #%d", rendered+1)
		}
		received := firstNonEmpty(
			stringField(attempt, "master_received_at"),
			stringMap(attempt["report"], "received_at"))
		committed := firstNonEmpty(
			stringField(attempt, "master_committed_at"),
			stringMap(attempt["report"], "persisted_at"))
		printWaterfall(label,
			stringField(attempt, "status"),
			stringField(attempt, "worker_id"),
			masterStampLabel(received, committed), *wf)
		rendered++
	}
	if rendered == 0 {
		fmt.Fprintln(os.Stderr, "no waterfall in job snapshot: server predates milestone support or the report has no milestone timeline")
	}
}

func decodeWaterfallAny(raw any) *waterfallView {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	// The snapshot is already generic JSON; re-encode into the typed view so
	// field-name drift fails closed (nil view) instead of rendering zeros.
	bs, err := json.Marshal(obj)
	if err != nil {
		return nil
	}
	var wf waterfallView
	if json.Unmarshal(bs, &wf) != nil || (wf.WallMS <= 0 && len(wf.Buckets) == 0) {
		return nil
	}
	return &wf
}

func printWaterfall(label, status, workerID, stamps string, wf waterfallView) {
	header := strings.TrimSpace(label +
		statusSuffix(status) +
		workerSuffix(workerID))
	fmt.Printf("ATTEMPT %s\n", header)
	fmt.Printf("ATTEMPT WALL %s  coverage %.2f%%  accounted %s  unaccounted %s%s\n",
		comma(wf.WallMS), wf.CoveragePct, comma(wf.AccountedMS), comma(wf.UnaccountedMS),
		stampLine(stamps))
	fmt.Println("WATERFALL")
	if wf.Publish != nil {
		fmt.Printf("  %-24s %13s ms\n", "publish.slot_wait", comma(wf.Publish.SlotWaitMS))
		fmt.Printf("  %-24s %13s ms\n", "publish.declare", comma(wf.Publish.DeclareMS))
		fmt.Printf("  %-24s %13s ms\n", "publish.upload", comma(wf.Publish.UploadMS))
		fmt.Printf("  %-24s %13s ms\n", "publish.remote_finalize", comma(wf.Publish.RemoteFinalizeMS))
		fmt.Printf("  %-24s %13s ms\n", "publish.commit_wait", comma(wf.Publish.CommitWaitMS))
		fmt.Printf("  %-24s %13s ms\n", "publish.spool_commit", comma(wf.Publish.SpoolCommitMS))
	}
	buckets := append([]waterfallBucketView(nil), wf.Buckets...)
	sort.SliceStable(buckets, func(i, j int) bool { return buckets[i].StartMS < buckets[j].StartMS })
	for _, b := range buckets {
		pct := ""
		if wf.WallMS > 0 {
			pct = fmt.Sprintf("%5.1f%%", float64(b.DurationMS)/float64(wf.WallMS)*100)
		}
		fmt.Printf("  %-24s %13s ms  %s\n", b.Name, comma(b.DurationMS), pct)
	}
	for _, m := range wf.MissingMilestones {
		fmt.Printf("  MISSING MILESTONE       %s (segment left unattributed)\n", m)
	}
	for _, name := range wf.InvertedBuckets {
		fmt.Printf("  INVERTED BUCKET         %s (end milestone before start; excluded from accounting)\n", name)
	}
}

func statusSuffix(status string) string {
	if status == "" {
		return ""
	}
	return "  [" + status + "]"
}

func workerSuffix(workerID string) string {
	if workerID == "" {
		return ""
	}
	return "  worker=" + workerID
}

// masterStampLabel appends the Master receive→commit window to the final
// segment row: both stamps are Master-local so their delta is safe to show;
// absent stamps degrade to nothing rather than fake precision.
func masterStampLabel(received, committed string) string {
	if received == "" && committed == "" {
		return ""
	}
	return "master report receive→commit: " +
		firstNonEmpty(received, "?") + " → " + firstNonEmpty(committed, "?")
}

func stampLine(stamps string) string {
	if stamps == "" {
		return ""
	}
	return "\n" + stamps
}

func stringField(obj map[string]any, key string) string {
	v, _ := obj[key].(string)
	return v
}

func stringMap(obj any, key string) string {
	m, _ := obj.(map[string]any)
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// comma groups digits with thin underscores for readability at 6-digit
// millisecond counts: 298421 -> 298_421.
func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	out := strings.Join(parts, "_")
	if neg {
		out = "-" + out
	}
	return out
}
