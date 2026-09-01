package native

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// failedRenderEvidence is deliberately a diagnostic artifact, not a second
// render-plan contract. It records what the worker gave the engine without
// retaining credentials embedded in remote URLs.
type failedRenderEvidence struct {
	CapturedAt    time.Time              `json:"captured_at"`
	JobID         string                 `json:"job_id,omitempty"`
	PlanSHA256    string                 `json:"plan_sha256"`
	PlanPath      string                 `json:"plan_path"`
	OutputPath    fileEvidence           `json:"output"`
	AudioBindings []audioBindingEvidence `json:"audio_bindings,omitempty"`
	EngineError   string                 `json:"engine_error"`
	Stderr        string                 `json:"stderr,omitempty"`
	Stdout        string                 `json:"stdout,omitempty"`
	FFprobe       []ffprobeEvidence      `json:"ffprobe,omitempty"`
}

type audioBindingEvidence struct {
	Key   string       `json:"key"`
	Value string       `json:"value"`
	File  fileEvidence `json:"file"`
}

type fileEvidence struct {
	Path      string `json:"path,omitempty"`
	Exists    bool   `json:"exists"`
	Regular   bool   `json:"regular"`
	Readable  bool   `json:"readable"`
	Size      int64  `json:"size,omitempty"`
	Mode      string `json:"mode,omitempty"`
	StatError string `json:"stat_error,omitempty"`
}

type ffprobeEvidence struct {
	Path   string `json:"path"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// captureFailedRenderEvidence persists the exact plan shape and filesystem
// facts needed to diagnose binding_missing. It never changes render outcome:
// diagnostics are best-effort and failures are reported to the caller.
func captureFailedRenderEvidence(jobID, planPath, outputPath, engineErr, stderr, stdout string, logf func(string, ...interface{})) {
	planJSON, readErr := os.ReadFile(planPath)
	if readErr != nil {
		if logf != nil {
			logf("[NATIVE] failed render diagnostics: read plan: %s", readErr)
		}
		return
	}
	if strings.TrimSpace(jobID) == "" {
		jobID = planJobID(planJSON)
	}
	root := strings.TrimSpace(os.Getenv("VELOX_RENDER_DIAGNOSTICS_DIR"))
	if root == "" {
		root = filepath.Join(os.TempDir(), "velox-render-failures")
	}
	dir := filepath.Join(root, fmt.Sprintf("%s-%d", safeDiagnosticID(jobID), time.Now().UnixNano()))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		if logf != nil {
			logf("[NATIVE] failed render diagnostics: create directory: %s", err)
		}
		return
	}

	safePlan := sanitizeJSON(planJSON)
	_ = os.WriteFile(filepath.Join(dir, "render-plan.failed.json"), safePlan, 0o640)

	sha := sha256.Sum256(planJSON)
	evidence := failedRenderEvidence{
		CapturedAt: time.Now().UTC(), JobID: jobID,
		PlanSHA256: hex.EncodeToString(sha[:]), PlanPath: planPath,
		OutputPath: inspectFile(outputPath), EngineError: engineErr,
		Stderr: stderr, Stdout: stdout,
	}
	for _, binding := range collectAudioBindings(planJSON) {
		safeValue := sanitizeString(binding.value)
		evidence.AudioBindings = append(evidence.AudioBindings, audioBindingEvidence{
			Key: binding.key, Value: safeValue, File: inspectFile(safeValue),
		})
	}
	if isAudioFailure(engineErr, stderr, stdout) {
		for _, binding := range evidence.AudioBindings {
			if binding.File.Regular {
				evidence.FFprobe = append(evidence.FFprobe, runDiagnosticFFprobe(binding.File.Path))
			}
		}
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err == nil {
		if err := os.WriteFile(filepath.Join(dir, "audio-diagnostic.json"), append(data, '\n'), 0o640); err != nil && logf != nil {
			logf("[NATIVE] failed render diagnostics: write evidence: %s", err)
		}
	}
	if logf != nil {
		logf("[NATIVE] failed render evidence saved: %s", dir)
	}
}

func planJobID(data []byte) string {
	var root map[string]interface{}
	if json.Unmarshal(data, &root) != nil {
		return ""
	}
	jobID, _ := root["job_id"].(string)
	return strings.TrimSpace(jobID)
}

type audioBinding struct{ key, value string }

func collectAudioBindings(data []byte) []audioBinding {
	var root interface{}
	if json.Unmarshal(data, &root) != nil {
		return nil
	}
	var out []audioBinding
	var walk func(interface{})
	walk = func(value interface{}) {
		switch node := value.(type) {
		case map[string]interface{}:
			for key, child := range node {
				lower := strings.ToLower(key)
				if lower == "source_url" || lower == "audio_url" || lower == "audio_path" || lower == "voiceover_path" || lower == "voiceover" {
					if text, ok := child.(string); ok && strings.TrimSpace(text) != "" {
						out = append(out, audioBinding{key: key, value: text})
					}
				}
				walk(child)
			}
		case []interface{}:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(root)
	return out
}

func inspectFile(path string) fileEvidence {
	e := fileEvidence{Path: path}
	if strings.TrimSpace(path) == "" {
		return e
	}
	info, err := os.Stat(path)
	if err != nil {
		e.StatError = err.Error()
		return e
	}
	e.Exists, e.Regular, e.Size, e.Mode = true, info.Mode().IsRegular(), info.Size(), info.Mode().String()
	if e.Regular {
		f, err := os.Open(path)
		if err == nil {
			e.Readable = true
			_ = f.Close()
		} else {
			e.StatError = "open: " + err.Error()
		}
	}
	return e
}

func runDiagnosticFFprobe(path string) ffprobeEvidence {
	e := ffprobeEvidence{Path: path}
	probe, err := exec.LookPath("ffprobe")
	if err != nil {
		e.Error = "ffprobe unavailable: " + err.Error()
		return e
	}
	cmd := exec.Command(probe, "-v", "error", "-show_streams", "-show_format", "-of", "json", path)
	output, err := cmd.CombinedOutput()
	e.Output = string(output)
	if err != nil {
		e.Error = err.Error()
	}
	return e
}

func isAudioFailure(values ...string) bool {
	joined := strings.ToLower(strings.Join(values, " "))
	return strings.Contains(joined, "audio") || strings.Contains(joined, "binding_missing")
}

func sanitizeJSON(data []byte) []byte {
	var value interface{}
	if json.Unmarshal(data, &value) != nil {
		return []byte("null\n")
	}
	sanitizeValue(value)
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return []byte("null\n")
	}
	return append(out, '\n')
}

func sanitizeValue(value interface{}) {
	switch node := value.(type) {
	case map[string]interface{}:
		for key, child := range node {
			if text, ok := child.(string); ok {
				node[key] = sanitizeString(text)
			} else {
				sanitizeValue(child)
			}
		}
	case []interface{}:
		for _, child := range node {
			sanitizeValue(child)
		}
	}
}

func sanitizeString(value string) string {
	parsed, err := url.Parse(value)
	if err == nil && parsed.IsAbs() && parsed.Host != "" {
		parsed.RawQuery, parsed.Fragment, parsed.User = "", "", nil
		return parsed.String()
	}
	return value
}

func safeDiagnosticID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown-job"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "unknown-job"
	}
	return b.String()
}
