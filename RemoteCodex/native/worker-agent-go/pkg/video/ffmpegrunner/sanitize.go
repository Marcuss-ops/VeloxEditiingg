package ffmpegrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// Fingerprint returns the SHA-256 hex of the canonical argument vector
// for one request (operation + args, NUL-separated). It is stable for
// identical invocations and changes when any argument changes; because
// it is a hash, the raw argument contents (paths, tokens) never leak.
func Fingerprint(req FFmpegRequest) string {
	h := sha256.New()
	h.Write([]byte(string(req.Operation)))
	h.Write([]byte{0})
	for _, arg := range req.Args {
		h.Write([]byte(arg))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Sanitize projects an invocation into its safe, comparable parameter
// surface: codec names, filter names, and input count. Paths, URLs,
// tokens, and raw filter values are never included.
func Sanitize(req FFmpegRequest) SanitizedParameters {
	p := SanitizedParameters{}
	for i := 0; i < len(req.Args); i++ {
		arg := req.Args[i]
		switch arg {
		case "-i":
			p.InputCount++
			i++ // skip the input path
		case "-c", "-c:a", "-c:v":
			if i+1 < len(req.Args) {
				codec := strings.TrimSpace(req.Args[i+1])
				if codec != "" {
					p.Codecs = append(p.Codecs, codec)
				}
				i++
			}
		case "-filter_complex", "-vf", "-af":
			if i+1 < len(req.Args) {
				p.Filters = append(p.Filters, extractFilterNames(req.Args[i+1])...)
				i++
			}
		}
	}
	p.Codecs = dedupeSort(p.Codecs)
	p.Filters = dedupeSort(p.Filters)
	return p
}

// extractFilterNames parses an ffmpeg filter graph string and returns
// only the filter names. Label groups ([0:a], [vout], ...) and option
// values (volume=0.5, adelay=100|100, ass=/path/to/file) are stripped:
// a path-valued option yields just its filter name (e.g. "ass").
func extractFilterNames(graph string) []string {
	var names []string
	for _, group := range strings.Split(graph, ";") {
		for _, token := range strings.Split(group, ",") {
			name := stripLabels(strings.TrimSpace(token))
			if eq := strings.IndexByte(name, '='); eq >= 0 {
				name = name[:eq]
			}
			name = strings.TrimSpace(name)
			if name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

// stripLabels removes every [..] label group from a filter chain token.
func stripLabels(token string) string {
	var b strings.Builder
	for i := 0; i < len(token); {
		if token[i] == '[' {
			end := strings.IndexByte(token[i:], ']')
			if end < 0 {
				b.WriteString(token[i:])
				break
			}
			i += end + 1
			continue
		}
		b.WriteByte(token[i])
		i++
	}
	return b.String()
}

func dedupeSort(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
