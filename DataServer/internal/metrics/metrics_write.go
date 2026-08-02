package metrics

// metrics_write.go: Prometheus exposition — family write helpers +
// label/format internals. Split out of metrics.go; the Family/Registry
// core lives in metrics.go and the histogram data type in
// metrics_histogram.go.

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// formatBucket formats a bucket boundary using the canonical Prometheus
// representation. We use 'g' with -1 precision so 1.0 stays "1",
// 0.5 stays "0.5", and 0.05 stays "0.05". Stable across runs.
func formatBucket(v float64) string {
	return strconvFormatFloat(v, 'g', -1, 64)
}

// write emits one family to `w`.
func (f *Family) write(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "# HELP %s %s\n", f.Name, strings.ReplaceAll(f.Help, "\n", " ")); err != nil {
		return err
	}
	typeName := "untyped"
	switch f.Kind {
	case CounterFamily:
		typeName = "counter"
	case GaugeFamily:
		typeName = "gauge"
	case HistogramFamily:
		typeName = "histogram"
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", f.Name, typeName); err != nil {
		return err
	}
	switch f.Kind {
	case CounterFamily:
		return f.writeCounter(w)
	case GaugeFamily:
		return f.writeGauge(w)
	case HistogramFamily:
		return f.writeHistogram(w)
	}
	return errors.New("metrics: unknown family type")
}

func (f *Family) writeCounter(w io.Writer) error {
	f.labelMu.Lock()
	keys := make([]string, 0, len(f.counterVals))
	mapping := make(map[string]uint64, len(f.counterVals))
	labelList := append([]string(nil), f.labels...)
	for k, v := range f.counterVals {
		keys = append(keys, k)
		mapping[k] = v.Load()
	}
	f.labelMu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		lblVals := splitLabelKey(k)
		if _, err := fmt.Fprintf(w, "%s%s %d\n", f.Name, formatLabelInline(labelList, lblVals), mapping[k]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Family) writeGauge(w io.Writer) error {
	f.labelMu.Lock()
	keys := make([]string, 0, len(f.gaugeVals))
	mapping := make(map[string]int64, len(f.gaugeVals))
	labelList := append([]string(nil), f.labels...)
	for k, v := range f.gaugeVals {
		keys = append(keys, k)
		mapping[k] = v.Load()
	}
	f.labelMu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		lblVals := splitLabelKey(k)
		if _, err := fmt.Fprintf(w, "%s%s %d\n", f.Name, formatLabelInline(labelList, lblVals), mapping[k]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Family) writeHistogram(w io.Writer) error {
	f.labelMu.Lock()
	keys := make([]string, 0, len(f.histVals))
	snapshots := make(map[string]*histogramData, len(f.histVals))
	labelList := append([]string(nil), f.labels...)
	buckets := append([]float64(nil), f.buckets...)
	for k, v := range f.histVals {
		keys = append(keys, k)
		snapshots[k] = v.snapshot()
	}
	f.labelMu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		h := snapshots[k]
		if len(labelList) == 0 {
			for _, b := range buckets {
				fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", f.Name, formatBucket(b), h.bucketLE(b))
			}
			fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", f.Name, h.count)
			fmt.Fprintf(w, "%s_sum %s\n", f.Name, strconvFormatFloat(h.sum, 'g', -1, 64))
			fmt.Fprintf(w, "%s_count %d\n", f.Name, h.count)
			continue
		}
		lblVals := splitLabelKey(k)
		for _, b := range buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", f.Name,
				formatHistogramLabelInline(labelList, lblVals, "le", formatBucket(b)),
				h.bucketLE(b))
		}
		// +Inf comes BEFORE _sum/_count in canonical exposition format.
		fmt.Fprintf(w, "%s_bucket%s %d\n", f.Name,
			formatHistogramLabelInline(labelList, lblVals, "le", "+Inf"),
			h.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", f.Name,
			formatLabelInline(labelList, lblVals),
			strconvFormatFloat(h.sum, 'g', -1, 64))
		fmt.Fprintf(w, "%s_count%s %d\n", f.Name,
			formatLabelInline(labelList, lblVals),
			h.count)
	}
	return nil
}

// ── label/format internals ────────────────────────────────────────────────

// labelKey is the canonical label-tuple key (deterministic). We
// join label values with `\x00` to avoid collisions on labels like
// "a,b" + "c" vs "a" + "b,c".
func labelKey(vals []string) string {
	return strings.Join(vals, "\x00")
}

// splitLabelKey maps a label key list to its value list for exposition.
// Splits labelKey back to a slice.
func splitLabelKey(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}

// quote escapes \ and " for the Prometheus exposition label-value format.
func quote(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "\"", "\\\"")
}

// formatLabelInline formats a (labels, label-values) pair as
// `{name="value",name2="value2"}`. Empty label list ⇒ "".
func formatLabelInline(names, vals []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("{")
	for i, n := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(quote(vals[i]))
		b.WriteString(`"`)
	}
	b.WriteString("}")
	return b.String()
}

// formatHistogramLabelInline extends formatLabelInline with a single
// extra `extraKey="extraVal"` entry (e.g. `le="0.5"` on a histogram bucket).
func formatHistogramLabelInline(names, vals []string, extraKey, extraVal string) string {
	if len(names) == 0 {
		return "{" + extraKey + `="` + extraVal + `"}`
	}
	var b strings.Builder
	b.WriteString("{")
	for i, n := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(quote(vals[i]))
		b.WriteString(`"`)
	}
	b.WriteString(",")
	b.WriteString(extraKey)
	b.WriteString(`="`)
	b.WriteString(extraVal)
	b.WriteString(`"`)
	b.WriteString("}")
	return b.String()
}
