package mediarange

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	// ErrRangeUnsupported means the server ignored a byte range; callers must
	// download the whole source instead.
	ErrRangeUnsupported = errors.New("mediarange: source server does not support byte ranges")
	// ErrUnsupportedContainer means the container/box layout cannot be served
	// safely with sample ranges; callers must download the whole source.
	ErrUnsupportedContainer = errors.New("mediarange: source cannot be read with sample ranges")
)

const (
	defaultMaxBytes  = 512 << 20
	maxTopLevelBoxes = 64
	rangeMergeGap    = 128 << 10
	rangeChunkBytes  = 8 << 20
)

// Options declares the source window to materialize.
type Options struct {
	Size       int64
	StartFrame int64
	FrameCount int64
	StartUS    int64
	DurationUS int64
	FPSNum     int
	FPSDen     int
	Dest       string
	MaxBytes   int64
}

// Stats reports what was actually transferred.
type Stats struct {
	SourceSize      int64
	DownloadedBytes int64
	RangeCount      int
	FirstSample     int64
	LastSample      int64
	PreRollSamples  int64
}

type byteRange struct {
	start int64
	end   int64
}

type rangeReader struct {
	ctx        context.Context
	client     *http.Client
	url        string
	size       int64
	downloaded int64
	limit      int64
}

func (r *rangeReader) readAt(offset int64, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}
	want := int64(len(buf))
	if offset+want > r.size {
		want = r.size - offset
	}
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+want-1))
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return 0, ErrRangeUnsupported
	}
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("mediarange: range request failed with HTTP %d", resp.StatusCode)
	}
	if err := validateContentRange(resp.Header.Get("Content-Range"), offset, want, r.size); err != nil {
		return 0, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, want+1))
	if err != nil {
		return 0, err
	}
	if int64(len(body)) != want {
		return 0, fmt.Errorf("mediarange: short range response: got %d want %d", len(body), want)
	}
	r.downloaded += int64(len(body))
	if r.limit > 0 && r.downloaded > r.limit {
		return 0, fmt.Errorf("mediarange: sparse fetch exceeded %d bytes", r.limit)
	}
	copy(buf, body)
	return len(body), nil
}

func validateContentRange(header string, offset, want, total int64) error {
	trimmed := strings.TrimSpace(header)
	if !strings.HasPrefix(trimmed, "bytes ") {
		return fmt.Errorf("mediarange: missing Content-Range")
	}
	spec := strings.TrimPrefix(trimmed, "bytes ")
	parts := strings.SplitN(spec, "/", 2)
	bounds := strings.SplitN(parts[0], "-", 2)
	if len(parts) != 2 || len(bounds) != 2 {
		return fmt.Errorf("mediarange: invalid Content-Range %q", header)
	}
	start, err1 := strconv.ParseInt(strings.TrimSpace(bounds[0]), 10, 64)
	end, err2 := strconv.ParseInt(strings.TrimSpace(bounds[1]), 10, 64)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("mediarange: invalid Content-Range %q", header)
	}
	if start != offset || end != offset+want-1 {
		return fmt.Errorf("mediarange: Content-Range %q does not cover %d-%d", header, offset, offset+want-1)
	}
	if parts[1] != "*" {
		size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || size != total {
			return fmt.Errorf("mediarange: source size changed (Content-Range %q, want %d)", header, total)
		}
	}
	return nil
}

func (r *rangeReader) boxHeader(offset int64) (box, error) {
	var header [16]byte
	n, err := r.readAt(offset, header[:])
	if err != nil {
		return box{}, err
	}
	if n < 8 {
		return box{}, fmt.Errorf("%w: truncated top-level box", ErrUnsupportedContainer)
	}
	size := int64(binary.BigEndian.Uint32(header[0:4]))
	typ := string(header[4:8])
	headerLen := int64(8)
	if size == 1 {
		if n < 16 {
			return box{}, fmt.Errorf("%w: truncated 64-bit box header", ErrUnsupportedContainer)
		}
		size = int64(binary.BigEndian.Uint64(header[8:16]))
		headerLen = 16
	} else if size == 0 {
		size = r.size - offset
	}
	if size < headerLen || offset+size > r.size {
		return box{}, fmt.Errorf("%w: invalid %q box size", ErrUnsupportedContainer, typ)
	}
	return box{typ: typ, start: offset, payload: offset + headerLen, end: offset + size}, nil
}

func scanTopLevel(r *rangeReader) ([]box, error) {
	var boxes []box
	for offset := int64(0); offset < r.size; {
		if len(boxes) >= maxTopLevelBoxes {
			return nil, fmt.Errorf("%w: too many top-level boxes", ErrUnsupportedContainer)
		}
		current, err := r.boxHeader(offset)
		if err != nil {
			return nil, err
		}
		boxes = append(boxes, current)
		offset = current.end
		if current.typ == "mdat" && len(boxes) > 1 {
			// mdat is normally last; keep scanning only for a trailing moov
			// while a bounded box count protects against adversarial files.
		}
	}
	return boxes, nil
}

func findTopLevel(boxes []box, typ string) (box, bool) {
	for _, candidate := range boxes {
		if candidate.typ == typ {
			return candidate, true
		}
	}
	return box{}, false
}

// FetchWindow writes a sparse local copy of the source containing the
// container metadata and the byte ranges needed to decode opts' window.
// Everything outside those ranges is a hole (zero bytes). The returned error
// is wrapped with ErrRangeUnsupported or ErrUnsupportedContainer when the
// caller must fall back to a full download.
func FetchWindow(ctx context.Context, client *http.Client, sourceURL string, opts Options) (Stats, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return Stats{}, fmt.Errorf("mediarange: source URL is required")
	}
	if opts.Size <= 0 {
		return Stats{}, fmt.Errorf("mediarange: source size is required")
	}
	if strings.TrimSpace(opts.Dest) == "" {
		return Stats{}, fmt.Errorf("mediarange: destination is required")
	}
	if client == nil {
		client = &http.Client{}
	}
	limit := opts.MaxBytes
	if limit <= 0 {
		limit = defaultMaxBytes
	}
	reader := &rangeReader{ctx: ctx, client: client, url: sourceURL, size: opts.Size, limit: limit}
	boxes, err := scanTopLevel(reader)
	if err != nil {
		return Stats{}, err
	}
	moov, ok := findTopLevel(boxes, "moov")
	if !ok {
		return Stats{}, fmt.Errorf("%w: no moov box", ErrUnsupportedContainer)
	}
	moovSize := moov.end - moov.payload
	if moovSize <= 0 || moovSize > maxMoovBytes {
		return Stats{}, fmt.Errorf("%w: unusable moov size %d", ErrUnsupportedContainer, moovSize)
	}
	moovData := make([]byte, moovSize)
	if _, err := reader.readAt(moov.payload, moovData); err != nil {
		return Stats{}, err
	}
	index, err := parseSampleIndex(moovData)
	if err != nil {
		return Stats{}, err
	}
	first, last, err := index.resolveWindow(opts)
	if err != nil {
		return Stats{}, err
	}
	preRoll := index.preRoll(first)
	decodeEnd := index.decodeEnd(last)
	ranges := buildRanges(boxes, index, preRoll, decodeEnd)
	stats, err := writeSparseFile(reader, ranges, opts.Dest)
	if err != nil {
		return Stats{}, err
	}
	stats.SourceSize = opts.Size
	stats.FirstSample, stats.LastSample = first, last
	stats.PreRollSamples = first - preRoll
	return stats, nil
}

func buildRanges(boxes []box, index *sampleIndex, first, last int64) []byteRange {
	ranges := make([]byteRange, 0, len(boxes)+int(last-first)+1)
	for _, current := range boxes {
		if current.typ == "mdat" {
			// Only the mdat header is metadata; its payload arrives through
			// the per-sample ranges below.
			ranges = append(ranges, byteRange{start: current.start, end: current.payload})
			continue
		}
		ranges = append(ranges, byteRange{start: current.start, end: current.end})
	}
	for i := first; i <= last; i++ {
		ranges = append(ranges, byteRange{start: index.offsets[i], end: index.offsets[i] + index.sizes[i]})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	merged := ranges[:0]
	for _, current := range ranges {
		if current.end <= current.start {
			continue
		}
		if len(merged) == 0 || current.start > merged[len(merged)-1].end+rangeMergeGap {
			merged = append(merged, current)
			continue
		}
		if current.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = current.end
		}
	}
	return merged
}

func writeSparseFile(reader *rangeReader, ranges []byteRange, dest string) (Stats, error) {
	if directory := filepath.Dir(dest); directory != "" && directory != "." {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return Stats{}, err
		}
	}
	// Write to a unique sibling and rename at the end: concurrent workers can
	// materialize the same window without ever exposing a half-written file.
	file, err := os.CreateTemp(filepath.Dir(dest), ".velox-sparse-*.tmp")
	if err != nil {
		return Stats{}, err
	}
	tmpPath := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tmpPath)
	}
	if err := file.Truncate(reader.size); err != nil {
		cleanup()
		return Stats{}, err
	}
	buffer := make([]byte, rangeChunkBytes)
	for _, current := range ranges {
		for offset := current.start; offset < current.end; {
			length := int64(len(buffer))
			if offset+length > current.end {
				length = current.end - offset
			}
			n, err := reader.readAt(offset, buffer[:length])
			if err != nil {
				cleanup()
				return Stats{}, err
			}
			if _, err := file.WriteAt(buffer[:n], offset); err != nil {
				cleanup()
				return Stats{}, err
			}
			offset += int64(n)
		}
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return Stats{}, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return Stats{}, err
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		_ = os.Remove(tmpPath)
		return Stats{}, err
	}
	return Stats{DownloadedBytes: reader.downloaded, RangeCount: len(ranges)}, nil
}
