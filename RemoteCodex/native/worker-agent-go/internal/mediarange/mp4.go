// Package mediarange materializes only the MP4 byte ranges required to decode
// and re-encode one source window. It reads the container index through HTTP
// Range requests, maps the requested source window to samples (including
// decoder pre-roll from the preceding sync sample), downloads only those
// sample ranges plus container metadata, and writes a sparse file with the
// declared source size. Callers must fall back to a full download whenever
// this package returns an error.
package mediarange

import (
	"encoding/binary"
	"fmt"
)

const (
	maxMoovBytes = 64 << 20
	maxSamples   = 5_000_000
)

// box is one top-level box located in the remote file.
type box struct {
	typ     string
	start   int64
	payload int64
	end     int64
}

type memBox struct {
	typ     string
	payload []byte
}

type stscEntry struct {
	firstChunk      uint32
	samplesPerChunk uint32
}

// sampleIndex is the subset of an MP4 sample table needed for window reads.
type sampleIndex struct {
	offsets   []int64
	sizes     []int64
	sync      []bool
	start     []int64  // decode start time in timescale units
	cts       []int64  // composition (display) time in timescale units
	durations []uint32 // per-sample duration in timescale units
	timescale uint32
	// mediaTimeOffset is the single edit-list presentation offset. A
	// requested presentation timestamp maps to CTS + offset.
	mediaTimeOffset int64
	count           int
}

func parseMemBoxes(data []byte) ([]memBox, error) {
	boxes := make([]memBox, 0, 8)
	for len(data) > 0 {
		if len(data) < 8 {
			return nil, fmt.Errorf("mediarange: truncated box header")
		}
		size := uint64(binary.BigEndian.Uint32(data[0:4]))
		typ := string(data[4:8])
		header := uint64(8)
		if size == 1 {
			if len(data) < 16 {
				return nil, fmt.Errorf("mediarange: truncated 64-bit box header")
			}
			size = binary.BigEndian.Uint64(data[8:16])
			header = 16
		} else if size == 0 {
			size = uint64(len(data))
		}
		if size < header || size > uint64(len(data)) {
			return nil, fmt.Errorf("mediarange: invalid %q box size %d", typ, size)
		}
		boxes = append(boxes, memBox{typ: typ, payload: data[header:size]})
		data = data[size:]
	}
	return boxes, nil
}

func findMemBox(data []byte, typ string) []byte {
	boxes, err := parseMemBoxes(data)
	if err != nil {
		return nil
	}
	for _, candidate := range boxes {
		if candidate.typ == typ {
			return candidate.payload
		}
	}
	return nil
}

func parseSampleIndex(moov []byte) (*sampleIndex, error) {
	if findMemBox(moov, "mvex") != nil {
		return nil, fmt.Errorf("%w: fragmented MP4 has no single sample table", ErrUnsupportedContainer)
	}
	traks, err := parseMemBoxes(moov)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedContainer, err)
	}
	for _, trak := range traks {
		if trak.typ != "trak" {
			continue
		}
		mdia := findMemBox(trak.payload, "mdia")
		if mdia == nil {
			continue
		}
		hdlr := findMemBox(mdia, "hdlr")
		if len(hdlr) < 12 || string(hdlr[8:12]) != "vide" {
			continue
		}
		// stts/ctts durations are expressed in the MEDIA timescale (mdhd), not
		// the movie timescale (mvhd). Mixing the two silently shifts every
		// window, so the track's own mdhd is the only accepted authority.
		timescale, err := parseMDHD(findMemBox(mdia, "mdhd"))
		if err != nil || timescale == 0 {
			continue
		}
		var mediaTimeOffset int64
		if edts := findMemBox(trak.payload, "edts"); edts != nil {
			if elst := findMemBox(edts, "elst"); elst != nil {
				mediaTimeOffset, err = parseElst(elst)
				if err != nil {
					return nil, err
				}
			}
		}
		minf := findMemBox(mdia, "minf")
		if minf == nil {
			continue
		}
		stbl := findMemBox(minf, "stbl")
		if stbl == nil {
			continue
		}
		index, err := parseStbl(stbl, timescale)
		if err != nil {
			return nil, err
		}
		index.mediaTimeOffset = mediaTimeOffset
		return index, nil
	}
	return nil, fmt.Errorf("%w: no video track sample table", ErrUnsupportedContainer)
}

func parseStbl(stbl []byte, timescale uint32) (*sampleIndex, error) {
	stts := findMemBox(stbl, "stts")
	stsc := findMemBox(stbl, "stsc")
	stsz := findMemBox(stbl, "stsz")
	stco := findMemBox(stbl, "stco")
	co64 := findMemBox(stbl, "co64")
	stss := findMemBox(stbl, "stss")
	ctts := findMemBox(stbl, "ctts")
	if stts == nil || stsc == nil || stsz == nil || (stco == nil && co64 == nil) {
		return nil, fmt.Errorf("%w: incomplete video sample table", ErrUnsupportedContainer)
	}
	sizes, err := parseStsz(stsz)
	if err != nil {
		return nil, err
	}
	chunks, err := parseChunkOffsets(stco, co64)
	if err != nil {
		return nil, err
	}
	entries, err := parseStsc(stsc)
	if err != nil {
		return nil, err
	}
	offsets, err := assignSampleOffsets(entries, chunks, sizes)
	if err != nil {
		return nil, err
	}
	durations, err := parseStts(stts, len(sizes))
	if err != nil {
		return nil, err
	}
	sync, err := parseStss(stss, len(sizes))
	if err != nil {
		return nil, err
	}
	ctsOffsets, err := parseCtts(ctts, len(sizes))
	if err != nil {
		return nil, err
	}
	starts := cumulativeStarts(durations)
	cts := make([]int64, len(sizes))
	for i := range cts {
		cts[i] = starts[i] + int64(ctsOffsets[i])
	}
	return &sampleIndex{
		offsets: offsets, sizes: sizes, sync: sync,
		start: starts, cts: cts, durations: durations, timescale: timescale, count: len(sizes),
	}, nil
}

func parseStsz(payload []byte) ([]int64, error) {
	if len(payload) < 12 {
		return nil, fmt.Errorf("%w: truncated stsz", ErrUnsupportedContainer)
	}
	constant := binary.BigEndian.Uint32(payload[4:8])
	count := int(binary.BigEndian.Uint32(payload[8:12]))
	if count <= 0 || count > maxSamples {
		return nil, fmt.Errorf("%w: unsupported sample count %d", ErrUnsupportedContainer, count)
	}
	sizes := make([]int64, count)
	if constant != 0 {
		for i := range sizes {
			sizes[i] = int64(constant)
		}
		return sizes, nil
	}
	if len(payload) < 12+count*4 {
		return nil, fmt.Errorf("%w: truncated stsz table", ErrUnsupportedContainer)
	}
	for i := range sizes {
		sizes[i] = int64(binary.BigEndian.Uint32(payload[12+i*4:]))
		if sizes[i] <= 0 {
			return nil, fmt.Errorf("%w: sample %d has zero size", ErrUnsupportedContainer, i)
		}
	}
	return sizes, nil
}

func parseChunkOffsets(stco, co64 []byte) ([]int64, error) {
	payload := stco
	width := 4
	if payload == nil {
		payload = co64
		width = 8
	}
	if len(payload) < 8 {
		return nil, fmt.Errorf("%w: truncated chunk offsets", ErrUnsupportedContainer)
	}
	count := int(binary.BigEndian.Uint32(payload[4:8]))
	if count <= 0 || count > maxSamples {
		return nil, fmt.Errorf("%w: unsupported chunk count %d", ErrUnsupportedContainer, count)
	}
	if len(payload) < 8+count*width {
		return nil, fmt.Errorf("%w: truncated chunk offset table", ErrUnsupportedContainer)
	}
	offsets := make([]int64, count)
	for i := range offsets {
		if width == 4 {
			offsets[i] = int64(binary.BigEndian.Uint32(payload[8+i*4:]))
		} else {
			offsets[i] = int64(binary.BigEndian.Uint64(payload[8+i*8:]))
		}
		if offsets[i] <= 0 {
			return nil, fmt.Errorf("%w: chunk %d has an invalid offset", ErrUnsupportedContainer, i)
		}
	}
	return offsets, nil
}

func parseStsc(payload []byte) ([]stscEntry, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("%w: truncated stsc", ErrUnsupportedContainer)
	}
	count := int(binary.BigEndian.Uint32(payload[4:8]))
	if count <= 0 || count > maxSamples || len(payload) < 8+count*12 {
		return nil, fmt.Errorf("%w: invalid stsc entry count", ErrUnsupportedContainer)
	}
	entries := make([]stscEntry, count)
	for i := range entries {
		base := 8 + i*12
		entries[i] = stscEntry{
			firstChunk:      binary.BigEndian.Uint32(payload[base:]),
			samplesPerChunk: binary.BigEndian.Uint32(payload[base+4:]),
		}
		if entries[i].firstChunk == 0 || entries[i].samplesPerChunk == 0 {
			return nil, fmt.Errorf("%w: invalid stsc entry %d", ErrUnsupportedContainer, i)
		}
	}
	return entries, nil
}

func assignSampleOffsets(entries []stscEntry, chunks []int64, sizes []int64) ([]int64, error) {
	offsets := make([]int64, len(sizes))
	entry := 0
	sample := 0
	for chunkIndex := 1; chunkIndex <= len(chunks); chunkIndex++ {
		for entry+1 < len(entries) && int(entries[entry+1].firstChunk) <= chunkIndex {
			entry++
		}
		if int(entries[entry].firstChunk) > chunkIndex {
			continue
		}
		offset := chunks[chunkIndex-1]
		for i := 0; i < int(entries[entry].samplesPerChunk) && sample < len(sizes); i++ {
			offsets[sample] = offset
			offset += sizes[sample]
			sample++
		}
	}
	if sample != len(sizes) {
		return nil, fmt.Errorf("%w: chunk table covers %d of %d samples", ErrUnsupportedContainer, sample, len(sizes))
	}
	return offsets, nil
}

func parseStts(payload []byte, samples int) ([]uint32, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("%w: truncated stts", ErrUnsupportedContainer)
	}
	count := int(binary.BigEndian.Uint32(payload[4:8]))
	if count <= 0 || count > maxSamples || len(payload) < 8+count*8 {
		return nil, fmt.Errorf("%w: invalid stts entry count", ErrUnsupportedContainer)
	}
	durations := make([]uint32, 0, samples)
	for i := 0; i < count; i++ {
		base := 8 + i*8
		repetitions := int(binary.BigEndian.Uint32(payload[base:]))
		delta := binary.BigEndian.Uint32(payload[base+4:])
		if repetitions <= 0 || delta == 0 || len(durations)+repetitions > samples {
			return nil, fmt.Errorf("%w: invalid stts entry %d", ErrUnsupportedContainer, i)
		}
		for j := 0; j < repetitions; j++ {
			durations = append(durations, delta)
		}
	}
	if len(durations) != samples {
		return nil, fmt.Errorf("%w: stts covers %d of %d samples", ErrUnsupportedContainer, len(durations), samples)
	}
	return durations, nil
}

func parseStss(payload []byte, samples int) ([]bool, error) {
	sync := make([]bool, samples)
	if payload == nil {
		for i := range sync {
			sync[i] = true
		}
		return sync, nil
	}
	if len(payload) < 8 {
		return nil, fmt.Errorf("%w: truncated stss", ErrUnsupportedContainer)
	}
	count := int(binary.BigEndian.Uint32(payload[4:8]))
	if count < 0 || len(payload) < 8+count*4 {
		return nil, fmt.Errorf("%w: invalid stss entry count", ErrUnsupportedContainer)
	}
	for i := 0; i < count; i++ {
		number := int(binary.BigEndian.Uint32(payload[8+i*4:]))
		if number <= 0 || number > samples {
			return nil, fmt.Errorf("%w: stss entry %d is outside the sample table", ErrUnsupportedContainer, number)
		}
		sync[number-1] = true
	}
	return sync, nil
}

func parseMDHD(payload []byte) (uint32, error) {
	if len(payload) < 20 {
		return 0, fmt.Errorf("%w: truncated mdhd", ErrUnsupportedContainer)
	}
	if payload[0] == 1 {
		if len(payload) < 24 {
			return 0, fmt.Errorf("%w: truncated mdhd v1", ErrUnsupportedContainer)
		}
		return binary.BigEndian.Uint32(payload[20:24]), nil
	}
	return binary.BigEndian.Uint32(payload[12:16]), nil
}

func parseCtts(payload []byte, samples int) ([]int32, error) {
	offsets := make([]int32, samples)
	if payload == nil {
		return offsets, nil
	}
	if len(payload) < 8 {
		return nil, fmt.Errorf("%w: truncated ctts", ErrUnsupportedContainer)
	}
	version := payload[0]
	count := int(binary.BigEndian.Uint32(payload[4:8]))
	if count <= 0 || count > maxSamples || len(payload) < 8+count*8 {
		return nil, fmt.Errorf("%w: invalid ctts entry count", ErrUnsupportedContainer)
	}
	cursor := 0
	for i := 0; i < count; i++ {
		base := 8 + i*8
		repetitions := int(binary.BigEndian.Uint32(payload[base:]))
		raw := binary.BigEndian.Uint32(payload[base+4:])
		offset := int32(raw)
		if version == 0 {
			offset = int32(raw & 0x7fffffff)
		}
		if repetitions <= 0 || cursor+repetitions > samples {
			return nil, fmt.Errorf("%w: invalid ctts entry %d", ErrUnsupportedContainer, i)
		}
		for j := 0; j < repetitions; j++ {
			offsets[cursor] = offset
			cursor++
		}
	}
	if cursor != samples {
		return nil, fmt.Errorf("%w: ctts covers %d of %d samples", ErrUnsupportedContainer, cursor, samples)
	}
	return offsets, nil
}

// parseElst accepts only the common single-entry edit list used by encoders to
// skip the initial composition delay. Multiple edits or a non-unity media rate
// change the timeline in ways the sample table alone cannot describe, so they
// fall back to a full download.
func parseElst(payload []byte) (int64, error) {
	if len(payload) < 8 {
		return 0, fmt.Errorf("%w: truncated elst", ErrUnsupportedContainer)
	}
	version := payload[0]
	count := int(binary.BigEndian.Uint32(payload[4:8]))
	if count == 0 {
		return 0, nil
	}
	if count != 1 {
		return 0, fmt.Errorf("%w: multi-entry edit list", ErrUnsupportedContainer)
	}
	entrySize := 12
	if version == 1 {
		entrySize = 20
	}
	if len(payload) < 8+entrySize {
		return 0, fmt.Errorf("%w: truncated elst entry", ErrUnsupportedContainer)
	}
	var mediaTime int64
	mediaRate := int32(binary.BigEndian.Uint32(payload[16:20]))
	if version == 1 {
		mediaTime = int64(binary.BigEndian.Uint64(payload[16:24]))
		mediaRate = int32(binary.BigEndian.Uint32(payload[24:28]))
	}
	if mediaTime < 0 {
		return 0, fmt.Errorf("%w: empty edit list entry", ErrUnsupportedContainer)
	}
	if mediaRate != 1<<16 {
		return 0, fmt.Errorf("%w: non-unity edit-list media rate", ErrUnsupportedContainer)
	}
	return mediaTime, nil
}

func cumulativeStarts(durations []uint32) []int64 {
	starts := make([]int64, len(durations))
	var elapsed int64
	for i, duration := range durations {
		starts[i] = elapsed
		elapsed += int64(duration)
	}
	return starts
}

func (s *sampleIndex) resolveWindow(opts Options) (int64, int64, error) {
	if s == nil || s.count <= 0 {
		return 0, 0, fmt.Errorf("%w: empty sample table", ErrUnsupportedContainer)
	}
	var first, last int64
	switch {
	case opts.StartFrame >= 0 && opts.FrameCount > 0:
		first, last = opts.StartFrame, opts.StartFrame+opts.FrameCount-1
	case opts.StartUS >= 0 && opts.DurationUS > 0:
		startTick := opts.StartUS*int64(s.timescale)/1_000_000 + s.mediaTimeOffset
		endTick := (opts.StartUS+opts.DurationUS)*int64(s.timescale)/1_000_000 + s.mediaTimeOffset
		// Select by composition time, not decode time: with B-frames the two
		// orders differ and a display window can span non-adjacent decode
		// indices. The min/max index then bounds the whole decode span.
		first, last = -1, -1
		for i := 0; i < s.count; i++ {
			end := s.cts[i] + int64(s.durations[i])
			if end <= startTick || s.cts[i] >= endTick {
				continue
			}
			if first < 0 {
				first = int64(i)
			}
			last = int64(i)
		}
		if first < 0 {
			first = int64(s.count - 1)
			last = first
		}
	default:
		return 0, 0, fmt.Errorf("mediarange: a source frame or timestamp window is required")
	}
	if first < 0 {
		first = 0
	}
	if last >= int64(s.count) {
		last = int64(s.count - 1)
	}
	if first > last {
		return 0, 0, fmt.Errorf("mediarange: requested window is outside the source")
	}
	return first, last, nil
}

// preRoll returns the preceding sync sample required before the first decoded
// frame. Without it FFmpeg cannot reconstruct the requested GOP.
func (s *sampleIndex) preRoll(first int64) int64 {
	for i := first; i >= 0; i-- {
		if s.sync[i] {
			return i
		}
	}
	return 0
}

// decodeEnd extends the read through the next sync sample (bounded) so
// decoder references after the requested window stay available.
func (s *sampleIndex) decodeEnd(last int64) int64 {
	const maxLookahead = 600
	end := last
	for i := last + 1; i < int64(s.count) && i-last <= maxLookahead; i++ {
		end = i
		if s.sync[i] {
			break
		}
	}
	return end
}
