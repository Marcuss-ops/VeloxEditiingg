package downloader

import (
	"context"
	"io"
	"strings"
	"testing"
)

type contractAssetSource struct {
	data          string
	supportsRange bool
	openedOffsets []int64
}

func (s *contractAssetSource) Open(_ context.Context, offset int64) (io.ReadCloser, SourceMetadata, error) {
	if err := ValidateSourceOffset(offset); err != nil {
		return nil, SourceMetadata{}, err
	}
	if offset > int64(len(s.data)) {
		return nil, SourceMetadata{}, io.ErrUnexpectedEOF
	}
	if offset > 0 && !s.SupportsRange() {
		return nil, SourceMetadata{}, errRangeUnsupported
	}
	s.openedOffsets = append(s.openedOffsets, offset)
	return io.NopCloser(strings.NewReader(s.data[offset:])), SourceMetadata{
		SizeBytes: int64(len(s.data)),
		SHA256:    "sha256:test",
		MIMEType:  "video/mp4",
	}, nil
}

func (s *contractAssetSource) SupportsRange() bool { return s.supportsRange }

var errRangeUnsupported = &sourceContractError{"asset source: range unsupported"}

type sourceContractError struct{ message string }

func (e *sourceContractError) Error() string { return e.message }

func TestAssetSourceOpenZeroReturnsCompleteAssetAndMetadata(t *testing.T) {
	source := &contractAssetSource{data: "abcdef", supportsRange: true}
	body, metadata, err := source.Open(context.Background(), 0)
	if err != nil {
		t.Fatalf("Open(0): %v", err)
	}
	defer body.Close()
	content, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read Open(0): %v", err)
	}
	if string(content) != "abcdef" {
		t.Fatalf("content = %q, want complete asset", content)
	}
	if metadata.SizeBytes != 6 || metadata.SHA256 != "sha256:test" || metadata.MIMEType != "video/mp4" {
		t.Fatalf("metadata = %+v, want size/hash/mime", metadata)
	}
	if len(source.openedOffsets) != 1 || source.openedOffsets[0] != 0 {
		t.Fatalf("opened offsets = %v, want [0]", source.openedOffsets)
	}
}

func TestAssetSourceRangeOpenStartsAtOffset(t *testing.T) {
	source := &contractAssetSource{data: "abcdef", supportsRange: true}
	body, metadata, err := source.Open(context.Background(), 2)
	if err != nil {
		t.Fatalf("Open(2): %v", err)
	}
	defer body.Close()
	content, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read Open(2): %v", err)
	}
	if string(content) != "cdef" {
		t.Fatalf("content = %q, want suffix cdef", content)
	}
	if metadata.SizeBytes != 6 {
		t.Fatalf("SizeBytes = %d, want 6 total bytes", metadata.SizeBytes)
	}
	if !source.SupportsRange() {
		t.Fatal("source should advertise range support")
	}
}

func TestAssetSourceRejectsRangeWhenUnsupported(t *testing.T) {
	source := &contractAssetSource{data: "abcdef"}
	if source.SupportsRange() {
		t.Fatal("source should not advertise range support")
	}
	if _, _, err := source.Open(context.Background(), 2); err != errRangeUnsupported {
		t.Fatalf("Open(2) error = %v, want range unsupported", err)
	}
}

func TestAssetSourceRejectsNegativeOffset(t *testing.T) {
	source := &contractAssetSource{data: "abcdef", supportsRange: true}
	if _, _, err := source.Open(context.Background(), -1); err == nil {
		t.Fatal("Open(-1) must reject a negative offset")
	}
	if err := ValidateSourceOffset(-1); err == nil {
		t.Fatal("ValidateSourceOffset(-1) must return an error")
	}
}
